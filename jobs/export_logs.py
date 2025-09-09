# jobs/export_logs.py
import os, requests, pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone

LOKI_URL = os.environ.get("LOKI_URL", "http://loki:3100")
OUT = Path(os.environ.get("OUT", "data/unified_logs/latest.parquet"))

def query_loki(query, start, end, step="30s", limit=5000):
    url = f"{LOKI_URL}/loki/api/v1/query_range"
    params = {
        "query": query,
        "start": int(start.timestamp() * 1e9),  # ns
        "end": int(end.timestamp() * 1e9),
        "step": step,
        "limit": limit,
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()["data"]["result"]

def normalize(results, source="app"):
    rows = []
    for res in results:
        labels = res.get("metric", {})
        for ts, line in res.get("values", []):
            t = pd.to_datetime(float(ts), unit="s", utc=True)
            rows.append({
                "ts": t,
                "source": source,
                "namespace": labels.get("namespace") or labels.get("kubernetes_namespace_name"),
                "pod": labels.get("pod") or labels.get("kubernetes_pod_name"),
                "node": labels.get("node") or labels.get("kubernetes_node_name"),
                "level": "error" if "error" in line.lower() else "info",
                "verb": None,
                "code": None,
                "route": None,
                "msg": line[:400],
                "container_restart": 1 if "Restarted container" in line else 0,
                "rollout_in_window": 1.0 if "Scaled up" in line else 0.0,
            })
    return rows

def main():
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=1)   # pull last 1h logs

    # Example queries (adjust labels to your cluster)
    queries = {
        "app": '{kubernetes_namespace_name=~"shop|payments"}',
        "infra": '{job="kubelet"}',
        "event": '{stream="events"}',
        "audit": '{component="kube-apiserver", stream="audit"}'
    }

    all_rows = []
    for source, q in queries.items():
        print(f"Querying {source} logs…")
        res = query_loki(q, start, end)
        rows = normalize(res, source)
        all_rows.extend(rows)

    df = pd.DataFrame(all_rows)
    df = df.sort_values("ts")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT, index=False)
    print(f"Wrote {len(df)} rows to {OUT}")

if __name__ == "__main__":
    main()
