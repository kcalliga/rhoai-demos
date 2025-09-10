# jobs/export_logs.py
# Query Loki on a schedule, normalize logs, and write data/unified_logs/latest.parquet
# Auth: ServiceAccount projected token (recommended) or Basic auth via secret.

import os, json, time, math, logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
import requests
import pandas as pd

# ----------------------------
# Config via environment
# ----------------------------
LOKI_URL           = os.environ["LOKI_URL"]  # e.g., https://loki-gateway.your.domain
OUT_PATH           = Path(os.environ.get("OUT", "/data/unified_logs/latest.parquet"))
TOKEN_FILE         = os.environ.get("LOKI_TOKEN_FILE")        # e.g., /var/run/secrets/tokens/loki-token
BASIC_AUTH_HDR     = os.environ.get("LOKI_BASIC_AUTH")        # e.g., "Basic base64(user:pass)"
REQUESTS_CA_BUNDLE = os.environ.get("REQUESTS_CA_BUNDLE")     # custom CA bundle path (optional)
TIMEOUT_SEC        = int(os.environ.get("HTTP_TIMEOUT", "60"))

# Window controls (defaults: last 60m)
WINDOW_MINUTES     = int(os.environ.get("WINDOW_MINUTES", "60"))
# Offsets let you avoid racing recent writes; e.g. end_offset=60 pulls until 1 minute ago
START_OFFSET_MIN   = int(os.environ.get("START_OFFSET_MIN", str(WINDOW_MINUTES)))
END_OFFSET_MIN     = int(os.environ.get("END_OFFSET_MIN", "0"))

# Chunking (to keep each call modest)
CHUNK_MINUTES      = int(os.environ.get("CHUNK_MINUTES", "10"))
STEP               = os.environ.get("STEP", "30s")
QUERY_LIMIT        = int(os.environ.get("QUERY_LIMIT", "5000"))   # per stream per call
SOURCES_JSON       = os.environ.get("QUERIES_JSON")               # JSON dict: {"app":"{...}", "infra":"{...}", ...}

# Default queries (override with QUERIES_JSON)
DEFAULT_QUERIES = {
    "app": '{kubernetes_namespace_name=~"shop|payments"}',
    "infra": '{job=~"kubelet|node-exporter"}',
    "event": '{stream="events"}',
    "audit": '{component="kube-apiserver", stream="audit"}'
}

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
log = logging.getLogger("export_logs")

# ----------------------------
# HTTP helpers
# ----------------------------
def build_headers() -> Dict[str, str]:
    headers = {}
    if BASIC_AUTH_HDR:
        headers["Authorization"] = BASIC_AUTH_HDR.strip()
    elif TOKEN_FILE and Path(TOKEN_FILE).exists():
        token = Path(TOKEN_FILE).read_text().strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
    return headers

def loki_query_range(query: str, start: datetime, end: datetime,
                     step: str = STEP, limit: int = QUERY_LIMIT) -> Dict[str, Any]:
    url = f"{LOKI_URL.rstrip('/')}/loki/api/v1/query_range"
    params = {
        "query": query,
        "start": int(start.timestamp() * 1e9),  # nanoseconds
        "end":   int(end.timestamp()   * 1e9),
        "step":  step,
        "limit": limit,
        # NOTE: you can add direction=backward if you prefer
    }
    headers = build_headers()
    verify = REQUESTS_CA_BUNDLE if REQUESTS_CA_BUNDLE else True
    r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT_SEC, verify=verify)
    r.raise_for_status()
    return r.json().get("data", {}).get("result", [])

def fetch_chunked(query: str, start: datetime, end: datetime, chunk_minutes: int) -> List[Dict[str, Any]]:
    """Call query_range over smaller windows and concatenate results."""
    results: List[Dict[str, Any]] = []
    n_chunks = max(1, math.ceil((end - start).total_seconds() / (chunk_minutes * 60)))
    cur = start
    for i in range(n_chunks):
        chunk_end = min(end, cur + timedelta(minutes=chunk_minutes))
        try:
            part = loki_query_range(query, cur, chunk_end)
            # Extend like-for-like: each result is a stream (metric + values)
            results.extend(part)
        except Exception as e:
            log.warning(f"Loki query chunk failed ({cur} → {chunk_end}): {e}")
        cur = chunk_end
        # polite small sleep to avoid hammering gateway
        time.sleep(0.25)
    return results

# ----------------------------
# Normalization utilities
# ----------------------------
def try_parse_json_line(line: str) -> Optional[Dict[str, Any]]:
    try:
        if line and line.strip().startswith("{"):
            return json.loads(line)
    except Exception:
        pass
    return None

def infer_level(line: str) -> str:
    s = (line or "").lower()
    if any(w in s for w in ["error", "exception", "stacktrace", "backoff", "fail "]):
        return "error"
    if "warn" in s or "throttle" in s:
        return "warn"
    return "info"

def infer_http_code(line: str) -> Optional[int]:
    for code in [500, 503, 502, 504, 429, 404, 401]:
        if f" {code} " in f" {line} ":
            return code
    return None

def infer_route(line: str) -> Optional[str]:
    # VERY naive; replace with your app’s format or JSON field
    for verb in (" GET ", " POST ", " PUT ", " PATCH ", " DELETE "):
        if verb in line:
            try:
                return line.split(verb, 1)[1].split()[0]
            except Exception:
                return None
    return None

def map_labels(labels: Dict[str, Any]) -> Dict[str, Any]:
    # Common Loki label keys (adjust to your promtail/logging stack)
    return {
        "namespace": labels.get("kubernetes_namespace_name") or labels.get("namespace"),
        "pod":       labels.get("kubernetes_pod_name")       or labels.get("pod"),
        "node":      labels.get("kubernetes_node")           or labels.get("node") or labels.get("host"),
        "verb":      labels.get("verb")  # often only present for audit streams
    }

def normalize_streams(streams: List[Dict[str, Any]], source: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for res in streams:
        labels = res.get("metric", {}) or {}
        for ts, line in res.get("values", []) or []:
            # ts might be stringified unix float or ns; handle both safely
            try:
                t = pd.to_datetime(float(ts), unit="s", utc=True)
            except Exception:
                # Loki sometimes returns ns as string int
                t = pd.to_datetime(int(ts), unit="ns", utc=True)
            line = line or ""
            json_line = try_parse_json_line(line)
            label_map = map_labels(labels)
            # Prefer JSON fields if available
            ns   = json_line.get("namespace") if json_line else None
            pod  = json_line.get("pod")       if json_line else None
            node = json_line.get("node")      if json_line else None
            level= (json_line.get("level") or infer_level(line)) if json_line else infer_level(line)
            code = json_line.get("status") or json_line.get("code") if json_line else infer_http_code(line)
            try:
                code = int(code) if code is not None else None
            except Exception:
                code = None
            route = (json_line.get("path") or json_line.get("route")) if json_line else infer_route(line)
            verb  = json_line.get("verb") if json_line else label_map.get("verb")
            rows.append({
                "ts": t,
                "source": source,
                "namespace": ns or label_map.get("namespace"),
                "pod": pod or label_map.get("pod"),
                "node": node or label_map.get("node"),
                "level": level,
                "verb": verb,
                "code": code,
                "route": route,
                "msg": (json.dumps(json_line) if json_line else line)[:400],
                "container_restart": 1 if "Restarted container" in line else 0,
                "rollout_in_window": 1.0 if ("Scaled up" in line or "deployment created" in line) else 0.0
            })
    return rows

# ----------------------------
# Main
# ----------------------------
def main():
    # Resolve time window
    now = datetime.now(timezone.utc)
    end   = now - timedelta(minutes=END_OFFSET_MIN)
    start = end - timedelta(minutes=START_OFFSET_MIN)  # typically = now - WINDOW_MINUTES
    log.info(f"Pulling Loki logs from {start.isoformat()} to {end.isoformat()}")

    # Load queries
    try:
        queries = json.loads(SOURCES_JSON) if SOURCES_JSON else DEFAULT_QUERIES
    except Exception as e:
        log.warning(f"Invalid QUERIES_JSON; using defaults. Error: {e}")
        queries = DEFAULT_QUERIES

    all_rows: List[Dict[str, Any]] = []
    for source, q in queries.items():
        log.info(f"Querying source={source} LogQL={q}")
        streams = fetch_chunked(q, start, end, CHUNK_MINUTES)
        rows = normalize_streams(streams, source)
        log.info(f"  → {len(rows)} rows")
        all_rows.extend(rows)

    if not all_rows:
        log.warning("No rows returned from Loki. Nothing to write.")
        return

    df = pd.DataFrame(all_rows).sort_values("ts")

    # Ensure target dir exists
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Try Parquet, fallback to CSV if pyarrow/fastparquet not available
    try:
        df.to_parquet(OUT_PATH, index=False)
        log.info(f"Wrote {len(df):,} rows → {OUT_PATH}")
    except Exception as e:
        csv_path = OUT_PATH.with_suffix(".csv")
        df.to_csv(csv_path, index=False)
        log.warning(f"Parquet write failed ({e}); wrote CSV instead → {csv_path}")

if __name__ == "__main__":
    main()

