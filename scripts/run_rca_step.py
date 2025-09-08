# scripts/run_rca_step.py
import argparse, json, pandas as pd
from pathlib import Path
from utils.graph import build_from_snapshot
from utils.episodes import build_episodes, load_rules, apply_rules, episode_to_incident

ap = argparse.ArgumentParser()
ap.add_argument("--logs", required=True)             # parquet/csv
ap.add_argument("--snapshot", required=True)         # topology_snapshot.json
ap.add_argument("--rules", default="rules/rules.yaml")
ap.add_argument("--out", default="incidents")
ap.add_argument("--window", default="10min")
args = ap.parse_args()

df = pd.read_parquet(args.logs) if args.logs.endswith(".parquet") else pd.read_csv(args.logs, parse_dates=["ts"])
graph = build_from_snapshot(json.loads(Path(args.snapshot).read_text()))
episodes = build_episodes(df, window=args.window)
rules = load_rules(args.rules)

out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
for ep in episodes:
    inc = episode_to_incident(ep, apply_rules(ep, rules, graph))
    (out / f"{ep.episode_id}.json").write_text(json.dumps(inc, indent=2))
print(f"Wrote {len(list(out.glob('*.json')))} incidents to {out}/")
