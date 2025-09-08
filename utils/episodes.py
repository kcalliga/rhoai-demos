# utils/episodes.py
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional, Tuple
from datetime import timedelta
import pandas as pd
import yaml
from collections import defaultdict
from .graph import Graph

# ---------- Data contracts ----------
@dataclass
class Event:
    ts: pd.Timestamp
    source: str   # audit|app|infra|metric|event
    namespace: Optional[str] = None
    pod: Optional[str] = None
    node: Optional[str] = None
    level: Optional[str] = None
    verb: Optional[str] = None
    code: Optional[int] = None
    route: Optional[str] = None
    msg: Optional[str] = None
    fields: Dict[str, Any] = field(default_factory=dict)  # free-form extras

@dataclass
class Episode:
    episode_id: str
    start: pd.Timestamp
    end: pd.Timestamp
    entities: Dict[str, List[str]]            # e.g. {"pod": ["pod/ns/a"], "node": ["node/ip-..."]}
    features: Dict[str, float]                # aggregated stats (error_ratio, restarts_5m, p95_latency, etc.)
    events: List[Event] = field(default_factory=list)

@dataclass
class CandidateRoot:
    component: str               # e.g. "node/ip-...", "deployment/ns/foo"
    reason: str                  # short sentence
    evidence: List[str]          # text pointers (k8s events, log snippets, metrics keys)
    score_breakdown: Dict[str, float]
    score: float

# ---------- Episode building ----------
def build_episodes(df: pd.DataFrame,
                   window: str = "10min",
                   keys: List[str] = ["namespace","pod","node"]) -> List[Episode]:
    """
    df: expects columns 'ts' (datetime64[ns]) + keys + 'source','level','code','verb','msg'
    """
    if "ts" not in df:
        raise ValueError("df must include a 'ts' column (datetime)")
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    episodes: List[Episode] = []
    # resample into windows, then within each window group by entity keys
    for (wstart, wdf) in df.groupby(pd.Grouper(freq=window)):
        if wdf.empty:
            continue
        wend = wstart + pd.to_timedelta(window)
        key_cols = [k for k in keys if k in wdf.columns]
        if not key_cols:
            grp = {"_all": wdf}
        else:
            grp = dict(tuple(wdf.groupby(key_cols, dropna=False)))
        for gkey, gdf in grp.items():
            # compute simple features
            total = len(gdf)
            errors = (gdf.get("level","")=="error").sum() if "level" in gdf.columns else 0
            error_ratio = float(errors)/float(total) if total else 0.0
            restarts = gdf.get("container_restart", pd.Series([0]*total, index=gdf.index)).sum() if "container_restart" in gdf.columns else 0
            http5xx = (gdf.get("code", pd.Series(dtype=int))>=500).sum() if "code" in gdf.columns else 0
            features = {
                "count": float(total),
                "error_ratio": float(error_ratio),
                "restarts": float(restarts),
                "http5xx": float(http5xx)
            }
            # collect entities present
            entities: Dict[str, List[str]] = defaultdict(list)
            for col in ["namespace","pod","node","route"]:
                if col in gdf.columns:
                    vals = [v for v in gdf[col].dropna().astype(str).unique().tolist() if v]
                    if vals:
                        entities[col] = vals
            # events sample (cap)
            evs: List[Event] = []
            for _, row in gdf.head(200).reset_index().iterrows():
                evs.append(Event(
                    ts=row["ts"], source=row.get("source",""),
                    namespace=row.get("namespace"), pod=row.get("pod"), node=row.get("node"),
                    level=row.get("level"), verb=row.get("verb"), code=int(row["code"]) if "code" in row and pd.notna(row["code"]) else None,
                    route=row.get("route"), msg=str(row.get("msg"))[:400],
                    fields={}
                ))
            ep_id = f"{str(wstart.value)}::{hash(str(gkey)) & 0xfffffff:07x}"
            episodes.append(Episode(ep_id, wstart, wend, dict(entities), features, evs))
    return episodes

# ---------- Rule engine ----------
def load_rules(path: str) -> List[Dict[str, Any]]:
    with open(path, "r") as f:
        return yaml.safe_load(f) or []

def _has_signal(ep: Episode, sig: Dict[str, Any]) -> bool:
    """
    sig examples:
      {"metric": "error_ratio", "op": ">", "value": 0.2}
      {"event": "NodeNotReady"}
      {"log_pattern": "ImagePullBackOff"}
    """
    if "metric" in sig:
        name, op, val = sig["metric"], sig.get("op",">"), sig.get("value", 0)
        x = ep.features.get(name)
        if x is None: 
            return False
        return {"<": x<val, "<=": x<=val, ">": x>val, ">=": x>=val, "==": x==val, "!=": x!=val}[op]
    if "event" in sig:
        pat = sig["event"].lower()
        return any(pat in (e.msg or "").lower() for e in ep.events)
    if "log_pattern" in sig:
        pat = sig["log_pattern"].lower()
        return any(pat in (e.msg or "").lower() for e in ep.events)
    return False

def apply_rules(ep: Episode, rules: List[Dict[str, Any]], graph: Graph) -> List[CandidateRoot]:
    cands: List[CandidateRoot] = []
    for r in rules:
        cond = r.get("when", {})
        ok = False
        if "all" in cond:
            ok = all(_has_signal(ep, s) for s in cond["all"])
        elif "any" in cond:
            ok = any(_has_signal(ep, s) for s in cond["any"])
        else:
            ok = True
        if not ok:
            continue

        # select a focus component: prefer pod, then node, then namespace
        focus = None
        for k in ("pod","node","namespace"):
            if k in ep.entities and ep.entities[k]:
                focus = f"{k}/{ep.entities[k][0]}" if "/" not in ep.entities[k][0] else ep.entities[k][0]
                break

        # topology distance score: if rule provides root_component (prefix), find nearest match
        topo_score = 0.0
        root_component = r.get("root_component")  # e.g. "node"
        target = None
        if root_component and focus:
            # search neighborhood for nodes that start with this prefix
            neigh = graph.bfs([focus], max_hops=3) if hasattr(graph, "bfs") else set()
            matches = [n for n in neigh if n.startswith(root_component + "/")]
            hop = None
            if matches:
                # choose closest by path len
                distances = [(n, graph.shortest_path_len(focus, n) or 99) for n in matches]
                target, hop = min(distances, key=lambda x: x[1])
            if hop is not None and hop != 99:
                topo_score = max(0.0, 1.0 - 0.2*hop)  # decay per hop

        temporal = r.get("score",{}).get("temporal", 0.3)
        magnitude = r.get("score",{}).get("magnitude", 0.3) * min(1.0, ep.features.get("error_ratio", 0))
        change = r.get("score",{}).get("change_flag", 0.0) * float(ep.features.get("rollout_in_window", 0.0))
        topo_w = r.get("score",{}).get("topology", 0.4)
        total = temporal + topo_w*topo_score + magnitude + change

        component = target or focus or "cluster"
        reason = r.get("reason", r.get("id","rule"))
        evidence = r.get("evidence", [])
        cands.append(CandidateRoot(
            component=component,
            reason=reason,
            evidence=evidence,
            score_breakdown={"temporal": temporal, "topology": topo_w*topo_score, "magnitude": magnitude, "change": change},
            score=round(float(total), 4)
        ))
    # sort by score desc
    cands.sort(key=lambda c: c.score, reverse=True)
    return cands[:3]

# ---------- Incident serialization ----------
def episode_to_incident(ep: Episode, cands: List[CandidateRoot]) -> Dict[str, Any]:
    return {
        "episode_id": ep.episode_id,
        "start": ep.start.isoformat(),
        "end": ep.end.isoformat(),
        "entities": ep.entities,
        "features": ep.features,
        "candidates": [
            {
                "component": c.component,
                "reason": c.reason,
                "evidence": c.evidence,
                "score": c.score,
                "score_breakdown": c.score_breakdown
            } for c in cands
        ],
        "exemplars": [
            {"ts": e.ts.isoformat(), "source": e.source, "ns": e.namespace, "pod": e.pod, "node": e.node, "code": e.code, "msg": e.msg}
            for e in ep.events[:10]
        ]
    }
