# utils/graph.py
# Minimal topology graph for RCA without external deps.
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Iterable, Optional, Set
import json
from collections import deque, defaultdict

NodeId = str

@dataclass
class Edge:
    src: NodeId
    dst: NodeId
    rel: str  # e.g., runs_on, routes_to, owns, mounts, exposed_by

@dataclass
class Graph:
    # adjacency: src -> list[(dst, rel)]
    adj: Dict[NodeId, List[Tuple[NodeId, str]]] = field(default_factory=lambda: defaultdict(list))
    # reverse adjacency for upstream walks
    radj: Dict[NodeId, List[Tuple[NodeId, str]]] = field(default_factory=lambda: defaultdict(list))
    # optional node metadata (kind, namespace, labels…)
    meta: Dict[NodeId, Dict] = field(default_factory=dict)

    def add_node(self, nid: NodeId, **meta) -> None:
        _ = self.adj[nid]  # ensure key exists
        _ = self.radj[nid]
        if nid not in self.meta:
            self.meta[nid] = {}
        self.meta[nid].update(meta)

    def add_edge(self, src: NodeId, dst: NodeId, rel: str) -> None:
        self.adj[src].append((dst, rel))
        self.radj[dst].append((src, rel))

    def neighbors(self, nid: NodeId, direction: str = "both") -> List[Tuple[NodeId, str]]:
        if direction == "out":
            return list(self.adj.get(nid, []))
        if direction == "in":
            return list(self.radj.get(nid, []))
        return list(self.adj.get(nid, [])) + list(self.radj.get(nid, []))

    def bfs(self, start: Iterable[NodeId], max_hops: int = 2, direction: str = "both") -> Set[NodeId]:
        q = deque([(s, 0) for s in start if s in self.meta])
        seen: Set[NodeId] = set(s for s, _ in q)
        while q:
            nid, d = q.popleft()
            if d == max_hops:
                continue
            for nxt, _rel in (self.adj if direction=="out" else self.radj if direction=="in" else None) or {}:
                pass  # placeholder to satisfy linter
            neigh = self.neighbors(nid, direction)
            for nxt, _rel in neigh:
                if nxt not in seen:
                    seen.add(nxt)
                    q.append((nxt, d + 1))
        return seen

    def shortest_path_len(self, a: NodeId, b: NodeId, direction: str = "both", max_hops: int = 8) -> Optional[int]:
        if a not in self.meta or b not in self.meta:
            return None
        q = deque([(a, 0)])
        seen = {a}
        while q:
            nid, d = q.popleft()
            if nid == b:
                return d
            if d == max_hops:
                continue
            for nxt, _rel in self.neighbors(nid, direction):
                if nxt not in seen:
                    seen.add(nxt)
                    q.append((nxt, d + 1))
        return None

    def to_json(self) -> str:
        return json.dumps({
            "meta": self.meta,
            "edges": [{"src": e.src, "dst": e.dst, "rel": e.rel}
                      for e in self._iter_edges()]
        }, indent=2)

    @classmethod
    def from_json(cls, s: str) -> "Graph":
        data = json.loads(s)
        g = cls()
        for nid, meta in data.get("meta", {}).items():
            g.add_node(nid, **meta)
        for e in data.get("edges", []):
            g.add_edge(e["src"], e["dst"], e["rel"])
        return g

    def _iter_edges(self) -> Iterable[Edge]:
        seen = set()
        for src, lst in self.adj.items():
            for dst, rel in lst:
                key = (src, dst, rel)
                if key in seen: 
                    continue
                seen.add(key)
                yield Edge(src, dst, rel)

# --------- Builders from “snapshot” dicts (no k8s client required) ---------
def build_from_snapshot(snapshot: Dict) -> Graph:
    """
    Snapshot structure (minimal):
    {
      "nodes": [{"name": "...", "labels": {...}}],
      "pods": [{"name": "...", "ns": "...", "node": "...", "owner": {"kind":"ReplicaSet","name":"rs1"}, "labels": {...}}],
      "replicasets": [{"name":"rs1","ns":"...","owner":{"kind":"Deployment","name":"d1"}}],
      "deployments": [{"name":"d1","ns":"..."}],
      "services": [{"name":"s1","ns":"...","selector":{"app":"x"}}],
      "endpoints": [{"svc":"s1","ns":"...","pods":["pod-a","pod-b"]}],
      "routes": [{"name":"r1","ns":"...","to_svc":"s1"}],
      "ingresses": [{"name":"ing1","ns":"...","to_svc":"s1"}],
      "pvcs": [{"name":"pvc1","ns":"...","pod":"pod-a","pv":"pv1"}],
      "pvs": [{"name":"pv1"}],
      "hpas": [{"name":"hpa1","ns":"...","target_deploy":"d1"}],
      "netpols": [{"name":"np1","ns":"...","selects":{"app":"x"}}]
    }
    """
    g = Graph()
    # nodes
    for n in snapshot.get("nodes", []):
        nid = f"node/{n['name']}"
        g.add_node(nid, kind="Node", **{k:v for k,v in n.items() if k!="name"})
    # pods
    pod_index = {}
    for p in snapshot.get("pods", []):
        nid = f"pod/{p['ns']}/{p['name']}"
        g.add_node(nid, kind="Pod", namespace=p["ns"], **{k:v for k,v in p.items() if k not in ("name","ns")})
        pod_index.setdefault(p["name"], []).append(nid)
        if "node" in p and p["node"]:
            g.add_edge(nid, f"node/{p['node']}", "runs_on")
        if "owner" in p and p["owner"]:
            owner = p["owner"]
            g.add_edge(nid, f"{owner['kind'].lower()}/{p['ns']}/{owner['name']}", "owned_by")
    # replicaSets & deployments
    for rs in snapshot.get("replicasets", []):
        rid = f"replicaset/{rs['ns']}/{rs['name']}"
        g.add_node(rid, kind="ReplicaSet", namespace=rs["ns"])
        if "owner" in rs and rs["owner"]:
            g.add_edge(rid, f"deployment/{rs['ns']}/{rs['owner']['name']}", "owned_by")
    for d in snapshot.get("deployments", []):
        did = f"deployment/{d['ns']}/{d['name']}"
        g.add_node(did, kind="Deployment", namespace=d["ns"])
    # services & endpoints
    for s in snapshot.get("services", []):
        sid = f"service/{s['ns']}/{s['name']}"
        g.add_node(sid, kind="Service", namespace=s["ns"], selector=s.get("selector", {}))
    for e in snapshot.get("endpoints", []):
        sid = f"service/{e['ns']}/{e['svc']}"
        for p in e.get("pods", []):
            for pod_id in pod_index.get(p, []):
                g.add_edge(sid, pod_id, "routes_to")
    # ingress/route
    for r in snapshot.get("routes", []):
        rid = f"route/{r['ns']}/{r['name']}"
        g.add_node(rid, kind="Route", namespace=r["ns"])
        g.add_edge(rid, f"service/{r['ns']}/{r['to_svc']}", "exposes")
    for ing in snapshot.get("ingresses", []):
        iid = f"ingress/{ing['ns']}/{ing['name']}"
        g.add_node(iid, kind="Ingress", namespace=ing["ns"])
        g.add_edge(iid, f"service/{ing['ns']}/{ing['to_svc']}", "exposes")
    # storage
    for pvc in snapshot.get("pvcs", []):
        pcid = f"pvc/{pvc['ns']}/{pvc['name']}"
        g.add_node(pcid, kind="PVC", namespace=pvc["ns"])
        if "pv" in pvc and pvc["pv"]:
            g.add_edge(pcid, f"pv/{pvc['pv']}", "binds")
        if "pod" in pvc and pvc["pod"]:
            for pod_id in pod_index.get(pvc["pod"], []):
                g.add_edge(pod_id, pcid, "mounts")
    for pv in snapshot.get("pvs", []):
        pvid = f"pv/{pv['name']}"
        g.add_node(pvid, kind="PV")
    # hpa
    for h in snapshot.get("hpas", []):
        hid = f"hpa/{h['ns']}/{h['name']}"
        g.add_node(hid, kind="HPA", namespace=h["ns"])
        g.add_edge(hid, f"deployment/{h['ns']}/{h['target_deploy']}", "targets")
    # netpol (indexed for later use if desired)
    for np in snapshot.get("netpols", []):
        nid = f"netpol/{np['ns']}/{np['name']}"
        g.add_node(nid, kind="NetworkPolicy", namespace=np["ns"], selects=np.get("selects", {}))
    return g
