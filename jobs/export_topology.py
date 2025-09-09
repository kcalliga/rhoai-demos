# jobs/export_topology.py
import os, json
from pathlib import Path
from datetime import datetime, timezone

from kubernetes import client, config
from openshift.dynamic import DynamicClient

OUT = Path(os.environ.get("OUT", "data/topology_snapshot.json"))

def k8s_clients():
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
    k8s = client.ApiClient()
    dyn = DynamicClient(k8s)
    return k8s, dyn

def list_all():
    _, dyn = k8s_clients()
    # Core / Apps
    v1 = dyn.resources.get(api_version="v1", kind="Pod")
    node = dyn.resources.get(api_version="v1", kind="Node")
    svc  = dyn.resources.get(api_version="v1", kind="Service")
    ep   = dyn.resources.get(api_version="v1", kind="Endpoints")
    pvc  = dyn.resources.get(api_version="v1", kind="PersistentVolumeClaim")
    pv   = dyn.resources.get(api_version="v1", kind="PersistentVolume")
    dep  = dyn.resources.get(api_version="apps/v1", kind="Deployment")
    rs   = dyn.resources.get(api_version="apps/v1", kind="ReplicaSet")
    ing  = dyn.resources.get(api_version="networking.k8s.io/v1", kind="Ingress")
    hpa  = dyn.resources.get(api_version="autoscaling/v2", kind="HorizontalPodAutoscaler", default=False) \
          or dyn.resources.get(api_version="autoscaling/v1", kind="HorizontalPodAutoscaler")
    # OpenShift Route (if present)
    try:
        route = dyn.resources.get(api_version="route.openshift.io/v1", kind="Route")
        routes = route.get().items
    except Exception:
        routes = []

    pods = v1.get().items
    nodes = node.get().items
    svcs  = svc.get().items
    eps   = ep.get().items
    pvcs  = pvc.get().items
    pvs   = pv.get().items
    deps  = dep.get().items
    rss   = rs.get().items
    ings  = ing.get().items
    hpas  = hpa.get().items if hpa else []

    return {
        "pods": pods, "nodes": nodes, "services": svcs, "endpoints": eps,
        "pvcs": pvcs, "pvs": pvs, "deployments": deps, "replicasets": rss,
        "ingresses": ings, "routes": routes, "hpas": hpas
    }

def to_snapshot(raw):
    # Transform API objects into the simple dicts that utils/graph.py expects
    pods = []
    podns_by_name = {}
    for p in raw["pods"]:
        ns = p.metadata.namespace
        name = p.metadata.name
        podns_by_name.setdefault(name, []).append(f"pod/{ns}/{name}")
        owner = None
        if p.metadata.owner_references:
            o = p.metadata.owner_references[0]
            owner = {"kind": o.kind, "name": o.name}
        pods.append({
            "name": name, "ns": ns,
            "node": (p.spec.nodeName or None),
            "owner": owner,
            "labels": p.metadata.labels or {}
        })

    rss = []
    for r in raw["replicasets"]:
        owner = None
        if r.metadata.owner_references:
            o = r.metadata.owner_references[0]
            owner = {"kind": o.kind, "name": o.name}
        rss.append({"name": r.metadata.name, "ns": r.metadata.namespace, "owner": owner})

    deps = [{"name": d.metadata.name, "ns": d.metadata.namespace} for d in raw["deployments"]]

    svcs = []
    for s in raw["services"]:
        svcs.append({
            "name": s.metadata.name, "ns": s.metadata.namespace,
            "selector": (s.spec.selector or {})
        })

    eps = []
    # map service -> pods by Endpoints
    for e in raw["endpoints"]:
        svc = e.metadata.name
        ns  = e.metadata.namespace
        pods = []
        for subset in (e.subsets or []):
            for addr in (subset.addresses or []):
                target = getattr(addr, "targetRef", None)
                if target and target.kind == "Pod":
                    pods.append(target.name)
        eps.append({"svc": svc, "ns": ns, "pods": pods})

    routes = [{"name": r.metadata.name, "ns": r.metadata.namespace, "to_svc": r.spec.to.name}
              for r in raw["routes"]]

    ings = [{"name": i.metadata.name, "ns": i.metadata.namespace,
             "to_svc": i.spec.defaultBackend.service.name if i.spec.defaultBackend else None}
            for i in raw["ingresses"] if i.spec and i.spec.defaultBackend and i.spec.defaultBackend.service]

    pvcs = [{"name": c.metadata.name, "ns": c.metadata.namespace,
             "pod": None, "pv": (c.spec.volumeName or None)}
            for c in raw["pvcs"]]

    pvs  = [{"name": v.metadata.name} for v in raw["pvs"]]

    hpas = [{"name": h.metadata.name, "ns": h.metadata.namespace,
             "target_deploy": (h.spec.scaleTargetRef.name if h.spec and h.spec.scaleTargetRef else None)}
            for h in raw["hpas"]]

    nodes = [{"name": n.metadata.name, "labels": n.metadata.labels or {}} for n in raw["nodes"]]

    snapshot = {
      "nodes": nodes, "pods": pods, "replicasets": rss, "deployments": deps,
      "services": svcs, "endpoints": eps, "routes": routes, "ingresses": ings,
      "pvcs": pvcs, "pvs": pvs, "hpas": hpas, "netpols": []  # add if you need them
    }
    return snapshot

def main():
    raw = list_all()
    snap = to_snapshot(raw)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(snap, indent=2))
    print(f"Wrote {OUT} at {datetime.now(timezone.utc).isoformat()}")

if __name__ == "__main__":
    main()

