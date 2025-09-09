import pandas as pd, numpy as np, yaml
from mlxtend.frequent_patterns import fpgrowth, association_rules

# 1) Load episodes (one row per episode)
df = pd.read_parquet("incidents/episodes_index.parquet")  # or construct from incidents/*.json

# 2) Weak anomaly label (unsupervised)
df["anomaly"] = (df["error_ratio"] > df["error_ratio"].quantile(0.95)) | \
                (df["http5xx"] >= 5) | \
                (df.get("isoforest_score", 0) > np.quantile(df.get("isoforest_score", 0), 0.95))

# 3) Booleanize features
X = pd.DataFrame({
    "error_high": df["error_ratio"] > df["error_ratio"].quantile(0.90),
    "latency_high": df.get("latency_p95", 0) > df.get("latency_p95", 0).quantile(0.90) if "latency_p95" in df else False,
    "restarts_any": df["restarts"] > 0,
    "http5xx_any": df["http5xx"] > 0,
    "rollout_flag": df.get("rollout_in_window", 0) > 0,
    "has_evicted": df.get("has_Evicted", 0).astype(bool) if "has_Evicted" in df else False,
    "has_notready": df.get("has_NodeNotReady", 0).astype(bool) if "has_NodeNotReady" in df else False,
    "has_readonlyfs": df.get("has_read_only_fs", 0).astype(bool) if "has_read_only_fs" in df else False,
    "pvc_attached": df.get("has_pvc_mount", 0).astype(bool) if "has_pvc_mount" in df else False,
})
X = X.fillna(False)

# 4) Frequent itemsets conditioned on anomalies
dfA = X.copy()
dfA["__anomaly__"] = df["anomaly"].astype(bool)

# Mine itemsets among episodes (we’ll filter to those correlated with anomaly)
itemsets = fpgrowth(dfA.drop(columns="__anomaly__"), min_support=0.02, use_colnames=True)
rules = association_rules(itemsets, metric="lift", min_threshold=1.2)
# Attach anomaly correlation: confidence that antecedent => anomaly
# Compute P(anomaly | antecedent)
ante_mask = dfA.apply(lambda row: False, axis=1)
def antecedent_mask(ant):
    cols = list(ant)
    m = np.ones(len(dfA), dtype=bool)
    for c in cols:
        m &= dfA[c].values
    return m

learned = []
for _, r in rules.iterrows():
    ant = list(r['antecedents'])
    m = antecedent_mask(ant)
    if m.sum() < 15:  # require minimum support as absolute count
        continue
    conf_anom = dfA.loc[m, "__anomaly__"].mean()  # P(anomaly | antecedent)
    lift_anom = conf_anom / dfA["__anomaly__"].mean() if dfA["__anomaly__"].mean() > 0 else 0
    if lift_anom < 1.5 or conf_anom < 0.4:
        continue
    learned.append({
        "antecedent": ant,
        "support": float(m.mean()),
        "conf_anomaly": float(conf_anom),
        "lift_anomaly": float(lift_anom)
    })

# 5) Map antecedents -> root_component + reason (data-driven heuristics)
def infer_root(ant):
    if "has_notready" in ant: return "node", "NodeNotReady correlated with errors"
    if "pvc_attached" in ant and "has_readonlyfs" in ant: return "pvc", "Read-only filesystem with PVC attached"
    if "rollout_flag" in ant and ("http5xx_any" in ant or "latency_high" in ant): 
        return "deployment", "Rollout associated with 5xx/latency spike"
    if "error_high" in ant and "restarts_any" in ant:
        return "pod", "High errors with container restarts"
    return "deployment", "Feature pattern associated with anomalies"

yaml_rules = []
for i, r in enumerate(sorted(learned, key=lambda x: (x["lift_anomaly"], x["conf_anomaly"], x["support"]), reverse=True)[:10], start=1):
    root, reason = infer_root(r["antecedent"])
    # convert antecedent list into conditions
    conds = []
    for a in r["antecedent"]:
        if a in ("error_high","latency_high","http5xx_any","restarts_any","rollout_flag"):
            conds.append({"metric": a, "op": "==", "value": True})
        else:
            conds.append({"log_pattern": a.replace("has_", "").replace("_", " ")})
    yaml_rules.append({
        "id": f"auto_{i}",
        "reason": f"{reason} (lift={r['lift_anomaly']:.2f}, conf={r['conf_anomaly']:.2f})",
        "when": {"all": conds},
        "root_component": root,
        "score": {
            "temporal": 0.2,
            "topology": 0.4,
            "magnitude": 0.3 if "error_high" in r["antecedent"] or "http5xx_any" in r["antecedent"] else 0.2,
            "change_flag": 0.1 if "rollout_flag" in r["antecedent"] else 0.0
        },
        "evidence": [f"Auto-learned from {int(r['support']*len(dfA))} episodes; lift={r['lift_anomaly']:.2f}"]
    })

with open("rules/learned_rules.yaml", "w") as f:
    yaml.safe_dump(yaml_rules, f, sort_keys=False)
print(f"Wrote {len(yaml_rules)} rules → rules/learned_rules.yaml")
