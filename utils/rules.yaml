# rules/rules.yaml
- id: node_notready_eviction
  reason: "Many evictions after a NodeNotReady signal"
  when:
    any:
      - event: "NodeNotReady"
      - log_pattern: "evicted"
  then:
    correlate_within: 5m
    look_for:
      - log_pattern: "ImagePullBackOff|ErrImagePull"
      - log_pattern: "Evicted"
  root_component: node
  score:
    temporal: 0.3     # base boost when this rule triggers
    topology: 0.5     # importance of graph proximity
    magnitude: 0.2    # uses episode error_ratio
    change_flag: 0.0
  evidence:
    - "kubelet reported NotReady"
    - "Pods evicted on same node"

- id: rollout_regression
  reason: "Error spike following a deployment rollout"
  when:
    all:
      - metric: "error_ratio"
        op: ">"
        value: 0.2
  root_component: deployment
  score:
    temporal: 0.2
    topology: 0.4
    magnitude: 0.3
    change_flag: 0.1  # requires Episode.features.rollout_in_window = 1.0 if change found
  evidence:
    - "Recent Deployment rollout detected"
    - "HTTP 5xx increased"

- id: pvc_io_errors
  reason: "Container errors alongside PVC I/O issues"
  when:
    any:
      - log_pattern: "read-only file system"
      - log_pattern: "I/O error"
  root_component: pvc
  score:
    temporal: 0.25
    topology: 0.5
    magnitude: 0.25
  evidence:
    - "Filesystem read-only or I/O errors found in logs"
