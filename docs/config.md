# Config

## Table of Contents

<!-- mdformat-toc start --slug=github --no-anchors --maxlevel=6 --minlevel=1 -->

- [Config](#config)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Partitions](#partitions)
  - [Nodes](#nodes)
    - [External Nodes](#external-nodes)
    - [Hybrid Nodes](#hybrid-nodes)
    - [Topology](#topology)
    - [Hybrid Workload Isolation](#hybrid-workload-isolation)
      - [Production label authorization](#production-label-authorization)
  - [Lost Hosts](#lost-hosts)

<!-- mdformat-toc end -->

## Overview

When using slurm-bridge, there are some configuration requirements and
considerations to be made.

## Partitions

Slurm bridge external jobs are submitted to a default partition (e.g.
`slurm-bridge`) in Slurm, or the one specified by
`slurmjob.slinky.slurm.net/partition` on the workload. Any partition where these
external jobs are submitted to should only consist of Slurm nodes that map to
Kubernetes nodes, whether they are nodes with co-located kubelet and slurmd or
Kubernetes nodes modeled as Slurm external nodes.

## Nodes

Slurm bridge supports two ways to make Kubernetes nodes available to Slurm:

- **External nodes**: `slurm-bridge` registers labeled Kubernetes nodes with
  Slurm as external Slurm nodes. No slurm-operator `Nodeset` is required for the
  bridge partition.
- **Hybrid nodes**: `slurmd` runs side-by-side with kubelet and registers a
  Slurm dynamic node. slurm-operator can manage `slurmd` with a DaemonSet-mode
  `Nodeset`, or host automation can install it as a native systemd service.

In both modes, Slurm should only schedule bridge jobs to Slurm nodes that map
back to Kubernetes nodes.

### External Nodes

External nodes are Kubernetes nodes that `slurm-bridge` registers with Slurm
using `State=External`. The node controller watches for the
`scheduler.slinky.slurm.net/external-node` label and creates or removes the
matching Slurm node.

```yaml
apiVersion: v1
kind: Node
metadata:
  name: worker-1
  labels:
    scheduler.slinky.slurm.net/external-node: "true"
  annotations:
    scheduler.slinky.slurm.net/external-node-partitions: slurm-bridge
```

The `scheduler.slinky.slurm.net/external-node-partitions` annotation is a
comma-separated list of Slurm partitions. The bridge validates that each
partition exists, then registers the Slurm node with matching features. Removing
a partition from the annotation removes its feature. Slurm `Nodeset`
configuration can then map those features into partitions:

```conf
Nodeset=slurm-bridge Feature=slurm-bridge
PartitionName=slurm-bridge Nodes=slurm-bridge State=UP Default=NO
```

### Hybrid Nodes

Hybrid nodes run kubelet and `slurmd` on the same physical host. Bridge jobs run
on the dynamic Slurm nodes registered by those daemons. `slurmd` can run in a
slurm-operator pod or as a native host service. See
[Host-installed Hybrid Slurm Nodes](native-hybrid-nodes.md) for a bare-metal
package and configless-registration procedure.

Hybrid nodes share capacity between workload managers over time. A physical node
must not run a native Slurm user workload and a Slurm-bridge-managed Kubernetes
user workload simultaneously. System components such as kubelet, `slurmd`, CNI,
device plugins, and monitoring DaemonSets are expected exceptions.

For Kubernetes-managed daemons, a DaemonSet-mode `Nodeset` can place one
`slurmd` pod on each worker node selected for bridge scheduling:

```yaml
nodesets:
  slurm-bridge:
    enabled: true
    scalingMode: DaemonSet
    partition:
      enabled: true
    podSpec:
      nodeSelector:
        scheduler.slinky.slurm.net/slurm-bridge: worker
```

For DRA-backed devices, the bridge node controller records the stable device
inventory in the Slurm node's `Extra` field. The hybrid node's `gres.conf` must
provide matching `Name`, `Type`, and `Count` values. Other GRES entries are
allowed and remain available to native Slurm workloads. If the DRA-managed
entries do not match, the controller sets the Kubernetes node's
`SlinkySlurmGRESCompatible` condition to `False` with the required `gres.conf`
inventory. It does not check device order or `Cores=`, which
[NUMA alignment](workload.md#numa-alignment) also needs. This condition reports
compatibility to administrators; Slurm node features control bridge-job
placement.

The controller adds the `slurm_bridge_gres_compatible` Slurm feature to every
external node and to hybrid nodes whose GRES configuration has been verified.
Every Slurm job submitted by the bridge requires this feature. An incompatible
hybrid node therefore remains available to native Slurm jobs while bridge jobs
cannot be allocated to it. Other hybrid-node features and native job constraints
are unaffected.

### Topology

Slurm supports dynamic node topology with `topology.yaml`. The topology file
must be available to Slurm. Kubernetes nodes can be annotated with the Slurm
topology units they belong to.

```yaml
configFiles:
  topology.yaml: |
    ---
    - topology: topo-switch
      cluster_default: true
      tree:
        switches:
          - switch: sw_root
            children: s[1-2]
          - switch: s1
            nodes: slurm-node[1-2]
          - switch: s2
            nodes: slurm-node[3-4]
    - topology: topo-block
      cluster_default: false
      block:
        block_sizes:
          - 2
          - 4
        blocks:
          - block: b1
            nodes: slurm-node[1-2]
          - block: b2
            nodes: slurm-node[3-4]
```

Annotate the Kubernetes node with the topology units for the corresponding Slurm
node:

```yaml
apiVersion: v1
kind: Node
metadata:
  name: worker-1
  annotations:
    topology.slinky.slurm.net/spec: topo-switch:s1,topo-block:b1
```

External nodes and hybrid nodes managed by slurm-operator use the same
Kubernetes annotation, but different controllers apply it to Slurm:

- In external mode, `slurm-bridge` reads `topology.slinky.slurm.net/spec` from
  the labeled Kubernetes node and reconciles the external Slurm node's dynamic
  topology.
- For hybrid nodes managed by slurm-operator, the operator reads
  `topology.slinky.slurm.net/spec` from the Kubernetes node where the `Nodeset`
  pod is scheduled, copies it to the pod, and reconciles the `slurmd` pod's
  Slurm node topology.

Removing the annotation clears the dynamic topology for those nodes.
Host-installed hybrid daemons require administrators to configure their Slurm
node topology directly; neither controller applies the annotation to them. When
using multiple topologies, Slurm partitions can select a specific topology with
the partition `Topology` setting; otherwise Slurm uses the `cluster_default`
topology.

### Hybrid Workload Isolation

Bridge jobs receive exclusive whole-node Slurm allocations by default. Workloads
requesting `slurmjob.slinky.slurm.net/exclusive: "false"` are always submitted
with `Shared=mcs` and the configured `schedulerConfig.mcsLabel`. This allows
bridge-managed Kubernetes workloads in the same MCS category to share a node
without enabling unprotected sharing with native Slurm jobs.

Configure both the bridge and Slurm to enable [MCS] isolation:

```yaml
# slurm-bridge values.yaml
schedulerConfig:
  mcsLabel: kubernetes
```

```text
# slurm.conf
MCSPlugin=mcs/label
MCSParameters=ondemand,ondemandselect
```

With `ondemandselect`, the bridge's `Shared=mcs` value activates MCS node
filtering for non-exclusive jobs. Native Slurm jobs with a different or empty
MCS label cannot share those nodes while a `kubernetes`-labeled allocation is
running.

#### Production label authorization

Slurm's `mcs/label` plugin controls category-based sharing but accepts arbitrary
labels; it does not authorize their use. Production clusters must reserve
`schedulerConfig.mcsLabel` at the `slurmctld` boundary with a server-side
[`job_submit` plugin][job-submit] or equivalent site policy, using a trusted
bridge identity and rejecting native submissions or modifications that request
the reserved label. Without that Slurm-side policy, a native user can
deliberately join the bridge's MCS category.

MCS only governs workloads represented by active Slurm allocations. Continue to
use the `slinky.slurm.net/managed-node` `NoExecute` taint and admission policy
to keep Kubernetes workloads that bypass slurm-bridge off hybrid nodes.
Operational or controller-driven cancellation must also keep a node unavailable
to native Slurm work until its Kubernetes pods have actually stopped.

## Lost Hosts

Slurm can keep a job's allocation while one of its nodes is `DOWN`, for example
for an external job submitted with `kill_on_node_fail=0` on a `slurmctld` that
keeps such jobs' allocations. A pod whose host died then stays `Terminating`,
because only its kubelet can confirm that it stopped, and the bridge would hold
the allocation until the pod is finished. The bridge stops waiting for such a
pod when all of these hold:

- the pod was deleted, for example by Kubernetes once its toleration for
  `node.kubernetes.io/unreachable` expired (300 seconds by default);
- its node's kubelet has stopped reporting (`Ready` is `Unknown`), and the node
  carries the `node.kubernetes.io/unreachable` `NoExecute` taint;
- Slurm reports its node `NOT_RESPONDING`.

The bridge checks these each time it resyncs its allocations, every 30 seconds,
and cancels the job once every other member has exited. It needs the `node`
controller: without it, the bridge waits for such pods as it does for any other.
Before cancelling, it drains the pod's Slurm node with the reason
`slurm-bridge: waiting for pods on a lost host to stop`, so no new work starts
on resources the pod's containers may still use if the host returns. The `node`
controller lifts that drain once no deleted pod scheduled by the bridge is bound
to the node any more, or keeps the node drained if the Kubernetes node is
cordoned. A drain set by someone else is left as it is. The bridge records a
`SlurmAllocationReleasedNodeLost` event on the pod, and the job ends `CANCELLED`
rather than `NODE_FAIL`. Requiring both the kubelet and `slurmd` to be silent
keeps a kubelet failure, or a node cut off only from the Kubernetes API server,
from releasing resources that the pod may still use. Slurm marks a node
`NOT_RESPONDING` only when its pings fail, and it does not ping nodes when
`SlurmdTimeout` is 0, so the bridge then never stops waiting for the pod. The
same holds for nodes the bridge registers in Slurm as external nodes, which run
no `slurmd`.

The pod itself stays `Terminating` until Kubernetes finishes it. For a host that
will not return, delete the Node object or add the
`node.kubernetes.io/out-of-service` taint to it. If the host returns, its
kubelet stops the pod, and the drain is lifted once it has.

<!-- Links -->

[job-submit]: https://slurm.schedmd.com/job_submit_plugins.html
[mcs]: https://slurm.schedmd.com/mcs.html
