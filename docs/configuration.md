# Configuration

## Pack Multiple Pods on a Node

By default, Slurm will reserve a full node for each job. To enable packing, adjust slurm.conf:

- **OverSubscribe** — Set to `YES` or `FORCE` on the partition so multiple jobs (pods) can share nodes.

- **SchedulerParameters** - Set:
  - `bf_busy_nodes` — Backfill scheduler prefers nodes that are already busy, packing jobs onto fewer nodes and leaving others idle for whole-node jobs.
  - `pack_serial_at_end` — Schedules serial jobs at the end of the backfill window to reduce fragmentation and improve packing.

When using slinky, this can be set by adjusting its `values.yaml`:

```yaml
controller:
  extraConf: |
    SchedulerParameters=pack_serial_at_end,bf_busy_nodes
nodesets:
  slinky: # or other nodeset name
    partition:
      config: |
        OverSubscribe=YES
```

By default, slurm-bridge will schedule jobs with "shared: none". In order to allow jobs to share nodes, set the Pod's `slurmjob.slinky.slurm.net/shared` annotation to `user`.

For more details, see:
- [cons_tres resource sharing](https://slurm.schedmd.com/cons_tres_share.html).
- [Scheduler Params](https://slurm.schedmd.com/slurm.conf.html#OPT_SchedulerParameters)
- [Job settings](https://slurm.schedmd.com/sbatch.html)