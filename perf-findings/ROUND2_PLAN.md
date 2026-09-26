# Round 2: end-to-end investigations on a real local cluster

Each investigator follows BRIEF2.md and works on its own cluster port range (BASE). Only 2 run at once, because the machine has 4 vCPUs.
Results go to `perf-findings/<ID>.md` and `<ID>.patch` (copied from the scratchpad once each finishes).

| Wave | ID | Workload | BASE | Status |
|---|---|---|---|---|
| 1 | P1-point-read | oltp_point_select through vtgate (prepared and text; 8/32 threads): per-query CPU/latency breakdown, syscalls, gRPC, pools, GC | 30000 | done |
| 1 | P5-operations | PRS/ERS/VTOrc failover windows, buffering, backup/restore wall time | 40000 | done |
| 2 | P2-writes-tx | oltp_write_only / read_write / insert: transactions, commit path, tx pool, reserved connections | 30000 | done |
| 2 | P4-vreplication | MoveTables/Reshard copy + catch-up rows/s, VStream CDC throughput | 40000 | done |
| 3 | P3-scatter | cross-shard scatter / aggregation / ORDER BY LIMIT / joins / IN lists | 30000 | done |
| 3 | P6-runtime | many connections and churn, GC (GOGC/GOMEMLIMIT) vs p99, gRPC and pool settings, mutex/block profiles | 40000 | running |
| 4 | P7-validate-round1 | apply the round-1 patches and measure their end-to-end effect on the P1-P3 workloads | 30000 | pending |
