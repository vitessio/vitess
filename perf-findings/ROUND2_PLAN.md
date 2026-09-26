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
| 3 | P6-runtime | many connections and churn, GC (GOGC/GOMEMLIMIT) vs p99, gRPC and pool settings, mutex/block profiles | 40000 | done |
| 4 | P7-combined | combine the safe round-1 + round-2 code changes into one build; measure base vs combined on standard sysbench + scatter workloads, then add P1 config tuning | 40000 | done |

## Hop-overhead deep dive

Prompted by the ~15x QPS gap to plain MySQL. Preliminary data is in HOP-OVERHEAD-prelim.md. Measurements are serialized through `flock /home/vt/perf/bench.lock`.

| ID | Focus | BASE | Status |
|---|---|---|---|
| P7-finish | write-up of the combined build: applied/skipped patches, tests, provenance | – | done |
| H1-hop-rootcause | exact per-query cost breakdown (syscalls, context switches, goroutine hops, perf/kernel), Go-networking floor in this environment, mysqld CPU inflation, runtime/kernel/gRPC-option levers | 30000 | running |
| H2-grpc-transport | unary vs bidi stream vs stream pool vs raw framing (microbenchmarks); evaluate PRs #19620/#20215; prototype pooled-stream Execute with raw MySQL rows | 40000 | done |
