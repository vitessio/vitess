# End-to-end baseline (commit aa9ccf9, local cluster)

## Setup

- `perf-findings/harness/cluster.sh`: BASE=20000, keyspace `sbtest` with 2 shards (-80, 80-), 1 primary per shard, durability none.
- 4 sysbench tables × 100k rows.
- MySQL 8.0.46 (Ubuntu), innodb_flush_log_at_trx_commit=2, sync_binlog=0.
- Machine: 4 vCPU Xeon, shared with sysbench, and the machine is oversubscribed during runs.

## oltp_point_select, 8 threads, 20s

CPU per query: µs of process CPU divided by total queries. Each tablet serves half the queries.

| client mode | QPS | avg | p95 | vtgate | vttablet (each) | mysqld (each) |
|---|---|---|---|---|---|---|
| prepared (`--db-ps-mode=auto`) | ~5.5k | 1.44ms | 2.66ms | 195 µs | ~107 µs | ~72 µs |
| text (`--db-ps-mode=disable`) | ~5.0k | 1.59ms | 2.91ms | 237 µs | ~112 µs | ~75 µs |
| direct mysqld (shard -80, socket) | 122k | 0.07ms | 0.16ms | – | – | – |

## Top of the vtgate CPU profile (prepared statements)

- `Syscall6`: 20% flat.
- `futex`: 4%.
- gRPC `http2Client.reader`: 11% cumulative.
- The rest is spread widely: mallocs, map lookups.

In vttablet, `Syscall6` is 20% and `futex` 10%.
