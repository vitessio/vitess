# Per-hop overhead: preliminary measurements

Setup: baseline binaries, 2 shards, 1 table × 100k rows, oltp_point_select with the text protocol, load average 2–4. CPU µs/query is summed over processes of each kind (2 vttablets, 2 mysqlds); each serves half the queries.

## 1 client thread

| Path | Latency | vtgate | vttablets | mysqlds |
|---|---|---|---|---|
| sysbench → mysqld (unix socket) | 0.06 ms | – | – | 45 µs |
| sysbench → mysqld (TCP) | 0.05 ms | – | – | 33 µs |
| vtbench gRPC → vttablet → mysqld | 0.48 ms | – | 387 µs | 146 µs |
| vtbench gRPC → vtgate → vttablet → mysqld | 0.98 ms | 751 µs | 502 µs | 177 µs |
| sysbench → vtgate (MySQL protocol) → vttablet → mysqld | 0.78 ms | 530 µs | 470 µs | 172 µs |
| same, GOMAXPROCS=1 on vtgate and vttablet | 0.58 ms | 349 µs | 337 µs | 141 µs |
| same, GOMAXPROCS=2 | 0.76 ms | 511 µs | 446 µs | 175 µs |

- Each vtgate query arriving over the MySQL protocol costs 343 µs user and 209 µs sys CPU.
- Each vttablet costs about 135 µs user and 105 µs sys per total query.
- Each mysqld costs about 72 µs user and 15 µs sys per total query, which is 2–3x the direct cost for the same single statement. vttablet sends exactly 1 statement per query (Questions=1.00, Com_select=1.00), with 52 bytes in and 191 bytes out.
- vtgate makes about 2 voluntary context switches per query.

## 8 client threads

| Config | QPS | Latency | vtgate | vttablets | mysqlds |
|---|---|---|---|---|---|
| default | 4.7k | 1.70 ms | 210 µs | 214 µs | 139 µs |
| GOMAXPROCS=1 | 4.8k | 1.67 ms | 144 µs | 174 µs | 129 µs |

## CPU profile at 1 client thread

**vttablet:**
- `TabletServer.execute`, including the MySQL socket I/O: 23%.
- Scheduler: `schedule`/`findRunnable` 20%, `futex` 19.5% flat, `wakep`/`startm` 12%.
- gRPC `loopyWriter.run`: 18%.
- gRPC `readyreader`: 8.7%.

**vtgate:** `VTGate.Execute` 26%, `park_m`/`schedule` 21%, futex 16%, `loopyWriter.run` 22%, `http2Client.reader` 10%.

## Interpretation

Most of the CPU goes to goroutine and OS-thread hand-offs (futex wake and sleep, scheduler) and to per-message syscalls in gRPC's separate reader and writer goroutines, not to query logic. On this VM, waking idle vCPUs is expensive. mysqld CPU inflation still needs explaining; candidates are HT-sibling contention from spinning Go threads, the query text, and session settings.

## In-flight upstream work (closed drafts)

- vitessio/vitess#19620: "Add StreamExecuteRaw gRPC for zero-parse MySQL streaming".
- vitessio/vitess#20215: "StreamExecuteRaw: bidirectional *Raw streaming RPCs (no stream pool)".
