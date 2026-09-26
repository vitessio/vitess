# P1-point-read: single-row OLTP read path (oltp_point_select through vtgate)

Investigator P1, BASE=30000, base commit aa9ccf9. Patched binaries: `/home/vt/bin-P1` (this patch),
`/home/vt/bin-P1r1` (this patch + round-1 F10/F11/F15/F19/F25).
Patch: `findings/P1-point-read.patch`. Negative-result prototype (not in the patch): `findings/P1-multiconn-prototype.patch`.

## TL;DR

- Most of the ~190 µs (vtgate) and ~215 µs (vttablet) per point select is not Vitess query logic. It is fixed cost per hop:
  gRPC (≈35% of each process), syscalls (1 read + 1 write per socket per hop), and above all **Go scheduler wake-up
  overhead** (idle Ps spinning, futex wakes, `nanosleep` in work stealing), which grows as load drops. CPU/query is
  4–5x higher at 1 client thread than at 32 (vtgate 528 → 114 µs, tablets 488 → 128 µs; A/B 5).
- Vitess-owned logic (protocol parsing, planning, routing, the tablet query path) is ≈25–30% of vtgate CPU and ≈25% of
  vttablet CPU, spread over many 1–3% items. No single large target remains there for point selects.
- Ranked end-to-end wins (A/B with 2–3 alternating rounds; CPU/query is the reliable metric, QPS/latency swing ±20%
  with the neighbour's load):
  1. **Right-size GOMAXPROCS** (config, S). Fewer Ps = less spinning. With the patch, GOMAXPROCS=2 for vtgate+vttablet:
     vtgate −15% and tablets −15% CPU/query at 8 threads (−24%/−20% vs base, QPS +11%, avg latency −10%); at 32 threads
     −9%/−2% CPU but −7% QPS. GOMAXPROCS=1: −35% CPU and 0.80 → 0.62 ms latency at 1 thread (+31% QPS), but −18% QPS
     and 2x p99 at 8 threads. On a host shared with mysqld (or with CPU limits) the Go default is too high.
  2. **vttablet gRPC stream workers: default max(GOMAXPROCS, 64) and a new flag `--grpc-server-num-stream-workers`**
     (code, S). Today the pool has GOMAXPROCS workers; long-lived streams (StreamHealth from every vtgate, VStreams)
     hold a worker for their whole life, so most unary RPCs fall back to `go f()` and grow a fresh stack (copystack
     3.8% of tablet CPU). Tablet CPU/query −4..−6% at 8 threads and −10..−14% at 32 threads; QPS +6..+14% at 32
     threads (A/B 2, 3, 5).
  3. **GOGC=400** (config, S): a further −1..−10% CPU/query on both processes (A/B 1, 3). The heap is only ~15 MB and
     vtgate collects ~9 times/s. Pair it with GOMEMLIMIT.
  4. **gRPC static flow-control windows** (config, S): `--grpc-initial-window-size`/`--grpc-initial-conn-window-size`
     on vtgate and `--grpc-server-initial-window-size`/`--grpc-server-initial-conn-window-size` on vttablet. Setting any
     of them turns off gRPC's BDP estimator, which otherwise sends a PING (and gets a PING ack) about once per RTT in each
     direction. Syscalls/query drop (vtgate write 2.12 → 1.78, read 2.73 → 2.21; vttablet write 2.03 → 1.73, read
     3.16 → 2.90); CPU/query −2..−6%. Latency effect inconclusive (+31% in one A/B, −23% QPS in the next; both under load).
  5. **Per-query micro fixes** (code, S): drop a per-query goroutine wake-up in vtgate (TabletStatusAggregator channel),
     a per-query timer in vttablet (checkPermissions), a per-query viper lookup (TransactionMode), protobuf-reflection
     enum `String()`, `fmt.Sprintf` + global mutex (getStatsAggregator), and remote-address formatting. vtgate −3..−5%
     CPU/query at 8 threads, ≈0 at 32 threads; tablet side within noise (A/B 3 `p1w4`).
- Negative: multiple gRPC connections per tablet (+2..+9% CPU/query, −10% QPS at 32 threads), consolidator off
  (no change), round-1 patches F10/F11/F15/F19/F25 on top (no measurable change for point selects, A/B 4).

## Environment and method

- 4 vCPU shared VM; another investigator's cluster (BASE 40000 / P5) ran concurrently most of the time. Load average
  during runs was 5–15 (reported per row). Treat QPS/latency differences under ~10% as noise; CPU µs/query
  (`/proc/<pid>/stat` utime+stime delta ÷ queries) is much more stable (typically ±2–3%).
- Harness: copy of cluster.sh with a `restart` command (restarts only vtgate+vttablets with a different BIN, flags or
  env, keeping mysqld and data) and `GATE_ENV`/`TABLET_ENV`; `bench.sh` (QPS, avg, p95/p99 from sysbench histogram, CPU
  µs/query for vtgate, both tablets summed, both mysqld summed); `ab.sh` (alternates configs per round, restart + 5 s
  warm-up before each). Files in `perf-findings/harness/P1-*`.
- "tablets" = sum of both tablets' CPU ÷ total queries = CPU of the tablet that served a query (each tablet serves half).

## Baseline (base binaries, defaults)

| mode | threads | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | vttablet µs/q | mysqld µs/q |
|---|---|---|---|---|---|---|---|---|
| prepared (`--db-ps-mode=auto`) | 1 | 1361 | 0.73 | 0.93 | 1.25 | 528 | 488 | 180 |
| prepared | 8 | 4989 | 1.61 | 3.23 | 4.95 | 188 | 214 | 143 |
| prepared | 32 | 8440 | 3.79 | 7.49 | 10.62 | 117 | 132 | 110 |
| text (`--db-ps-mode=disable`) | 8 | 4768 | 1.68 | 3.25 | 4.96 | 222 | 218 | 143 |
| text | 32 | 6995 | 4.61 | 9.47 | 14.44 | 140 | 132 | 109 |

(1-thread row: A/B 5, 2 rounds, 15 s. Other rows: A/B 3.) Direct to mysqld (BASELINE.md): 0.07 ms avg, 122k QPS.

Allocation/GC (prepared, 8 threads): vtgate 12.4 KB and 209 allocs per query, ~9 GCs/s, heap in use 15 MB, ~0.9 ms
pause-total per GC; vttablet 9.0 KB and 142 allocs per query, ~2.5 GCs/s, heap 16.5 MB. Text protocol: vtgate 15.3 KB / 262 allocs.

## Where the time goes

CPU profiles: 15 s during a 25 s prepared-statement run at 8 threads (base binaries). µs = share of the profile × measured
CPU/query (vtgate 190 µs, vttablet 215 µs). Cumulative shares; the sub-rows of a stage are included in that stage.

### vtgate (≈190 µs/query, prepared statements)

| stage | share | ≈ µs/q | notes |
|---|---|---|---|
| MySQL protocol: read + parse COM_STMT_EXECUTE | 3.8% | 7 | `readEphemeralPacket` 2.0%, `parseComStmtExecute` 1.8% |
| MySQL protocol: write result + flush | 10.3% | 20 | `endWriterBuffering` 8.6% (the one `write(2)` per query), fields/rows 1.7% |
| handler glue (callinfo, span/Preview, caller id, cancel ctx) | 3.8% | 7 | `MysqlCallInfo` formats the remote address per query (0.7–1%) |
| VTGate.Execute + Executor.Execute bookkeeping (stats, logstats, temp-table lease) | 5.0% | 10 | |
| plan cache lookup (`fetchOrCreatePlan`: key hash + theine Get) | 2.6% | 5 | |
| executePlan + logExecutionEnd/setLogStats | 2.7% | 5 | enum `TabletType.String()` via protobuf reflection |
| routing: `findRoute` (evalengine + hash vindex + ResolveDestinations) | 4.6% | 9 | |
| ScatterConn / TabletGateway overhead | 8.0% | 15 | `TransactionMode()` viper lookup 1.5%, `getStatsAggregator` Sprintf+global mutex 1.4%, stats Record 0.9%, per-query channel send to the status aggregator goroutine |
| gRPC client call in the caller goroutine (`grpc.invoke`, newClientStream, Send/Recv, proto conversion) | 16% | 30 | newClientStream alone 6.7% |
| gRPC loopy writer goroutine (frame + `write(2)`) | 12.7% | 24 | |
| gRPC reader goroutine (`read(2)`, frame parse, hand-off) | 9.5% | 18 | |
| Go scheduler (findRunnable, futex, stealWork) | ≈10–12% | 20 | not attributable to a stage |
| GC (background mark + assist) | ≈4–5% | 8 | |

Text protocol adds ≈35 µs/query: parse 4.4%, `Normalize` 6.0%, `sqlparser.String` 1.5%, plan key hash/cache 1.3%,
`startSpan` 1.6% (comment regexp + Preview). Round 1 (F02/F12/F22/F23/F24) already covers the parser/normalizer.

### vttablet (≈215 µs/query served)

| stage | share | ≈ µs/q | notes |
|---|---|---|---|
| gRPC server read loop (`HandleStreams`: read, HPACK, `operateHeaders`) | 13.0% | 28 | |
| gRPC unary dispatch (decode request 3.5%, `sendResponse` 4.5%, status) | 8.3% | 18 | |
| goroutine start + stack growth for the handler (`copystack`) | 3.8% | 8 | streams that miss a free worker run under `go f()`; fixed by finding 2 |
| grpcqueryservice glue (callinfo, ResultToProto3) | 2.1% | 5 | `GRPCCallInfo` formats the peer address per RPC |
| execRequest (query-timeout ctx, StartRequest, logstats) | 3.7% | 8 | `context.WithTimeout` for the query timeout is required |
| plan cache (`GetPlan`) | 2.2% | 5 | |
| QueryExecutor.Execute bookkeeping: stats defer 3.1%, `checkPermissions` 2.0% | 5.1% | 11 | checkPermissions created a timer per query for QRBuffer only; enum String via reflection |
| execSelect: pool Get 2.4%, consolidator 1.3%, SQL generation 0.8% | 4.6% | 10 | smartconnpool: no contention (see below) |
| MySQL client: WriteComQuery 5.4% + ReadQueryResult 9.5% + execOnce 2.8% | 17.7% | 38 | 1 write + 1 read per query, no extra round trips |
| gRPC loopy writer (response frames + `write(2)`) | 14.0% | 30 | |
| Go scheduler (findRunnable 14.6%, futex 8.9%) | ≈15% | 31 | |
| GC | ≈3–4% | 7 | |

### Syscalls per query (strace -c -f, 5 s during a prepared 8-thread run)

strace slows everything down (QPS fell to ~1350/s), so wake-up syscalls (futex/nanosleep/epoll) are inflated relative to
an unstraced run; read/write counts are exact.

| process | total | read (EAGAIN) | write | futex | nanosleep | epoll_pwait | sched_yield |
|---|---|---|---|---|---|---|---|
| vtgate | 10.6 | 2.7 (0.9) | 2.1 | 1.9 | 2.0 | 1.6 | 0.1 |
| vttablet | 15.6 | 3.2 (1.4) | 2.0 | 3.8 | 3.3 | 3.0 | 0.3 |
| mysqld | ~5 | recvfrom 2.9 (0.9) | sendto 1.0 | 0.4 | – | ppoll 0.9 | – |

- vtgate: 1 write to the client + ~1.1 gRPC writes; vttablet: 1 write to MySQL + ~1.0 gRPC writes. HEADERS, DATA and
  trailers are coalesced into one write, and concurrent requests share writes. About 0.3 writes and 0.3–0.5 reads per
  query and process are gRPC BDP pings and their acks (see the gRPC section). No per-query flush storms, no SetDeadline
  calls, no timers in the syscall trace.
- No extra round trips on the MySQL connection: mysqld does exactly one `sendto` per query and two reads (header and
  body, which is mysqld's own behaviour). No SET/USE/`select 1` per query. Health checks are the 5 s StreamHealth only.
- The avoidable part is scheduler traffic: `futex` + `nanosleep` (runtime `usleep` in `runqgrab` while spinning) +
  `sched_yield` ≈ 4/query in vtgate and ≈ 7.4/query in vttablet.

### Goroutine hand-offs and timers per query

- vtgate: client conn goroutine → gRPC loopy writer (controlBuffer) → gRPC reader goroutine (netpoll) → caller goroutine
  (recvBuffer) → **plus a channel send to `processQueryInfo`** (TabletStatusAggregator) that wakes a background
  goroutine for every query. Removed by this patch (5 → 4 wake-ups per query).
  Timers: the MySQL `flushTimer.Reset` per packet (F25 covers it); no `context.WithTimeout` unless `--mysql-server-query-timeout`.
- vttablet: gRPC reader → stream worker (or new goroutine) → MySQL read (netpoll park) → loopy writer. Timers:
  `withTimeout` (query timeout, needed) and **`checkPermissions` created a second `context.WithTimeout` per query**,
  only used when a QRBuffer rule matches (removed by this patch). `connpool.execOnce` uses `context.AfterFunc`
  (no goroutine, already optimised).
- No per-query goroutine spawns in vtgate for single-shard routes (`multiGoTransaction` runs one shard inline).

### gRPC connection vtgate → vttablet

- One `grpc.ClientConn` (one TCP/HTTP2 connection) per tablet. All calls share one loopy writer and one reader goroutine.
- Mutex profile: the transport's `controlBuffer` mutex is the main contention point (≈7 µs/query of wait time at 8
  threads in vtgate; `http2Server.writeHeader` in vttablet). This is waiting, not CPU.
- The loopy writer is not saturated (12–14% of a core at 5 k QPS) and gains from batching: CPU/query drops sharply
  with concurrency (see the 32-thread rows).
- Prototype with 4 connections per tablet (round-robin for Execute): **worse** on every metric (A/B 2):
  vtgate +1.5..+7.5% CPU/query, tablets +4..+9%, QPS −10% at 32 threads. Less batching per connection costs more than
  the contention saves. Not recommended at this scale.
- BDP estimation: with the default dynamic flow control, each side sends a BDP PING whenever it receives DATA and no
  BDP ping is outstanding, and the peer answers with a PING ack. With ~300-byte messages the estimate never reaches its
  cap, so this continues forever at about one ping per RTT in each direction. Setting any of the initial window size
  flags turns it off. strace (prepared, 8 threads, per query): vtgate write 2.12 → 1.78 and read 2.73 → 2.21; vttablet
  write 2.03 → 1.73 and read 3.16 → 2.90. CPU/query −2..−6% (A/B 6/7). Trade-off: a static window caps per-stream
  throughput at window/RTT (1 MiB at 1 ms RTT ≈ 1 GB/s, at 50 ms ≈ 20 MB/s), so size it for cross-region links.

### vttablet connection pool (smartconnpool)

No contention: `Pool.Get` is 2.1% CPU, the mutex profile has no pool frames, and the block profile shows no waiting
in Get (only the background worker). Pool size 64 vs 8 concurrent queries.

### GC

- GC background workers ≈3–4% of CPU, plus assists inside `mallocgc` (8% total malloc in vtgate). The live heap is
  small (15 MB), so with GOGC=100 vtgate collects ~9 times per second at 5 k QPS.
- GOGC=400: vtgate −4..−10%, tablets −2..−14% CPU/query (A/B 1 and 3). Peak heap grows ~4x (to ~60 MB), which is
  negligible. A GOMEMLIMIT (e.g. 80% of the container limit) makes this safe for large-result workloads.

### After the patch (prepared, 8 threads, same method)

vtgate 170 µs/q, tablets 201 µs/q in this profiling run (base run: 193/214). In the tablet `copystack` fell from 3.8% to
0.8%, all Execute RPCs run on `serverWorker`, and `context.WithTimeout` from 2.4% to 0.7% (only the query timeout is
left). In vtgate `runtime.chansend` fell from 1.3% to 0.9% (gRPC only), and `TransactionMode`, `getStatsAggregator`'s
Sprintf and `MysqlCallInfo`'s address formatting no longer appear. Scheduler cost is unchanged (findRunnable 10.7% vtgate,
14.7% tablet): it is the next target, and only GOMAXPROCS moves it.

## A/B results

Mean of rounds; percentages vs the first row of each block; [min–max] for the CPU columns.

### A/B 1: config only, base binaries (3 rounds; round 2 ran at load ~15)

| mode | thr | config | n | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ps | 8 | base | 3 | 4257 | 1.97 | 4.43 | 7.85 | 197 [193–205] | 225 [220–233] | 148 | 10.8 |
| ps | 8 | procs2 | 3 | 4316 (+1.4%) | 2.07 (+4.9%) | 4.68 (+5.7%) | 9.17 (+16.8%) | 175 (-11.2%) [162–197] | 215 (-4.2%) [203–237] | 144 (-2.7%) | 9.5 |
| ps | 8 | gogc400 | 3 | 5002 (+17.5%) | 1.61 (-18.3%) | 3.13 (-29.4%) | 5.02 (-36.0%) | 187 (-5.2%) [182–193] | 215 (-4.2%) [210–219] | 144 (-2.5%) | 8.8 |
| text | 8 | base | 3 | 3578 | 2.35 | 5.40 | 9.71 | 238 [233–243] | 238 [231–252] | 153 | 11.8 |
| text | 8 | procs2 | 3 | 4351 (+21.6%) | 1.87 (-20.2%) | 3.85 (-28.7%) | 6.03 (-37.9%) | 200 (-16.0%) [194–210] | 218 (-8.5%) [210–227] | 147 (-3.7%) | 9.1 |
| text | 8 | gogc400 | 3 | 4899 (+36.9%) | 1.63 (-30.5%) | 3.02 (-44.0%) | 4.49 (-53.7%) | 222 (-6.6%) [220–226] | 218 (-8.5%) [214–222] | 146 (-4.8%) | 9.6 |

### A/B 2: this patch at an intermediate stage (stream workers set with a flag), gRPC multi-connection prototype (3 rounds)

`p1` = micro fixes (without the TransactionMode change) with 4 stream workers (the old default), `p1w64` = + 64 stream
workers, `p1conn4` = the `p1` binary + 4 gRPC connections per tablet in vtgate (env-var prototype, not in the patch).

| mode | thr | config | n | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ps | 8 | base | 3 | 4832 | 1.68 | 3.48 | 5.55 | 195 [193–198] | 221 [218–224] | 146 | 7.9 |
| ps | 8 | p1 | 3 | 4788 (-0.9%) | 1.68 (-0.4%) | 3.48 (+0.0%) | 5.64 (+1.6%) | 189 (-3.2%) [187–190] | 218 (-1.4%) [217–219] | 145 (-1.1%) | 7.9 |
| ps | 8 | p1w64 | 3 | 4944 (+2.3%) | 1.62 (-3.6%) | 3.37 (-3.0%) | 5.69 (+2.5%) | 188 (-3.6%) [186–191] | 212 (-3.9%) [210–216] | 144 (-1.4%) | 8.0 |
| ps | 8 | p1conn4 | 3 | 4918 (+1.8%) | 1.63 (-3.4%) | 3.33 (-4.2%) | 5.38 (-3.0%) | 198 (+1.5%) [197–199] | 230 (+4.4%) [229–232] | 144 (-1.6%) | 8.7 |
| ps | 32 | base | 3 | 7757 | 4.13 | 8.40 | 12.38 | 121 [119–122] | 134 [130–139] | 110 | 10.3 |
| ps | 32 | p1 | 3 | 8295 (+6.9%) | 3.86 (-6.6%) | 7.62 (-9.2%) | 10.90 (-11.9%) | 120 (-0.8%) [115–124] | 136 (+2.0%) [132–141] | 114 (+4.3%) | 10.5 |
| ps | 32 | p1w64 | 3 | 8223 (+6.0%) | 3.90 (-5.7%) | 7.86 (-6.4%) | 11.26 (-9.0%) | 119 (-1.1%) [119–120] | 121 (-9.7%) [120–121] | 111 (+0.9%) | 9.9 |
| ps | 32 | p1conn4 | 3 | 7013 (-9.6%) | 4.58 (+10.7%) | 9.95 (+18.5%) | 15.98 (+29.1%) | 130 (+7.5%) [126–132] | 146 (+9.0%) [144–147] | 115 (+5.2%) | 10.9 |

### A/B 3: final patch vs base, with and without GOGC=400 (3 rounds, 4 modes)

`p1w4` = final patch but `--grpc-server-num-stream-workers=4` (isolates the micro fixes), `p1` = final patch with
defaults (64 workers), `p1gogc400` = final patch + GOGC=400 for vtgate and vttablet.

| mode | thr | config | n | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ps | 8 | base | 3 | 4989 | 1.61 | 3.23 | 4.95 | 188 [185–193] | 214 [209–221] | 143 | 7.8 |
| ps | 8 | p1w4 | 3 | 5056 (+1.3%) | 1.59 (-1.2%) | 3.19 (-1.2%) | 5.83 (+17.6%) | 182 (-3.5%) [179–184] | 217 (+1.4%) [213–222] | 143 (+0.2%) | 9.2 |
| ps | 8 | p1 | 3 | 5547 (+11.2%) | 1.44 (-10.7%) | 2.74 (-15.1%) | 4.25 (-14.2%) | 178 (-5.3%) [175–183] | 206 (-3.9%) [202–210] | 139 (-2.3%) | 8.9 |
| ps | 8 | p1gogc400 | 3 | 5546 (+11.2%) | 1.44 (-10.7%) | 2.66 (-17.6%) | 4.01 (-19.1%) | 176 (-6.5%) [175–177] | 202 (-5.5%) [201–204] | 139 (-2.3%) | 8.5 |
| ps | 32 | base | 3 | 8440 | 3.79 | 7.49 | 10.62 | 117 [112–120] | 132 [130–134] | 110 | 11.0 |
| ps | 32 | p1w4 | 3 | 7689 (-8.9%) | 4.18 (+10.1%) | 8.57 (+14.4%) | 12.32 (+15.9%) | 120 (+2.8%) [119–122] | 143 (+8.1%) [141–144] | 119 (+7.6%) | 12.0 |
| ps | 32 | p1 | 3 | 9120 (+8.1%) | 3.51 (-7.5%) | 7.00 (-6.5%) | 10.04 (-5.5%) | 110 (-6.0%) [110–110] | 115 (-12.9%) [113–117] | 103 (-6.3%) | 12.1 |
| ps | 32 | p1gogc400 | 3 | 8607 (+2.0%) | 3.73 (-1.6%) | 7.46 (-0.4%) | 11.21 (+5.6%) | 109 (-6.8%) [106–112] | 114 (-13.9%) [108–117] | 108 (-1.8%) | 12.4 |
| text | 8 | base | 3 | 4768 | 1.68 | 3.25 | 4.96 | 222 [218–226] | 218 [217–220] | 143 | 8.5 |
| text | 8 | p1w4 | 3 | 4631 (-2.9%) | 1.73 (+3.2%) | 3.41 (+4.9%) | 5.40 (+8.8%) | 221 (-0.4%) [219–224] | 228 (+4.4%) [223–231] | 148 (+3.3%) | 9.6 |
| text | 8 | p1 | 3 | 5057 (+6.1%) | 1.58 (-6.0%) | 2.97 (-8.8%) | 4.62 (-6.8%) | 216 (-3.0%) [211–221] | 214 (-1.8%) [211–218] | 143 (-0.2%) | 9.2 |
| text | 8 | p1gogc400 | 3 | 4864 (+2.0%) | 1.65 (-2.0%) | 3.15 (-3.2%) | 4.77 (-3.9%) | 210 (-5.4%) [210–211] | 213 (-2.6%) [208–218] | 145 (+1.2%) | 9.6 |
| text | 32 | base | 3 | 6995 | 4.61 | 9.47 | 14.44 | 140 [137–144] | 132 [130–136] | 109 | 13.1 |
| text | 32 | p1w4 | 3 | 7284 (+4.1%) | 4.41 (-4.3%) | 9.05 (-4.4%) | 13.56 (-6.1%) | 140 (+0.0%) [138–144] | 137 (+3.8%) [135–141] | 112 (+2.7%) | 13.3 |
| text | 32 | p1 | 3 | 7936 (+13.5%) | 4.05 (-12.1%) | 8.23 (-13.1%) | 12.04 (-16.7%) | 138 (-1.4%) [136–140] | 122 (-7.6%) [122–123] | 109 (+0.0%) | 12.7 |
| text | 32 | p1gogc400 | 3 | 8413 (+20.3%) | 3.81 (-17.3%) | 7.36 (-22.3%) | 10.40 (-28.0%) | 127 (-9.7%) [118–135] | 112 (-15.6%) [104–118] | 105 (-3.7%) | 12.9 |

### A/B 4: round-1 patches on top (F10, F11, F15, F19, F25), load 9–16

`p1r1` = this patch + round-1 patches (applied with `patch -p1` to a copy of the tree).

| mode | thr | config | n | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ps | 8 | p1 | 3 | 4385 | 1.88 | 4.13 | 6.91 | 184 [177–193] | 214 [206–225] | 145 | 9.5 |
| ps | 8 | p1r1 | 3 | 4198 (-4.3%) | 1.92 (+2.1%) | 4.38 (+6.1%) | 8.68 (+25.6%) | 186 (+1.1%) [180–189] | 220 (+2.6%) [213–227] | 147 (+1.4%) | 10.1 |
| ps | 32 | p1 | 3 | 8489 | 3.80 | 7.78 | 12.24 | 111 [108–114] | 115 [113–117] | 106 | 11.0 |
| ps | 32 | p1r1 | 3 | 7005 (-17.5%) | 4.62 (+21.6%) | 10.17 (+30.7%) | 16.83 (+37.6%) | 108 (-2.7%) [102–114] | 113 (-2.0%) [107–118] | 105 (-0.9%) | 15.9 |
| text | 8 | p1 | 3 | 4242 | 1.90 | 4.03 | 6.93 | 222 [217–230] | 222 [216–230] | 149 | 10.0 |
| text | 8 | p1r1 | 3 | 3841 (-9.4%) | 2.12 (+11.8%) | 4.75 (+18.1%) | 8.45 (+21.9%) | 219 (-1.4%) [212–226] | 225 (+1.4%) [214–232] | 147 (-0.9%) | 11.1 |

### A/B 5: GOMAXPROCS (1, 8, 32 client threads; 2 rounds, 15 s)

`p1-procsN` = this patch with GOMAXPROCS=N for vtgate and both vttablets (mysqld unaffected).

| mode | thr | config | n | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ps | 1 | base | 2 | 1361 | 0.73 | 0.93 | 1.25 | 528 [524–533] | 488 [483–493] | 180 | 4.4 |
| ps | 1 | p1 | 2 | 1247 (-8.4%) | 0.80 (+9.5%) | 1.10 (+18.3%) | 2.06 (+64.9%) | 498 (-5.7%) [483–514] | 515 (+5.5%) [488–542] | 183 (+1.7%) | 6.3 |
| ps | 1 | p1-procs2 | 2 | 1313 (-3.5%) | 0.76 (+4.1%) | 1.08 (+15.9%) | 2.11 (+68.4%) | 446 (-15.7%) [428–463] | 445 (-8.8%) [437–453] | 180 (+0.0%) | 5.3 |
| ps | 1 | p1-procs1 | 2 | 1638 (+20.3%) | 0.62 (-15.6%) | 1.00 (+8.2%) | 1.78 (+42.2%) | 326 (-38.4%) [307–344] | 332 (-32.1%) [311–352] | 160 (-11.4%) | 8.4 |
| ps | 8 | base | 2 | 4700 | 1.75 | 3.58 | 5.94 | 199 [188–210] | 226 [210–243] | 149 | 6.7 |
| ps | 8 | p1 | 2 | 4869 (+3.6%) | 1.65 (-5.4%) | 3.42 (-4.6%) | 5.79 (-2.5%) | 178 (-10.3%) [178–179] | 213 (-6.0%) [208–218] | 141 (-5.4%) | 7.6 |
| ps | 8 | p1-procs2 | 2 | 5219 (+11.0%) | 1.56 (-10.3%) | 3.19 (-10.9%) | 6.01 (+1.2%) | 151 (-24.1%) [143–159] | 182 (-19.9%) [173–190] | 136 (-8.7%) | 6.3 |
| ps | 8 | p1-procs1 | 2 | 3995 (-15.0%) | 2.00 (+14.9%) | 4.67 (+30.3%) | 10.19 (+71.5%) | 133 (-33.2%) [132–134] | 156 (-31.1%) [156–156] | 134 (-10.1%) | 9.8 |
| ps | 32 | base | 2 | 7436 | 4.38 | 9.48 | 15.90 | 114 [109–120] | 128 [118–137] | 108 | 9.4 |
| ps | 32 | p1 | 2 | 8453 (+13.7%) | 3.81 (-13.1%) | 7.90 (-16.6%) | 11.93 (-24.9%) | 106 (-7.0%) [103–110] | 112 (-11.8%) [111–114] | 102 (-5.6%) | 9.6 |
| ps | 32 | p1-procs2 | 2 | 7840 (+5.4%) | 4.07 (-7.1%) | 8.50 (-10.2%) | 13.95 (-12.2%) | 96 (-16.2%) [94–98] | 110 (-14.1%) [108–111] | 106 (-1.9%) | 9.4 |
| ps | 32 | p1-procs1 | 2 | 6779 (-8.8%) | 4.75 (+8.3%) | 10.23 (+8.0%) | 17.87 (+12.4%) | 85 (-25.8%) [83–87] | 98 (-22.7%) [96–101] | 104 (-3.7%) | 11.8 |

### Scan: GOMAXPROCS 1..4 with the P1 binary and 64 workers (1 round)

| mode | thr | config | n | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ps | 1 | p1w64-procs1 | 1 | 1710 | 0.58 | 0.94 | 1.55 | 323 [323–323] | 317 [317–317] | 151 | 2.3 |
| ps | 1 | p1w64-procs2 | 1 | 1417 (-17.1%) | 0.70 (+20.7%) | 0.95 (+1.8%) | 1.27 (-18.0%) | 436 (+35.0%) [436–436] | 424 (+33.8%) [424–424] | 175 (+15.9%) | 3.2 |
| ps | 1 | p1w64-procs3 | 1 | 1352 (-20.9%) | 0.74 (+27.6%) | 0.95 (+1.8%) | 1.27 (-18.0%) | 511 (+58.2%) [511–511] | 503 (+58.7%) [503–503] | 182 (+20.5%) | 4.3 |
| ps | 1 | p1w64-procs4 | 1 | 1265 (-26.0%) | 0.79 (+36.2%) | 1.14 (+21.9%) | 1.70 (+9.4%) | 517 (+60.1%) [517–517] | 509 (+60.6%) [509–509] | 188 (+24.5%) | 5.2 |
| ps | 8 | p1w64-procs1 | 1 | 5114 | 1.56 | 3.19 | 4.91 | 123 [123–123] | 146 [146–146] | 130 | 2.9 |
| ps | 8 | p1w64-procs2 | 1 | 4695 (-8.2%) | 1.70 (+9.0%) | 3.62 (+13.4%) | 6.43 (+31.0%) | 161 (+30.9%) [161–161] | 189 (+29.5%) [189–189] | 142 (+9.2%) | 4.2 |
| ps | 8 | p1w64-procs3 | 1 | 5608 (+9.6%) | 1.42 (-9.0%) | 2.71 (-15.0%) | 4.18 (-14.9%) | 171 (+39.0%) [171–171] | 196 (+34.2%) [196–196] | 138 (+6.2%) | 5.0 |
| ps | 8 | p1w64-procs4 | 1 | 5501 (+7.6%) | 1.45 (-7.1%) | 2.76 (-13.4%) | 4.41 (-10.2%) | 183 (+48.8%) [183–183] | 207 (+41.8%) [207–207] | 140 (+7.7%) | 6.0 |
| ps | 32 | p1w64-procs1 | 1 | 7412 | 4.31 | 8.28 | 11.24 | 87 [87–87] | 99 [99–99] | 109 | 4.9 |
| ps | 32 | p1w64-procs2 | 1 | 8387 (+13.1%) | 3.81 (-11.6%) | 7.57 (-8.6%) | 11.24 (+0.0%) | 98 (+12.6%) [98–98] | 112 (+13.1%) [112–112] | 107 (-1.8%) | 6.4 |
| ps | 32 | p1w64-procs3 | 1 | 8513 (+14.9%) | 3.75 (-13.0%) | 7.70 (-6.9%) | 11.04 (-1.8%) | 105 (+20.7%) [105–105] | 111 (+12.1%) [111–111] | 101 (-7.3%) | 7.9 |
| ps | 32 | p1w64-procs4 | 1 | 9093 (+22.7%) | 3.52 (-18.3%) | 6.91 (-16.5%) | 9.91 (-11.8%) | 117 (+34.5%) [117–117] | 119 (+20.2%) [119–119] | 109 (+0.0%) | 9.7 |

### A/B 6: flags: consolidator off, gRPC window sizes

| mode | thr | config | n | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ps | 8 | base | 2 | 4064 | 2.08 | 5.02 | 9.49 | 190 [182–198] | 218 [210–227] | 142 | 8.9 |
| ps | 8 | noconsol | 2 | 4126 (+1.5%) | 1.99 (-4.6%) | 4.58 (-8.8%) | 8.57 (-9.7%) | 190 (+0.3%) [184–197] | 218 (-0.2%) [211–225] | 144 (+1.4%) | 8.4 |
| ps | 8 | window | 2 | 5321 (+30.9%) | 1.51 (-27.6%) | 3.01 (-40.0%) | 4.95 (-47.8%) | 174 (-8.2%) [174–175] | 196 (-10.3%) [194–198] | 139 (-2.5%) | 7.6 |
| ps | 32 | base | 2 | 8239 | 3.89 | 7.73 | 11.24 | 112 [105–119] | 126 [118–133] | 106 | 11.5 |
| ps | 32 | noconsol | 2 | 7468 (-9.4%) | 4.28 (+10.0%) | 9.14 (+18.2%) | 14.09 (+25.4%) | 114 (+2.2%) [112–117] | 129 (+2.8%) [128–130] | 111 (+4.7%) | 10.2 |
| ps | 32 | window | 2 | 8469 (+2.8%) | 3.79 (-2.4%) | 7.53 (-2.6%) | 10.84 (-3.5%) | 114 (+1.3%) [113–114] | 127 (+1.2%) [127–127] | 108 (+1.9%) | 10.7 |

Repeat of the window-size test (A/B 7, 3 rounds, prepared and text, 8 threads):

| mode | thr | config | n | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ps | 8 | base | 3 | 5302 | 1.51 | 2.90 | 4.52 | 187 [184–190] | 211 [207–215] | 140 | 8.1 |
| ps | 8 | window | 3 | 4074 (-23.2%) | 1.98 (+31.4%) | 4.55 (+56.7%) | 7.30 (+61.5%) | 180 (-3.6%) [180–181] | 206 (-2.4%) [205–207] | 144 (+2.9%) | 8.7 |
| ps | 8 | p1 | 3 | 5636 (+6.3%) | 1.41 (-6.2%) | 2.68 (-7.7%) | 4.13 (-8.7%) | 174 (-6.8%) [172–177] | 203 (-3.8%) [201–204] | 137 (-2.1%) | 7.3 |
| ps | 8 | p1window | 3 | 4000 (-24.6%) | 2.01 (+33.2%) | 4.62 (+59.1%) | 7.66 (+69.5%) | 170 (-9.1%) [163–177] | 202 (-4.3%) [196–209] | 143 (+2.4%) | 10.0 |
| text | 8 | base | 3 | 4611 | 1.74 | 3.49 | 5.68 | 222 [220–223] | 220 [216–225] | 145 | 9.1 |
| text | 8 | window | 3 | 4013 (-13.0%) | 2.05 (+17.4%) | 4.46 (+28.0%) | 6.80 (+19.7%) | 216 (-2.7%) [214–217] | 211 (-4.2%) [208–213] | 146 (+0.9%) | 10.1 |
| text | 8 | p1 | 3 | 4121 (-10.6%) | 2.00 (+14.5%) | 4.44 (+27.3%) | 6.99 (+23.0%) | 216 (-2.7%) [212–219] | 217 (-1.5%) [211–221] | 145 (+0.2%) | 8.4 |
| text | 8 | p1window | 3 | 4171 (-9.5%) | 1.96 (+12.2%) | 4.34 (+24.4%) | 6.88 (+21.1%) | 207 (-6.5%) [200–212] | 206 (-6.4%) [199–212] | 145 (+0.2%) | 11.1 |

## Hypotheses tested

| # | hypothesis | outcome |
|---|---|---|
| H1 | Syscalls: per-query flushes, SetDeadline, extra round trips | No. 1 read + 1 write per socket per hop; no SetDeadline; mysqld sees 1 command per query. |
| H2 | Go scheduler wake-ups are a large hidden cost | **Yes, the largest.** findRunnable+futex 15–25% of CPU at 8 threads; 2.7x CPU/query at 1 thread vs 32. GOMAXPROCS confirms it. |
| H3 | Per-query goroutines/channels/timers | Found one channel hand-off per query in vtgate (status aggregator) and one extra timer per query in vttablet; removed. Tablet unary RPCs were running on fresh goroutines (worker pool exhausted); fixed. |
| H4 | Single HTTP/2 connection serializes | Mild lock contention only; more connections are worse. |
| H5 | smartconnpool contention | None. |
| H6 | GC share / GOGC | 3–5% + assists; GOGC=400 saves a few %. |
| H7 | Consolidator overhead | 1.3% of tablet CPU; `--enable-consolidator=false` shows no measurable change (A/B 6). |
| H10 | gRPC BDP pings add syscalls/wake-ups | Yes, small: −0.3 write and −0.3..−0.5 read syscalls/query per process, −2..−6% CPU/query with static windows. |
| H11 | Round-1 patches move point-select numbers | No: F10/F11/F15/F19/F25 together are within noise (−3..+3% CPU/query, A/B 4). The rows are 1 per query, so per-row savings don't show. |
| H12 | GOMAXPROCS / Go scheduler | Largest effect found (A/B 1, 5, scan). |
| H8 | Query logging/streamlog without subscribers | `StreamLogger.Send` is a mutex + empty loop + counter (<0.3%). `logStats.BindVariables = CopyBindVariables` is done for every query even without a subscriber (1.6% of vtgate alloc bytes); not changed. |
| H9 | Tracing when disabled | Noop tracer, but `sqlparser.Preview` is computed twice per query only to annotate the noop span (~1% vtgate). Not changed (follow-up). |

## Code changes in the patch (all S)

1. `go/vt/servenv/grpc_server.go`: `--grpc-server-num-stream-workers` (0 = max(GOMAXPROCS, 64)); default raised
   from GOMAXPROCS. Rationale: long-lived streams pin workers (verified: a StreamHealth handler holds a
   `serverWorker` goroutine in the tablet's goroutine dump). With N vtgates ≥ GOMAXPROCS, the worker pool is permanently
   empty and every unary RPC takes the `go f()` path. Cost: up to 64 parked goroutines (~0.5–2 MB of stacks).
   Test: `TestNumStreamWorkers`. Flag docs updated in go/flags/endtoend.
2. `go/vt/vtgate/status.go`: `UpdateQueryInfo` records inline under the aggregator's own mutex instead of sending on a
   global channel to a background goroutine (one goroutine wake-up per query less; the channel lock was also a global
   serialization point). `GatewayStatsChanFullCount` stays registered (always 0) so dashboards don't break.
3. `go/vt/vtgate/tabletgateway.go`: `getStatsAggregator` uses a struct key and an RWMutex read path instead of
   `fmt.Sprintf` + `TabletType.String()` + a global Mutex per query. Test: `TestTabletGatewayStatsAggregator`
   (fails on main: counts are recorded asynchronously there).
4. `go/vt/vtgate/scatter_conn.go`: read the dynamic `TransactionMode` (viper lookup, 1.5% of vtgate CPU) only inside a
   transaction or reserved connection, where `actionInfo` uses it.
5. `go/vt/vttablet/tabletserver/query_executor.go`: `checkPermissions` creates the buffering timeout context only in the
   QRBuffer branch; formats the remote address only when there are query rules (`rules.Rules.Empty()`); stats use
   `topoproto.TabletTypeString` and `vtrpcpb.Code_name` instead of protobuf-reflection `String()`.
6. `go/vt/topo/topoproto/tablet.go`: `TabletTypeString` (map lookup, same output as `String()`, incl. aliases).
   Used on the vtgate per-query stats path and in `QueryEngine.AddStats` too. Test: `TestTabletTypeString`.
   (The `QueryEngine.AddStats` call site, ~1.3% of tablet CPU in the after-profile, was added after the A/B runs;
   the measured binaries don't include it.)
7. `go/vt/callinfo/plugin_{mysql,grpc}.go`: keep the `net.Addr` and format it in `RemoteAddr()` on demand.
   Test: `TestGRPCCallInfoFromPeer`.

Risk: low. (1) changes the number of idle goroutines; the fallback path is unchanged. (2) changes the per-query cost
from a non-blocking channel send to a short per-aggregator mutex; under extreme QPS on one keyspace/shard/type the
mutex could contend (previously the global channel lock did). (4) relies on `actionInfo` ignoring txMode outside
transactions (it returns before using it). Release compatibility: new flag only, no protocol change.

## Recommendations, ranked by user-visible impact

1. **Size GOMAXPROCS to the CPU really available** to each vtgate/vttablet: set a container CPU limit (Go ≥1.25 then
   picks GOMAXPROCS from it) or set GOMAXPROCS explicitly when vtgate/vttablet share a host with mysqld and each other.
   Measured here: −15..−25% CPU/query at moderate load, lower latency at low load; too few Ps costs peak throughput.
   Document that benchmark CPU/query at low concurrency is mostly scheduler overhead.
2. **Merge the stream-worker change** (`go/vt/servenv/grpc_server.go`): −4..−6% tablet CPU/query at 8 threads,
   −10..−14% at 32 threads, +6..+14% QPS at 32 threads. Production tablets with many vtgates (each holds a StreamHealth
   stream) and VStreams should gain more than this one-vtgate test. Risk: low (more idle goroutines).
3. **GOGC=200..400 with GOMEMLIMIT** for vtgate and vttablet: a few % CPU/query.
4. **Static gRPC windows on LAN deployments** (flags above): −2..−6% CPU/query and ~0.5–0.8 fewer syscalls per query per
   process. Size the window for the highest-latency link that carries streaming results.
5. **Merge the micro fixes**: −3..−5% vtgate CPU/query at moderate load. Low risk, small code.

## Follow-ups

- Skip `sqlparser.Preview` and span annotation when the tracer is the noop tracer (~1% vtgate).
- Copy bind variables for `logStats` only when a query-log subscriber/file exists.
- Cache the viper dynamic values (e.g. `TransactionMode`, `OnlineEnabled`) with a change notification instead of a
  locked viper lookup per call.
- gRPC: per-call allocations dominate vtgate's allocation profile (newClientStream/newStream/operateHeaders ≈ 40% of
  bytes). A persistent bidirectional stream per tablet for Execute, or a lighter transport, would remove ~2 goroutine
  wake-ups and most of the 30–50 µs/query gRPC cost on each side. Large (L).
- Measure on a machine with dedicated cores per process: the GOMAXPROCS and worker effects should be re-quantified there.
