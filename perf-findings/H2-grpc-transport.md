# H2-grpc-transport: the vtgate→vttablet Execute hop (unary gRPC vs streams vs plain TCP)

Investigator H2, BASE=40000, base commit aa9ccf9. Patched binaries: `/home/vt/bin-H2` (ExecuteStream + plain-TCP
prototype, both off unless flagged), `/home/vt/bin-H2pr` (PR #20215 ported to this base).
Patch: `findings/H2-grpc-transport.patch`. Port of #20215: `findings/H2-pr20215-port.patch` (measurement only).
Harness: `perf-findings/harness/H2/` in the patch (bench/ab/profile scripts, the `h2mb` microbenchmark source, table
aggregators). Raw results: `scratchpad/h2/ab/*.txt`, `scratchpad/h2/mb/*.txt`, profiles in `/home/vt/perf/H2/prof`.

## TL;DR

- **The per-call gRPC machinery, not Vitess logic, is the cost.** Profiles put gRPC at ≈73 µs/query in vtgate and
  ≈87 µs/query in vttablet for a point select at 8 threads (≈40% of each process), plus scheduler wake-ups caused by the
  hand-offs between the caller, gRPC's loopy writer and reader goroutines, and the server's handler goroutine.
- **Microbenchmark floors** (two processes on loopback, a realistic point-select Execute request (126 B) and a
  1-row response (180 B), GOMAXPROCS=4, median of 3 rounds; CPU µs per call client/server):

  | design | c=1 latency | c=1 CPU | c=8 CPU | c=64 CPU | c=64 QPS |
  |---|---|---|---|---|---|
  | unary gRPC (today) | 141 µs | 123 / 108 | 40 / 37 | 24 / 29 | 50k |
  | 1 long-lived bidi stream per caller | 107–118 µs | 80–97 / 78–92 | 21 / 20 | 9–10 / 8–10 | 115–134k |
  | pool of bidi streams, exclusive checkout | 109 µs | 85 / 80 | 22 / 20 | 11 / 10 | 133k |
  | shared bidi streams with request ids (1 or 4) | 112–115 µs | 90 / 82 | 21–23 / 16–19 | 10 / 6–8 | 144–150k |
  | plain TCP, 1 multiplexed conn + reader goroutine | 61 µs | 54 / 33 | 22 / 10 | 10 / 5 | 113k |
  | **plain TCP, connection pool, I/O in the caller** | **31 µs** | **20 / 19** | **17 / 16** | 10 / 10 | 165k |

  A long-lived stream halves gRPC's per-call cost under load but saves only ~30% at concurrency 1: the reader and
  loopy-writer hand-offs remain. Only a transport without per-call goroutine hand-offs (pooled connections, the caller
  writes and reads itself — the MySQL protocol's model) removes the idle-wake-up cost that dominates at low load.
- **Prototype 1 (recommended first step): `ExecuteStream`**, a pooled long-lived bidi gRPC stream for `Execute`
  (vtgate flag `--tablet-grpc-execute-stream-pool-size`, off by default, falls back to unary for older tablets).
  End to end (A/B 3, 3 rounds): point selects **vtgate −10..−23%, tablets −11..−31% CPU/query, QPS +12..+27%,
  avg latency −11..−21%, p99 −10..−17%**; oltp_read_write QPS +7..+24% (A/B 1 and 3), CPU −6..−13%; 1000-row reads −4%.
  Holds with GOMAXPROCS=2 (A/B 4: −14..−27% CPU, +16..+20% QPS).
- **Prototype 2 (measurement only): plain framed TCP with a connection pool** (`--tablet-raw-execute-port-offset`,
  no TLS/auth/cancel). End to end: point selects **vtgate −28..−55%, tablets −40..−63% CPU/query, QPS +37..+58%, avg
  latency −27..−37%**; oltp_read_write **QPS +27%, CPU −25/−28%**; 1000-row reads **QPS +25%, CPU −22..−26%**;
  mysqld −9..−23% too (fewer spinning threads next to it). Context switches per query at 1 thread 19 → 8.
- **PR #20215 (raw MySQL bytes for StreamExecute)** only touches the OLAP/streaming path, so it cannot change the
  default-workload numbers. Ported and measured with `--mysql-default-workload=OLAP`: **1000-row reads tablets
  −40..−49%, vtgate −18..−23%, QPS +25..+39%**; point selects tablets −8..−14%, vtgate ±0. It has no fallback for
  older tablets (N-1 tablets break OLAP queries while its flag defaults on) and uses a fresh stream per query.

## Environment and method

- 4 vCPU shared VM; H1 ran its cluster and benchmarks concurrently. Load average (1-min) during my runs 3–19,
  reported per row. All measurements ran under `flock -o /home/vt/perf/bench.lock` in sections < 3 min.
- **Incident**: at 08:44 I started my cluster inside `flock` (without `-o`); the daemons inherited the lock fd and
  held the lock until I tore the cluster down at ~08:48, blocking H1. Fixed immediately (cluster restarted outside the
  lock, all scripts use `flock -o`, restarts happen outside the lock). My niced compiles overlapped round 1 of A/B 1;
  its h2stream r1 rows ran at load 14–19 and are noisier (A/B 3 repeats everything without concurrent builds).
- Harness: `cluster-env.sh` (restart with other BIN/flags/env), `ab.sh` alternates configs per round (restart outside
  the lock, 5 s warm-up, then each workload 12 s), `bench.sh` computes QPS, avg/p95/p99 (sysbench histogram), CPU
  µs/query per process kind from `/proc/<pid>/stat` (vtgate; both tablets summed; both mysqlds summed ÷ total queries).
- Workloads: `ps` = oltp_point_select prepared, `text` = text protocol, 1/8/32 threads; `range1000` =
  `SELECT id,k,c,pad FROM sbtest1 WHERE id BETWEEN ? AND ?+999` (scatter, ~500 rows per shard); `rw` =
  oltp_read_write prepared, 8 threads. 2 shards, 4 tables × 100k rows.
- CPU µs/query is the robust metric (spread within a config typically ±2–4%); QPS and latency move ±10–20% with the
  neighbour's load.

## 1. Microbenchmarks (`perf-findings/harness/H2/mb/h2mb.go.txt`)

Two processes on loopback. Client: N callers in closed loop; each marshals a real `querypb.ExecuteRequest` for
`select c from sbtest1 where id = :vtg1` (with caller ids, target, options; 126 B marshaled) and unmarshals the response. Server:
unmarshals the request and marshals a real 1-row `ExecuteResponse` (full field metadata + 120-byte CHAR value; 180 B). Sizes are
at the low end of the 200–500 B range suggested; at these sizes per-call overhead dominates, not bytes.
Payloads travel as raw bytes in every design (same marshal cost everywhere). gRPC server with
`NumStreamWorkers(GOMAXPROCS)` like vttablet. CPU per call from getrusage (client) and `/proc/<pid>/stat` (server).
3 s per point after 0.5 s warm-up, 3 rounds, median. Designs:

- `unary`: one unary RPC per call (today). `-sw` = static 1 MiB/8 MiB windows (disables BDP pings).
- `bidi`: one long-lived bidi stream per caller goroutine (Send, Recv).
- `pool-sw`: pool of bidi streams; a caller checks one out exclusively per call (no ids, in order).
- `mux1-sw` / `mux4-sw`: 1 or 4 shared bidi streams, 8-byte request id, a reader goroutine per stream dispatches
  responses to per-call channels; the server answers in order.
- `tcpmux1`: one TCP connection, 12-byte framing (length + id), writer mutex, reader goroutine dispatches responses.
- `tcppool`: pool of TCP connections, exclusive checkout, the caller writes and reads inline; server: one goroutine
  per connection reads, processes and writes inline (the MySQL-protocol model).

Latency µs; CPU µs per call client / server; median of 3 rounds.

#### GOMAXPROCS=4 (default on this VM)

| design | c=1 lat | c=1 cli/srv CPU | c=8 lat | c=8 cli/srv CPU | c=64 lat | c=64 cli/srv CPU | c=64 QPS |
|---|---|---|---|---|---|---|---|
| unary | 141 | 123 / 108 | 286 | 40 / 37 | 1267 | 24 / 29 | 50k |
| unary-sw | 153 | 125 / 115 | 278 | 38 / 37 | 1261 | 23 / 28 | 51k |
| bidi | 118 | 97 / 92 | 159 | 21 / 20 | 553 | 9 / 8 | 115k |
| bidi-sw | 107 | 80 / 78 | 160 | 22 / 20 | 474 | 10 / 10 | 134k |
| pool-sw | 109 | 85 / 80 | 155 | 22 / 20 | 478 | 11 / 10 | 133k |
| mux1-sw | 115 | 89 / 82 | 168 | 21 / 16 | 442 | 10 / 6 | 144k |
| mux4-sw | 112 | 90 / 81 | 154 | 23 / 19 | 425 | 10 / 8 | 150k |
| tcpmux1 | 61 | 54 / 33 | 115 | 22 / 10 | 562 | 10 / 5 | 113k |
| tcppool | 31 | 20 / 19 | 93 | 17 / 16 | 386 | 10 / 10 | 165k |

#### GOMAXPROCS=2

| design | c=1 lat | c=1 cli/srv CPU | c=8 lat | c=8 cli/srv CPU | c=64 lat | c=64 cli/srv CPU | c=64 QPS |
|---|---|---|---|---|---|---|---|
| unary | 113 | 85 / 79 | 276 | 30 / 31 | 1203 | 19 / 21 | 53k |
| unary-sw | 143 | 109 / 102 | 242 | 30 / 31 | 1054 | 19 / 21 | 60k |
| bidi | 68 | 49 / 47 | 150 | 16 / 15 | 458 | 8 / 7 | 139k |
| bidi-sw | 109 | 83 / 79 | 146 | 18 / 17 | 430 | 8 / 7 | 148k |
| pool-sw | 107 | 80 / 76 | 148 | 19 / 17 | 440 | 8 / 7 | 145k |
| mux1-sw | 118 | 91 / 84 | 156 | 18 / 15 | 391 | 8 / 5 | 163k |
| mux4-sw | 114 | 89 / 81 | 165 | 20 / 17 | 407 | 8 / 6 | 157k |
| tcpmux1 | 67 | 58 / 35 | 112 | 18 / 10 | 688 | 15 / 9 | 93k |
| tcppool | 42 | 30 / 28 | 71 | 11 / 11 | 505 | 10 / 9 | 126k |

#### GOMAXPROCS=1

| design | c=1 lat | c=1 cli/srv CPU | c=8 lat | c=8 cli/srv CPU | c=64 lat | c=64 cli/srv CPU | c=64 QPS |
|---|---|---|---|---|---|---|---|
| unary | 70 | 48 / 44 | 206 | 17 / 20 | 936 | 12 / 13 | 68k |
| unary-sw | 70 | 48 / 44 | 214 | 18 / 20 | 1006 | 14 / 14 | 64k |
| bidi | 58 | 39 / 38 | 109 | 10 / 9 | 338 | 5 / 4 | 189k |
| bidi-sw | 44 | 26 / 26 | 102 | 10 / 9 | 372 | 5 / 4 | 171k |
| pool-sw | 58 | 39 / 38 | 91 | 9 / 8 | 317 | 5 / 4 | 201k |
| mux1-sw | 57 | 38 / 36 | 103 | 10 / 8 | 286 | 4 / 3 | 223k |
| mux4-sw | 58 | 40 / 38 | 109 | 10 / 8 | 276 | 4 / 4 | 231k |
| tcpmux1 | 43 | 31 / 28 | 69 | 8 / 7 | 497 | 7 / 7 | 128k |
| tcppool | 41 | 28 / 27 | 65 | 8 / 8 | 621 | 9 / 8 | 103k |

Findings:

1. **Unary → long-lived stream** saves 45–60% of the transport CPU at c ≥ 8 (stream setup, HEADERS/HPACK, per-RPC
   allocations, a new server goroutine per call) and 2.6× the peak QPS. At c=1 it saves only 20–35%: the loopy-writer
   and reader hand-offs (≈4 goroutine wake-ups per call across both sides) remain and each can wake an idle P/thread.
2. **Stream pool (checkout) ≈ stream per caller.** Checkout costs nothing measurable. **Shared streams with request
   ids are not better** for the caller (an extra dispatcher hand-off) and only slightly cheaper for the server at c=64
   (batched frames); with a real server they add head-of-line blocking or need a concurrent server. Not worth it.
3. **Pooled plain TCP with I/O in the caller** is the floor: 5× less CPU than unary gRPC and 4× less latency at c=1;
   at c=8 25–30% below streams. At c=64 on GOMAXPROCS 1–2 it loses throughput (one syscall pair per call, no
   batching), where gRPC's shared writer batches frames; with GOMAXPROCS=4 it is best everywhere.
4. A multiplexed single TCP connection (`tcpmux1`) keeps the reader hand-off and is in between.
5. Static flow-control windows (`-sw`): no consistent effect in isolation (within noise; P1 saw −2..−6% end to end).
6. The c=1 numbers are bimodal on this VM (e.g. `bidi` c=1 GOMAXPROCS=4 ranged 49–97 µs across rounds): the cost
   of waking an idle vCPU depends on what else runs. Pooled TCP is the only design that is always cheap at c=1.

## 2. The upstream PRs

Both are closed drafts by arthurschreiber, auto-closed by the stale bot. There was no human review; only Copilot
review comments on #20215. Both are based on 81e1a40, 371 commits behind this base.

### #19620 "Add StreamExecuteRaw gRPC for zero-parse MySQL streaming" (opened 2026-03-11, closed 2026-07-08)

- 25 files, ~3.6k lines of non-generated code.
- Proto: bidi `StreamExecuteRaw`, `BeginStreamExecuteRaw`, `ReserveStreamExecuteRaw`,
  `ReserveBeginStreamExecuteRaw`. The request carries the usual fields. The response carries `bytes raw` (whole MySQL
  packets), `done`, an in-band `RPCError`, and the transaction state.
- vttablet: `streamQueryResultPackets` reads packets with `ReadHeaderInto`/`ReadDataInto` straight from the MySQL
  connection and forwards them in ~256 KiB chunks, without building `sqltypes.Result` or proto rows.
- vtgate: `RawResultParser` (go/mysql) parses the packets once, in `scatter_conn.go`. All streaming dispatch used the
  raw path, with no flag.
- grpctabletconn: `rawstreampool.go`, a generic lock-free checkout pool of bidi streams (max idle 100, max lifetime,
  asynchronous creator goroutine).
- The author's arewefastyet comments: oltp-readonly-olap +~5% QPS with lower CPU and memory in vtgate and vttablet;
  tpcc-olap +30% QPS/TPS and −130 ms p95.

### #20215 "StreamExecuteRaw: bidirectional *Raw streaming RPCs (no stream pool)" (opened 2026-05-30, closed 2026-07-10)

- The first PR of a planned stack split out of #19620. 37 files, +4.6k lines of non-generated code.
- Same four RPCs, plus `insert_id`/`insert_id_changed` on the terminal message for `FetchLastInsertId`.
- Deliberately opens **one fresh bidi stream per query** (Send, CloseSend, drain until `done`), so that pooling can be
  added later without wire or server changes.
- vtgate flag `--experimental-raw-streaming`, **default on**. vttablet flag
  `--queryserver-config-raw-stream-buffer-size` (256 KiB). Rewrites the schema name to the keyspace name inside
  column-definition packets in place, for parity with StreamExecute.
- Copilot review threads left unresolved:
  - The raw path bypasses permission, query-rule and table ACL checks. The final head's `QueryExecutor.StreamRaw`
    does call `checkPermissions()` and the query throttler, but the thread stayed open.
  - Cancel inside a transaction called `terminate(insideTxn=false)`. The thread is outdated and `StreamRaw` now takes
    `insideTxn`.
  - Duplicate InsertID results (one on the wire and one synthetic).
  - Terminal-packet detection assumes CLIENT_DEPRECATE_EOF. The code now fails fast if that capability was not
    negotiated.

### What the PRs do not do, and why they stalled

- They change only the **streaming** (OLAP, `StreamExecute*`) path. `Execute` (the default OLTP workload, point
  selects, DML, all of oltp_read_write) is untouched, so the harness's default workloads cannot move. I verified this
  by code reading: `scatter_conn.ExecuteMultiShard` is unchanged.
- **No compatibility fallback.** With the flag at its default (on), vtgate calls `*Raw` on every tablet. An N-1
  tablet answers Unimplemented and the OLAP query fails. There is no capability negotiation and no retry over the old
  RPC. This must be fixed before the default can be on.
- #20215 has no stream pool, so each query still pays a full stream setup. The raw-bytes saving is real, but the
  per-call transport cost stays.
- Why they stalled, as far as the PRs show: no reviewer engaged. The size (3.6–4.6k hand-written lines plus
  regenerated protos, 16–18k lines of total diff) and a security-sensitive surface (ACL and rule checks re-implemented
  on a parallel execution path) make review expensive. The labels NeedsIssue, NeedsDescriptionUpdate,
  NeedsWebsiteDocsUpdate and NeedsBackportReason were never cleared, and the stale bot closed both PRs.

### Port of #20215 onto this base (`findings/H2-pr20215-port.patch`)

The source diff applied with 3 conflicts:
- `execStreamSQL` changed on main (isStateful/insideTxn split). I kept main's version and added the PR's
  `registerQueryDetail` and `execStreamRawSQL`.
- The conflicting test hunks and the changelog: I kept main's version, so those PR tests were not ported.
- Two API drifts: `txPool.GetAndLock` now takes a ctx, and `terminate` now takes a `fetchDone` channel.

I regenerated the protos with the PR's vtproto pool options. The PR's raw-parser, `streamQueryResultPackets` and
raw-stream client tests pass. Binaries: `/home/vt/bin-H2pr`.

### A/B 2: PR #20215 port, vtgate `--mysql-default-workload=OLAP`, 3 rounds, load 8–11

| mode | thr | config | n | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ps | 8 | base-olap | 3 | 5268 | 1.52 | 2.90 | 4.52 | 188 [183–195] | 217 [210–224] | 138 | 8.3 |
| ps | 8 | pr-olap | 3 | 5489 (+4.2%) | 1.46 (-4.0%) | 2.76 (-4.8%) | 4.25 (-5.9%) | 190 (+0.9%) [190–190] | 196 (-9.5%) [194–198] | 136 (-1.5%) | 8.2 |
| ps | 32 | base-olap | 3 | 8448 | 3.78 | 7.30 | 10.03 | 123 [121–124] | 135 [134–136] | 109 | 9.6 |
| ps | 32 | pr-olap | 3 | 8915 (+5.5%) | 3.59 (-5.2%) | 6.88 (-5.8%) | 9.51 (-5.2%) | 126 (+2.4%) [123–128] | 117 (-13.6%) [115–119] | 106 (-2.8%) | 10.2 |
| text | 8 | base-olap | 3 | 4956 | 1.61 | 2.97 | 4.45 | 229 [224–236] | 225 [219–233] | 140 | 10.0 |
| text | 8 | pr-olap | 3 | 5114 (+3.2%) | 1.56 (-2.9%) | 2.88 (-3.0%) | 4.25 (-4.3%) | 232 (+1.5%) [229–234] | 207 (-8.0%) [206–208] | 141 (+0.2%) | 10.2 |
| range1000 | 1 | base-olap | 3 | 388 | 2.57 | 3.96 | 5.47 | 2631 [2602–2689] | 2700 [2662–2773] | 869 | 10.0 |
| range1000 | 1 | pr-olap | 3 | 485 (+24.8%) | 2.06 (-19.9%) | 2.76 (-30.3%) | 3.87 (-29.4%) | 2152 (-18.2%) [2136–2170] | 1627 (-39.8%) [1616–1646] | 880 (+1.3%) | 9.2 |
| range1000 | 8 | base-olap | 3 | 718 | 11.13 | 18.28 | 22.83 | 1773 [1752–1812] | 1826 [1794–1875] | 832 | 10.0 |
| range1000 | 8 | pr-olap | 3 | 1000 (+39.1%) | 8.00 (-28.2%) | 13.46 (-26.4%) | 17.64 (-22.7%) | 1371 (-22.7%) [1337–1400] | 925 (-49.3%) [893–949] | 831 (-0.1%) | 10.2 |

- Raw bytes pay off for **large results**: 1000-row OLAP reads −40..−49% tablet CPU and +25..+39% QPS. Part of the
  base cost is P3's finding: the codec pool zeroes a 1 MiB buffer per ~33 KiB chunk, and the raw path sends via its
  own pooled 256 KiB buffer. OLAP range1000 with the PR (tablets 925 µs) is also 45% cheaper than the OLTP `Execute`
  path for the same query (tablets 1680–1695 µs, A/B 1 and 3). So raw passthrough would help `Execute` for big results
  as well.
- Point selects: tablets −8..−14% (no proto encoding of the result), vtgate unchanged (it parses packets instead of
  protos, and pays one stream setup per query as before).
- Not measured: the PR on the OLTP workloads. The code path is unchanged, so the effect is 0 by construction.

## 3. Prototype 1: `ExecuteStream`, a pooled bidi gRPC stream for Execute (in the patch)

### Design

- Proto (`proto/queryservice.proto`, `proto/query.proto`): new RPC
  `ExecuteStream(stream ExecuteStreamRequest) returns (stream ExecuteStreamResponse)`.
  - `ExecuteStreamRequest{ExecuteRequest request = 1; int64 timeout_ns = 2}`.
  - `ExecuteStreamResponse{QueryResult result = 1; vtrpc.RPCError error = 2}`.
  - Existing messages are reused, so the call is `Execute` with the same fields. The generated code is regenerated
    with protoc 21.3 and the pinned plugins, and only query and queryservice changed.
- vttablet (`grpcqueryservice/server.go`): the handler loops Recv → `TabletServer.Execute` → Send, one call at a time
  in order, inline in the stream's goroutine (no per-call goroutine).
  - The ctx is derived from the stream ctx, plus `context.WithTimeout(timeout_ns)` when set, the caller ids from the
    request, and the reserved-conn keepalive and activity-refresh markers, as in unary `Execute`.
  - Call errors are sent in-band. The stream stays usable.
- vtgate (`grpctabletconn/execute_stream.go`): each tablet connection gets a pool of idle streams: a buffered channel,
  capacity `--tablet-grpc-execute-stream-pool-size`.
  - A call checks a stream out (or opens one with a background-derived ctx, because the stream outlives the call),
    sends, receives, and returns the stream to the pool. A full pool closes the stream.
  - Off by default. Only `Execute` uses it. Begin*/Commit/Rollback/Reserve*/Stream* stay unary.
- Tests (`execute_stream_test.go`):
  - the full `tabletconntest` suite over the stream path (results, caller ids, every error code, panics);
  - streams are reused, and an error reply leaves the stream in the pool;
  - an error's text and code are identical to the unary path's;
  - the caller's deadline reaches the tablet;
  - cancel kills the tablet-side query, the stream is not reused, and the next call works;
  - a tablet without ExecuteStream gets unary calls, and every query runs exactly once.
- Flag docs updated for vtgate, vttablet, vtctld and vtbench.

### Semantics

- **Deadline**: a stream outlives its calls, so the per-call deadline cannot travel as the gRPC deadline. The client
  sends the remaining time (`timeout_ns`) and the tablet applies `context.WithTimeout`. This is relative, like
  grpc-timeout, so clock skew does not matter. An expired ctx fails before sending, with DEADLINE_EXCEEDED.
- **Cancellation**: `context.AfterFunc(ctx, stream.cancel)` guards Send/Recv. If the caller gives up, the stream is
  reset (RST_STREAM) and discarded. The tablet's stream ctx is then cancelled and the query killed, exactly as a
  cancelled unary call does. The cost is one stream setup, the same as today's per-call cost, and only on
  cancellation.
- **Errors**: the tablet sends `vterrors.ToVTRPC(err)`. The client rebuilds it through `vterrors.ToGRPC` and
  `tabletconn.ErrorFromGRPC`, so code and text (`vttablet: rpc error: code = … desc = …`) match unary byte for byte,
  including the MySQL errno/sqlstate that vtgate parses. Transport errors map as before.
  - An idle stream that died (tablet restart, connection loss) is detected on Send. That request did not leave the
    process and is retried once on a new stream.
  - A failure after a successful Send is returned as UNAVAILABLE or the gRPC status. The call may or may not have run,
    the same ambiguity as a unary call on a dying connection.
- **Panics**: a panic in the tablet's Execute ends that stream (HandlePanic at stream level). The client gets the
  error with the panic text (tested) and opens a new stream.

### Compatibility (N-1 / N+1)

- Adding an RPC is wire-compatible. N-1 and N+1 vtgates keep using unary `Execute` against an N tablet.
- An N vtgate with the flag on, against an N-1 tablet: the first Recv returns Unimplemented. Unimplemented is
  returned before the handler runs, so the query was not executed. The client retries over unary and marks the tablet
  connection unary-only for 1 minute, then probes again, because the tablet may have been upgraded behind the same
  address.
- Rollout: N ships the server side always registered, with the client flag default off. N+1 turns the flag on by
  default. The unary path stays for older tablets for at least one more release. No capability negotiation is needed
  because the fallback is free.
- Not yet covered:
  - Per-call gRPC interceptors (Prometheus per-RPC metrics, per-call tracing spans and metadata) now see one long
    stream. Tracing context would need to travel in-band, for example as a map in `ExecuteStreamRequest`.
  - vtadmin web proto bindings were not regenerated (they need npm).

### Flow control, head-of-line blocking, resources

- Each stream carries one call at a time. There is no HOL blocking between calls on a stream and no request ids.
- All streams to a tablet share one HTTP/2 connection, as unary calls do today, so connection-level flow control and
  the loopy writer are shared exactly as now. A multi-MB Execute result is interleaved in 16 KiB frames with other
  streams, as today.
- The number of streams equals peak concurrency per tablet. Idle streams hold one parked server goroutine each (~8–32
  KiB of stack). Long-lived streams also occupy the server's `NumStreamWorkers` (GOMAXPROCS) workers, as StreamHealth
  already does (P1). Other unary RPCs then take the `go f()` path, which P1's worker change addresses.
- **Graceful shutdown**: `GracefulStop` waits for open streams. I measured a vttablet SIGTERM with vtgate connected:
  12.0 s on **base** and 12.0 s with ExecuteStream. Both hit the 10 s `OnTermSync` timeout, because vtgate's
  StreamHealth stream already holds `GracefulStop`. ExecuteStream does not make shutdown worse today. A production
  version should still end idle ExecuteStream streams on shutdown:
  - one worker goroutine per stream plus an idle/busy state;
  - on shutdown the handler returns only for idle streams, with a status that tells the client "not executed, retry";
  - the client retries on that status.

### A/B 1: ExecuteStream (v1) vs base, 3 rounds, load 6–19 (my compile overlapped round 1)

| mode | thr | config | n | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ps | 1 | base | 3 | 1423 | 0.70 | 0.86 | 1.16 | 490 [485–493] | 467 [462–472] | 173 | 6.8 |
| ps | 1 | h2stream | 3 | 1474 (+3.6%) | 0.68 (-3.3%) | 0.88 (+3.2%) | 1.85 (+60.3%) | 423 (-13.5%) [378–460] | 408 (-12.8%) [371–428] | 169 (-2.3%) | 11.4 |
| ps | 8 | base | 3 | 5606 | 1.42 | 2.65 | 4.03 | 175 [171–178] | 210 [207–212] | 137 | 7.9 |
| ps | 8 | h2stream | 3 | 5705 (+1.8%) | 1.41 (-0.9%) | 2.89 (+9.2%) | 5.24 (+30.1%) | 152 (-13.0%) [147–155] | 175 (-16.5%) [170–179] | 134 (-2.2%) | 11.6 |
| ps | 32 | base | 3 | 8842 | 3.61 | 7.04 | 9.92 | 114 [113–115] | 132 [130–135] | 109 | 9.1 |
| ps | 32 | h2stream | 3 | 10813 (+22.3%) | 2.96 (-18.2%) | 5.89 (-16.4%) | 8.90 (-10.3%) | 84 (-25.8%) [70–93] | 89 (-32.8%) [75–96] | 94 (-14.3%) | 13.3 |
| text | 1 | base | 3 | 1230 | 0.82 | 1.02 | 1.67 | 568 [562–575] | 498 [492–501] | 182 | 8.2 |
| text | 1 | h2stream | 3 | 1373 (+11.6%) | 0.73 (-10.6%) | 0.92 (-9.2%) | 1.63 (-2.3%) | 504 (-11.2%) [483–519] | 430 (-13.7%) [427–435] | 177 (-2.4%) | 11.9 |
| text | 8 | base | 3 | 4452 | 1.82 | 3.67 | 6.23 | 210 [207–215] | 230 [220–241] | 144 | 9.1 |
| text | 8 | h2stream | 3 | 5392 (+21.1%) | 1.50 (-17.6%) | 2.96 (-19.3%) | 5.31 (-14.7%) | 181 (-13.8%) [176–186] | 182 (-20.8%) [178–186] | 134 (-6.9%) | 12.5 |
| text | 32 | base | 3 | 7271 | 4.45 | 9.18 | 13.86 | 125 [112–142] | 125 [115–134] | 100 | 10.4 |
| text | 32 | h2stream | 3 | 9570 (+31.6%) | 3.34 (-24.9%) | 6.81 (-25.8%) | 10.27 (-25.9%) | 109 (-12.5%) [95–121] | 96 (-23.7%) [87–101] | 96 (-4.0%) | 14.2 |
| range1000 | 8 | base | 3 | 691 | 11.99 | 23.04 | 32.51 | 1377 [1366–1396] | 1680 [1639–1717] | 776 | 12.0 |
| range1000 | 8 | h2stream | 3 | 774 (+12.0%) | 10.71 (-10.7%) | 20.04 (-13.0%) | 27.90 (-14.2%) | 1355 (-1.6%) [1322–1378] | 1591 (-5.3%) [1539–1629] | 801 (+3.3%) | 14.4 |
| rw | 8 | base | 3 | 3296 | 49.44 | 71.82 | 90.45 | 235 [235–236] | 296 [288–303] | 255 | 13.8 |
| rw | 8 | h2stream | 3 | 4088 (+24.0%) | 39.13 (-20.9%) | 53.10 (-26.1%) | 62.30 (-31.1%) | 222 (-5.5%) [219–226] | 262 (-11.6%) [258–266] | 250 (-1.7%) | 14.4 |

The higher h2stream p99 at 1 and 8 threads comes from round 1: p99 was 2.07/6.9 ms at load 14 during my compile,
and 1.10/3.8 ms in round 3 at the same load as base. A/B 3 repeats the measurement cleanly and shows p99 −10..−17%.

## 4. Prototype 2: plain framed TCP with a connection pool (in the patch, measurement only)

- `grpcqueryservice/rawexec.go`, flag `--queryserver-raw-execute-port-offset`: the tablet also listens on gRPC
  port + offset. One goroutine per connection reads `[len][ExecuteStreamRequest]`, calls the same `executeOnStream`,
  and writes `[len][ExecuteStreamResponse]`, with bufio and one flush per response.
- `grpctabletconn/rawexec.go`, flag `--tablet-raw-execute-port-offset`: a pool of TCP connections per tablet
  (idle cap 64, TCP_NODELAY). The caller writes and reads inline. Cancel sets the connection deadline and discards
  the connection. The deadline travels in-band.
- The prototype has no TLS, no auth, no tablet-side cancel when the client goes away, and no port in the tablet
  record (it uses an offset). These are listed below as the production work.

### A/B 3: base vs ExecuteStream (v2) vs plain TCP, 3 rounds, load 6–12, no concurrent builds

| mode | thr | config | n | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ps | 1 | base | 3 | 1369 | 0.73 | 0.90 | 1.17 | 512 [505–520] | 490 [484–498] | 180 | 6.5 |
| ps | 1 | h2stream | 3 | 1558 (+13.8%) | 0.64 (-11.9%) | 0.81 (-10.3%) | 1.05 (-10.3%) | 448 (-12.4%) [444–451] | 428 (-12.7%) [426–432] | 176 (-2.2%) | 7.3 |
| ps | 1 | h2raw | 3 | 2166 (+58.2%) | 0.46 (-37.0%) | 0.65 (-27.7%) | 0.90 (-22.9%) | 230 (-55.0%) [227–233] | 181 (-63.2%) [176–189] | 146 (-19.2%) | 7.8 |
| ps | 8 | base | 3 | 5443 | 1.47 | 2.76 | 4.19 | 181 [177–184] | 215 [210–218] | 140 | 7.0 |
| ps | 8 | h2stream | 3 | 6362 (+16.9%) | 1.26 (-14.3%) | 2.38 (-14.0%) | 3.75 (-10.5%) | 149 (-17.8%) [147–151] | 176 (-18.2%) [172–179] | 133 (-5.0%) | 8.0 |
| ps | 8 | h2raw | 3 | 7829 (+43.8%) | 1.02 (-30.5%) | 2.01 (-27.3%) | 3.25 (-22.3%) | 117 (-35.5%) [114–121] | 127 (-40.7%) [125–131] | 125 (-11.2%) | 9.0 |
| ps | 32 | base | 3 | 8736 | 3.66 | 7.04 | 9.62 | 114 [112–115] | 136 [133–138] | 112 | 9.0 |
| ps | 32 | h2stream | 3 | 11064 (+26.6%) | 2.89 (-21.0%) | 5.64 (-19.9%) | 7.94 (-17.4%) | 88 (-22.9%) [85–89] | 94 (-30.6%) [91–97] | 97 (-13.6%) | 9.7 |
| ps | 32 | h2raw | 3 | 11953 (+36.8%) | 2.67 (-27.0%) | 5.68 (-19.4%) | 8.65 (-10.1%) | 79 (-30.5%) [78–81] | 81 (-40.7%) [79–83] | 94 (-16.0%) | 11.4 |
| text | 1 | base | 3 | 1271 | 0.79 | 0.97 | 1.30 | 573 [567–576] | 499 [494–505] | 181 | 8.0 |
| text | 1 | h2stream | 3 | 1419 (+11.7%) | 0.70 (-10.6%) | 0.87 (-9.7%) | 1.13 (-13.0%) | 513 (-10.4%) [503–519] | 446 (-10.6%) [436–452] | 179 (-1.1%) | 8.4 |
| text | 1 | h2raw | 3 | 1945 (+53.1%) | 0.52 (-34.3%) | 0.71 (-26.8%) | 0.90 (-30.8%) | 285 (-50.2%) [284–286] | 184 (-63.2%) [182–185] | 152 (-16.4%) | 9.5 |
| text | 8 | base | 3 | 4991 | 1.60 | 2.97 | 4.52 | 219 [217–220] | 224 [224–224] | 144 | 8.2 |
| text | 8 | h2stream | 3 | 5724 (+14.7%) | 1.40 (-12.7%) | 2.58 (-12.9%) | 3.98 (-11.8%) | 186 (-15.2%) [181–189] | 188 (-16.2%) [182–191] | 139 (-3.2%) | 9.2 |
| text | 8 | h2raw | 3 | 7074 (+41.8%) | 1.13 (-29.4%) | 2.17 (-26.8%) | 3.41 (-24.6%) | 147 (-32.7%) [144–150] | 134 (-40.0%) [132–136] | 130 (-9.7%) | 9.8 |
| text | 32 | base | 3 | 7797 | 4.10 | 7.85 | 10.86 | 142 [141–144] | 144 [143–145] | 118 | 9.3 |
| text | 32 | h2stream | 3 | 9688 (+24.3%) | 3.30 (-19.7%) | 6.43 (-18.0%) | 9.06 (-16.6%) | 112 (-21.3%) [110–115] | 102 (-29.4%) [100–103] | 103 (-13.2%) | 11.2 |
| text | 32 | h2raw | 3 | 10733 (+37.7%) | 2.98 (-27.5%) | 6.32 (-19.5%) | 9.39 (-13.5%) | 103 (-27.6%) [102–104] | 85 (-40.7%) [85–86] | 99 (-16.1%) | 11.0 |
| range1000 | 8 | base | 3 | 814 | 9.83 | 16.91 | 21.90 | 1397 [1394–1401] | 1695 [1685–1702] | 817 | 10.2 |
| range1000 | 8 | h2stream | 3 | 841 (+3.4%) | 9.50 (-3.3%) | 16.62 (-1.7%) | 21.38 (-2.4%) | 1342 (-4.0%) [1330–1350] | 1624 (-4.2%) [1621–1627] | 815 (-0.2%) | 11.9 |
| range1000 | 8 | h2raw | 3 | 1015 (+24.8%) | 7.87 (-19.9%) | 14.38 (-15.0%) | 19.19 (-12.4%) | 1029 (-26.4%) [1021–1038] | 1322 (-22.0%) [1300–1333] | 803 (-1.7%) | 11.1 |
| rw | 8 | base | 3 | 3848 | 41.54 | 53.87 | 63.37 | 238 [235–241] | 296 [293–299] | 253 | 11.4 |
| rw | 8 | h2stream | 3 | 4126 (+7.2%) | 38.73 (-6.8%) | 50.71 (-5.9%) | 58.58 (-7.6%) | 219 (-7.8%) [216–223] | 269 (-9.0%) [265–273] | 251 (-0.9%) | 12.2 |
| rw | 8 | h2raw | 3 | 4902 (+27.4%) | 32.61 (-21.5%) | 42.62 (-20.9%) | 50.44 (-20.4%) | 178 (-25.4%) [176–179] | 212 (-28.2%) [211–214] | 240 (-5.4%) | 11.2 |

`rw` with ExecuteStream: +7% QPS here and +24% in A/B 1 (base rw QPS differed by 17% between the two A/Bs). CPU
−6..−13% in both. oltp_read_write still sends BeginExecute and Commit as unary RPCs, about 4 of ~24 RPCs per
transaction on 2 shards.

### A/B 4: the same with GOMAXPROCS=2 on vtgate and vttablets (P1's recommendation), 2 rounds, load 4–9

| mode | thr | config | n | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| ps | 1 | base-g2 | 2 | 1395 | 0.71 | 0.90 | 1.36 | 464 [460–467] | 450 [447–454] | 181 | 4.6 |
| ps | 1 | h2stream-g2 | 2 | 1623 (+16.4%) | 0.61 (-14.0%) | 0.77 (-14.2%) | 0.99 (-27.1%) | 399 (-13.9%) [393–405] | 390 (-13.5%) [384–395] | 174 (-3.9%) | 4.9 |
| ps | 1 | h2raw-g2 | 2 | 2258 (+61.9%) | 0.45 (-37.8%) | 0.62 (-30.3%) | 0.77 (-43.4%) | 223 (-51.9%) [222–224] | 170 (-62.2%) [169–172] | 140 (-22.7%) | 5.2 |
| ps | 8 | base-g2 | 2 | 5647 | 1.42 | 2.54 | 3.78 | 148 [148–149] | 197 [196–198] | 134 | 5.2 |
| ps | 8 | h2stream-g2 | 2 | 6683 (+18.4%) | 1.19 (-15.5%) | 2.18 (-14.2%) | 3.43 (-9.4%) | 122 (-17.5%) [121–124] | 150 (-23.6%) [149–152] | 128 (-4.5%) | 5.4 |
| ps | 8 | h2raw-g2 | 2 | 8205 (+45.3%) | 0.97 (-31.1%) | 1.86 (-27.0%) | 3.02 (-20.1%) | 98 (-34.0%) [98–98] | 110 (-44.4%) [109–110] | 118 (-12.0%) | 5.8 |
| ps | 32 | base-g2 | 2 | 8681 | 3.69 | 6.98 | 9.48 | 96 [96–97] | 128 [127–128] | 108 | 6.7 |
| ps | 32 | h2stream-g2 | 2 | 10357 (+19.3%) | 3.09 (-16.1%) | 5.77 (-17.3%) | 8.13 (-14.2%) | 80 (-17.1%) [79–81] | 94 (-26.7%) [93–94] | 101 (-6.5%) | 6.6 |
| ps | 32 | h2raw-g2 | 2 | 11229 (+29.4%) | 2.85 (-22.7%) | 5.93 (-15.0%) | 8.51 (-10.2%) | 76 (-21.2%) [74–78] | 82 (-36.1%) [80–83] | 97 (-10.2%) | 7.1 |
| text | 8 | base-g2 | 2 | 5186 | 1.54 | 2.76 | 4.10 | 178 [177–179] | 205 [203–207] | 137 | 6.8 |
| text | 8 | h2stream-g2 | 2 | 6228 (+20.1%) | 1.29 (-16.6%) | 2.31 (-16.5%) | 3.49 (-15.0%) | 148 (-17.1%) [147–148] | 158 (-23.2%) [157–158] | 129 (-5.8%) | 6.6 |
| text | 8 | h2raw-g2 | 2 | 7292 (+40.6%) | 1.09 (-28.9%) | 2.11 (-23.6%) | 3.25 (-20.8%) | 126 (-29.5%) [122–129] | 118 (-42.4%) [114–122] | 124 (-9.1%) | 7.8 |
| rw | 8 | base-g2 | 2 | 3945 | 40.50 | 50.56 | 58.40 | 200 [199–200] | 270 [269–270] | 246 | 7.2 |
| rw | 8 | h2stream-g2 | 2 | 4271 (+8.3%) | 37.41 (-7.6%) | 47.47 (-6.1%) | 54.84 (-6.1%) | 183 (-8.3%) [183–183] | 234 (-13.2%) [234–234] | 246 (-0.4%) | 6.7 |
| rw | 8 | h2raw-g2 | 2 | 4675 (+18.5%) | 34.23 (-15.5%) | 45.86 (-9.3%) | 54.36 (-6.9%) | 166 (-17.0%) [158–173] | 200 (-25.8%) [189–211] | 244 (-1.0%) | 8.6 |

The transport savings add to the GOMAXPROCS saving. Point select at 8 threads: base (GOMAXPROCS 4) 181/215 µs, then
GOMAXPROCS=2 148/197, then plus ExecuteStream 122/150, or plus plain TCP 98/110.

## 5. Where the time goes (profiles, ps 8 threads, 15 s, A/B-3 CPU/query × profile share)

| process | stage | base | ExecuteStream | plain TCP |
|---|---|---|---|---|
| vtgate | gRPC client call in caller (`grpc.invoke`, newClientStream…) / stream Send+Recv / inline write+read | 28 µs | 20 µs | 26 µs |
| vtgate | loopy writer goroutine | 24 µs | 20 µs | – |
| vtgate | reader goroutine | 21 µs | 13 µs | – |
| vtgate | **transport total** | **≈73 µs** | **≈53 µs** | **≈26 µs** |
| vtgate | scheduler (`findRunnable`) | 16 µs | 15 µs | 14 µs |
| vttablet | `TabletServer.Execute` (the actual work) | 81 µs | 73 µs | 69 µs |
| vttablet | server dispatch (handleStream − Execute / ExecuteStream − Execute / serveRawExecute − Execute) | 27 µs | 28 µs | 28 µs |
| vttablet | `HandleStreams` reader | 29 µs | 20 µs | – |
| vttablet | loopy writer | 31 µs | 19 µs | – |
| vttablet | **transport total** | **≈87 µs** | **≈67 µs** | **≈28 µs** |
| vttablet | scheduler (`findRunnable`) | 31 µs | 24 µs | 23 µs |

Context switches per query (all threads, 15 s window during the run):

| | vtgate 1 thr | tablets 1 thr | vtgate 8 thr | tablets 8 thr |
|---|---|---|---|---|
| base | 18.9 | 18.9 | 3.6 | 7.0 |
| ExecuteStream | 17.4 | 18.0 | 3.2 | 6.5 |
| plain TCP | 8.0 | 7.7 | 2.7 | 4.3 |

The stream removes per-call stream setup: `newClientStream`, HEADERS/HPACK, `operateHeaders`, and the server's
goroutine start plus stack growth. It does not remove the two goroutine hand-offs per direction. Those hand-offs are
the difference to plain TCP, and it is largest at low concurrency, where every hand-off wakes a sleeping thread.

## 6. Hypotheses tested

| # | hypothesis | outcome |
|---|---|---|
| T1 | Unary per-call overhead (stream setup, headers, new goroutine) is a large part of gRPC's cost | **Yes**: 45–60% of transport CPU at c≥8 in isolation; −12..−31% CPU/query per process end to end with ExecuteStream. |
| T2 | A long-lived stream removes the goroutine hand-offs | **No**: loopy writer + reader remain on both sides; at 1 thread ExecuteStream saves only 12%, context switches 19 → 17.4 per query. |
| T3 | A shared (multiplexed) stream pool with request ids beats exclusive checkout | **No**: an extra dispatcher hand-off on the client; slight server gain only at c=64; brings HOL risk. |
| T4 | A transport without per-call hand-offs (pooled TCP, I/O in caller) is much cheaper | **Yes**: 5× less CPU per call at c=1 in isolation; end to end −28..−63% CPU/query, +37..+58% QPS for point selects, +27% oltp_read_write QPS. |
| T5 | Static windows (no BDP pings) help | Not measurable in isolation (±10%, both directions); P1 found −2..−6% end to end. Not pursued. |
| T6 | Raw MySQL bytes (PR #20215) help point selects | Small: tablets −8..−14% in OLAP mode, vtgate 0. **Large for 1000-row results**: tablets −40..−49%, vtgate −18..−23%. |
| T7 | ExecuteStream helps large results | Barely: range1000 −4% (the cost is result encoding/decoding, not per-call overhead). Plain TCP −22..−26% (fewer copies than gRPC framing for ~100 KB responses). |
| T8 | Transport gains vanish with GOMAXPROCS=2 | **No**, they add up (A/B 4). |
| T9 | Idle long-lived streams make tablet shutdown slower | Not today: base already waits the full 10 s OnTermSync timeout because of vtgate's StreamHealth stream; ExecuteStream the same (12.0 s both). Needs a fix for both. |
| T10 | mysqld CPU inflation is partly caused by the Go processes' spinning | Supported: with plain TCP mysqld CPU/query drops 9–23% although mysqld does the same work. |

## 7. Recommendations (ranked by user-visible impact vs risk)

1. **Merge ExecuteStream (this patch, minus the plain-TCP prototype) behind `--tablet-grpc-execute-stream-pool-size`**
   (M). It saves −12..−31% CPU/query on each of vtgate and vttablet and adds +12..+27% QPS for point selects, with
   −10..−21% avg and p99 latency; oltp_read_write gains +7..+24% QPS. It is wire-compatible, falls back to unary for
   N-1 tablets (tested), and matches deadline, cancel and error semantics (tested).
   - Default off in N, on in N+1.
   - Before turning it on by default:
     - end idle streams on tablet shutdown (worker per stream);
     - propagate tracing in-band;
     - add per-call metrics (the gRPC interceptors now see streams);
     - extend the stream to BeginExecute/Commit/Rollback/Release/ReserveExecute with a `oneof`, which should add a few
       % for transactional workloads.
2. **Design a dedicated query transport for vtgate→vttablet** (L). This is the real prize: −28..−63% CPU/query,
   +37..+58% QPS and −27..−37% latency for point selects, +27% for oltp_read_write, −22..−26% CPU for 1000-row reads.
   Shape: pooled TLS connections per tablet, one call per connection, the caller writes and reads (no goroutine
   hand-offs), vtproto payloads, a hello frame for auth, the port advertised in the tablet record, and gRPC kept for
   all other RPCs and as the fallback. Work items and risks:
   - security (TLS, mTLS and the static-auth plugin re-done outside gRPC);
   - cancellation (needs an out-of-band cancel frame on another connection, like MySQL's KILL QUERY, because the
     tablet does not read the connection while it executes);
   - observability;
   - one more listening port;
   - many more TCP connections: N vtgates × concurrency per tablet, like MySQL client connections;
   - a second protocol to maintain.
3. **Raw MySQL row passthrough for large results** (#19620/#20215 idea, L). For OLAP it is already measured:
   1000-row reads −40..−49% tablet CPU and +25..+39% QPS. For `Execute` the same would need the consolidator, row
   limits, rows-affected, field caching and the keyspace rewrite handled on raw packets. Revive #20215 only after
   adding the Unimplemented fallback (it breaks N-1 tablets today) and stream pooling (reuse ExecuteStream's pool).
   Split it into reviewable pieces: parser, tablet reader, RPC.
4. Combine with P1's GOMAXPROCS/worker recommendations: the effects add up (A/B 4).

## 8. Negative results

- Multiplexed streams with request ids: no better than exclusive checkout (microbench), and they bring
  head-of-line blocking.
- A single multiplexed TCP connection: keeps the reader hand-off, about 2× the c=1 CPU of pooled TCP.
- Static flow-control windows: no effect in isolation.
- ExecuteStream for large results: −4% only.

## 9. Follow-ups

- Implement the ExecuteStream shutdown handling (worker per stream plus a "not executed, retry" status) and the
  `oneof` for transaction RPCs, then re-measure oltp_read_write.
- Measure on dedicated cores: the c=1 cost of waking an idle vCPU is VM-specific and inflates every hand-off here.
- Prototype the dedicated transport with TLS to see what the handshake-free pooled path costs with encryption
  (AES-GCM adds roughly 1 µs per KB).
- For a raw-row `Execute` path: prototype on `select … limit N` results ≥ 100 rows only (no consolidator), where
  A/B 2 predicts −40% tablet CPU.
