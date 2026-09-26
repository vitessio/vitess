# P6-runtime: many connections, connection churn, GC and tail latency, plan cache, idle CPU

Investigator P6, BASE 30000, base commit aa9ccf9. Patched binaries: `/home/vt/bin-P6` (vtgate and vttablet from this
patch; the other binaries are copies of `/home/vt/bin`). Patch: `findings/P6-runtime.patch`, uncommitted in worktree
`agent-a4d22412b39e034f0`. Raw result lines: `perf-findings/P6-raw/`. Harness: `perf-findings/harness/P6-*`.

## TL;DR

Ranked by what a user would notice:

1. **The generated SQL parser needs a 32 KiB goroutine stack; make it fit in 8 KiB** (code, S). `(*yyParserImpl).Parse`
   has a **22,928-byte stack frame** because every inlined `yySymType` getter (`yyDollar[1].expr()`, 2,400 call sites)
   gets its own spill slot. Every vtgate MySQL connection parses on its own goroutine, so every connection keeps a
   32 KiB stack, and the GC keeps shrinking and regrowing it (`copystack` + `newstack` = 4.7% + 4.0% of vtgate CPU at
   2000 connections, 7% under connection churn). Marking the getters `//go:noinline` (generator change) cuts the frame
   to 5,928 bytes; a parse on a new goroutine then uses 8 KiB instead of 32 KiB (unit test), parse throughput unchanged.
   End to end at 2000 connections: **vtgate stack memory −50..−64% (41–64 MB → 22–30 MB), RSS −19..−21%
   (221–243 MB → 179–196 MB)**, vtgate CPU/query −3..−5% (32 connections, low load) and ≈−4% at 2000 connections
   (noisy).
2. **Idle vttablet wakes up 250 times a second: every connection pool runs a 100 ms ticker** (code, S).
   smartconnpool's "expire worker" ticks every 100 ms even when nobody waits; a vttablet opens ~25 pools, which is 94%
   of its idle goroutine wake-ups. The patch parks the worker while the waitlist is empty. **Idle vttablet CPU
   −80% (3.7–4.8 → 0.7–0.8 ms/s per tablet; ≈8.5 ms/s on a quiet machine)**. Matters for fleets of mostly idle
   tablets and for low-QPS tablets, where P1 showed wake-ups dominate CPU/query.
3. **TLS on the vtgate MySQL port: RSA certificates make each new connection cost ~1.1–1.3 ms of vtgate CPU and
   ~4 ms of connect latency** (config/docs). With churn (connect, 1 query, disconnect) at 400 connections/s:
   vtgate CPU per connection+query 774 µs (no TLS) → 2095 (RSA-2048) → 1294 (ECDSA P-256) → 1148–1198 with client
   session resumption; connect p50 0.49 → 4.43 → 2.15 → 2.2 ms, p99 3.9 → 40.7 → 13.0 ms. At 800 connections/s on
   this box RSA saturated vtgate (latency 0.6–1.3 s). Recommend ECDSA certs and client TLS session resumption; the
   post-quantum hybrid key exchange (X25519MLKEM768) is half of the remaining handshake cost (trade-off, see below).
4. **Per-query process-wide mutexes in vtgate and vttablet** (code, S). Mutex profiles at 2000 connections show the
   query log `StreamLogger.Send` (every query, even with no subscriber), `srvtopo` `GetSrvKeyspace` (two mutexes per
   query: the watcher map and the keyspace entry), `HealthCheckImpl.GetHealthyTabletStats`, `Executor.VSchema`, and in
   vttablet `isCallerIDAppDebug` → `FileCredentialsServer` mutex + a `ConnParams` copy per query. The patch makes
   these reads lock-free or read-locked; the vtgate contention (399 + 124 + 79 + 12 s of wait per 15 s) disappears
   from the profile. **End-to-end latency/throughput effect: not measurable here** (the neighbour's load swung
   between 2 and 28 during the runs). These locks convoy when a holder is descheduled (CPU throttling, noisy
   neighbours), so the expected benefit is in the tail under CPU pressure.
5. **GC: GOGC=400 + GOMEMLIMIT (or GOMEMLIMIT alone) for larger results** (config, S). 1000-row results: vtgate
   −11%, tablets −10% CPU/query, GCs per 28 s run 125 → 27 (→ 4 with `GOGC=off GOMEMLIMIT=384MiB`), max STW pause
   5.8 → 2.4 → 1.0 ms. No CPU change for 100-row results. Tail latency looked better but was confounded by load.
6. **`QueryPlanCacheSize` / `QueryEnginePlanCacheSize` report only the admission window, ~4% of the real cache
   size** (code, S, observability). 13,086 cached plans showed as 0.3 MB; a unit test shows 192 for 5,000 entries.
   Operators cannot see when the 32 MB plan cache is full. Fixed in `theine.Store.UsedCapacity`.
7. **`--mysql-server-pool-conn-read-buffers` for churny clients** (flag, S): per connection+query allocation
   37.8 KB → 21.2 KB, GC cycles −43%, vtgate CPU −2%.

Plan cache (topic 4): a miss costs +133 µs vtgate CPU (+28%), +67 µs tablet CPU (+17%) and +0.55 ms p50 for a point
select; a cached plan is ~1.8 KB of heap, so the 32 MB default holds ~18k such plans; the doorkeeper admits a shape on
its second sighting. Text-protocol IN lists collapse to one plan; prepared statements get one plan per list length.

## Environment and method

- 4 vCPU shared VM. The neighbour (P3, cross-shard scatter queries on BASE 40000) ran most of the time: load average
  2–28, changing within minutes. **CPU µs/query from `/proc` is the reliable metric** (±3–10% here); latency and
  closed-loop throughput swung by 2–10x with the neighbour and are reported only where the effect is large.
- Cluster: P2's `cluster.sh` (restart, `GATE_ENV`/`TABLET_ENV`), 2 shards, 1 primary each, 4 × 100k rows.
- **Load generator** `perf-findings/harness/P6-loadgen` (Go, go-sql-driver): N client connections, fixed total rate
  with Poisson arrivals (open loop), latency measured from the scheduled start (`lat_*`, includes client-side
  queueing, no coordinated omission) and from the actual send (`svc_*`); `-churn K` reconnects after K queries and
  reports connect time and time-to-first-query; modes `point`, `shapes` (N distinct normalized queries), `inlist`,
  `range` (larger results); `-ps` for server-side prepared statements; `-tls skip-verify|resume` (resume adds a client
  session cache).
- `P6-run.sh`: one run + `/proc` CPU per process, Go memstats (GC count, max STW pause from `PauseNs`), goroutines, RSS
  and stacks sampled mid-run, vttablet pool waits, plan-cache hit ratios. `P6-ab.sh`: alternates configs per round
  (restart vtgate+tablets, purge binlogs, warm-up). `P6-agg.py`: mean (or `MEDIAN=1`) per config; CPU columns show
  [min–max]. `P6-mprof.sh`: CPU + mutex (fraction 10) + block (10 µs) profiles via `/debug/mutexprofilefraction` and
  `/debug/blockprofilerate` (servenv toggles). `P6-idle.sh`: idle CPU per process.
- "tab" = sum of both tablets ÷ queries = CPU of the tablet that served a query.

## 1. Many client connections (100 / 500 / 2000 at a fixed 2000 qps)

Baseline (base binaries, text point selects, load 5–8, `P6-raw/survey1.txt`):

| conns | p50 ms | p99 ms | vtgate µs/q | tab µs/q | vtgate RSS | vtgate heap | GCs/20 s |
|---|---|---|---|---|---|---|---|
| 8 | 1.44 | 3.9 | 415 | 358 | 99 MB | 30 MB | 36 |
| 100 | 1.45 | 3.5 | 415 | 358 | 105 MB | 42 MB | 33 |
| 500 | 1.46 | 3.5 | 428 | 357 | 138 MB | 53 MB | 25 |
| 2000 | 1.49 | 4.1 | 445 | 353 | 259 MB | 82 MB | 15 |

- Latency does not depend on the number of connections at a fixed rate; nothing serializes at 2000 qps. No
  vttablet pool waits (pool size 64). One goroutine per connection (2070 goroutines at 2000 connections).
- **Memory: ~80 KB RSS per connection.** Heap profile: `bufio.NewReaderSize` 16 KB per connection (55% of the
  in-use heap at 2000 connections); `StackInuse` 60 MB = **~30 KB of stack per connection**.
- **CPU/query +7% at 2000 connections.** Profile at 2000 vs 8 connections: `copystack` 4.7%, `newstack` 4.0%,
  `scanstack` 1.4%, `shrinkstack` 0.8% (≈0 at 8 connections). All `newstack` samples come from
  `sqlparser.(*yyParserImpl).Parse`: idle connections get their stacks shrunk by the GC and regrown on the next
  query.
- Root cause: `Parse` has `locals=0x5990` (22,928 bytes). `go build -gcflags=-S` shows ~21 KB of it are spill slots
  named `r0.*` — the results of the 169 inlined `yySymType` getters, one slot set per call site (the compiler does not
  share slots between differently named locals). The getters are generated by `github.com/vitessio/goyacc`
  (goyacc.go:1194).
- Fix: `//go:noinline` on the getters (in `sql.go` and, for regeneration, `P6-goyacc-noinline.patch` for the
  generator). Frame 22,928 → 5,928 bytes. `TestParseStackUsage` (new): stack in use per goroutine after one Parse on a
  new goroutine 32,768 → 8,224 bytes (fails on main). `BenchmarkParseStress`/`ParseTraces` within ±3% (no cost).

A/B 1 (2000 qps, 3 rounds, load 15–21, `ab1.txt`), GODEBUG alternative included:

| config | conns | vtgate µs/q | tab µs/q | vtgate RSS | vtgate stacks | gate heap |
|---|---|---|---|---|---|---|
| base | 8 | 283 [277–291] | 276 | 65 MB | 1.1 MB | 13 MB |
| base | 2000 | 342 [296–431] | 284 | 226 MB | 41.4 MB | 66 MB |
| parser fix | 2000 | 300 [281–320] | 274 | **179 MB (−21%)** | **29.1 MB (−30%)** | 75 MB |
| `GODEBUG=gcshrinkstackoff=1` | 2000 | 284 [269–308] | 261 | 192 MB | 63.5 MB | 73 MB |

A/B 5 and 7 (full patch, `ab5.txt`/`ab7.txt`, 3–4 rounds, medians): at 2000 connections vtgate RSS 222–243 →
179–196 MB and stacks 51–64 → 22–32 MB; vtgate CPU/query 409 → 394 (2000 qps) and 257 → 246 (4000 qps), 396 → 366
at 32 connections (ranges overlap; ±15% noise). A/B 2 (low load 3, 32 connections): vtgate 537 → 512 µs/q (−4.7%).
Latency in these runs tracked the neighbour's load (p99 4–55 ms for both binaries), no difference attributable to the
patch.

`gcshrinkstackoff=1` removes the regrowth but keeps every stack at 32 KiB; `adaptivestackstart=1` (A/B 3) did
nothing measurable. Fixing the frame is better than either.

## 2. Connection churn (connect, 1 query, disconnect)

What a connection costs in vtgate (no TLS, `--mysql-auth-server-impl none`): **~230 µs of vtgate CPU** on top of the
query (A/B 2: 767 vs 537 µs per query at churn 1 vs persistent connections), 0.5 ms connect p50 on loopback.
Breakdown (CPU profile, strace):
- ~14 syscalls per connection: accept4, epoll_ctl ×2, getsockname, 6 × setsockopt (Go sets TCP_NODELAY and
  keepalive, then `setTcpConnProperties` sets SO_KEEPALIVE again), 3 writes (greeting, auth switch, OK), 2–3 reads,
  close.
- **Stack growth of the new connection goroutine: `copystack` 7% of vtgate CPU** (8 → 16 → 32 KiB in the first
  Parse; the parser fix makes it 8 KiB).
- A new 16 KB `bufio.Reader` per connection: 43% of all bytes allocated by vtgate under churn.
- The client's default database runs a full `use `db`` through the executor (parse, plan cache, logstats):
  ~35 µs per connection (A/B 3, `churn1` vs `churn1nodb` pairs: −21..−69 µs, mean −36). Not changed; a direct
  `SetTarget` for the handshake database is a follow-up.
- With an empty password (auth `none`, dev setups) the server always sends an AuthSwitchRequest because the
  client's scramble is empty (`len(clientAuthResponse) == 0`): one extra round trip per connection. Real passwords
  (A/B 3/4 `static`) don't trigger it.

A/B 2 (3 rounds, load ~3, `ab2.txt`):

| config | churn | vtgate µs/q | tab µs/q | vtgate alloc/q | vtgate GCs | conn p50/p99 ms |
|---|---|---|---|---|---|---|
| base | 1 | 767 [760–774] | 429 | 37.8 KB | 119 | 0.50 / 1.69 |
| `--mysql-server-pool-conn-read-buffers` | 1 | 751 [745–755] | 428 | 21.2 KB | 68 | 0.49 / 1.70 |
| parser fix | 1 | 744 [735–753] | 442 | 37.8 KB | 119 | 0.50 / 1.75 |
| parser fix + pool flag | 1 | 740 [737–742] | 449 | 21.2 KB | 68 | 0.49 / 1.51 |
| base | 0 | 537 [535–541] | 457 | 15.4 KB | 45 | – |
| parser fix | 0 | 512 [504–522] | 469 | 15.4 KB | 46 | – |

(The tablet column moved +2..+3% with the patched binary although no tablet code on this path changed; treat as
binary-layout noise.)

**TLS** (A/B 4, churn 1 at 400 connections/s, 2 rounds, load 14–18, `ab4.txt`; certs made with `openssl req -x509
-newkey rsa:2048` / `-newkey ec -pkeyopt ec_paramgen_curve:prime256v1`):

| server cert / client | vtgate µs per conn+query | conn p50 | conn p99 | ttfq p50 |
|---|---|---|---|---|
| no TLS (static auth, real password) | 774 | 0.49 ms | 3.9 ms | 1.79 ms |
| RSA-2048, full handshake | 2095 | 4.43 ms | 40.7 ms | 6.70 ms |
| RSA-2048, session resumption | 1198 | 2.23 ms | 17.7 ms | 4.00 ms |
| ECDSA P-256, full handshake | 1294 | 2.15 ms | 13.0 ms | 3.66 ms |
| ECDSA P-256, session resumption | 1148 | 2.20 ms | 12.9 ms | 3.96 ms |

In a quiet round (A/B 3 round 1, load 3–4): no TLS ~800, RSA 1902, ECDSA 1201 µs. With 800 connections/s and the
neighbour busy, RSA could not keep up (connect p50 13.7 ms, query latency 0.6–1.3 s from queueing); ECDSA held.
Profile of ECDSA churn: TLS handshake 33% of vtgate CPU, of which `hybridKeyExchange` (X25519MLKEM768, Go's default
since 1.24) is 15% (ML-KEM encapsulation 5.3%, X25519 6.6%) and the ECDSA signature 4.8%. `GODEBUG=tlsmlkem=0`
would cut another ~100 µs per connection at the price of post-quantum protection — a policy decision, not a default
change.

## 3. GC and memory under larger results

A/B 6 (3 rounds, 16 connections, `range` = `SELECT id,k,c,pad ... BETWEEN` 100 rows at 600 qps / 1000 rows at 80 qps,
`ab6.txt`):

| config | rows | vtgate µs/q | tab µs/q | vtgate GCs | max STW pause | vtgate RSS | tablet RSS |
|---|---|---|---|---|---|---|---|
| base | 100 | 897 [777–977] | 931 | 186 | 9.3 ms | 65 MB | 93 MB |
| GOGC=400 GOMEMLIMIT=1GiB | 100 | 915 [843–959] | 947 | 41 | 5.5 ms | 92 MB | 127 MB |
| GOGC=off GOMEMLIMIT=384MiB | 100 | 877 [742–950] | 944 | 4 | 1.3 ms | 396 MB | 421 MB |
| base | 1000 | 2478 [2464–2490] | 2837 | 125 | 5.8 ms | 75 MB | 103 MB |
| GOGC=400 GOMEMLIMIT=1GiB | 1000 | **2203 (−11%)** [2053–2306] | **2543 (−10%)** | 27 | 2.4 ms | 115 MB | 151 MB |
| GOGC=off GOMEMLIMIT=384MiB | 1000 | **2176 (−12%)** [2061–2254] | **2513 (−11%)** | 4 | 1.0 ms | 400 MB | 424 MB |

- STW pauses of several ms (normally < 0.1 ms) are the oversubscribed machine: stopping the world waits for
  descheduled threads. Fewer GCs means fewer such stalls; p99 improved with both settings (e.g. 1000 rows median p99
  19.5 → 6.5 → 5.7 ms) but the base rounds ran at higher load, so I do not claim the latency number.
- `GOGC=off` + `GOMEMLIMIT` makes the process grow to the limit (400 MB) — only for dedicated containers. GOGC=400 +
  GOMEMLIMIT is the safer recommendation (P1 found −1..−10% for point selects; here −10..−12% for large results).
- Memory growth: no growth across rounds in any A/B (plan cache bounded by its cost limit; the query log only buffers per subscriber). No multi-hour soak test.

## 4. Plan cache with many distinct query shapes

`plancost.txt` (text protocol, 800 qps, 16 connections, 2 rounds): `shapes=1` vs `shapes=10,000,000` (every query a
new shape, i.e. all misses):

| | vtgate µs/q | tab µs/q | p50 | p99 | vtgate alloc/q | plan hit | tablet plan hit |
|---|---|---|---|---|---|---|---|
| 1 shape | 476 | 398 | 1.88 ms | 15.6 ms | 15.0 KB | 100% | 100% |
| all misses | 609 (+28%) | 465 (+17%) | 2.42 ms | 25.9 ms | 29.1 KB | 0.3% | 0.15% |

- 30,000 uniformly used shapes at 1500 qps for 40 s: 13,086 plans cached, vtgate heap after GC 11 → 31 MB ⇒
  **~1.8 KB per plan**, so 32 MB (default `--gate-query-cache-memory`) ≈ 18k point-select plans; hit ratio 22.5%
  (each shape needs two sightings to pass the doorkeeper). Tablets cached 6,509 plans (16 MB heap).
- **Metric bug:** `QueryPlanCacheSize` stayed at 0.3 MB (327,808) while 13k plans were cached: `Store.UsedCapacity`
  sums only the shards' admission windows, not the main SLRU policy. Fixed (`go/cache/theine/store.go`), test
  `TestUsedCapacityIncludesPolicy` (main: 192 for 5,000 entries).
- IN lists: text protocol normalizes `IN (1,2,..)` to one list bind variable (1 plan for lengths 1..200); with
  prepared statements each length is its own statement and plan (202 plans, 91.5% hit).
- F01 (sketch fix) not measured end to end: with uniform shapes the admission policy barely matters, and the round-1
  simulation already bounds its effect at ≤ 1.8 pp hit ratio.

## 5. Idle background CPU

Idle cluster, 60 s (quiet machine): vtgate 0.7 ms/s, **vttablet 8–9 ms/s each**, mysqld 4.3 ms/s each (InnoDB's own;
vttablet sends 0.3 queries/s). Execution trace of an idle vttablet (5 s): 1,320 goroutine wake-ups, of which
**25 goroutines × 10/s are `smartconnpool.(*ConnPool).runWorker` (the 100 ms expire worker, one per pool)**; the rest
(throttler 125/250 ms tickers, theine maintenance, stats) is ~14/s. The expire worker only has work while clients
wait (evict cancelled waiters, anti-starvation hand-off).

Patch: `runExpireWorker` sleeps on a wake channel that `waitForConn` signals (non-blocking) when a client joins the
waitlist, and ticks every 100 ms only while the waitlist is non-empty. Test `TestExpireWorkerRunsOnlyWithWaiters`
(fails on main). A/B (`idle1.txt`, 3 rounds, 60 s, load 17–22):

| | vtgate | vttablet 100 | vttablet 101 | mysqld |
|---|---|---|---|---|
| base | 0.5–0.7 ms/s | 3.7–4.8 ms/s | 3.7–4.7 ms/s | 3.2–3.5 ms/s |
| patch | 0.5–0.7 ms/s | **0.7–0.8 ms/s** | **0.7–0.8 ms/s** | 3.5–3.8 ms/s |

(On a quiet machine each wake-up is more expensive — idle vttablet was 8–9 ms/s — so the saving is larger there.)

## 6. Serialization: mutex and block profiles

2000 closed-loop connections (vtgate CPU-bound, load ~21), 15 s, mutex fraction 10 (`P6-mprof.sh`). vtgate mutex wait
by site (base):

| site | wait / 15 s | per query |
|---|---|---|
| `streamlog.(*StreamLogger).Send` (query log, runs for every query) | 399 s | global mutex, even with no subscriber |
| `TabletGateway.getStatsAggregator` | 144 s | fixed by P1's patch |
| `srvtopo.(*resilientWatcher).getValue` (GetSrvKeyspace) | 124 s | watcher-map mutex + per-keyspace mutex |
| `HealthCheckImpl.GetHealthyTabletStats` | 79 s | global healthcheck mutex |
| `Executor.VSchema` | 12 s | executor mutex |
| gRPC `http2Client.NewStream` / loopy writer | 17 + 16 s | transport controlBuffer |

vttablet: `FileCredentialsServer.GetUserAndPassword` 87 s (via `connpool.isCallerIDAppDebug` → `MysqlParams()` →
`withCredentials`, which also copies `ConnParams` per query), `consolidator.Create` (next biggest, not changed).

With the patch, the vtgate list is gRPC `NewStream` 108 s, loopy writer 12 s, `updateStats` 1.3 s (P1 fixes that);
the tablet list is `consolidator.Create` 62 s and pool internals. Closed-loop throughput/latency A/B (`ab8.txt`)
was dominated by the neighbour (base 7.6k–15k qps, patch 9.1k–13k qps across rounds) — no conclusion possible.

Changes:
- `go/streamlog/streamlog.go`: `Send` reads a copy-on-write subscriber slice (`atomic.Pointer`), rebuilt under the
  mutex by Subscribe/Unsubscribe. Test `TestSendDoesNotWaitForSubscriptionChanges` (fails on main).
  Semantics: a Send racing with Unsubscribe can still put one message into the abandoned (never closed) channel.
- `go/vt/srvtopo/watch.go`: entries map under an RWMutex (read path), and a `running atomic.Pointer` holding the
  value while the watch is running (set in `onValueLocked`, cleared in `onErrorLocked`), read by `getValue` without
  the entry mutex. Test `TestGetValueWhileRunningDoesNotTakeEntryLock` (fails on main).
- `go/vt/discovery/healthcheck.go`: `mu` becomes an RWMutex; `GetHealthyTabletStats`/`GetTabletStats` read-lock.
- `go/vt/vtgate/executor.go`: `mu` becomes an RWMutex; `VSchema()`/`VSchemaStats()` read-lock.
- `go/vt/vttablet/tabletserver/connpool/pool.go` + `dbconfigs.Connector.UserName()`: compare the caller with the
  configured app-debug user name first; only a match goes through the credentials server. Assumes the credentials
  server does not rename users (the file and Vault servers return the user name they are given).

## Hypotheses tested

| # | hypothesis | outcome |
|---|---|---|
| H1 | Many idle connections raise latency or serialize | No at 2000 qps; +7% vtgate CPU/query and ~80 KB RSS per connection, from stacks and read buffers → finding 1 |
| H2 | vttablet pool waits with many connections | None at ≤ 2000 qps, 73–92 waits in two 4000-qps runs (64-conn pool). Under 2000 closed-loop connections p99/p50 ≈ 2.8 (queueing spread over gRPC, pool and CPU; not broken down further). The waitlist hands connections to the front-most waiter (FIFO with aging) |
| H3 | Global mutexes on the query path | Yes, 5 in vtgate + 1 in vttablet (finding 4); end-to-end effect not measurable on this box |
| H4 | Handshake cost / auth plugin | ~230 µs per connection without TLS; auth none vs static: same CPU; empty-password AuthSwitch round trip; USE db 35 µs |
| H5 | TLS cost | Dominant: RSA +1.1–1.3 ms CPU, ECDSA +0.4–0.5 ms, resumption helps RSA most (finding 3) |
| H6 | `--mysql-server-pool-conn-read-buffers` | −44% alloc per new connection, −2% CPU; no effect on persistent connections (buffers are only returned at close) |
| H7 | GOGC/GOMEMLIMIT vs tail | CPU −10..−12% for 1000-row results, 4.6–30x fewer GCs, lower STW max; tail plausibly better, confounded |
| H8 | GODEBUG `adaptivestackstart=1` / `gcshrinkstackoff=1` | First: no effect. Second: removes regrowth but keeps 32 KiB stacks (63 MB at 2000 conns). Parser fix is better |
| H9 | Plan-cache misses with many shapes | +28% vtgate / +17% tablet CPU per miss; ~1.8 KB per plan; size metric under-reports ~25x (fixed) |
| H10 | Idle CPU at scale | vttablet dominated by per-pool 100 ms tickers (fixed, −80%) |
| H11 | Memory leak over longer runs | None seen: vtgate/vttablet RSS and heap after equal runs were stable across rounds (runs of 15–45 s, 10–30 min per A/B); no multi-hour soak done |

## Code changes in the patch

All S; all unit tests of the touched packages pass (`sqlparser`, `streamlog`, `srvtopo`, `discovery`,
`smartconnpool` (+`-race`), `cache/...` (+`-race`), `connpool`, `dbconfigs`, `vtgate`); `scripts/fmt` run.

1. `go/vt/sqlparser/sql.go`: `//go:noinline` on the 169 `yySymType` getters (+ `perf-findings/P6-goyacc-noinline.patch`
   for the generator, which must land first or `make parser` drops it). Test `go/vt/sqlparser/parse_stack_test.go`.
2. `go/pools/smartconnpool/{pool,waitlist}.go`: expire worker only runs while clients wait. Test in `pool_test.go`.
3. `go/streamlog/streamlog.go`, `go/vt/srvtopo/watch.go`, `go/vt/discovery/healthcheck.go`, `go/vt/vtgate/executor.go`,
   `go/vt/vttablet/tabletserver/connpool/pool.go`, `go/vt/dbconfigs/dbconfigs.go`: lock-free / read-locked per-query
   reads. Tests for streamlog and srvtopo.
4. `go/cache/theine/store.go`: `UsedCapacity` includes the main policy. Test in `store_test.go`.
5. Harness (`perf-findings/harness/P6-*`), raw data (`perf-findings/P6-raw/`), generator patch.

Release compatibility: no flags, protocol or metric names change; `QueryPlanCacheSize`/`QueryEnginePlanCacheSize`
values become larger (correct).

## Recommendations, ranked by user-visible impact

1. Merge the parser frame fix (and the goyacc change): −50% vtgate stack memory, −20% RSS at 2000 connections, the
   GC stack shrink/regrow churn gone, a few % vtgate CPU. Larger fleets with 10k+ connections per vtgate gain
   proportionally (~24 KB per connection).
2. Merge the smartconnpool expire-worker change: −80% idle vttablet CPU.
3. Document TLS guidance for the vtgate MySQL listener: ECDSA P-256 certificates; clients should enable TLS session
   resumption; mention `GODEBUG=tlsmlkem=0` as an explicit trade-off.
4. Merge the per-query lock removals (low risk, tail robustness under CPU pressure), together with P1's
   `getStatsAggregator` change; next: shard `consolidator.Create` in vttablet.
5. GOGC=400 + GOMEMLIMIT for vtgate/vttablet, especially with larger results (−10..−12% CPU).
6. Merge the plan-cache size metric fix; size `--gate-query-cache-memory` from `QueryPlanCacheMisses` and the
   corrected `QueryPlanCacheSize` (~1.8 KB per simple plan).
7. Enable `--mysql-server-pool-conn-read-buffers` for connection-churning clients (PHP, serverless).

## Follow-ups

- Release the 16 KB read buffer of idle connections (return it to the pool while waiting for the next command, or
  use a ~1–2 KB server-side buffer; large packets bypass bufio anyway): −16 KB per idle connection.
- Handle the handshake's default database with a direct `SetTarget` instead of executing `use db` (~35 µs/connection),
  and skip the AuthSwitchRequest for an empty scramble when the client already used the negotiated method.
- Drop the duplicate `SO_KEEPALIVE` setsockopt per accepted connection (Go's listener already enables keepalive).
- `consolidator.Create` global mutex in vttablet; gRPC `NewStream` controlBuffer contention (P1).
- Re-measure tail latency on a dedicated machine (or with CPU quotas to reproduce CFS throttling), where the lock and
  GC changes should show.
