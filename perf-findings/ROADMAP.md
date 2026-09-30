# Performance roadmap: ranked by payoff vs effort

Each item is ranked on four things:
- **User-visible payoff:** latency, throughput, CPU, and operation wall time.
- **How many users hit it:** default paths rank above niche ones.
- **Effort and risk.**
- **Dependencies.**

Evidence for each item is in the report named in the right-hand column (all under `perf-findings/`). Effort: S is under ~1 day plus review, M is days to a couple of weeks, and L is a multi-week project.

## Upstream tracking (searched 2026-09-30; full table in `ISSUES-D.md`)

Most items have no issue or PR upstream. The ones that do:
- **Wave 2 #10 (raw MySQL rows over gRPC):** [PR #20215](https://github.com/vitessio/vitess/pull/20215) and [PR #19620](https://github.com/vitessio/vitess/pull/19620) were **closed unmerged by the stale bot**, not rejected. Open issue [#17172](https://github.com/vitessio/vitess/issues/17172) tracks the idea. Reopening them is the cheapest restart.
- **Wave 1 #7 (big/scatter reads):** open [PR #20952](https://github.com/vitessio/vitess/pull/20952) (pre-sorted merge, for [#20951](https://github.com/vitessio/vitess/issues/20951)). [PR #20670](https://github.com/vitessio/vitess/pull/20670) did F08's MergeSort batching and was closed as stale.
- **R10 / R14 (VStream running phase):** open [#18273](https://github.com/vitessio/vitess/issues/18273) (remove the deep row clone), and open [PR #21289](https://github.com/vitessio/vitess/pull/21289) for [#21287](https://github.com/vitessio/vitess/issues/21287) (format the GTID once). Nothing covers the keyrange pre-filter.
- **R16:** [PR #19535](https://github.com/vitessio/vitess/pull/19535) is open and under review.
- **R18 (shared binlog reader):** open [#7437](https://github.com/vitessio/vitess/issues/7437) since 2021, no PR.
- **Related:**
  - draft [PR #21257](https://github.com/vitessio/vitess/pull/21257) is a SIMD proof of concept overlapping F02, F03 and F20;
  - [#4302](https://github.com/vitessio/vitess/issues/4302), [#16244](https://github.com/vitessio/vitess/issues/16244) and [#3929](https://github.com/vitessio/vitess/issues/3929) cover GOMAXPROCS, GOGC and gRPC windows;
  - [#7802](https://github.com/vitessio/vitess/issues/7802) proposes zstd backups;
  - [#19735](https://github.com/vitessio/vitess/issues/19735) is the VDiff3 RFC;
  - [#8056](https://github.com/vitessio/vitess/issues/8056) discusses parallel multi-table copy;
  - [PR #20313](https://github.com/vitessio/vitess/pull/20313) (merged) sets stream workers to GOMAXPROCS, and P1 builds on it.

24 items have no tracking found: Wave 1–3 #3, #4, #6, #8, #14, #15, #17, #18, #19, the ops guidance, the Online DDL review tick, VDiff `--wait`, and R2–R4, R6, R7, R9, R12, R13, R17 and R20–R22. The search was rate-limited, so the lower-ranked items got only one or two queries.

## Fixed waits: long-running vs time-critical operations

Many findings removed fixed waits or polled faster. They are NOT all equal: VReplication workflows (MoveTables, Reshard, Online DDL, VDiff on large tables) and continuous loops (VTOrc, health checks, heartbeats) run for days or forever, and polling faster there burns resources for the whole duration and across the whole fleet. Cutover windows (SwitchTraffic, failover steps) last seconds, and every millisecond is user-visible downtime.

**How each fix works, and what it costs:**
1. **Skip a wait that isn't needed** (a condition check): costs nothing extra, even on multi-day operations.
2. **Poll faster, or back off from a short start:** costs as long as the wait lasts. Fine on a switchover lasting seconds; a real cost on a migration running for days, multiplied across the fleet.
3. **Wake on an event** instead of a timer: fast and cheap at the same time, but needs more code.

**Rule:**
- Long-running operations and continuous loops get only type 1 or type 3 fixes, or intervals that scale with elapsed time. Never faster fixed polling.
- Time-critical windows may poll aggressively, because the window itself is short. Bound it anyway: back off once a wait runs longer than a few seconds.
- Anything that runs continuously is judged by its cost per second times the fleet size, not by the latency gain in a single test.

**Throughput beats waits for long-running operations.** The dramatic wait numbers came from small test tables (MoveTables 47 s → 5.6 s, Online DDL 84 s → 40 s); on real multi-day migrations they're rounding errors. There, throughput matters: bulk UPDATE (R9), concurrent table copy (R19), deferred keys and JSON as text (R6), and the shared binlog reader (R18).

### Long-running operations

| Wait | Proposed fix | Type | Cost over days | Decision |
|---|---|---|---|---|
| Copy phase waits ≥1 s before each table (P4 #1) and in VStream copy (V5 #1) | Skip the catch-up when the last snapshot is younger than the lag tolerance | 1 | None | **Keep** (R2). Only matters with many tables. |
| Online DDL 1-minute review tick (P4 #3) | Re-review a running migration every 5 s if it isn't ready | 2 | Review queries every 5 s for the whole migration | **Rework:** keep the 1-minute tick; trigger an immediate review when the stream reports copy complete and caught up, and poll fast only after that point (type 3). |
| `VDiff create --wait` polled every minute (V3 #5) | Poll every 1 s, backing off to 10 s | 2 | A VDiff can run for hours; each poll fans out to every target shard | **Rework:** interval proportional to elapsed time (e.g. 10% of elapsed, capped at 1 minute). Short diffs return fast; long ones cost the same as today. |
| Tablet picker flat 30 s retry (P4 #4) | Back off 1 s → 30 s | 2 (bounded) | Same steady state as today; only the first retries are faster | **Keep.** Recovery after an outage, bounded by the 30 s cap. |
| VTOrc poll flags (P5 #2): `--instance-poll-time=1s`, faster recovery polling | Change defaults | 2 | Continuous 5x RPC and query load across the whole fleet | **Don't change defaults.** Fix only the off-by-one bug (free). Faster detection should come from push-based health signals (type 3). |
| Heartbeat / `time_updated` writes (V4b #6) | (the opposite problem) | – | An idle 2000-table workflow writes ~700 MB/h of binlog | **Make long-running workflows cheaper:** raise the default interval, or move frequently updated fields out of the row that holds the rules (R21). |

### Time-critical windows

| Wait | Proposed fix | Type | Cost | Decision |
|---|---|---|---|---|
| WaitForPos polls every 1 s (P4 #2, V4b #1) | 5 ms doubling to 100 ms; the stream saves its position immediately while someone waits | 2 | Only for the duration of the wait | **Keep for cutovers.** WaitForPos is also called from longer waits, so back off further (e.g. to 1 s) once a wait passes a few seconds. |
| SwitchTraffic: trailing 100 ms sleep, ReloadSchema per lock cycle, duplicate allowTargetWrites (V6 #6, V4) | Remove | 1 | None | **Keep** (R3). Pure critical-path time. |
| SwitchTraffic 100 ms pause between lock cycles (V4b) | Tablet-side deny-list barrier | 3 | None | **Keep** (R22): ~170 ms → ~60 ms. |
| Per-table VDiff setup while holding the keyspace lock (V3 #2) | Covered by the WaitForPos fix | 2 | Short | **Keep.** A keyspace-wide lock blocks other operations, so this path is time-critical. |
| PRS with semi-sync (P5 #1); backup/restore mysqld shutdown wait (P5 #3); mysqld start-up socket poll | Remove the wait; poll at 100 ms during start-up only | 1 / 2 (bounded) | None / start-up only | **Keep.** |

## Wave 1: small changes with large or broad payoff (do first)

| # | Item | Payoff (measured) | Effort | Notes / evidence |
|---|---|---|---|---|
| 1 | **Correctness bugs** | Prevents wrong data, panics, races and failover delays | S each | Details below the table |
| 2 | **Config guidance:** static gRPC windows, GOMAXPROCS = real cores, GOGC=400 + GOMEMLIMIT, 256 KiB stream buffers (docs and examples; defaults unchanged for now) | +10–16% QPS; −20–27% CPU on every workload; p99 −15–26%; 1-thread latency 0.80 → 0.54 ms | S | P1, H1, P7. Windows cap per-stream throughput at window/RTT, so size them for cross-cell links. |
| 3 | **Time-critical outage windows** (see "Fixed waits" below; only waits on a short critical path) | SwitchTraffic write outage 2.3 → 0.4 s (1.3 s → 173 ms with R3); PRS outage with semi-sync 1.16 → 0.21 s; every backup/restore −2–3 s (mysqld shutdown wait); stream resume after a restart 30 → 1.3 s (bounded backoff) | S each | P4 #2 and #4, P5 #1 and #3, V4b #1 |
| 4 | **Autocommit UPDATE/DELETE by PK:** 3 MySQL round trips → 1 | update TPS +16% (+30% tuned); p99 −30% at 1 thread; tablet and mysqld CPU −14–20% | S | P2 #1. Stats labels change; endtoend fixtures need a CI run. |
| 5 | **vttablet gRPC stream workers**, plus the per-query micro fixes (P1) and per-query mutex removals (P6) | vttablet CPU −4–14%, QPS +6–14% at high concurrency; removes lock waits at many connections | S | P1 #2 and #5, P6 #4. Best combined with item 2. |
| 6 | **Concurrent join RHS** (bounded, outside transactions) | Cross-shard joins 2x QPS, −50% latency | S/M | P3 #1. vtexplain needs concurrency 1; the flag must also be added to vtcombo. |
| 7 | **Big / scatter reads:** gRPC codec non-zeroing pool, MergeSort chunking (F08), pre-sorted merge, DISTINCT hasher reuse | −16–34% CPU on streaming, ORDER BY and GROUP BY over many rows | S each | P3 #2, #3, #5; F08 |
| 8 | **Bulk sharded inserts:** per-row AST caching (F09) and shard binary search (F04) | vtgate −20–30% CPU on multi-row inserts; lookup up to 64x at many shards | S/M | F09 and F04 overlap in `insert.go`; land F04 first. |

**Item 1 in detail:**
- Plan-cache sketch reset and `indexOf` (F01).
- VTOrc polling off-by-one (P5).
- VReplication `select *` drops charset and enum conversions: silent wrong data (F16, F30).
- VReplication panic on a target-generated column (F30).
- `FetchNext` uncapped slices and consolidator over-count (F11, F30).
- Races: `Timings.Reset`, `Throttler.checkScope`, `processExactKeyRange` (F30, F27).
- vtgate accepts a quoted-int `SET` that breaks the session (P2).
- Plan-cache size metric shows about 4% of the real size (P6).

**More configuration and ops guidance** (documentation only; complements item 2):
- **vtgate `--enable-buffer`:** planned failovers go from 30–80 client errors to 0, at the cost of a 60–230 ms latency blip. A second failover within `--buffer-min-time-between-failovers` (1 min) isn't buffered. (P5 #6)
- **TLS on the vtgate MySQL port:** use ECDSA P-256 certificates and client session resumption. At one connection per query, vtgate CPU per connection is 0.8 ms without TLS, 2.1 ms with RSA-2048 and 1.3 ms with ECDSA, and connect p99 is 41 ms with RSA vs 13 ms with ECDSA. RSA saturated vtgate at 800 connections/s. `GODEBUG=tlsmlkem=0` would halve the remaining handshake cost, but it drops post-quantum key exchange, so it's a security trade-off. (P6 #3)
- **`--mysql-server-pool-conn-read-buffers`** for connection-churn workloads: −44% allocation per connection, −43% GCs, −2% CPU. (P6 #7)
- **`--compression-engine-name=zstd` for backups:** −52% backup CPU; readable back to v15. Making it the default changes the backup format, so it needs staging. (P5 #4)
- **Transaction mode:** TWOPC costs +43–58% vtgate, +69–85% tablet and +96–122% mysqld CPU, and 33–43% less TPS, compared with MULTI. MULTI commits shards sequentially (1.2–1.5 ms vs 0.6 ms single-shard), but costs the same as SINGLE for single-shard transactions. (P2 #4)
- **Large reads:** OLTP reads of more than 10,000 rows per shard fail at the tablet's `max-result-size`; use OLAP/streaming for them. (P3)

## Wave 2: medium effort, big payoff

| # | Item | Payoff | Effort | Notes |
|---|---|---|---|---|
| 9 | **Pooled `ExecuteStream`** (bidi stream pool per tablet, unary fallback for N-1, flag-gated) | Point selects: QPS +12–27%, CPU −10–31%, p99 −10–17%; read_write +7–24% | M | H2. Stepping stone to item 18. Tracing and metrics must travel in the request; streams must close on tablet shutdown. |
| 10 | **Raw MySQL rows for large results** (revive #20215 with an Unimplemented fallback and a stream pool) | 1000-row reads: tablet CPU −40–49%, vtgate −18–23%, QPS +25–39% | M/L | H2 port patch. #20215 as written breaks against N-1 tablets. |
| 11 | **VReplication CPU bundle:** F05 binlog formatting, F16 applier, F17 vindex fast path, F26 GTID, F27 throttler, F13 charset conversion, `--vstream-packet-size=1MB` | Steady-state source −6% and target −3.5% CPU; copy −8% CPU/row; much more for temporal-heavy tables, charset-converting workflows and many-UUID GTID sets | S each | P4 #5 and #7, round-1 reports |
| 12 | **Row-path allocation bundle:** F11 `ReadQueryResult`, F10 send side (plus receive side capped at 128 rows), F15 binary row writer, F19 stats labels, P6 idle pool ticker | −9–17% tablet CPU on large results; prepared statements −44%/row; idle tablet CPU −80% | S each | P3 #6, P6 #2 |
| 13 | **Backups:** F07 pooled pargzip fork, then zstd as the default, staged over releases | Backup CPU −25–43%, peak heap 300 MB → 10 MB; zstd another −52% CPU | S/M | F07, P5 #4. Changing the zstd default changes the backup format. |
| 14 | **Sequence block caching** (opt-in, vschema setting) | Inserts into sequence tables: +32–36% TPS, −25% latency | M | P2 #2. Needs a design: ids no longer in time order across vtgates, and cache invalidation on reset. |
| 15 | **Upstream asks** (file now; long lead time) | Unlocks item 2 without the window caveat; wake-up fix −36–42% CPU at 1 thread; parser memory −20% RSS at 2k connections | S to file | grpc-go: start BDP only for bulk data. Go runtime: don't wake an idle P on a direct hand-off. goyacc: `//go:noinline` getters, then P6's parser fix. |

## Wave 3: low marginal gain; batch it opportunistically

| # | Item | Payoff | Effort | Notes |
|---|---|---|---|---|
| 16 | **Round-1 byte-level bundle:** F02 tokenizer, F03+F21 escaping and literal formatting, F12, F22, F23, F24 (parse, normalize, format), F06/F14 collations, F18 JSON, F20 BIT, F29 utf8, the rest of F30 | About 5–10% of parse+normalize+format on OLTP; 10x+ on huge literals and big IN lists; collation items only for non-0900 collations | S each, low risk | Merge-order notes are in `SUMMARY.md` and `P7-combined.md`. Most are already integrated and tested in `P7-combined.patch`. |
| 17 | **Changes that need a behaviour or semantics review** | – | S/M | F25 flush timer (flushes after the first write, not the last); PAD SPACE collation fix (matches MySQL, but changes results). VTOrc faster poll defaults moved to "Fixed waits": keep the defaults and fix only the off-by-one. |

## Strategic projects (plan separately)

| # | Item | Payoff | Effort | Notes |
|---|---|---|---|---|
| 18 | **Dedicated pooled TCP transport**, where the caller does its own I/O (vtgate → vttablet) | Point selects: QPS +37–58%, CPU −28–63%; context switches per query 19 → 8. This is the biggest lever on the Vitess-vs-MySQL gap. | L | H2. Needs TLS and auth, cancellation over a side channel, and a port in the tablet record. Do after #9 settles the pooling, fallback and semantics questions. |
| 19 | **In-process co-located tablets** | Ceiling measured with vtcombo: 2.6x QPS at 8 threads, 0.30 ms vs 0.78 ms at 1 thread | L | H1. Only for co-located deployments. |

## Measured and not worth doing

- Parallel commit across shards (and it loses a guarantee).
- Deferred BEGIN (in the noise).
- Multiple gRPC connections per tablet (worse).
- Parallel insert workers (+19% mysqld CPU).
- The vstreamer event-queue change (unexplained +5–9% target CPU).
- Shared request-id streams over exclusive stream checkout.
- `GOEXPERIMENT=simd` kernels (a few ns per literal).

## Before committing to items 9, 18 and 19

This VM makes waking a halted vCPU unusually expensive (about 20 µs), which inflates hop costs. Re-measure the Wave 1 config changes and items 9 and 18 on dedicated hardware, or with halt-polling enabled. The ranking of the transport items is the one most likely to move.

---

# VReplication roadmap (round 3: V1–V6, plus P4 and round-1 items)

Evidence for each item is in `V1-copy.md`, `V2-apply.md`, `V3-vdiff.md`, `V4-scale.md`, `V4b-scale-cont.md`, `V5-vstream.md`, `V6-static.md` and `P4-vreplication.md`. Bugs are listed with priorities in `BUGS.md`.

## VR Wave 1: small changes, large user-visible gains

| # | Item | Payoff (measured) | Effort | Source |
|---|---|---|---|---|
| R1 | **Correctness bugs** | Prevents data drift, stuck workflows and failed streams | S–M | Details below the table |
| R2 | **Skip waits that aren't needed** (free condition checks, no extra polling): per-table copy catch-up when the last snapshot is younger than the lag tolerance (MoveTables and VStream copy) | Matters with many tables: seconds per table, i.e. minutes to hours for thousands of small tables; negligible for a few huge tables. VStream copy of 40 tables 39.5 s → 0.5 s. | S | P4 #1, V5 #1 |
| R3 | **SwitchTraffic outage (time-critical):** drop the trailing 100 ms sleep and ReloadSchema per lock cycle, skip the second allowTargetWrites, WaitForPos backoff from 5 ms (capped; longer waits back off to ~1 s) | Median write gap 1.3 s → 173 ms, with no client errors | S | V6 #6, V4, V4b #1 |
| R4 | **Many-table workflows:** indexed plan builder, per-table plan build in the copy, per-table PK query instead of a schema scan under the schema-engine lock, `GetSchema` reading only the named tables | 2000-table MoveTables 177 s → 110 s; per-stream start 20–28 ms → 0.8 ms | S | V6 #12/#13, V4, V4b #2 |
| R5 | **VDiff:** byte-equal compare fast path and direct row pipeline | VDiff tablet CPU −47%, wall time −30%, whole-cluster CPU −28% | S–M | V3 #3/#4, V6 #7 |
| R6 | **Online DDL copy:** JSON as text (always on) and deferred non-unique secondary keys | Copy of a JSON table ~30% faster; sbtest copy −15–20% | S | V1 #1/#2 |
| R7 | **gRPC codec non-zeroing buffer pool** (all gRPC, including VReplication and VStream) | vtgate −11–16% and vttablet −5–8% on VStream; −16–27% on large result streams | S | P3 #2, V5 #5 |
| R8 | **Ops guidance:** `--defer-secondary-keys=false` for many tiny tables, a larger `--vreplication-heartbeat-update-interval` for long-running workflows with large rule sets, `--vstream-packet-size=1MB`, per-table workflows for faster multi-table copies | 2000 tables 110 → 64 s; heartbeat binlog growth 10x lower; copy −8% CPU per row; multi-table copy −29% | docs | V4b #4/#6, P4, V1 #5 |

**R1 in detail:**
- **P0:** JSON doubles silently become DECIMAL in the copy; parallel-insert-worker connections skip session setup; the generated-column panic or shifted values; `select *` dropping conversions.
- **P1:** the orphaned VDiff workflow lock (24 h); cancelling a large MoveTables leaves a broken workflow; VStream `minimize_skew` stalls with 3+ shards.

## VR Wave 2: medium effort, big throughput gains

| # | Item | Payoff | Effort | Source |
|---|---|---|---|---|
| R9 | **Bulk UPDATE for multi-row UPDATE events** (CASE on an integer PK, capped at 100 rows) | Drain 2.8–2.9x; lag under 300 × 100-row updates/s goes from 24 s and growing to 0; target mysqld −62% per trx | M | V2 #1 |
| R10 | **VStream running-phase bundle:** no deep row clone, lazy SizeVT, GTID string once, P4 event queue, response and transaction coalescing (opt-in) | Drain +48%; vtgate −37–58%, vttablet −24–31% CPU | M | V5 #4, V6 #2/#10, V4 |
| R11 | **VStream `batch_copy_rows`** (opt-in proto option) | Copy 1.75 → 1.10 s; vtgate −48%, vttablet −30%, client −47% | M | V5 #3, V6 #11 |
| R12 | **Coalesce empty/filtered transactions** with a small window (experimental flag; for upstream, a VStreamOptions field) | 50 workflows on one source: target vttablet −51%, all processes −27% | M | V4b #5, V4 #3, V6 #4 |
| R13 | **Buffer row changes across consecutive transactions** of the same table (experimental flag bit) | Single-row insert drain +65–80%, target mysqld −55% | M | V2 #2 |
| R14 | **Source-side per-stream cost:** GTID string once per GTID, keyrange pre-filter before full row decode | Source vttablet −6–11% per stream; more on wide rows | S | V4 #2, V6 #3 |
| R15 | **Round-1 VReplication CPU bundle:** F05, F16, F17, F26, F27, F13, relay-log timers | Steady-state source −6%, target −3.5%; per-event costs down 2–10x | S each | round 1, P4 #7, V6 #5 |
| R16 | **Parallel applier #19535:** experimental only, with a batching cap that adapts to transaction size, combined with R9 | 100-row updates 634–718 trx/s (with R9); write_only +35%; but update_index −15–25% and +80% mysqld CPU per trx without adaptive batching | M/L | V2 #3 |
| R17 | **JSON number types:** one consistent rule across copy and running phases (fixes the P0 bug), plus an opt-in JSON-as-text MoveTables copy | Correctness, then −24% copy time on JSON tables | M | V1 #3 |

## VR strategic projects

| # | Item | Payoff | Effort |
|---|---|---|---|
| R18 | **Shared binlog reader per source tablet** (all streams and CDC consumers) | Removes the per-stream source cost of ~15–45 µs/trx, which hits Reshard fan-out, many workflows and multiple VStream consumers | L |
| R19 | **Concurrent table copy within one workflow** (multiple streams per shard, or a multi-table snapshot) | −29% multi-table copy (measured via the per-table-workflow workaround) | L |
| R20 | **Shared source scan for Reshard copy** | Source copy cost drops from N× to 1× (it only pays off at high fan-out, 1→16+) | L |
| R21 | **`_vt.vreplication` row-image redesign:** keep the frequently updated columns out of the row that holds the filter rules | Heartbeat binlog growth of ~700 MB/h for a 2000-table workflow goes to near zero | M/L |
| R22 | **Tablet-side deny-list barrier** instead of the 100 ms LOCK TABLES pause | SwitchTraffic write gap ~170 ms → ~60 ms | S/M |
