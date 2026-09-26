# Performance roadmap: ranked by payoff vs effort

Each item is ranked on four things:
- **User-visible payoff:** latency, throughput, CPU, and operation wall time.
- **How many users hit it:** default paths rank above niche ones.
- **Effort and risk.**
- **Dependencies.**

Evidence for each item is in the report named in the right-hand column (all under `perf-findings/`). Effort: S is under ~1 day plus review, M is days to a couple of weeks, and L is a multi-week project.

## Wave 1: small changes with large or broad payoff (do first)

| # | Item | Payoff (measured) | Effort | Notes / evidence |
|---|---|---|---|---|
| 1 | **Correctness bugs** | Prevents wrong data, panics, races and failover delays | S each | Details below the table |
| 2 | **Config guidance:** static gRPC windows, GOMAXPROCS = real cores, GOGC=400 + GOMEMLIMIT, 256 KiB stream buffers (docs and examples; defaults unchanged for now) | +10–16% QPS; −20–27% CPU on every workload; p99 −15–26%; 1-thread latency 0.80 → 0.54 ms | S | P1, H1, P7. Windows cap per-stream throughput at window/RTT, so size them for cross-cell links. |
| 3 | **Operation stalls** | MoveTables of 40 tables 47 s → 5.6 s; SwitchTraffic outage 2.3 → 0.4 s; PRS outage (semi-sync) 1.16 → 0.21 s; stream resume after restart 30 → 1.3 s; Online DDL 84 → 40 s; every backup/restore −2–3 s | S each | P4 #1–4, P5 #1, #3 |
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
| 17 | **Changes that need a behaviour or semantics review** | – | S/M | F25 flush timer (flushes after the first write, not the last); VTOrc faster poll defaults (scale-test first); PAD SPACE collation fix (matches MySQL, but changes results) |

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
| R2 | **Remove fixed waits:** per-table copy catch-up tick (MoveTables and VStream copy), WaitForPos polling with a 5 ms→100 ms backoff, tablet-picker 30 s sleep, Online DDL 1-minute review tick, 1-minute `VDiff --wait` poll | MoveTables of 40 tables 47 s → 5.6 s; VStream copy of 40 tables 39.5 s → 0.5 s (1M rows 6.3 → 1.75 s); VDiff of 30 tables 65 s → 11 s; Online DDL 84 → 40 s; stream resume 30 s → 1.3 s; `VDiff create --wait` 60 s → 7 s | S each | P4, V5 #1, V4b #1, V3 #2 and #5 |
| R3 | **SwitchTraffic outage:** drop the trailing 100 ms sleep and ReloadSchema per lock cycle, skip the second allowTargetWrites, add the WaitForPos backoff | Median write gap 1.3 s → 173 ms, with no client errors | S | V6 #6, V4, V4b #1 |
| R4 | **Many-table workflows:** indexed plan builder, per-table plan build in the copy, per-table PK query instead of a schema scan under the schema-engine lock, `GetSchema` reading only the named tables | 2000-table MoveTables 177 s → 110 s; per-stream start 20–28 ms → 0.8 ms | S | V6 #12/#13, V4, V4b #2 |
| R5 | **VDiff:** byte-equal compare fast path and direct row pipeline | VDiff tablet CPU −47%, wall time −30%, whole-cluster CPU −28% | S–M | V3 #3/#4, V6 #7 |
| R6 | **Online DDL copy:** JSON as text (always on) and deferred non-unique secondary keys | Copy of a JSON table ~30% faster; sbtest copy −15–20% | S | V1 #1/#2 |
| R7 | **gRPC codec non-zeroing buffer pool** (all gRPC, including VReplication and VStream) | vtgate −11–16% and vttablet −5–8% on VStream; −16–27% on large result streams | S | P3 #2, V5 #5 |
| R8 | **Ops guidance:** `--defer-secondary-keys=false` for many tiny tables, `--vreplication-heartbeat-update-interval` for large rule sets, `--vstream-packet-size=1MB`, per-table workflows for faster multi-table copies | 2000 tables 110 → 64 s; heartbeat binlog growth 10x lower; copy −8% CPU per row; multi-table copy −29% | docs | V4b #4/#6, P4, V1 #5 |

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
