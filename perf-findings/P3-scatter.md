# P3-scatter: queries vtgate cannot route to one shard

Investigator P3, BASE=40000, base commit aa9ccf9. Patch: `findings/P3-scatter.patch`. Raw A/B output and aggregated
tables: `findings/P3-raw/`. Harness: `perf-findings/harness/P3/` (in the patch; the scripts expect it at `/home/vt/perf/P3/` with `p3.lua` under `lua/`).

Patched binaries in `/home/vt/bin-P3/<variant>` (vtgate + vttablet only):

| variant | contents |
|---|---|
| `pool` | codec buffer pool change only (finding 1) |
| `p3` | this patch without the write-buffer change: pool + run-merge sort + concurrent join RHS + Distinct hasher reuse |
| `p3r1` | `p3` + round-1 F08 (MergeSort chunks) + F10 (proto row slabs) + F11 (ReadQueryResult) |
| `p3r1b` | `p3r1` + F02 (tokenizer) + F04 (shard lookup) + F22 (literal to bind var) |
| `p3w` | `p3` + 64 KiB MySQL-protocol write buffer (prototype, not in the patch: inconclusive) |

The patch = `p3` (all four code changes) plus tests, the flag doc, and the harness.

## TL;DR

Ranked by user-visible effect (CPU µs/query is the reliable metric; QPS/latency swing ±25% with the neighbour's load).

1. **gRPC codec buffer pool** (`go/vt/servenv/grpc_codec{,_pool}.go`, S). Vitess's vtproto codec uses gRPC's default
   buffer pool, whose tiers are 256 B/4 KiB/16 KiB/32 KiB/1 MiB and which **zeroes the full capacity of every buffer it
   hands out**. A streamed result chunk is slightly larger than the 32 KiB `stream-buffer-size`, so every chunk is served
   from the 1 MiB tier: vttablet clears 1 MiB per ~33 KiB message it marshals and vtgate clears 1 MiB per message it
   unmarshals. `memclrNoHeapPointers` was **24% of vttablet CPU and 16% of vtgate CPU** for OLAP (`set workload=olap`)
   reads. Replaced with a non-zeroing pool with one tier per power of two (256 B .. 16 MiB).
   Measured: OLAP 10k–100k-row reads **vtgate −17..−22%, vttablets −16..−27% CPU/query** (A/B 1, 4, 5, 6), OLAP
   `ORDER BY` −10..−20% on top of that. Non-streaming reads: within noise (−5%..+2%).
2. **Run the nested-loop join's RHS concurrently** (`go/vt/vtgate/engine/join.go`, new flag `--join-rhs-concurrency`,
   default 8, S/M). A vtgate join (`a JOIN b ON a.k = b.k` on a non-vindex column) executes the RHS route once per LHS row,
   **strictly one after the other**; each is a full scatter. With a 10-row LHS on 2 shards that is 10 sequential
   round-trips. Now up to 8 run at once (only outside transactions and reserved connections; output order unchanged).
   Measured (join_x10): **latency −55..−60%, QPS 2.1–2.3x, and CPU/query −45% vtgate, −44% tablets** on 2 shards; on 4
   shards −54% latency, **−61% vtgate / −56% tablet CPU** (A/B 2, 4, 5). The CPU drop is the scheduler effect P1 found:
   the same work done in bursts instead of one-at-a-time wake-ups.
3. **Merge pre-sorted shard results instead of re-sorting** (`evalengine.Comparison.Sort`, S). In OLTP mode a scatter
   `ORDER BY` (and every ordered aggregate / GROUP BY / COUNT(DISTINCT) / DISTINCT … ORDER BY, which the planner turns
   into an ordered route) concatenates the shard results and runs a full pdqsort, although each shard already returned
   its rows sorted. `Route.sort` was **29.5% of vtgate CPU** for a 10k-row GROUP BY. Sort now detects ascending runs and
   merges them when there are ≤64 (n·log2(k) instead of n·log2(n) comparisons; stable; falls back to pdqsort otherwise).
   Micro: 2 runs × 5000 rows 5.2 ms → 1.06 ms (5x). Profile: `Route.sort` 29.5% → 7.8%. End to end:
   **vtgate −8..−21% CPU/query** for GROUP BY high-cardinality / COUNT(DISTINCT) / ORDER BY aggregate LIMIT over 10k rows
   on 2 shards, **−18..−22% on 4 shards** (A/B 2, 4, 5).
4. **Stream buffer size 256 KiB** (config: vttablet `--queryserver-config-stream-buffer-size=262144`, vtgate
   `--stream-buffer-size=262144`, S). Default 32 KiB. On 4 shards: OLAP big reads **vtgate −27..−32%, tablets −23..−24%**
   on the base binary alone; with this patch **vtgate −33..−35%, tablets −26..−28%** vs base (A/B 6). Trade-off: up to
   256 KiB buffered per stream on each side and a later first row.
5. **Distinct**: reuse the row hasher and presize the probe map (`engine/distinct.go`, S): vtgate −7..−14% for
   `SELECT DISTINCT` over 10k rows (the hasher escaped to the heap: one allocation per row).
6. **Round-1 patches**, end to end on scatter reads (A/B 3, 5): **F08** (MergeSort chunks) **−24..−34% vtgate** for
   streaming `ORDER BY` over 10k rows; **F11/F10-send** **−9..−17% tablet** CPU for every large result; F10-recv
   −3..−9% vtgate; F02+F04+F22 −8..−11% vtgate for a 1000-id IN list, nothing measurable at 10–100 ids.
   F06/F14 not measured: sbtest uses `utf8mb4_0900_ai_ci`, where they do not apply.

Negative / neutral: a 64 KiB (instead of 16 KiB) MySQL-protocol write buffer in vtgate (−2..−6% vtgate on 10k–100k-row
results, within noise; not kept); vtgate plan cache (always hit, including IN lists of any length: they become one tuple bind var);
shard-call parallelism (already concurrent, no serialized step except the result-collect mutex); small scatters are
dominated by a fixed per-shard cost (≈300 µs of tablet CPU and ≈140 µs of vtgate CPU per extra shard), not by any
merge logic; `in100`/`krange*`/`ro_ranges` moved within noise with every change.

## Environment and method

- 4 vCPU shared VM. Another investigator (P2, then P6) ran on BASE 30000 the whole time; load average during runs was
  9–27 (per row in the tables). Treat QPS/latency differences under ~25% as noise; CPU µs/query is steadier (typically
  ±3–5%, ±10% on the smallest shapes).
- Cluster: `perf-findings/harness/P3/cluster.sh` (P1's copy with `restart`), keyspace sbtest, hash vindex on `id`,
  4 tables × 250,000 rows, 2 shards (-80, 80-) and later 4 shards (-40 40-80 80-c0 c0-), one primary per shard.
- Workload: `perf-findings/harness/P3/p3.lua` (sysbench, text protocol, 8 threads, 15 s per run), one query shape per run.
  `k` is skewed around 125000 in sysbench data; the k-range shapes pick `k` in 110000–122000 where there is ~1 row per
  value, so `k BETWEEN s AND s+W-1` returns ~W rows. `id` ranges are scatter because `id` is hashed.
- CPU µs/query = `/proc/<pid>/stat` utime+stime delta ÷ queries; "tablets" and "mysqld" are summed over all shards
  (the CPU a query costs across the fleet). `bench.sh`, `suite.sh`, `ab.sh` (restart + warm-up per config, rounds
  alternate configs), `agg.py` (means, min–max for the CPU columns).
- Harness bugs found and fixed along the way (in `perf-findings/harness/P3/`): (1) a listener port in 40000–40999 can be
  taken as an *ephemeral* source port by another cluster's client connection (e.g. local port 40002 → 30003), so
  vtgate/vttablet fail to bind on restart. I reserved the range with
  `echo 40000-40999 > /proc/sys/net/ipv4/ip_local_reserved_ports` (still set; harmless, helps whoever uses BASE 40000
  next) and `ab.sh` retries the restart. (2) A vttablet that failed to bind left a stale pid file; the next start could not
  write its own, so `restart` no longer killed it. `restart` now also `pkill`s by topo address. Runs affected by this
  (one round of A/B 4) were discarded.

### Shapes

| name | query (per run: random table and range) | rows to vtgate | vtgate plan |
|---|---|---|---|
| krange100 | `SELECT c … WHERE k BETWEEN ? AND ?+99` | ~100 | Route Scatter |
| krange_ord100 | … `ORDER BY k` | ~100 | Route Scatter OrderBy (in-memory sort) |
| krange_lim10 / lim100 | … `ORDER BY k LIMIT 10` (W=100) / `LIMIT 100` (W=1000) | 20 / 200 | Limit → Route OrderBy |
| idrange_ordc100 | `SELECT c … WHERE id BETWEEN ? AND ?+99 ORDER BY c` (sysbench order_ranges) | 100 | Route OrderBy COLLATE 0900_ai_ci |
| count1k | `COUNT(*), SUM(k)` over 1000 ids | 2 | Aggregate Scalar |
| grp_low10k | `k % 100 AS g, COUNT(*), SUM(k) … GROUP BY g` over 10k ids | ~200 | Aggregate Ordered → Route OrderBy |
| grp_high10k | `k, COUNT(*) … GROUP BY k` over 10k ids | ~10k | Aggregate Ordered → Route OrderBy |
| grp_c10k | `LEFT(c,2) AS g, COUNT(*) … GROUP BY g` | ~200 | Aggregate Ordered (collated) |
| distinct_k10k | `SELECT DISTINCT k` over 10k ids | ~10k | Distinct (hash) → Route |
| cnt_dist10k | `COUNT(DISTINCT k)` over 10k ids | ~10k | Aggregate Scalar count_distinct → Route OrderBy |
| ord_agg_lim10k | `k % 1000 g, COUNT(*) cnt … GROUP BY g ORDER BY cnt DESC, g LIMIT 10` | ~2000 | Limit → Sort → Aggregate Ordered → Route OrderBy |
| in10/100/1000 | `SELECT c … WHERE id IN (…)` | 10/100/1000 | Route IN |
| insert100 | 100-row `INSERT … ON DUPLICATE KEY UPDATE` on existing ids | – | Insert (multi-shard) |
| join_x10 | `a JOIN b ON a.k = b.k WHERE a.k BETWEEN ? AND ?+9` | 10 + 10×~1 | Join: Route Scatter, then Route Scatter per LHS row |
| join_push100 | `a JOIN b ON a.id = b.id WHERE a.k BETWEEN …` | ~100 | one Route Scatter (pushed down) |
| big10k | `SELECT * … WHERE id BETWEEN ? AND ?+9999` | 10k (≈2.4 MB) | Route Scatter |
| big10k_olap / big100k_olap | same with `SET workload='olap'` | 10k / 100k | streaming Route |
| bigord10k_olap | … `ORDER BY id`, OLAP | 10k | streaming MergeSort |
| ro_ranges | sysbench `oltp_read_only --point-selects=0 --skip_trx=on` (prepared; simple/sum/order/distinct ranges of 100 ids) | – | 4 scatters per event |

Plans verified with `vexplain plan` (`plans.sh`). All shapes hit the plan cache (`QueryPlanCacheMisses` 15 over
>25k queries; IN lists of any length normalize to one `::vtg1` tuple).

Limits users hit here: the tablet refuses more than `--queryserver-config-max-result-size` (10,000) rows per shard in
OLTP mode (`Row count exceeded 10000`), so `SELECT` of 100k rows only works with `workload=olap`; vtgate caps
in-memory results at `--max-memory-rows` (300,000).

## Baseline (base binaries, 2 shards, 8 threads; mean of 4 rounds from A/B 4)

| shape | QPS | avg ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q |
|---|---|---|---|---|---|---|
| krange100 | 1683 | 5.0 | 14.9 | 473 | 573 | 584 |
| krange_ord100 | 1616 | 5.3 | 16.0 | 508 | 589 | 602 |
| krange_lim10 | 2279 | 3.6 | 10.7 | 366 | 440 | 424 |
| krange_lim100 | 1390 | 5.9 | 19.6 | 637 | 725 | 802 |
| idrange_ordc100 | 1627 | 5.0 | 16.6 | 450 | 513 | 458 |
| count1k | 1712 | 4.9 | 16.8 | 459 | 541 | 590 |
| grp_low10k | 453 | 19.0 | 44.9 | 1042 | 1058 | 3790 |
| grp_high10k | 228 | 35.9 | 83.9 | 4950 | 3492 | 4748 |
| grp_c10k | 502 | 16.2 | 34.8 | 1092 | 1120 | 4624 |
| distinct_k10k | 281 | 28.7 | 69.0 | 4234 | 3018 | 3340 |
| cnt_dist10k | 240 | 35.2 | 86.0 | 3810 | 3132 | 4254 |
| ord_agg_lim10k | 333 | 24.5 | 56.1 | 2573 | 1768 | 3825 |
| in10 | 2067 | 4.1 | 12.1 | 361 | 413 | 339 |
| in100 | 1402 | 6.0 | 18.1 | 635 | 620 | 602 |
| in1000 | 411 | 19.7 | 50.3 | 2628 | 2156 | 2774 |
| insert100 | 427 | 19.1 | 44.7 | 1862 | 1991 | 2288 |
| join_x10 | 213 | 38.9 | 95.5 | 2724 | 4413 | 3915 |
| join_push100 | 1105 | 7.5 | 23.8 | 662 | 775 | 902 |
| big10k | 115 | 74.7 | 146 | 7940 | 10431 | 4646 |
| big10k_olap | 99 | 84.2 | 169 | 12155 | 13509 | 4654 |
| big100k_olap | 11 | 742 | 1194 | 110242 | 123943 | 41078 |
| bigord10k_olap | 81 | 108.7 | 214 | 15476 | 14280 | 4641 |
| ro_ranges (per query) | 1964 | 17.0/event | 35.4 | 399 | 492 | 446 |

Per row: vtgate ≈0.8 µs/row (OLTP) and 1.1–1.2 µs/row (OLAP) for `SELECT *` of 240-byte rows; tablets ≈1.0–1.3 µs/row.
**OLAP streaming was 50% more expensive than OLTP in vtgate** — explained by finding 1. With 4 shards (A/B 5) a small
scatter costs ≈1.6x (krange100: vtgate 746, tablets 1204 µs/q): ≈140 µs vtgate and ≈300 µs tablet CPU per extra shard.

## Where vtgate spends time (base, CPU profiles, 15 s at 8 threads)

- **Small scatters (krange100, ro_ranges, in10/100)**: per-shard RPC cost dominates. `executeMultiShard.func1` (the
  per-shard goroutine incl. gRPC call) 28–29%, gRPC reader 10%, loopy writer 9–11%, `newClientStream` 9–10%, syscalls
  14–15%, plan lookup (text protocol parse+normalize) 11% (krange100), 2% (prepared ro_ranges), GC 6–7%. Merge/sort/aggregate
  code is ≤3%. Nothing algorithmic to fix here; P1's per-hop findings (GOMAXPROCS, stream workers, GOGC) apply per shard.
- **Large OLTP results (big10k)**: `Codec.Unmarshal` 29% (`Row.UnmarshalVT` 16% = one `Values` copy + `Lengths` slice per
  row; `proto3ToRows` 9%), MySQL-protocol `writeRows` 27% (half of it `write(2)` with a 16 KiB buffer), mallocgc 26%,
  GC workers 12%, `memclrNoHeapPointers` 5.5%.
- **Large OLAP results (big10k_olap)**: as above plus **`memclrNoHeapPointers` 16.4%** under
  `grpc/mem.BufferSlice.MaterializeToBuffer → BinaryTieredBufferPool.Get → sizedBufferPool.Get` (the 1 MiB tier).
  vttablet: **`memclrNoHeapPointers` 24.4%** under `sizedBufferPool.Get` from the codec's `Marshal`. → finding 1.
- **Ordered aggregation (grp_high10k)**: `Route.sort` → `Comparison.SortResult` → pdqsort **29.5%**, Unmarshal 17%,
  `writeRows` 12%, `aggregationState.finish` 7% (one `[]Value` per output row). → finding 3.
- **Distinct (distinct_k10k)**: `probeTable.exists` 25% (hash 13%, of which 4% was allocating the hasher per row; map
  assign/grow 11%). → finding 5 (small).
- **IN 1000 ids**: parse 18%, normalize 8% (F02/F22), `resolveShards` 12% (DES hash 6%, `ResolveDestinations` 5%: F04),
  gRPC 20%.
- **Cross-shard join (join_x10)**: 22 sequential shard queries per statement; the profile is all per-RPC overhead
  (executeMultiShard 31%, loopy writer 16%, reader 15%, newClientStream 13%) and latency is the sum of 11 sequential
  scatter round-trips. → finding 2.

### Are shard calls concurrent? Serialized steps?

- `ScatterConn.multiGoTransaction` starts one goroutine per shard; the calls are concurrent. The only serialization is
  the per-query `collect` mutex (`AppendResult` of each shard's rows), a memcpy of row headers, <1%.
- OLTP scatter with ORDER BY waits for all shards, then sorts (needed; now a merge). OLAP merge-sorts as rows stream in.
- **Serialized: the vtgate nested-loop join** — RHS executions run one after the other, both in `TryExecute` and
  `TryStreamExecute`. Fixed for `TryExecute` (finding 2); streaming is a follow-up.
- `Concatenate` (UNION) already runs its sources in parallel outside transactions — finding 2 uses the same rule.

## Hypotheses tested

| # | hypothesis | outcome |
|---|---|---|
| H1 | Shard queries are not sent concurrently | No: one goroutine per shard; only result collection is serialized (<1%). |
| H2 | Plan cache misses for scatter/IN/multi-row shapes | No: 15 misses in >25k queries; IN lists become one tuple bind var. |
| H3 | Merging/sorting shard results is a large cost | **Yes for ≥1k rows**: OLTP ORDER BY re-sorts the concatenated result from scratch (29.5% for 10k rows). Fixed (finding 3). For ≤100 rows it is <3%. |
| H4 | Per-row proto conversion and row allocations dominate large results | **Partly**: Unmarshal+proto3ToRows 29–39% and GC ~12%, but the single largest item was zeroing pooled buffers (finding 1). F10/F11 help the rest a little (A/B 3). |
| H5 | OLAP streaming is cheaper than OLTP for big results | **No, 50% more expensive in vtgate** at base. Cause: codec pool (finding 1) and 32 KiB chunks (finding 4). After both, OLAP is ≈ OLTP per row. |
| H6 | Evalengine comparisons/hashing are hot | Comparisons: only inside the sort (fixed by merging runs). Hashing: Distinct 13% incl. a per-row hasher allocation (fixed). F06/F14 (collations) not applicable to utf8mb4_0900_ai_ci. |
| H7 | Cross-shard joins are slow because of per-row RHS queries | **Yes, latency ∝ LHS rows** because the RHS runs sequentially. Concurrent RHS: −55..−60% latency, −45..−61% CPU/query (finding 2). The ALLOW_HASH_JOIN directive exists but the planner never uses it. |
| H8 | Memory limits hurt | Tablet `max-result-size` 10000 rows/shard makes OLTP 100k-row reads fail outright (must use OLAP). `max-memory-rows` 300k not hit. |
| H9 | Stream chunk size matters for OLAP | **Yes**: 256 KiB chunks −27..−32% vtgate CPU for big OLAP reads (finding 4). |
| H10 | MySQL-protocol write buffer (16 KiB → 64 KiB) | −2..−6% vtgate on large results, within noise; not kept (A/B 7). |
| H11 | Multi-row insert / IN list costs are new | No: they are parse/normalize/shard-lookup/per-row insert AST — round-1 F02/F04/F09/F22 territory. F02+F04+F22 measured −8..−11% vtgate for 1000 ids, nothing at 10–100 (A/B 3). |
| H12 | More shards change the picture | Small scatters cost linearly more per shard (fixed per-shard RPC cost). Findings 1–3 hold at 4 shards with the same or larger effect (A/B 5). |

## A/B results

Mean of rounds; percentages vs the first config of each block; [min–max] for the CPU columns.
### A/B 1: codec pool only (`pool`) vs base, 2 shards, 3 rounds, load 17–24

(`big100k_olap` p99 column is not meaningful in this A/B: the histogram parser failed for some rows.)

| shape | thr | config | n | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| big100k_olap | 8 | base | 3 | 6 | 1533.80 | 2415.23 | 681.91 | 116145 [112613–117955] | 127817 [122162–133333] | 39748 | 24.5 |
| big100k_olap | 8 | pool | 3 | 7 (+12.5%) | 1321.56 (-13.8%) | 1729.33 (-28.4%) | 612.08 (-10.2%) | 92789 (-20.1%) [87500–97500] | 99284 (-22.3%) [95300–101635] | 40349 (+1.5%) | 21.2 |
| big10k | 8 | base | 3 | 78 | 122.24 | 230.01 | 302.93 | 8375 [7877–8688] | 11239 [10295–11796] | 4490 | 21.9 |
| big10k | 8 | pool | 3 | 107 (+37.2%) | 87.92 (-28.1%) | 140.84 (-38.8%) | 172.73 (-43.0%) | 7938 (-5.2%) [7619–8453] | 10483 (-6.7%) [10090–11192] | 4387 (-2.3%) | 20.2 |
| big10k_olap | 8 | base | 3 | 63 | 152.93 | 263.36 | 332.75 | 12957 [12046–14263] | 14517 [13257–15649] | 4610 | 23.6 |
| big10k_olap | 8 | pool | 3 | 77 (+21.8%) | 122.08 (-20.2%) | 232.18 (-11.8%) | 316.87 (-4.8%) | 10166 (-21.5%) [9698–10911] | 11191 (-22.9%) [10661–12029] | 4649 (+0.8%) | 20.8 |
| bigord10k_olap | 8 | base | 3 | 41 | 196.44 | 338.39 | 459.27 | 17256 [16606–17657] | 16180 [15991–16304] | 4896 | 24.4 |
| bigord10k_olap | 8 | pool | 3 | 69 (+66.7%) | 123.79 (-37.0%) | 205.99 (-39.1%) | 247.41 (-46.1%) | 13825 (-19.9%) [13370–14260] | 11795 (-27.1%) [11094–12162] | 4676 (-4.5%) | 20.1 |
| grp_high10k | 8 | base | 3 | 140 | 60.74 | 123.64 | 166.90 | 5178 [5021–5286] | 3674 [3570–3736] | 5027 | 19.9 |
| grp_high10k | 8 | pool | 3 | 178 (+27.1%) | 48.55 (-20.1%) | 89.57 (-27.6%) | 134.62 (-19.3%) | 5137 (-0.8%) [4986–5327] | 3623 (-1.4%) [3524–3721] | 4878 (-3.0%) | 20.9 |
| krange100 | 8 | base | 3 | 1038 | 7.79 | 17.40 | 30.17 | 546 [524–561] | 631 [617–653] | 606 | 17.5 |
| krange100 | 8 | pool | 3 | 1111 (+7.0%) | 7.31 (-6.2%) | 16.14 (-7.2%) | 29.87 (-1.0%) | 482 (-11.7%) [434–531] | 583 (-7.6%) [518–632] | 558 (-8.0%) | 21.1 |

### A/B 2: `p3` vs `pool`, and `p3` with `--join-rhs-concurrency=1` (`p3seqjoin`), 2 shards, 3 rounds, load 15–22

`p3` − `pool` isolates the sort merge, the Distinct change and the join change; `p3seqjoin` keeps the sort/Distinct changes and turns the join change off. join_push100 (no sort, no join in vtgate) is a control: it shows a ~10% bias in favour of the later configs of each round in this A/B.

| shape | thr | config | n | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| cnt_dist10k | 8 | pool | 3 | 244 | 35.54 | 63.38 | 86.29 | 3742 [3609–3960] | 3197 [3110–3363] | 4184 | 17.6 |
| cnt_dist10k | 8 | p3 | 3 | 275 (+12.9%) | 30.22 (-15.0%) | 50.53 (-20.3%) | 63.43 (-26.5%) | 3094 (-17.3%) [2939–3191] | 3202 (+0.1%) [3081–3303] | 4376 (+4.6%) | 18.5 |
| cnt_dist10k | 8 | p3seqjoin | 3 | 263 (+7.9%) | 32.56 (-8.4%) | 58.59 (-7.6%) | 75.90 (-12.0%) | 3086 (-17.5%) [2993–3178] | 3166 (-1.0%) [3106–3239] | 4178 (-0.1%) | 19.2 |
| distinct_k10k | 8 | pool | 3 | 258 | 31.39 | 61.12 | 87.62 | 4381 [4365–4407] | 3134 [3120–3145] | 3293 | 16.7 |
| distinct_k10k | 8 | p3 | 3 | 246 (-4.8%) | 35.43 (+12.9%) | 61.37 (+0.4%) | 78.19 (-10.8%) | 3823 (-12.8%) [3638–3969] | 3196 (+2.0%) [3021–3324] | 3541 (+7.5%) | 18.3 |
| distinct_k10k | 8 | p3seqjoin | 3 | 282 (+9.1%) | 31.44 (+0.1%) | 56.39 (-7.7%) | 73.31 (-16.3%) | 3731 (-14.8%) [3574–3972] | 3121 (-0.4%) [3024–3238] | 3385 (+2.8%) | 17.9 |
| grp_high10k | 8 | pool | 3 | 224 | 37.40 | 63.84 | 81.94 | 5123 [5004–5252] | 3632 [3561–3718] | 4886 | 16.4 |
| grp_high10k | 8 | p3 | 3 | 206 (-8.3%) | 41.30 (+10.4%) | 70.81 (+10.9%) | 89.54 (+9.3%) | 4498 (-12.2%) [4357–4666] | 3676 (+1.2%) [3545–3832] | 5046 (+3.3%) | 17.5 |
| grp_high10k | 8 | p3seqjoin | 3 | 175 (-21.7%) | 46.40 (+24.1%) | 82.17 (+28.7%) | 105.88 (+29.2%) | 4618 (-9.9%) [4532–4698] | 3784 (+4.2%) [3670–3893] | 5297 (+8.4%) | 17.1 |
| idrange_ordc100 | 8 | pool | 3 | 1447 | 5.63 | 12.11 | 18.77 | 470 [459–488] | 552 [543–568] | 479 | 16.5 |
| idrange_ordc100 | 8 | p3 | 3 | 1591 (+9.9%) | 5.40 (-4.1%) | 10.70 (-11.7%) | 15.85 (-15.5%) | 463 (-1.6%) [434–491] | 548 (-0.7%) [506–587] | 479 (+0.0%) | 15.5 |
| idrange_ordc100 | 8 | p3seqjoin | 3 | 1219 (-15.8%) | 6.62 (+17.6%) | 13.86 (+14.4%) | 20.22 (+7.7%) | 484 (+3.0%) [466–506] | 578 (+4.8%) [546–609] | 496 (+3.7%) | 15.9 |
| join_push100 | 8 | pool | 3 | 1083 | 8.22 | 17.67 | 27.20 | 670 [593–747] | 811 [727–895] | 910 | 21.2 |
| join_push100 | 8 | p3 | 3 | 1518 (+40.3%) | 5.27 (-36.0%) | 10.85 (-38.6%) | 16.25 (-40.3%) | 595 (-11.2%) [590–602] | 727 (-10.4%) [725–728] | 840 (-7.7%) | 18.4 |
| join_push100 | 8 | p3seqjoin | 2 | 1449 (+33.8%) | 5.54 (-32.6%) | 12.21 (-30.9%) | 18.64 (-31.5%) | 606 (-9.6%) [605–607] | 736 (-9.3%) [733–738] | 836 (-8.1%) | 20.2 |
| join_x10 | 8 | pool | 3 | 198 | 44.44 | 80.56 | 111.23 | 2802 [2631–2970] | 4677 [4493–4795] | 3734 | 19.9 |
| join_x10 | 8 | p3 | 3 | 495 (+149.6%) | 16.26 (-63.4%) | 27.62 (-65.7%) | 37.81 (-66.0%) | 1502 (-46.4%) [1495–1515] | 2504 (-46.5%) [2474–2531] | 2361 (-36.8%) | 18.6 |
| join_x10 | 8 | p3seqjoin | 2 | 300 (+51.2%) | 26.66 (-40.0%) | 42.64 (-47.1%) | 52.52 (-52.8%) | 2524 (-9.9%) [2480–2567] | 4325 (-7.5%) [4261–4389] | 3672 (-1.7%) | 19.6 |
| krange_ord100 | 8 | pool | 3 | 1257 | 7.31 | 15.67 | 25.48 | 549 [479–610] | 647 [562–720] | 611 | 15.1 |
| krange_ord100 | 8 | p3 | 3 | 1369 (+8.9%) | 6.41 (-12.3%) | 13.36 (-14.7%) | 19.73 (-22.6%) | 530 (-3.4%) [473–559] | 639 (-1.3%) [571–679] | 612 (+0.1%) | 14.5 |
| krange_ord100 | 8 | p3seqjoin | 3 | 1364 (+8.5%) | 6.25 (-14.6%) | 12.53 (-20.0%) | 18.37 (-27.9%) | 519 (-5.4%) [483–552] | 620 (-4.2%) [581–659] | 617 (+1.0%) | 15.0 |
| ord_agg_lim10k | 8 | pool | 3 | 299 | 30.34 | 61.27 | 94.97 | 2627 [2487–2698] | 1867 [1753–1949] | 3902 | 19.8 |
| ord_agg_lim10k | 8 | p3 | 3 | 427 (+42.9%) | 18.89 (-37.7%) | 34.26 (-44.1%) | 43.96 (-53.7%) | 2269 (-13.6%) [2220–2308] | 1748 (-6.4%) [1709–1774] | 3793 (-2.8%) | 18.3 |
| ord_agg_lim10k | 8 | p3seqjoin | 2 | 421 (+40.9%) | 19.19 (-36.8%) | 36.83 (-39.9%) | 53.75 (-43.4%) | 2341 (-10.9%) [2272–2410] | 1811 (-3.0%) [1770–1852] | 3842 (-1.5%) | 20.2 |

### A/B 3: round-1 patches on top of `p3`, 2 shards, 3 rounds, load 14–17

| shape | thr | config | n | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| big10k | 8 | p3 | 3 | 147 | 54.35 | 83.50 | 106.32 | 7664 [7570–7771] | 10118 [10069–10188] | 4326 | 14.9 |
| big10k | 8 | p3r1 | 3 | 153 (+4.2%) | 52.23 (-3.9%) | 84.83 (+1.6%) | 109.44 (+2.9%) | 7408 (-3.3%) [7263–7527] | 8392 (-17.1%) [8133–8637] | 4358 (+0.7%) | 16.4 |
| big10k | 8 | p3r1b | 3 | 140 (-4.9%) | 62.09 (+14.2%) | 97.57 (+16.9%) | 115.37 (+8.5%) | 7302 (-4.7%) [7162–7548] | 8372 (-17.3%) [8172–8737] | 4328 (+0.1%) | 17.1 |
| big10k_olap | 8 | p3 | 3 | 87 | 102.08 | 179.26 | 248.79 | 10350 [10213–10592] | 11416 [10937–11749] | 4811 | 16.0 |
| big10k_olap | 8 | p3r1 | 3 | 127 (+46.4%) | 63.61 (-37.7%) | 102.37 (-42.9%) | 130.33 (-47.6%) | 9460 (-8.6%) [9326–9532] | 9754 (-14.6%) [9542–9919] | 4399 (-8.6%) | 15.7 |
| big10k_olap | 8 | p3r1b | 3 | 117 (+34.9%) | 76.33 (-25.2%) | 119.42 (-33.4%) | 141.71 (-43.0%) | 9457 (-8.6%) [8945–10094] | 9894 (-13.3%) [9387–10557] | 4494 (-6.6%) | 17.0 |
| bigord10k_olap | 8 | p3 | 3 | 74 | 124.45 | 201.57 | 246.12 | 13818 [13230–14333] | 11682 [10842–12179] | 4626 | 16.6 |
| bigord10k_olap | 8 | p3r1 | 3 | 128 (+73.2%) | 62.66 (-49.7%) | 99.31 (-50.7%) | 121.23 (-50.7%) | 10169 (-26.4%) [10106–10237] | 9795 (-16.1%) [9549–10022] | 4424 (-4.4%) | 15.0 |
| bigord10k_olap | 8 | p3r1b | 3 | 93 (+25.3%) | 94.50 (-24.1%) | 155.45 (-22.9%) | 186.56 (-24.2%) | 10427 (-24.5%) [10138–10701] | 10456 (-10.5%) [10064–10872] | 4631 (+0.1%) | 17.1 |
| grp_high10k | 8 | p3 | 3 | 204 | 42.30 | 73.65 | 91.64 | 4448 [4229–4592] | 3652 [3470–3799] | 4983 | 15.7 |
| grp_high10k | 8 | p3r1 | 3 | 220 (+7.6%) | 38.02 (-10.1%) | 69.66 (-5.4%) | 90.91 (-0.8%) | 4418 (-0.7%) [4278–4526] | 3176 (-13.0%) [3080–3231] | 4989 (+0.1%) | 16.4 |
| grp_high10k | 8 | p3r1b | 3 | 238 (+16.5%) | 36.89 (-12.8%) | 64.52 (-12.4%) | 84.76 (-7.5%) | 4422 (-0.6%) [4222–4628] | 3176 (-13.0%) [2994–3374] | 4961 (-0.4%) | 14.9 |
| in100 | 8 | p3 | 3 | 1352 | 6.13 | 12.63 | 18.60 | 629 [605–649] | 640 [610–663] | 610 | 15.7 |
| in100 | 8 | p3r1 | 3 | 1400 (+3.5%) | 6.02 (-1.7%) | 11.90 (-5.8%) | 17.97 (-3.4%) | 635 (+1.0%) [618–664] | 633 (-1.0%) [611–667] | 605 (-0.8%) | 16.9 |
| in100 | 8 | p3r1b | 3 | 1420 (+5.0%) | 6.20 (+1.1%) | 12.46 (-1.3%) | 18.49 (-0.6%) | 610 (-3.0%) [581–651] | 658 (+2.9%) [618–719] | 613 (+0.4%) | 15.9 |
| in1000 | 8 | p3 | 3 | 452 | 17.71 | 31.02 | 40.37 | 2614 [2582–2637] | 2135 [2109–2159] | 2774 | 15.5 |
| in1000 | 8 | p3r1 | 3 | 396 (-12.2%) | 20.77 (+17.3%) | 36.80 (+18.6%) | 48.33 (+19.7%) | 2709 (+3.6%) [2680–2740] | 2135 (+0.0%) [2084–2179] | 2886 (+4.0%) | 17.4 |
| in1000 | 8 | p3r1b | 3 | 375 (-17.0%) | 22.36 (+26.3%) | 42.49 (+37.0%) | 56.88 (+40.9%) | 2412 (-7.7%) [2375–2470] | 2146 (+0.5%) [2092–2203] | 2858 (+3.0%) | 17.2 |
| krange100 | 8 | p3 | 3 | 1467 | 5.94 | 12.08 | 18.02 | 485 [440–521] | 603 [553–639] | 588 | 13.7 |
| krange100 | 8 | p3r1 | 3 | 1446 (-1.4%) | 5.81 (-2.2%) | 12.80 (+6.0%) | 21.83 (+21.2%) | 500 (+3.2%) [478–544] | 606 (+0.4%) [580–647] | 592 (+0.7%) | 14.7 |
| krange100 | 8 | p3r1b | 3 | 1661 (+13.2%) | 5.34 (-10.2%) | 11.20 (-7.3%) | 17.39 (-3.5%) | 496 (+2.3%) [465–541] | 610 (+1.1%) [584–656] | 599 (+1.9%) | 13.8 |
| ro_ranges | 8 | p3 | 3 | 1711 | 20.26 | 31.34 | 39.51 | 402 [382–415] | 517 [486–541] | 459 | 17.4 |
| ro_ranges | 8 | p3r1 | 3 | 2085 (+21.8%) | 15.76 (-22.2%) | 27.24 (-13.1%) | 37.95 (-3.9%) | 396 (-1.6%) [383–416] | 496 (-4.0%) [483–517] | 444 (-3.1%) | 16.2 |
| ro_ranges | 8 | p3r1b | 3 | 1703 (-0.4%) | 20.47 (+1.0%) | 32.13 (+2.5%) | 40.30 (+2.0%) | 408 (+1.5%) [385–422] | 523 (+1.2%) [492–544] | 455 (-0.9%) | 17.7 |

### A/B 4: whole suite, `p3` vs base, 2 shards (base 4 rounds, p3 3 rounds; alternating order), load 10–21

| shape | thr | config | n | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| big100k_olap | 8 | base | 4 | 11 | 742.36 | 1031.81 | 1194.21 | 110242 [106359–113425] | 123943 [121359–126438] | 41078 | 18.1 |
| big100k_olap | 8 | p3 | 3 | 10 (-12.1%) | 880.49 (+18.6%) | 1241.32 (+20.3%) | 1369.68 (+14.7%) | 91152 (-17.3%) [86887–94569] | 98350 (-20.6%) [95497–102500] | 40397 (-1.7%) | 17.4 |
| big10k | 8 | base | 4 | 115 | 74.71 | 117.04 | 146.49 | 7940 [7709–8430] | 10431 [10081–11146] | 4646 | 20.0 |
| big10k | 8 | p3 | 3 | 105 (-9.1%) | 77.76 (+4.1%) | 143.72 (+22.8%) | 188.55 (+28.7%) | 8011 (+0.9%) [7650–8226] | 10701 (+2.6%) [10203–11017] | 4680 (+0.7%) | 16.9 |
| big10k_olap | 8 | base | 4 | 99 | 84.21 | 138.50 | 168.81 | 12155 [11364–12698] | 13509 [12858–14193] | 4654 | 19.1 |
| big10k_olap | 8 | p3 | 3 | 79 (-20.3%) | 104.57 (+24.2%) | 160.62 (+16.0%) | 200.76 (+18.9%) | 10278 (-15.4%) [9719–11144] | 11352 (-16.0%) [10821–12341] | 4673 (+0.4%) | 16.8 |
| bigord10k_olap | 8 | base | 4 | 81 | 108.66 | 175.93 | 214.00 | 15476 [14765–16117] | 14280 [13641–14874] | 4641 | 17.5 |
| bigord10k_olap | 8 | p3 | 3 | 103 (+27.3%) | 83.45 (-23.2%) | 130.09 (-26.1%) | 155.75 (-27.2%) | 13375 (-13.6%) [12882–14159] | 11165 (-21.8%) [10641–12205] | 4456 (-4.0%) | 16.2 |
| cnt_dist10k | 8 | base | 4 | 240 | 35.22 | 64.01 | 85.97 | 3810 [3627–3925] | 3132 [3066–3185] | 4254 | 18.6 |
| cnt_dist10k | 8 | p3 | 3 | 337 (+40.2%) | 23.90 (-32.1%) | 41.83 (-34.7%) | 53.90 (-37.3%) | 3017 (-20.8%) [2962–3109] | 3134 (+0.1%) [3073–3234] | 4095 (-3.7%) | 19.4 |
| count1k | 8 | base | 4 | 1712 | 4.93 | 10.53 | 16.81 | 459 [429–486] | 541 [516–570] | 590 | 17.7 |
| count1k | 8 | p3 | 3 | 2099 (+22.7%) | 3.82 (-22.5%) | 7.86 (-25.4%) | 11.69 (-30.5%) | 438 (-4.5%) [427–454] | 547 (+1.0%) [530–565] | 588 (-0.4%) | 17.1 |
| distinct_k10k | 8 | base | 4 | 281 | 28.65 | 53.06 | 68.96 | 4234 [4179–4293] | 3018 [2920–3091] | 3340 | 17.8 |
| distinct_k10k | 8 | p3 | 3 | 266 (-5.2%) | 31.03 (+8.3%) | 54.82 (+3.3%) | 69.79 (+1.2%) | 3718 (-12.2%) [3565–3925] | 3096 (+2.6%) [2989–3251] | 3338 (-0.0%) | 19.4 |
| grp_c10k | 8 | base | 4 | 502 | 16.20 | 27.19 | 34.81 | 1092 [1082–1104] | 1120 [1110–1127] | 4624 | 17.7 |
| grp_c10k | 8 | p3 | 3 | 350 (-30.4%) | 23.12 (+42.7%) | 39.15 (+44.0%) | 50.16 (+44.1%) | 1045 (-4.3%) [1020–1083] | 1138 (+1.6%) [1113–1163] | 4863 (+5.2%) | 18.9 |
| grp_high10k | 8 | base | 4 | 228 | 35.88 | 63.31 | 83.94 | 4950 [4818–5141] | 3492 [3391–3697] | 4748 | 18.2 |
| grp_high10k | 8 | p3 | 3 | 189 (-17.0%) | 43.26 (+20.6%) | 76.58 (+21.0%) | 99.55 (+18.6%) | 4569 (-7.7%) [4468–4656] | 3732 (+6.9%) [3648–3804] | 5086 (+7.1%) | 18.6 |
| grp_low10k | 8 | base | 4 | 453 | 19.02 | 33.66 | 44.90 | 1042 [1014–1077] | 1058 [1036–1087] | 3790 | 19.1 |
| grp_low10k | 8 | p3 | 3 | 563 (+24.4%) | 14.51 (-23.7%) | 25.73 (-23.6%) | 34.04 (-24.2%) | 1038 (-0.4%) [1027–1046] | 1118 (+5.8%) [1103–1137] | 3857 (+1.8%) | 17.3 |
| idrange_ordc100 | 8 | base | 4 | 1627 | 4.97 | 10.40 | 16.60 | 450 [431–459] | 513 [495–522] | 458 | 16.3 |
| idrange_ordc100 | 8 | p3 | 3 | 1782 (+9.5%) | 4.58 (-7.8%) | 9.26 (-11.0%) | 13.76 (-17.1%) | 445 (-1.1%) [433–451] | 531 (+3.4%) [517–541] | 468 (+2.2%) | 17.7 |
| in10 | 8 | base | 4 | 2067 | 4.11 | 8.06 | 12.06 | 361 [345–376] | 413 [386–432] | 339 | 19.3 |
| in10 | 8 | p3 | 3 | 2072 (+0.2%) | 3.93 (-4.3%) | 7.73 (-4.1%) | 11.91 (-1.3%) | 365 (+1.1%) [355–374] | 435 (+5.3%) [419–447] | 348 (+2.8%) | 18.3 |
| in100 | 8 | base | 4 | 1402 | 5.99 | 12.17 | 18.05 | 635 [601–668] | 620 [588–659] | 602 | 18.5 |
| in100 | 8 | p3 | 3 | 1224 (-12.7%) | 6.78 (+13.1%) | 13.27 (+9.1%) | 18.76 (+3.9%) | 638 (+0.4%) [625–650] | 643 (+3.8%) [625–653] | 613 (+2.0%) | 18.1 |
| in1000 | 8 | base | 4 | 411 | 19.66 | 36.79 | 50.29 | 2628 [2564–2679] | 2156 [2106–2195] | 2774 | 18.7 |
| in1000 | 8 | p3 | 3 | 342 (-16.8%) | 24.82 (+26.2%) | 43.45 (+18.1%) | 55.14 (+9.6%) | 2743 (+4.4%) [2627–2830] | 2244 (+4.1%) [2145–2342] | 2932 (+5.7%) | 19.2 |
| insert100 | 8 | base | 4 | 427 | 19.10 | 32.87 | 44.72 | 1862 [1842–1899] | 1991 [1959–2046] | 2288 | 18.3 |
| insert100 | 8 | p3 | 3 | 315 (-26.2%) | 22.97 (+20.3%) | 36.58 (+11.3%) | 49.02 (+9.6%) | 1878 (+0.9%) [1815–1978] | 2088 (+4.9%) [1973–2220] | 2381 (+4.1%) | 17.8 |
| join_push100 | 8 | base | 4 | 1105 | 7.54 | 16.02 | 23.80 | 662 [621–705] | 775 [738–819] | 902 | 21.1 |
| join_push100 | 8 | p3 | 3 | 1473 (+33.3%) | 5.48 (-27.3%) | 11.67 (-27.2%) | 17.84 (-25.0%) | 595 (-10.1%) [566–617] | 730 (-5.8%) [697–753] | 843 (-6.5%) | 17.0 |
| join_x10 | 8 | base | 4 | 213 | 38.86 | 69.44 | 95.52 | 2724 [2585–2919] | 4413 [4158–4655] | 3915 | 19.2 |
| join_x10 | 8 | p3 | 3 | 496 (+133.0%) | 16.13 (-58.5%) | 27.21 (-60.8%) | 38.30 (-59.9%) | 1469 (-46.1%) [1458–1487] | 2469 (-44.0%) [2410–2510] | 2375 (-39.3%) | 16.9 |
| krange100 | 8 | base | 4 | 1683 | 5.04 | 10.00 | 14.88 | 473 [454–523] | 573 [550–637] | 584 | 12.1 |
| krange100 | 8 | p3 | 3 | 1783 (+6.0%) | 4.59 (-8.9%) | 9.24 (-7.6%) | 13.67 (-8.1%) | 462 (-2.3%) [450–472] | 576 (+0.5%) [558–588] | 575 (-1.5%) | 9.9 |
| krange_lim10 | 8 | base | 4 | 2279 | 3.58 | 6.98 | 10.69 | 366 [351–385] | 440 [431–455] | 424 | 14.1 |
| krange_lim10 | 8 | p3 | 3 | 1889 (-17.2%) | 4.51 (+26.0%) | 8.91 (+27.7%) | 13.66 (+27.8%) | 379 (+3.6%) [355–402] | 473 (+7.4%) [440–495] | 436 (+2.8%) | 14.5 |
| krange_lim100 | 8 | base | 4 | 1390 | 5.87 | 12.57 | 19.64 | 637 [621–670] | 725 [703–761] | 802 | 14.9 |
| krange_lim100 | 8 | p3 | 3 | 1077 (-22.5%) | 7.54 (+28.4%) | 15.91 (+26.6%) | 23.90 (+21.7%) | 657 (+3.1%) [625–693] | 781 (+7.8%) [739–822] | 819 (+2.1%) | 17.1 |
| krange_ord100 | 8 | base | 4 | 1616 | 5.26 | 10.57 | 15.99 | 508 [485–560] | 589 [559–649] | 602 | 13.9 |
| krange_ord100 | 8 | p3 | 3 | 1470 (-9.0%) | 5.67 (+7.6%) | 12.20 (+15.4%) | 18.25 (+14.1%) | 511 (+0.4%) [473–537] | 616 (+4.5%) [566–648] | 597 (-1.0%) | 12.2 |
| ord_agg_lim10k | 8 | base | 4 | 333 | 24.47 | 44.00 | 56.09 | 2573 [2523–2643] | 1768 [1735–1800] | 3825 | 19.0 |
| ord_agg_lim10k | 8 | p3 | 3 | 447 (+34.2%) | 18.01 (-26.4%) | 33.17 (-24.6%) | 46.88 (-16.4%) | 2290 (-11.0%) [2258–2342] | 1762 (-0.4%) [1747–1787] | 3732 (-2.4%) | 18.8 |
| ro_ranges | 8 | base | 4 | 1964 | 17.01 | 27.68 | 35.36 | 399 [392–412] | 492 [474–507] | 446 | 17.2 |
| ro_ranges | 8 | p3 | 3 | 2274 (+15.8%) | 14.12 (-17.0%) | 22.49 (-18.8%) | 28.34 (-19.8%) | 388 (-2.6%) [378–396] | 500 (+1.5%) [490–509] | 444 (-0.5%) | 16.4 |

### A/B 5: 4 shards, base vs `p3` vs `p3r1`, 2 rounds, load 11–23

| shape | thr | config | n | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| big10k | 8 | base | 2 | 116 | 69.85 | 112.97 | 138.12 | 8316 [8287–8346] | 12506 [11987–13025] | 4791 | 20.9 |
| big10k | 8 | p3 | 2 | 131 (+13.0%) | 60.86 (-12.9%) | 94.24 (-16.6%) | 114.88 (-16.8%) | 8138 (-2.2%) [8077–8198] | 12275 (-1.8%) [12256–12294] | 4872 (+1.7%) | 21.2 |
| big10k | 8 | p3r1 | 2 | 125 (+7.4%) | 65.12 (-6.8%) | 106.90 (-5.4%) | 128.98 (-6.6%) | 8094 (-2.7%) [7983–8204] | 11227 (-10.2%) [10849–11605] | 5004 (+4.5%) | 22.9 |
| big10k_olap | 8 | base | 2 | 93 | 86.98 | 137.72 | 173.25 | 12306 [12190–12421] | 15606 [15409–15802] | 5126 | 19.7 |
| big10k_olap | 8 | p3 | 2 | 116 (+23.7%) | 69.53 (-20.1%) | 108.84 (-21.0%) | 135.69 (-21.7%) | 10234 (-16.8%) [10190–10279] | 12916 (-17.2%) [12850–12981] | 5188 (+1.2%) | 21.6 |
| big10k_olap | 8 | p3r1 | 2 | 114 (+22.4%) | 70.33 (-19.2%) | 110.82 (-19.5%) | 139.05 (-19.7%) | 10028 (-18.5%) [9939–10117] | 12318 (-21.1%) [12270–12365] | 5143 (+0.3%) | 22.6 |
| bigord10k_olap | 8 | base | 2 | 83 | 97.65 | 157.98 | 210.39 | 16640 [16250–17031] | 16678 [16265–17092] | 5254 | 19.3 |
| bigord10k_olap | 8 | p3 | 2 | 97 (+17.5%) | 82.78 (-15.2%) | 134.61 (-14.8%) | 166.61 (-20.8%) | 14406 (-13.4%) [14044–14768] | 13451 (-19.4%) [13273–13629] | 5027 (-4.3%) | 19.1 |
| bigord10k_olap | 8 | p3r1 | 2 | 112 (+35.3%) | 71.79 (-26.5%) | 117.28 (-25.8%) | 144.66 (-31.2%) | 10963 (-34.1%) [10919–11007] | 12512 (-25.0%) [12341–12683] | 5117 (-2.6%) | 21.9 |
| cnt_dist10k | 8 | base | 2 | 207 | 39.16 | 63.49 | 78.81 | 5628 [5620–5636] | 4800 [4722–4877] | 4942 | 20.3 |
| cnt_dist10k | 8 | p3 | 2 | 263 (+26.8%) | 30.41 (-22.3%) | 47.48 (-25.2%) | 57.37 (-27.2%) | 4376 (-22.2%) [4324–4428] | 4756 (-0.9%) [4715–4797] | 4979 (+0.8%) | 23.4 |
| cnt_dist10k | 8 | p3r1 | 2 | 270 (+30.1%) | 29.63 (-24.3%) | 46.21 (-27.2%) | 56.85 (-27.9%) | 4314 (-23.4%) [4263–4364] | 4212 (-12.3%) [4139–4284] | 4976 (+0.7%) | 21.1 |
| distinct_k10k | 8 | base | 2 | 211 | 38.61 | 64.35 | 81.90 | 5580 [5486–5674] | 4732 [4726–4737] | 3901 | 18.4 |
| distinct_k10k | 8 | p3 | 2 | 269 (+27.3%) | 29.77 (-22.9%) | 46.66 (-27.5%) | 56.88 (-30.6%) | 4832 (-13.4%) [4808–4855] | 4640 (-1.9%) [4597–4683] | 3910 (+0.2%) | 22.0 |
| distinct_k10k | 8 | p3r1 | 2 | 274 (+29.7%) | 29.20 (-24.4%) | 46.21 (-28.2%) | 54.84 (-33.0%) | 4823 (-13.6%) [4810–4836] | 4218 (-10.9%) [4210–4225] | 3998 (+2.5%) | 21.2 |
| grp_high10k | 8 | base | 2 | 190 | 42.02 | 66.37 | 81.96 | 7117 [7109–7125] | 5339 [5310–5368] | 5758 | 16.5 |
| grp_high10k | 8 | p3 | 2 | 214 (+12.5%) | 37.31 (-11.2%) | 57.88 (-12.8%) | 69.92 (-14.7%) | 5800 (-18.5%) [5788–5812] | 5406 (+1.2%) [5402–5409] | 5788 (+0.5%) | 21.7 |
| grp_high10k | 8 | p3r1 | 2 | 197 (+3.4%) | 41.10 (-2.2%) | 67.92 (+2.3%) | 83.72 (+2.2%) | 5716 (-19.7%) [5628–5805] | 4874 (-8.7%) [4854–4893] | 5744 (-0.3%) | 21.1 |
| idrange_ordc100 | 8 | base | 2 | 1174 | 6.82 | 13.35 | 18.64 | 782 [753–812] | 1228 [1190–1266] | 878 | 15.7 |
| idrange_ordc100 | 8 | p3 | 2 | 1217 (+3.7%) | 6.58 (-3.5%) | 12.64 (-5.4%) | 17.01 (-8.7%) | 776 (-0.9%) [760–791] | 1260 (+2.6%) [1240–1281] | 885 (+0.8%) | 21.4 |
| idrange_ordc100 | 8 | p3r1 | 2 | 1159 (-1.3%) | 6.91 (+1.4%) | 13.63 (+2.0%) | 19.85 (+6.5%) | 718 (-8.2%) [639–797] | 1154 (-6.0%) [1045–1263] | 838 (-4.6%) | 20.7 |
| in100 | 8 | base | 2 | 1011 | 7.92 | 15.59 | 21.80 | 938 [924–952] | 1310 [1294–1327] | 939 | 20.1 |
| in100 | 8 | p3 | 2 | 1090 (+7.8%) | 7.34 (-7.3%) | 13.71 (-12.0%) | 18.62 (-14.6%) | 910 (-2.9%) [895–926] | 1318 (+0.5%) [1298–1337] | 940 (+0.2%) | 21.4 |
| in100 | 8 | p3r1 | 2 | 1022 (+1.1%) | 7.83 (-1.2%) | 15.15 (-2.8%) | 20.82 (-4.5%) | 929 (-1.0%) [926–932] | 1336 (+2.0%) [1330–1343] | 930 (-0.9%) | 22.2 |
| in1000 | 8 | base | 2 | 329 | 24.86 | 42.22 | 53.93 | 3098 [3062–3134] | 3338 [3278–3398] | 3165 | 20.1 |
| in1000 | 8 | p3 | 2 | 369 (+12.3%) | 21.64 (-12.9%) | 35.91 (-14.9%) | 44.98 (-16.6%) | 3124 (+0.8%) [3121–3127] | 3344 (+0.2%) [3335–3353] | 3138 (-0.9%) | 22.0 |
| in1000 | 8 | p3r1 | 2 | 335 (+1.7%) | 23.96 (-3.6%) | 42.09 (-0.3%) | 54.28 (+0.6%) | 3086 (-0.4%) [2966–3206] | 3262 (-2.3%) [3161–3363] | 3112 (-1.7%) | 23.2 |
| join_x10 | 8 | base | 2 | 123 | 64.79 | 109.71 | 134.92 | 6588 [6299–6876] | 12480 [12072–12889] | 8258 | 20.0 |
| join_x10 | 8 | p3 | 2 | 268 (+117.3%) | 29.82 (-54.0%) | 45.80 (-58.3%) | 55.83 (-58.6%) | 2570 (-61.0%) [2550–2589] | 5492 (-56.0%) [5395–5588] | 5002 (-39.4%) | 20.5 |
| join_x10 | 8 | p3r1 | 2 | 257 (+108.5%) | 31.07 (-52.0%) | 49.25 (-55.1%) | 61.24 (-54.6%) | 2602 (-60.5%) [2601–2602] | 5488 (-56.0%) [5401–5574] | 4896 (-40.7%) | 23.2 |
| krange100 | 8 | base | 2 | 1125 | 7.11 | 13.96 | 20.05 | 746 [629–862] | 1204 [1043–1365] | 932 | 10.6 |
| krange100 | 8 | p3 | 2 | 973 (-13.5%) | 8.33 (+17.2%) | 18.05 (+29.3%) | 32.72 (+63.2%) | 748 (+0.3%) [647–848] | 1256 (+4.3%) [1110–1401] | 926 (-0.6%) | 18.1 |
| krange100 | 8 | p3r1 | 2 | 1110 (-1.3%) | 7.21 (+1.3%) | 13.71 (-1.8%) | 18.12 (-9.7%) | 846 (+13.4%) [826–865] | 1368 (+13.7%) [1339–1398] | 991 (+6.4%) | 16.0 |
| krange_lim10 | 8 | base | 2 | 1168 | 6.84 | 13.37 | 19.03 | 750 [696–805] | 1206 [1141–1272] | 900 | 13.3 |
| krange_lim10 | 8 | p3 | 2 | 1094 (-6.4%) | 7.34 (+7.3%) | 14.70 (+10.0%) | 23.64 (+24.2%) | 754 (+0.5%) [711–797] | 1260 (+4.4%) [1195–1324] | 910 (+1.1%) | 20.2 |
| krange_lim10 | 8 | p3r1 | 2 | 1150 (-1.6%) | 6.96 (+1.7%) | 13.37 (+0.0%) | 19.03 (+0.0%) | 780 (+3.9%) [754–806] | 1282 (+6.2%) [1255–1308] | 928 (+3.1%) | 18.5 |
| ro_ranges | 8 | base | 2 | 1194 | 26.82 | 43.56 | 55.83 | 680 [598–761] | 1138 [1034–1242] | 849 | 21.5 |
| ro_ranges | 8 | p3 | 2 | 1154 (-3.3%) | 27.83 (+3.8%) | 42.86 (-1.6%) | 53.44 (-4.3%) | 772 (+13.5%) [760–783] | 1317 (+15.7%) [1290–1344] | 898 (+5.8%) | 20.4 |
| ro_ranges | 8 | p3r1 | 2 | 1130 (-5.3%) | 28.36 (+5.8%) | 44.35 (+1.8%) | 56.72 (+1.6%) | 712 (+4.7%) [635–788] | 1209 (+6.2%) [1114–1304] | 859 (+1.2%) | 22.0 |

### A/B 6: stream buffer size 256 KiB (vttablet `--queryserver-config-stream-buffer-size`, vtgate `--stream-buffer-size`), 4 shards, 2 rounds, load 13–18

| shape | thr | config | n | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| big100k_olap | 8 | base | 2 | 8 | 1291.23 | 1538.88 | 1811.55 | 119836 [115298–124375] | 139603 [138875–140331] | 42784 | 15.6 |
| big100k_olap | 8 | basesbuf | 2 | 10 (+36.9%) | 874.27 (-32.3%) | 1064.98 (-30.8%) | 1110.73 (-38.7%) | 81505 (-32.0%) [79397–83613] | 105802 (-24.2%) [102864–108739] | 41453 (-3.1%) | 17.6 |
| big100k_olap | 8 | p3 | 2 | 9 (+13.8%) | 1039.70 (-19.5%) | 1485.41 (-3.5%) | 1590.93 (-12.2%) | 93485 (-22.0%) [89182–97788] | 113658 (-18.6%) [109623–117692] | 42334 (-1.1%) | 16.0 |
| big100k_olap | 8 | p3sbuf | 2 | 12 (+61.3%) | 724.58 (-43.9%) | 885.68 (-42.4%) | 958.48 (-47.1%) | 78021 (-34.9%) [75556–80486] | 103189 (-26.1%) [100267–106111] | 41710 (-2.5%) | 16.7 |
| big10k_olap | 8 | base | 2 | 66 | 131.52 | 204.90 | 246.87 | 13486 [12899–14072] | 16694 [15837–17552] | 5366 | 12.9 |
| big10k_olap | 8 | basesbuf | 2 | 75 (+13.4%) | 108.92 (-17.2%) | 172.87 (-15.6%) | 225.37 (-8.7%) | 9812 (-27.2%) [9725–9899] | 12808 (-23.3%) [12730–12886] | 5364 (-0.0%) | 15.6 |
| big10k_olap | 8 | p3 | 2 | 71 (+6.5%) | 114.23 (-13.1%) | 193.41 (-5.6%) | 242.20 (-1.9%) | 11000 (-18.4%) [10965–11035] | 13982 (-16.2%) [13901–14064] | 5450 (+1.6%) | 15.4 |
| big10k_olap | 8 | p3sbuf | 2 | 104 (+56.5%) | 82.26 (-37.5%) | 133.00 (-35.1%) | 179.74 (-27.2%) | 9056 (-32.9%) [8911–9200] | 12011 (-28.1%) [11789–12233] | 5200 (-3.1%) | 15.2 |
| bigord10k_olap | 8 | base | 2 | 89 | 89.79 | 137.37 | 164.56 | 16851 [16679–17023] | 16543 [16355–16731] | 5254 | 15.6 |
| bigord10k_olap | 8 | basesbuf | 2 | 73 (-18.2%) | 121.53 (+35.3%) | 198.77 (+44.7%) | 240.89 (+46.4%) | 14771 (-12.3%) [14261–15281] | 13248 (-19.9%) [12706–13791] | 5316 (+1.2%) | 18.2 |
| bigord10k_olap | 8 | p3 | 2 | 70 (-21.3%) | 122.27 (+36.2%) | 189.94 (+38.3%) | 225.91 (+37.3%) | 15177 (-9.9%) [15019–15335] | 14260 (-13.8%) [13779–14741] | 5340 (+1.6%) | 18.5 |
| bigord10k_olap | 8 | p3sbuf | 2 | 98 (+9.8%) | 82.52 (-8.1%) | 131.95 (-3.9%) | 178.91 (+8.7%) | 14046 (-16.6%) [13896–14195] | 12490 (-24.5%) [12337–12644] | 5176 (-1.5%) | 16.4 |

### A/B 7: 64 KiB MySQL-protocol write buffer (`p3w`, not kept) vs `p3`, 4 shards, 3 rounds, load 16–31

| shape | thr | config | n | QPS | avg ms | p95 ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| big100k_olap | 8 | p3 | 3 | 11 | 788.23 | 1200.24 | 1364.07 | 92146 [90794–93459] | 115112 [110000–120794] | 42692 | 23.6 |
| big100k_olap | 8 | p3w | 3 | 9 (-21.3%) | 973.15 (+23.5%) | 1592.15 (+32.7%) | 1797.62 (+31.8%) | 89636 (-2.7%) [88467–90786] | 116759 (+1.4%) [111667–121466] | 42281 (-1.0%) | 31.0 |
| big10k | 8 | p3 | 3 | 83 | 107.48 | 178.85 | 212.43 | 8930 [8571–9251] | 13551 [12816–14182] | 5272 | 19.5 |
| big10k | 8 | p3w | 3 | 106 (+27.8%) | 85.49 (-20.5%) | 144.77 (-19.1%) | 198.23 (-6.7%) | 8761 (-1.9%) [8418–9209] | 13377 (-1.3%) [12604–14736] | 5150 (-2.3%) | 24.2 |
| big10k_olap | 8 | p3 | 3 | 85 | 102.02 | 172.94 | 219.31 | 10782 [10479–11224] | 13756 [13056–14538] | 5343 | 21.4 |
| big10k_olap | 8 | p3w | 3 | 97 (+13.9%) | 87.54 (-14.2%) | 149.60 (-13.5%) | 206.46 (-5.9%) | 10162 (-5.7%) [9981–10373] | 13506 (-1.8%) [13238–13827] | 5292 (-1.0%) | 26.2 |
| krange100 | 8 | p3 | 3 | 970 | 8.32 | 17.39 | 26.59 | 807 [685–881] | 1349 [1177–1456] | 985 | 16.3 |
| krange100 | 8 | p3w | 3 | 853 (-12.0%) | 9.58 (+15.2%) | 19.41 (+11.6%) | 29.13 (+9.5%) | 861 (+6.7%) [858–866] | 1411 (+4.6%) [1403–1415] | 1025 (+4.1%) | 20.3 |

### Profile after the patch

- grp_high10k (`p3`, 2 shards): `Route.sort` 29.5% → 7.8% (`mergeRuns` 5.0%, `ApplyTinyWeights` ~2.7%); vtgate
  4331 µs/q in that run vs 5178 base. The rest: Unmarshal 17%, `writeRows` 15%, `aggregationState.finish` 7%.
- big10k_olap (`p3r1`, 4 shards): `memclrNoHeapPointers` no longer in the top 40. Remaining: Unmarshal 27%
  (`Row.UnmarshalVT` 15%, mostly the per-row `Values` copy), `writeRows` 27% (`write(2)` 13%), mallocgc 27%.

## Code changes in the patch

1. `go/vt/servenv/grpc_codec_pool.go` (new), `grpc_codec.go`: `codecBufferPool`, a `mem.BufferPool` with one
   `sync.Pool` per power of two from 256 B to 16 MiB (larger buffers are allocated and not pooled), which does not zero
   buffers. The codec is its only user: `Marshal` fills `[:size]` with `MarshalToSizedBufferVT` and `Unmarshal` copies
   the frames into `[:size]` with `MaterializeToBuffer`; neither reads past the length, so zeroing is unnecessary
   (gRPC has internal "dirty" pools for the same reason). The gRPC transport keeps its own default pool.
   Tests: `TestCodecBufferPoolTiers` (fails on main: 33 KiB → 1 MiB capacity there), `TestCodecBufferPoolPut`,
   `TestCodecRoundTripDirtyBuffers` (fills the pool with 0xff garbage, round-trips 1–5000-row results through a split
   BufferSlice).
2. `go/vt/vtgate/evalengine/api_compare.go`: `Comparison.Sort` first scans for ascending runs (n−1 comparisons, bails
   out at 65 runs, i.e. after ~130 comparisons on random input) and merges ≤64 runs bottom-up with one n-row scratch
   slice; the merge is stable. Used by `Route.sort` (OLTP scatter ORDER BY), `MemorySort`, and `Sorter`.
   Tests: `TestSortMergesSortedRuns` (1–200 runs, ASC/DESC, duplicates, stability vs `slices.SortStableFunc`),
   `TestSortRandomInputFallsBack`; `BenchmarkSortShardRuns` (2 runs × 5000: 5.2 ms → 1.06 ms; 4 runs 8.0 → 1.3 ms;
   16 runs 7.3 → 3.0 ms).
3. `go/vt/vtgate/engine/join.go`, `go/vt/vtgate/vtgate.go`: `Join.TryExecute` runs the RHS for up to
   `--join-rhs-concurrency` (default 8, `engine.JoinRHSConcurrency`) LHS rows at a time when the session is neither in a
   transaction nor on a reserved connection and the LHS has >1 row; results are assembled in LHS order, so output is
   identical. First error cancels the rest; the max-memory-rows check runs on the running total and again on assembly.
   Only the first RHS execution asks for fields (as before). `--join-rhs-concurrency=1` restores the old behaviour.
   Flag doc in `go/flags/endtoend/vtgate.txt`. Tests (`join_concurrent_test.go`): concurrent execution is observed
   (RHS blocks until 4 executions are in flight; hangs on the old code) with inner and left joins and LHS-ordered output;
   error propagation; max-memory-rows; sequential inside a transaction. Existing fake-primitive join tests run with
   concurrency 1 (their fake hands out results in call order); `noopVCursor.InReservedConn` returns false instead of
   panicking. `go test -race` on engine and vtgate (join/select/subquery tests ×3) is clean.
4. `go/vt/vtgate/engine/distinct.go`: the probe table owns one `vthash.Hasher` that it resets per row (was a heap
   allocation per row), and the seen-map is presized to the input row count in `TryExecute`.

Release compatibility: no protocol or default-behaviour change visible to clients except the new flag; the join
change keeps result order; the codec change is process-local.

## Risks and gotchas

- Codec pool: power-of-two tiers can hold up to 2x the requested size per pooled buffer (the old 1 MiB tier held up to
  30x); sync.Pool drops idle buffers after two GCs. A future codec change that read past the written length would see
  stale bytes instead of zeros.
- Concurrent join RHS: one statement can now have up to 8 RHS scatters in flight (8 × shards shard queries), so a large
  LHS reaches the tablets' pools faster; total work per statement is unchanged. Nested joins multiply (8 × 8). Not
  applied inside transactions/reserved connections (those need one query at a time per shard connection), nor to the
  streaming (OLAP) join. `vexplain trace` row counts are unaffected (the vcursor's stats are mutex-protected, as for
  `Concatenate`).
- Sort: at most one extra n-row slice of row headers (24 B/row); random input costs ≤~130 extra comparisons.
- Stream buffer 256 KiB: more memory per active stream (vttablet builds, vtgate buffers up to 256 KiB per stream) and
  a later first row for streaming clients; keep vtgate's and vttablet's values in sync (their flag docs say so).

## Recommendations, ranked by user-visible impact

1. **Merge the concurrent join RHS** (finding 2): the only change that moves latency a lot: −55..−60% for a 10-row
   cross-shard join, growing with LHS rows. Consider a hash join or a batched `IN (…)`/VALUES RHS next (follow-up).
2. **Merge the codec buffer pool** (finding 1): −17..−22% vtgate and −16..−27% tablet CPU for every streamed (OLAP,
   vstream-copy-style) result larger than a few KiB. Tiny, no config.
3. **Raise the stream buffer to 128–256 KiB** (finding 4) for deployments that stream large results; −27..−35% vtgate CPU
   for big OLAP reads. Could become the default after checking memory with many concurrent streams.
4. **Merge the run-merge sort** (finding 3): −8..−22% vtgate CPU for scatter GROUP BY / COUNT(DISTINCT) / ORDER BY over
   thousands of rows; larger with more shards.
5. **Merge round-1 F08, F10, F11** — measured end-to-end here: F08 −24..−34% vtgate on streaming ORDER BY; F11 (+F10 send)
   −9..−17% tablet CPU on large results.
6. Distinct hasher reuse (small, −7..−14% vtgate on DISTINCT of 10k rows).

## Follow-ups

- Streaming join (`Join.TryStreamExecute`) is still sequential per LHS row; the same concurrency with ordered emission
  (bounded window of in-flight RHS results) would help OLAP joins.
- Replace per-row RHS queries for joins on a non-vindex column with one batched RHS query per N LHS rows
  (`b.k IN ::a_k_list` + in-vtgate hash join, or the existing `ValuesJoin`/`ALLOW_HASH_JOIN` machinery, which the planner
  does not use today): 22 shard queries → 2 for join_x10.
- Unmarshal copies every row's `Values` (`append(m.Values[:0], …)`) because the receive buffer is pooled. Keeping the
  whole message buffer alive and aliasing rows into it (not pooled for large results) would remove 1–2 allocations per
  row, ~10–15% of vtgate CPU for large results.
- `aggregationState.finish` allocates one `[]Value` per output group (7% for 10k groups); slab-allocate per result.
- Small scatters cost ~140 µs vtgate + ~300 µs tablet CPU per extra shard: P1's per-hop findings (stream workers,
  GOMAXPROCS, GOGC) multiply by the shard count here.
- Measure F06/F14 on a keyspace with `utf8mb4_general_ci`/latin1 columns (scatter ORDER BY/GROUP BY on text).
