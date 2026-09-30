# ISSUES-D: existing vitessio/vitess issues and PRs for the performance roadmap

Source: `perf-findings/ROADMAP.md` (general waves 1–3, strategic projects, fixed-waits table, VReplication R1–R22).
Searches: only `repo:vitessio/vitess`, issues (semantic search) and PRs (keyword search), open and closed, as of 2026-09-30.
The semantic issue search was heavily rate-limited, so for some lower-ranked items only one or two searches ran.
I also swept every issue and PR labelled `Type: Performance` created since 2026-01, and all open `Type: Performance` issues.
Read-only: nothing was posted or changed on GitHub.

Verdicts: **EXISTING** means an issue or PR proposes the same change. **RELATED** means overlapping or adjacent work. **NONE** means nothing found.
Items 1 and R1 are correctness bugs and are out of scope; they are listed only for completeness.

## General roadmap

| Roadmap item | Verdict | Matches | Note |
|---|---|---|---|
| Wave 1 #1 Correctness bugs | (out of scope) RELATED | #11351 "Feature Request: cache line optimized CountMin4", open, https://github.com/vitessio/vitess/issues/11351 · #19967 "discovery: cap health check reconnect backoff at 10s", merged, https://github.com/vitessio/vitess/pull/19967 | #11351 is about the same plan-cache sketch (F01), but it asks for a cache-line layout, not the reset/`indexOf` fix. #19967 is cited in BUGS.md (bug 38 refuted); it's merged. |
| Wave 1 #2 Config guidance (gRPC windows, GOMAXPROCS, GOGC/GOMEMLIMIT, stream buffers) | RELATED | #4302 "Investigate setting $GOMAXPROCS to Linux limits", open, https://github.com/vitessio/vitess/issues/4302 · #16244 "Investigate GOGC performance impact", open, https://github.com/vitessio/vitess/issues/16244 · #3929 "gRPC settings improvement", open, https://github.com/vitessio/vitess/issues/3929 | #3929 explicitly proposes static `InitialWindowSize`/`InitialConnWindowSize` instead of BDP. These are good homes for the P1/H1/P7 numbers. No docs PR exists. |
| Wave 1 #3 Time-critical outage windows (WaitForPos, PRS semi-sync, mysqld shutdown, tablet picker backoff) | NONE | (adjacent: #17871 "SwitchWrites: improve performance of sequence initialisation steps", open, https://github.com/vitessio/vitess/issues/17871 · #16536 "Retry VStream errors with exponential backoff…", closed unmerged, https://github.com/vitessio/vitess/pull/16536) | Nothing on the WaitForPos 1 s poll, the DemotePrimary semi-sync step, or the mysqld shutdown wait. #16536 is VStream retry in vtgate, not the vreplication tablet picker. |
| Wave 1 #4 Autocommit UPDATE/DELETE by PK: 3 round trips → 1 | NONE | – | Searched UpdateLimit/execAsTransaction, autocommit round trips. |
| Wave 1 #5 vttablet gRPC stream workers + P1 micro fixes + P6 mutex removals | RELATED (partly done) | #20313 "grpc: use fixed pool of stream workers", merged 2026-06-16, https://github.com/vitessio/vitess/pull/20313 · #20148 "perf: reduce query path overhead by ~12% (vtgate) and ~3% (gRPC)", closed unmerged (stale), https://github.com/vitessio/vitess/pull/20148 · #19652 "stats: reduce counter lock contention with RWMutex and atomics", closed, https://github.com/vitessio/vitess/pull/19652 | #20313 already sets `NumStreamWorkers(GOMAXPROCS)`. P1 builds on it: max(GOMAXPROCS, 64) plus a `--grpc-server-num-stream-workers` flag. #20148 was an unverified AI batch of per-query micro fixes that overlaps P1. |
| Wave 1 #6 Concurrent join RHS | NONE | – | (#20444 "avoid redundant right-side GetFields in streaming joins" is a different optimisation) |
| Wave 1 #7 Big/scatter reads: codec non-zeroing pool, MergeSort chunking (F08), pre-sorted merge, DISTINCT hasher reuse | EXISTING (partial) | #20952 "vtgate: merge ordered scatter result runs", open, https://github.com/vitessio/vitess/pull/20952 (fixes #20951 "VTGate: merge ordered non-streaming scatter results without a full sort", open, https://github.com/vitessio/vitess/issues/20951) · #20670 "vtgate: batch merge-sort rows instead of streaming them one at a time", closed unmerged (stale), https://github.com/vitessio/vitess/pull/20670 · #19398 "vtgate: replace heap with tournament tree for k-way merging", merged, https://github.com/vitessio/vitess/pull/19398 | #20952 is the same idea as the P3 pre-sorted merge (non-streaming `Route` sort). #20670 is the same as F08 (MergeSort batching). #19398 is already in base. Nothing for the codec buffer pool or DISTINCT hasher reuse. |
| Wave 1 #8 Bulk sharded inserts (F09 per-row AST cache, F04 shard binary search) | NONE | – | |
| Wave 2 #9 Pooled `ExecuteStream` (bidi stream pool per tablet) | RELATED | #20215 "StreamExecuteRaw: bidirectional *Raw streaming RPCs (no stream pool)", closed unmerged (draft, stale), https://github.com/vitessio/vitess/pull/20215 | #20215 made the RPCs bidi so that "a later change can add stream pooling". No issue or PR for a pooled Execute. |
| Wave 2 #10 Raw MySQL rows for large results | EXISTING (stalled) | #20215 (above), closed 2026-07-10, unmerged · #19620 "Add StreamExecuteRaw gRPC for zero-parse MySQL streaming", closed 2026-07-08, unmerged, https://github.com/vitessio/vitess/pull/19620 · #17172 "VTTablet allocation reduction", open, https://github.com/vitessio/vitess/issues/17172 · #17135 "VTTablet: Transmit raw MySQL packets for Fields and Rows over wire with grpc buffer", closed unmerged, https://github.com/vitessio/vitess/pull/17135 | Both PRs the roadmap cites were closed by the stale bot, not rejected. #17172 is an open tracking issue for the same idea. |
| Wave 2 #11 VReplication CPU bundle (F05, F16, F17, F26, F27, F13, packet size) | RELATED | #21288 "replication: Mysql56GTID.String() allocates three or four times per call through fmt.Sprintf", open, https://github.com/vitessio/vitess/issues/21288 → PR #21290, open draft, https://github.com/vitessio/vitess/pull/21290 | Same area as F26 (GTID formatting on the per-trx path). #21290 fixes the single-GTID `String()`; F26 also covers `Mysql56GTIDSet.String`/`EncodePosition`/`AddGTID`. Nothing for F05/F16/F17/F27/F13. |
| Wave 2 #12 Row-path allocation bundle (F11, F10, F15, F19, P6 idle pool ticker) | RELATED | #20492 / #20671 "tabletserver: pool the proto messages of streamed query results", closed unmerged, https://github.com/vitessio/vitess/pull/20492, https://github.com/vitessio/vitess/pull/20671 · #19651 "stats: zero-allocation CountersWithMultiLabels via xxhash + atomics", closed unmerged, https://github.com/vitessio/vitess/pull/19651 · #20136 "smartconnpool: bulk sweep idle stacks", merged, https://github.com/vitessio/vitess/pull/20136 · #17172 (above) | #20492 overlaps F10's send side; #19651 overlaps F19. #20136 touched the same expire worker, but it doesn't park the 100 ms ticker the way P6 proposes. |
| Wave 2 #13 Backups (F07 pooled pargzip, zstd default) | RELATED | #7802 "Backup/restore performance issues", open, https://github.com/vitessio/vitess/issues/7802 · #8175 "Pluggable compression algorithms", open, https://github.com/vitessio/vitess/issues/8175 · history: #7037, #8174, #11029 "revert default compression engine", closed, https://github.com/vitessio/vitess/pull/11029 | #7802 suggests moving to zstd. #11029 shows an earlier default change was reverted, so the default switch needs staging. Nothing for the F07 pargzip fork. |
| Wave 2 #14 Sequence block caching | NONE | – | |
| Wave 2 #15 Upstream asks (grpc-go, Go runtime, goyacc) | NONE (in vitess) | – | These are upstream asks and I searched only vitessio/vitess; nothing tracks them here. |
| Wave 3 #16 Round-1 byte-level bundle | RELATED | #21257 "POC: SIMD fast paths on the hot query path", open draft, https://github.com/vitessio/vitess/pull/21257 · #19695 "sqlparser: faster parsing via slab allocations", closed unmerged (stale), https://github.com/vitessio/vitess/pull/19695 | #21257's scalar rewrites overlap F03 (EncodeSQL −79%), F20 (BIT literal table) and F02 (tokenizer scan). It also overlaps the "GOEXPERIMENT=simd not worth it" finding. #19695 includes a flat keyword hash table (F24). |
| Wave 3 #17 Behaviour/semantics review (F25 flush timer, PAD SPACE) | NONE | – | |
| Strategic #18 Dedicated pooled TCP transport | NONE | – | |
| Strategic #19 In-process co-located tablets | NONE | – | |
| Config/ops guidance (enable-buffer, TLS ECDSA, conn read buffers, zstd backups, TWOPC, large reads) | NONE | – | Docs-only items. |
| Fixed waits: Online DDL 1-minute review tick | NONE | – | |
| Fixed waits: `VDiff create --wait` polling | NONE | (#10799 "VDiff2: Add --wait flag…", closed, https://github.com/vitessio/vitess/pull/10799, the origin of the flag) | |
| Fixed waits: VTOrc poll flags | RELATED | #19686 "VTOrc: Leverage gossip protocol to ERS when primary tablet is down", closed, https://github.com/vitessio/vitess/pull/19686 | Push-based detection, which is the type-3 direction the roadmap prefers. Nothing on the off-by-one. |

## VReplication roadmap

| Roadmap item | Verdict | Matches | Note |
|---|---|---|---|
| R1 Correctness bugs | (out of scope) RELATED | #20959 "fix(vstreamer): bound rowstreamer per-stream row buffer retention", open, https://github.com/vitessio/vitess/pull/20959 · #19878 / #19916 JSON OOM fixes, closed | Cited in BRIEF3/V1. |
| R2 Skip unneeded per-table copy catch-up (MoveTables + VStream copy) | NONE | (#13137 "MoveTables: allow copying all tables in a single atomic copy phase cycle", closed, https://github.com/vitessio/vitess/pull/13137) | Atomic copy avoids per-table cycles, but only in a different mode. |
| R3 SwitchTraffic outage (trailing sleep, ReloadSchema, 2nd allowTargetWrites, WaitForPos backoff) | NONE | (#17871 "SwitchWrites: improve performance of sequence initialisation steps", open, https://github.com/vitessio/vitess/issues/17871) | #17871 is another SwitchWrites latency step, not the same change. |
| R4 Many-table workflows (plan builder, PK query, GetSchema by table) | NONE | – | |
| R5 VDiff byte-equal fast path + direct row pipeline | RELATED | #19735 "RFC: VDiff3 - a checksum-based, streaming diff", open, https://github.com/vitessio/vitess/issues/19735 | A different approach to VDiff cost; R5 is an incremental fix to the current VDiff. |
| R6 Online DDL copy: JSON as text, deferred secondary keys | NONE | (#11700 "VReplication: Defer Secondary Index Creation", closed 2022, https://github.com/vitessio/vitess/pull/11700) | #11700 added deferral for MoveTables only; R6 extends it to Online DDL. |
| R7 gRPC codec non-zeroing buffer pool | NONE | (#16790 "grpc: upgrade to 1.66.2 and use Codec v2", closed, https://github.com/vitessio/vitess/pull/16790) | Nothing on buffer zeroing or pool tiers. |
| R8 Ops guidance (defer-secondary-keys=false, heartbeat interval, packet size, per-table workflows) | RELATED | #7659 "Make the frequency at which heartbeats update the _vt.vreplication table configurable", closed 2021, https://github.com/vitessio/vitess/pull/7659 | That PR added the flag; there's no docs issue. |
| R9 Bulk UPDATE for multi-row UPDATE events | NONE | (#17166 "VReplication: Optimize replication on target tablets", merged 2024, https://github.com/vitessio/vitess/pull/17166) | VPlayerBatching (bulk INSERT/DELETE) is in base; bulk UPDATE isn't. |
| R10 VStream running-phase bundle | EXISTING | #18273 "Feature Request: Increase max throughput of VStream", open, https://github.com/vitessio/vitess/issues/18273 · #21287 "vstreamer: EventGtid is formatted once per binlog event instead of once per GTID", open, https://github.com/vitessio/vitess/issues/21287 → PR #21289, open, https://github.com/vitessio/vitess/pull/21289 | #18273 asks to remove vtgate's deep clone of ROW events (V6 #2). #21289 is "GTID string once". Nothing yet for lazy SizeVT or coalescing. |
| R11 VStream `batch_copy_rows` | RELATED | #18273 (above) | Same goal (VStream copy throughput), but a different mechanism. |
| R12 Coalesce empty/filtered transactions | NONE | – | |
| R13 Buffer row changes across consecutive transactions | NONE | (#17166 above) | |
| R14 Source-side per-stream cost (GTID string once, keyrange pre-filter) | EXISTING (GTID part) | #21287 / PR #21289 (above), open | The keyrange pre-filter before full row decode has no match. |
| R15 Round-1 VReplication CPU bundle | RELATED | #21288 / PR #21290 (above), open | F26-adjacent. |
| R16 Parallel applier #19535 | EXISTING | #19535 "VReplication: Implement Experimental Parallel Applier", open (not draft; review requested; last updated 2026-09-16), https://github.com/vitessio/vitess/pull/19535 | V2's adaptive batching cap would be review feedback on this PR. |
| R17 JSON number types consistent across copy/running | NONE | – | (#20722 "mysql/json: read numbers the way MySQL does", merged, is evalengine parsing, not VReplication.) |
| R18 Shared binlog reader per source tablet | EXISTING | #7437 "VReplication: pooling vstreamers on source tablets", open since 2021, https://github.com/vitessio/vitess/issues/7437 | Same design: single producer, many consumers. Labelled Type: Performance, assigned to rohit-nayak-ps, and it has no PR. |
| R19 Concurrent table copy within one workflow | RELATED | #8056 "Analyzing VReplication behavior", open, https://github.com/vitessio/vitess/issues/8056 · #13137 (above) | #8056 discusses parallelising multi-table copy; #13137 is the multi-table snapshot. |
| R20 Shared source scan for Reshard copy | NONE | – | (#17677 pushes VStream filters down to MySQL: merged, but it isn't the shared scan.) |
| R21 `_vt.vreplication` row-image redesign | NONE | (#7877 "VReplication: ability to compress gtid when stored in _vt.vreplication's pos column", closed 2021, https://github.com/vitessio/vitess/pull/7877) | Nothing on the heartbeat binlog volume. |
| R22 Tablet-side deny-list barrier | NONE | – | |

## Counts

- EXISTING (full or partial): Wave 1 #7, Wave 2 #10, R10, R14, R16, R18. That's 6.
- RELATED: #2, #5, #9, #11, #12, #13, #16, VTOrc poll, R5, R8, R11, R15, R19, plus the two bug items (#1, R1). That's 15.
- NONE: #3, #4, #6, #8, #14, #15, #17, #18, #19, ops guidance, Online DDL tick, VDiff --wait, R2, R3, R4, R6, R7, R9, R12, R13, R17, R20, R21, R22. That's 24.
