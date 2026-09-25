# Vitess tuning investigation: summary

One investigator per finding. Each has a full report `<ID>.md` and a prototype `<ID>.patch` in this directory.
Prototypes are also left uncommitted in `/home/user/vitess/.claude/worktrees/agent-*`.

- **Machine:** shared 4-vCPU Xeon under heavy load. Trust ratios, allocation counts and cachegrind/callgrind instruction counts more than absolute ns.
- **Measured vs estimated:** every "end-to-end" figure is an estimate unless it says measured.
- **SIMD:** none of the findings needs `GOEXPERIMENT=simd`. F02 measured an archsimd kernel on top of the stdlib version and found it worth only a few ns per literal.

## Performance findings

| ID | Area | Verdict | Measured micro | End-to-end (mostly est.) | Size | Main gotcha |
|---|---|---|---|---|---|---|
| F02 | tokenizer scan loops | do | Parse3 normal −92%, escaped −55%/−78% mem | typical traces ~1–3% of parse | S | 620-line reference copy in tests; Pos quirk kept |
| F03 | SQL escaping | do | 4–20x; GenerateQuery 4KiB −70%, 64KiB −80% | big bind vars / vrepl strings | S | invalid-UTF-8 → U+FFFD quirk kept via fallback |
| F04 | shard lookup | do | −58% (4 shards) … −90% (256); ResolveDestinations 1000 rows −36…−80% | 3–10% vtgate CPU for sharded inserts (32 shards) | S | overlapping ranges may pick a different shard |
| F05 | binlog cell format | do | 6–11x per temporal/decimal cell; 11-col row −76% | 30–50% of vstreamer CPU for temporal-heavy tables | S–M | 690-line reference copy in tests |
| F06 | collation hash | do | 2–3.5x (latin1, general_ci, bin); DISTINCT −45% | hash join/DISTINCT on non-0900 text | S | no gain for 0900 / legacy UCA |
| F07 | backup pargzip | do (fork + pool, keep default) | CPU −25…−43%, garbage 6→0.15 B/B, peak heap 200–300MB→10MB | backup CPU −25–40% | S–M | Vitess owns a fork; switching to pgzip re-risks #5613 OOM |
| F08 | MergeSort | do | −71%/row, −99% allocs (256-row chunks) | streaming scatter ORDER BY −30–50% vtgate CPU | S | must Clone rows (pooled); ≤255 rows past LIMIT |
| F09 | Insert per-row AST | do | −68…−70% (100/1000 rows) | 20–30% vtgate CPU for bulk sharded inserts | S/M | +85% plan memory, not seen by plan-cache sizing |
| F10 | proto row slabs | send: do / recv: maybe | send −54%, 3001→4 allocs | 1–3% vttablet on large reads | S | recv side pins chunks (streaming MemorySort+LIMIT) |
| F11 | ReadQueryResult | do | −24…−36% CPU, −50…−90% allocs | 50–350 ns/row vttablet | S | value retains its row |
| F12 | compliantName | do | 10x; ReserveColName −50% | ~1.7% normalize | S | fast path may alias query string on rare paths |
| F13 | charset convert | do | −75…−88% (1KB), 3–4x less memory | Online DDL / MoveTables charset conversion | S/M | sjis / swe7 not ASCII-transparent (handled) |
| F14 | collate fast path | do | 10–65x equal/prefix (general_ci), 2–4x latin1 | sort/compare on general_ci text | S | invalid-UTF-8 prefix falls back |
| F15 | binary row writer | do | −44% typical row, −95% blob row, 10→1 allocs | prepared-stmt result sets | S | size mismatch would now panic instead of corrupting |
| F16 | vrepl applier | do | bulk insert −68%, update −36%, −80…−98% allocs | 8–20% target CPU when CPU-bound | S–M | cache keyed by len(Fields) |
| F17 | vstreamer vindex filter | do | keyspace id −43…−85%; row event −26…−34% | 10–20% source vstreamer CPU / filtered stream | S | relies on Hashing.Map == Hash (all 9 in-tree OK) |
| F18 | JSON escaping | do | escape −45…−77%; MarshalTo −41…−61% | vrepl JSON −17…−24% | S | keep MySQL escape set exactly |
| F19 | stats labels | do | −39…−55%, 1→0 allocs | 0.5–1% vtgate/vttablet | S | statsd hook still allocates |
| F20 | BIT encoding | do | 10–35x | vrepl rows with BIT cols | S | — |
| F21 | literal formatting | do | −16…−39% alone; −74% with F03 (1KiB) | schemadiff/DDL/no-normalize | S | depends on F03 for full effect |
| F22 | literalToBindvar & co | do | 1000-IN list 3–4x, 3036→24 allocs; validate 10x | a few % normally, large for big IN/INSERT | S | bind vars alias query; HEXNUM kept private |
| F23 | astfmt ints / String buffer | do | String −25%, −47…−58% allocs | ~4% parse+normalize+format | S | generator + regenerated file; size hint cap |
| F24 | keyword lookup | do | −37…−92% per lookup | parse −2.2%, format −7.5% (callgrind) | S | 1-letter keyword would need key change |
| F25 | flush timer | do (semantic change) | −41% buffered row writes | low single-digit % vtgate | S | flush after first write (bounded), not last |
| F26 | GTID set String/AddGTID | do | EncodePosition −59%; 50 SIDs 68→24 µs/txn | 10–40% vstreamer CPU with many UUIDs | S | AddGTID now shares interval slices |
| F27 | throttler client | do | −24…−37% single, −88% contended | 0.5–1%+ per stream; removes cross-stream lock | S | map never shrinks (as before) |
| F29 | utf8 validate/slice/length | do (small real impact) | −76…−97% | rarely evaluated in vtgate | S | Go stdlib RuneCount allocates (upstream issue) |
| F30 | cold batch | see F30-cold-batch.md | writePacket header 1→0 allocs/row; Preview 5→0 allocs | — | S | — |

## Correctness bugs found

| Bug | Severity | Location | Status |
|---|---|---|---|
| Count-min sketch reset corrupts neighbours, and indexOf uses only half the counters | medium (cache hit ratio up to −1.8pp) | go/cache/theine/sketch.go | fixed in F01, with tests that fail on main |
| PAD SPACE not honoured ('a' vs 'a ') in general_ci/latin1/unicode_ci/bin Collate and Hash | medium, diverges from MySQL | colldata | confirmed (F30 #16), not fixed |
| vrepl `select *` drops ConvertCharset / ConvertIntToEnum | medium, silently wrong data | replicator_plan.go | confirmed (F16, F30 #17a) |
| vrepl explicit column list with a target-generated column panics or shifts values | low–medium | replicator_plan.go appendFromRow | confirmed (F30 #17b) |
| processExactKeyRange sorts the shared cached shard list in place | low (race) | go/vt/key/destination.go | fixed in F30 |
| FetchNext uncapped sub-slices: appends can clobber the next column; consolidator over-counts size | low–medium | go/mysql/encoding.go | confirmed (F11, F30 #15), 1-line fix |
| Timings.Reset swaps map under RLock | low (tests only) | go/stats/timings.go | fixed in F30 |
| Throttler.checkScope mutates the global okMetricCheckResult | low–medium (race) | throttle | reported by F27, not fixed |
| Collation_binary.Hash panics if numCodepoints > len | latent | colldata | reported by F06 |
| writePacket header escapes to heap (1 alloc/row) | perf | go/mysql/conn.go | fixed in F30 |

## Patch overlap (merge order matters)

- go/sqltypes/value.go: F03, F20 (disjoint hunks); F21 builds on F03.
- go/vt/sqlparser: ast_format(.go/_fast.go) + tracked_buffer.go: F21, F23. ast_funcs.go: F12, F23, F24. token.go: F02 only.
- go/mysql/query.go: F11, F15. encoding.go: F11 (+ bug #15). conn.go: F25, F30.
- go/vt/vtgate/engine/insert.go: F04, F09.
- colldata 8bit.go / unicode.go: F06, F14.
- go/stats timings.go: F19, F30. go/vt/key/destination.go: F04, F30.
