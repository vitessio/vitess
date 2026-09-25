# F11: ReadQueryResult allocates one buffer per column value

## Verdict
**Do it.** This is a small change to one function. It cuts allocations per row by 50 to 90%, and CPU time falls 24 to 36% on multi-row results.
The recommended variant reads each kept row packet straight into a buffer the row owns. The values sub-slice that buffer, with capacity capped to each value's length.

## Finding re-verified
- `go/mysql/query.go` `ReadQueryResult` (row loop at about line 459): `readEphemeralPacket()` reads into a pooled buffer, which goes back to `bufPool` right after the row is parsed. `parseRow(..., readLenEncStringAsBytesCopy, nil)` then runs `make([]byte, n)` and a copy for every non-NULL column. The cost per row is 1 allocation for `[]Value` plus 1 per column.
- A copy is mandatory for the ephemeral buffer. The question is where it happens. `readPacket()` (used by streaming `FetchNext` via `ReadPacket()`) already reads each packet into a new `make([]byte, length)` with no pool and no extra copy. The streaming path sub-slices with `readLenEncStringAsBytes`.
- Other users of `readLenEncStringAsBytesCopy`: only the server-side `COM_STMT_EXECUTE` bind-var parsing (`query.go` about line 919, vtgate reading client parameters). That is not a hot row path, so it is left unchanged. `parseRow` is also used by `FetchNext` (streaming).
- vtgate does not read result rows through `go/mysql`: vtgate to vttablet is gRPC, and vtgate's MySQL server only reads commands. The hot callers are vttablet `connpool.Conn.Exec`, which runs `ExecuteFetch` then `ReadQueryResult`, plus vreplication, schema engine, messager, throttler/heartbeat, `mysqlctl` and flavors. None are latency-critical except the tablet query path.

## Variants evaluated
| id | description | allocs/row | notes |
|---|---|---|---|
| 0 | current: copy per column | 1 + ncols | baseline |
| 1 | `bytes.Clone(ephemeral)` + capped sub-slice | 2 | one extra memcpy + pool Get/Put per row |
| **1b (prototype)** | `readPacket()` for kept rows (owned buffer) + capped sub-slice | 2 | no pool, no extra copy; +1 small alloc per result for the terminating EOF/OK packet |
| 2 | arena: compact-copy values into 256B..64KB growing slabs | ~1 | 1 retained value pins up to a 64KB slab; closure alloc; 1-row results got slower (+10–30%) and larger (+29% B/op) |
| 3 | 1 + chunked `[]Value` backing (up to 256 values) | ~1.1 | retention cascade: a retained row pins the chunk, whose values pin other rows' buffers; +96% B/op on 1-row results |
| 4 | parse with sub-slices, then copy all values into 1 exact-size buffer | 2 | exact B/op (same as old); an extra pass; CPU similar to 1 |

Variants 2 and 3 save about 1 malloc per row more than 1b, estimated at 20 to 40 ns per row. They have worse retention and are slower or larger for small results. Not recommended.

## Measurements (4 vCPU shared, load average 13–22, so wall-clock numbers are very noisy)
### Allocations/bytes (deterministic), `BenchmarkReadQueryResult` (full ReadQueryResult over an in-memory conn, `go/mysql/read_query_result_test.go`)
```
                                     copyPerColumn   clonePerRow        ownPacket (prototype)
allocs narrow3x8_1row                9               7   (-22%)         7   (-22%)
allocs narrow3x8_1000rows            4015            2015 (-50%)        2015 (-50%)
allocs wide20x12_1000rows            21015           2015 (-90%)        2016 (-90%)
allocs blob1x64K_20rows              50              50                 50
B/op   narrow3x8_1row                812             820  (+1%)         810  (-0.3%)
B/op   narrow3x8_1000rows            176.1Ki         183.9Ki (+4.5%)    185.0Ki (+5.1%)
B/op   wide20x12_1000rows            1.037Mi         1.007Mi (-2.9%)    1.012Mi (-2.4%)
B/op   blob1x64K_20rows              1.255Mi         1.413Mi (+12.6%)   1.411Mi (+12.4%)
```
The blob +12% is a page-rounding edge case. A 65536-byte value plus its 4-byte length prefix spills into a 9th 8KB page. On average, large rows see about the same rounding as before.

### CPU time (process rusage user+sys, GOMAXPROCS=1, including GC; 15 interleaved rounds; harness in scratchpad `F11-cpu_scratch_test.go.txt`)
```
case                   impl            min ns/op   median ns/op
narrow3x8_1row         copyPerColumn        904         1004
narrow3x8_1row         clonePerRow          907         1031
narrow3x8_1row         ownPacket            784          911   (-9%)
narrow3x8_1000rows     copyPerColumn     157923       192820
narrow3x8_1000rows     clonePerRow       147757       179043   (-7%)
narrow3x8_1000rows     ownPacket         116060       146050   (-24%)  ~47 ns/row saved
wide20x12_1000rows     copyPerColumn     905133      1006067
wide20x12_1000rows     clonePerRow       569300       708067   (-30%)
wide20x12_1000rows     ownPacket         543083       641267   (-36%)  ~365 ns/row saved
blob1x64K_20rows       copyPerColumn     287300       363190
blob1x64K_20rows       clonePerRow       269333       342487
blob1x64K_20rows       ownPacket         256433       314453   (-13%)  one fewer 64KB memcpy per row
```
parseRow-only micro, min of 20 wall-clock samples: narrow3x8 198 ns (old), 161 ns (clone), 136 ns (compact). wide20x12 1332 ns (old), 741 ns (clone), 901 ns (compact). This is consistent with the earlier scratch figure of 330 ns and 11 allocs down to 245 ns and 2 allocs.

The wall-clock benchstat runs (`F11-final.txt`, `F11-e2e.txt`) had ±30–400% variance from machine load and are not reliable for time. The allocation columns in them are exact.

### End-to-end estimate (not measured)
In a non-streaming vttablet Execute, each row goes through ReadQueryResult, then `ResultToProto3`, which copies values into one buffer per row, then gRPC marshal. The saving is about 50 ns per row for narrow rows and about 350 ns per row for 20-column rows. It also cuts GC pressure: 50–90% fewer allocation objects on the tablet's hottest allocation site for large results. For typical OLTP point selects (1 row), the change is roughly neutral to slightly positive: -2 allocs, -9% CPU in this harness.

## Prototype (uncommitted in the worktree; patch `F11-readqueryresult-allocs.patch`)
- `encoding.go`: new `readLenEncStringAsBytesCapped` returns `data[pos:pos+s:pos+s]`.
- `query.go` `ReadQueryResult` row loop:
  - It computes `keepRow := maxrows != FETCH_NO_ROWS && (maxrows == FETCH_ALL_ROWS || len(result.Rows) != maxrows)`. This is exactly the old accept condition.
  - When the row is kept, it reads with `c.readPacket()` (owned buffer) and parses with the capped sub-slicing reader.
  - Otherwise it uses `readEphemeralPacket()` and recycles. For `FETCH_NO_ROWS` it discards and continues; when over the limit it drains and returns "Row count exceeded" as before.
  - `recycleReadPacket()` is only called on the ephemeral branch.
- Tests (`read_query_result_test.go`, `read_query_result_ref_test.go`):
  - `readQueryResultRef` is a verbatim copy of the old function, with an optional clone-per-row mode for the A/B.
  - `TestReadQueryResultMatchesReference` compares the new function against the old one. It covers both EOF modes (classic EOF and DEPRECATE_EOF), maxrows ∈ {ALL, NO_ROWS, 0, 1, 3, 4, 10}, and wantfields ∈ {true, false}. The rows include NULL, empty, 300-byte and 70000-byte (multi-byte lenenc prefix) values. It compares results, errors, `more`, warnings, and NULL vs empty.
  - `TestReadQueryResultValuesDoNotAlias` asserts `cap == len` for each value and that appending to one value does not clobber the next. I checked that it fails when the uncapped `readLenEncStringAsBytes` is used.
  - `BenchmarkReadQueryResult` covers narrow, wide and blob rows.
- Difficulty: **S**, about 30 production LOC in 2 files, no generated code.

## Gotchas
- **Memory retention:** a retained Value, or a string from `Value.ToString()`, now keeps the whole row packet alive, not just its own bytes. `ToString` and `RawStr` are zero-copy through `hack.String`, so strings derived from the row pin it too.
  - The schema engine keeps table names and similar via `ToString`, so it pins schema rows (small; a comment column at most).
  - Messager `BuildMessageRow` already keeps `row[4:]`, so it already retains the whole row.
  - The query consolidator shares the whole `*Result` anyway.
  - The main Execute path copies into proto3 and drops the Result.
  - So retention grows by at most the row's length-prefix bytes and sibling columns. This is why the arena and chunk variants were rejected.
- **Aliasing:** sub-slices must be capacity-capped, otherwise `append(v.Raw(), ...)` would overwrite the next column. I grepped for in-place `append` to or mutation of `Raw()` in non-test code and found none. Mutation within bounds behaves as before.
- **Memory accounting:**
  - `queryserver-config-max-result-size` and vtgate's `max-memory-rows` count rows, so they are unaffected.
  - `Result.CachedSize`, which sums `cap(val)`, is used only by the stream consolidator on streaming results. With capped slices, it stays exact per value.
- **Side finding (streaming, not changed here):** `FetchNext` uses the uncapped `readLenEncStringAsBytes`. Each streamed Value's capacity therefore runs to the end of the packet.
  - The stream consolidator's `result.CachedSize(true)` over-counts by about ncols/2 times the row size, around 10x for 20 columns.
  - `append(v.Raw(), ...)` on a streamed value can clobber the next column.
  - A one-line follow-up would switch `FetchNext` to `readLenEncStringAsBytesCapped`. It changes stream-consolidator accounting (fewer bytes counted), so it deserves its own PR.
- The EOF/OK terminator packet of a fetched result is now read into a small heap buffer, not a pooled one (+1 tiny alloc per result, already included in the numbers above).
- Rows of 16MB or more (multi-packet) go through `readPacket`'s append loop, the same as the old ephemeral large-packet path.
- Malformed packets: behaviour is unchanged. An empty row packet still panics on `data[0]` in `parseRow` exactly as before, and a huge lenenc size overflows `int` the same way.
- Wire format and API are unchanged, so there is no release-compatibility concern. No arm64-specific code.

## Tests run
- New tests pass. `go test ./go/mysql/`: all pass except TestServer, TestServerStats, TestClearTextServer and TestDialogServer. Those need the `mysql` client binary ("VT_MYSQL_ROOT is not set and no mysqld could be found"). For the same reason `go/mysql/endtoend` and `collations/integration` fail. These are environment failures, not caused by the change.
- Also pass: `go/vt/vttablet/tabletserver`, `.../connpool`, `.../schema`, `.../messager`, `go/vt/dbconnpool`, all other `go/mysql/...` subpackages.
