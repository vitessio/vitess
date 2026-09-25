# F15: binary-protocol row writer (writeBinaryRow / val2MySQL)

## Verdict
**Do it.** This is a small, self-contained rewrite. Output is byte-identical to the old encoder, and the change removes every per-value allocation from the COM_STMT_EXECUTE row path.

## Finding verification
- `go/mysql/query.go` `(*Conn).writeBinaryRow` first sizes the packet with `val2MySQLLen` (two-pass sizing was already in place). For each non-NULL value it then called `val2MySQL`, which did `out = make([]byte, n)`, encoded into `out`, and returned it. The caller then ran `copy(data[pos:], out)`.
  - Result: 1 heap allocation per non-NULL value, plus a second full copy of every string or blob. For a 64 KiB blob that means a 64 KiB allocation, zeroing it, and copying it twice.
- The TIME path used `strings.Split(string(raw), ":")` and `strings.Split(sub1[2], ".")`. Each call allocates the result slice, and the `string(raw)` escapes, so it is copied. `[]byte(sub1[0])` was an extra conversion.
- Callers: `(*Conn).writeBinaryRows` is called from `(*Conn).handleComStmtExecute` (conn.go:1415). That is the only user, and it serves every result row of every COM_STMT_EXECUTE that vtgate handles (`vtgateHandler.ComStmtExecute`, go/vt/vtgate/plugin_mysql_server.go:1375).
- How common: go-sql-driver/mysql uses server-side prepared statements by default for any query with args (`interpolateParams=false` is the default). The same applies to JDBC with `useServerPrepStmts=true`, MySQL Connector/NET, mysql2 (Node) `execute()`, and others. The text protocol (`writeRows`) has no per-value allocation, so until now the binary protocol was the slower of the two.

## Implementation (prototype, uncommitted in the worktree)
- `val2MySQL(v) ([]byte, error)` is replaced by `val2MySQLTo(dst []byte, pos int, v) (int, error)`, which writes directly into the packet buffer. That buffer is already sized with `val2MySQLLen`, which is unchanged.
- The DATE/DATETIME/TIMESTAMP path moves to `writeBinaryDateTime`. It parses with `strconv.ParseUint(hack.String(sub), …)` (strconv clones its input into any `NumError`, verified in the Go 1.27.1 source, so there is no aliasing). Microseconds use a stack `[6]byte`.
- The TIME path moves to `writeBinaryTime`, which uses `bytes.IndexByte` instead of `strings.Split` and keeps exactly the same validation:
  - exactly 2 `:` separators, otherwise the error "':' is not found";
  - if `.` appears anywhere in the value, the seconds part must hold exactly 1 `.`, otherwise the error "'.' is not found";
  - the same parse order, so the same strconv error text comes first.
- Integer and float paths are unchanged. They already used `v.ToString()`, which is `hack.String` and does not allocate. The default branch (a raw copy) is also unchanged.
- In `writeBinaryRow`, `pos += copy(data[pos:], v)` becomes `pos, err = val2MySQLTo(data, pos, val)`. Error handling (`recycleWritePacket` and the same error message) is unchanged.
- Files: `go/mysql/query.go` (+227 / -257). There is no generated code.
- New tests:
  - `go/mysql/binary_row_ref_test.go`: a verbatim copy of the old `writeBinaryRow` and `val2MySQL`, kept as the reference and the benchmark baseline. It could be dropped before upstreaming, keeping only golden-byte tests.
  - `go/mysql/binary_row_test.go`: the equivalence tests, the benchmark, and a test-only `val2MySQL` helper so the existing `TestVal2MySQL*` tests compile unchanged.
- Difficulty: **S**, about 250 lines of production code (mostly moved), 1 production file.

## Benchmarks (measured; shared 4 vCPU host, count=8, benchtime 300ms, old and new interleaved in the same binary)
```
                           │   old        │    new                          │
WriteBinaryRow/typical10-4    617.4n ± 14%   347.9n ± 15%  -43.65% (p=0.000 n=8)
WriteBinaryRow/blob64k-4     30.534µ ± 21%   1.528µ ± 12%  -95.00% (p=0.000 n=8)
WriteBinaryRow/time-4         449.0n ± 12%   172.1n ±  8%  -61.68% (p=0.000 n=8)

B/op:      typical10 136 -> 4 ; blob64k 73805 -> 4 ; time 192 -> 4
allocs/op: typical10 10 -> 1  ; blob64k 3 -> 1     ; time 8 -> 1
```
- typical10 row: int64, int32, 2 varchar, datetime, decimal, int8, timestamp(6), NULL, uint64.
- The 1 allocation left (4 B) is in the pre-existing packet/bufpool path and is the same in both versions.
- The writer is a discarding `net.Conn` with no bufferedWriter.

## End-to-end estimate (not measured)
- For a prepared-statement SELECT returning N typical rows, the change saves about 0.27 µs and 9 allocations (about 130 B) per row in vtgate. For 1000 rows that is about 0.27 ms of CPU and 9k allocations per query. vtgate's other per-row costs are mainly proto decoding of the vttablet result, which vtgate pays either way.
- My guess is that this is on the order of 10–25% of vtgate's per-row CPU for binary-protocol streaming. For BLOB/TEXT-heavy results the saving is much larger: the old code duplicated every value (up to 1 GB per value with max_allowed_packet) on the heap.

## Correctness / tests
- `TestVal2MySQLToMatchesReference` compares the old encoder against the new one. The result includes output bytes, error text and panic behaviour, and it also checks `len == val2MySQLLen`. It covers:
  - all 34 types × about 110 edge values: int boundaries and overflow, NaN, zero dates with 0–7 fractional digits, malformed dates, negative and zero times, 838:59:59, uint32 hour overflow, `12:34`, `12:34:56:78`, `1.2:03:04`, `12:34:56.7.8`, a trailing `.`, empty values, and 250/251/65535/65536-byte strings;
  - 20k random typed values;
  - 60k byte-mutated TIME/DATETIME/DATE values.
- `TestWriteBinaryRowMatchesReference` compares full wire output for:
  - 500 random rows of 1–20 columns with NULLs (NULL bitmap);
  - the error path followed by recycle;
  - a row larger than MaxPacketSize (16 MiB + 10 blob), to check packet splitting.
- I checked that the tests can fail by flipping the TIME sign byte: both tests then fail.
- `go test ./go/mysql/...` passes, except for tests that need a local `mysql` / `mysqld` binary: TestServer, TestServerStats, TestClearTextServer, TestDialogServer, collations/integration and endtoend. These fail for environmental reasons, on main too.
- `go test -run 'Stmt|Prepare' ./go/vt/vtgate/` passes.

## Gotchas
- Output is byte-identical, including the existing quirks:
  - more than 6 fractional digits are truncated;
  - `raw[19]` is not checked to be `.`;
  - malformed short DATE/DATETIME values (for example `"2020-01"` or a 12–16 byte datetime) panic with index-out-of-range, exactly as before. Values come from MySQL, so this does not happen in practice.
- New failure mode: if `val2MySQLLen` ever under-estimates, the in-place write panics with index-out-of-range. Before, `copy` silently truncated and sent a corrupt packet. The equivalence test asserts the lengths agree for every case. A reviewer might want a debug assertion that `pos == len(data)` at the end.
- `hack.String` on value sub-slices: this is safe because strconv clones strings into errors and nothing retains them. It is the same pattern already used through `v.ToString()`.
- No protocol or behaviour change, so there are no release-compatibility concerns. Error texts are identical.
- Portability: plain byte code, nothing specific to an architecture.
- Before upstreaming: decide whether to keep the ~390-line reference copy in `_test.go`. It is useful as an oracle but is dead weight. An alternative is to generate golden bytes once and keep a table test.
- The `-trimpath` build flag was used.
