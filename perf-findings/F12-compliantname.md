# F12-compliantname: compliantName fast path + ReserveVariable round-trip removal

Worktree: /home/user/vitess/.claude/worktrees/agent-a9a74b5c1565926bc (uncommitted)
Patch: findings/F12-compliantname.patch
Raw benchstat: scratchpad/f12/micro.stat, scratchpad/f12/e2e.stat (raw runs *_old.txt / *_new.txt alongside)

## Verdict: do it
Small, self-contained, exactly equivalent output. Cuts 2-4% of allocations in the normalize benchmarks; CPU gain is about 1.7% of the parse+normalize path, measured from profile share (wall-clock deltas are below the noise on this shared host).

## Code verified
- `go/vt/sqlparser/ast_funcs.go` `compliantName`: strings.Builder + WriteRune per rune. It always allocates (1 alloc for names of 8 bytes or fewer, more for longer ones because the builder grows).
- `go/vt/sqlparser/reserved_vars.go` `ReserveVariable`: does `[]byte(name)` then `string(joinVar)` on every call, even when the name is free.
- Hot caller: `normalizer.decideBindVarName` -> `ReserveColName` -> `ColName.CompliantName` -> `compliantName`, once per `col = literal`.
- Semantics: `isLetter(uint16)` accepts a-z, A-Z, `_` and `$`. `isDigit(uint16)` accepts 0-9. `range` yields RuneError (0xFFFD) for each invalid byte, so each invalid byte becomes one `_`. A valid multibyte rune becomes a single `_`. Runes >= 0x10000 are truncated by the uint16 cast: U+10061 is treated as 'a' and KEPT verbatim (4 bytes), and U+10031 is kept at any position except the start. The "leading" check uses the byte index 0.

## Change (~45 LOC non-test, 2 files)
1. `compliantName`: scans the bytes. If every byte is ASCII and is either a letter, `_` or `$`, or a digit that is not at the start, it returns `in` unchanged with 0 allocs. At the first non-ASCII or non-compliant byte it hands off to `compliantNameSlow(in, prefix)`. That function is the original rune loop with the same semantics (uint16 truncation, the `prefix+i == 0` check). It adds `Grow(len(in))` and copies the already-validated ASCII prefix in bulk.
2. `ReserveVariable`: looks up the map with the string directly. If the name is free, it reserves it and returns it as is (0 allocs). On a collision it builds the suffixes in a `[64]byte` stack buffer; `-gcflags=-m` shows "append does not escape". That leaves 1 alloc, for the final name. The naming sequence is unchanged: name, name1, name2, ... (first free).
3. `ReserveColName`: `compliantName` can now return the identifier itself. Identifiers are substrings of the SQL text (`Tokenizer.scanIdentifier` returns `tkn.buf[start:pos]`). So unqualified, unprefixed names are `strings.Clone`d, so that bind variable names (bindVars map keys, Argument nodes, cached plans) do not pin the whole query string. Qualified names (the `q + "_" + n` concat) and `"_" + name` prefixed names are already fresh strings and are not cloned.

## Micro A/B (benchstat, 8 interleaved runs, 200ms; load avg ~16 on 4 vCPU, so time has large ±)
```
                        │ micro_old.txt │            micro_new.txt            │
                        │    sec/op     │    sec/op     vs base               │
CompliantName/c_w_id-4    78.510n ± 32%   7.311n ± 44%  -90.69% (p=0.000 n=8)
CompliantName/user_id-4   71.635n ± 63%   7.592n ± 23%  -89.40% (p=0.000 n=8)
CompliantName/a.b-4        43.48n ± 26%   49.28n ± 40%        ~ (p=0.195 n=8)
ReserveColName-4           892.4n ± 25%   445.6n ± 16%  -50.06% (p=0.000 n=8)
allocs/op: CompliantName compliant 1 -> 0; a.b 1 -> 1; ReserveColName 12 -> 7 (includes NewReservedVars map build)
B/op: ReserveColName 96 -> 48
```

## End-to-end A/B (10 interleaved runs, 3x each)
```
                                     │ e2e_old.txt  │             e2e_new.txt             │
                                     │    sec/op    │    sec/op     vs base               │
NormalizeTraces/django_queries.txt-4   642.3µ ± 19%   616.2µ ± 19%       ~ (p=0.631 n=10)
NormalizeTraces/lobsters.sql.gz-4      32.46m ± 23%   32.49m ± 22%       ~ (p=0.529 n=10)
NormalizeVTGate-4                      123.3m ± 42%   109.7m ± 39%       ~ (p=0.436 n=10)
NormalizeTPCC-4                        85.84m ± 43%   80.79m ± 40%       ~ (p=0.579 n=10)
B/op:      -0.40% / -0.38% / -0.80% / -0.65%   (all p=0.000)
allocs/op: 9.122k->9.016k (-1.16%), 298.6k->292.4k (-2.05%), 657.3k->630.9k (-4.02%), 426.1k->410.4k (-3.69%)
```
The time deltas are within the noise. The allocation deltas are deterministic.

## CPU profile (BenchmarkNormalizeVTGate + BenchmarkNormalizeTPCC, 80x, covers parse+normalize+String)
- old: ReserveColName cumulative 3.51% (ColName.CompliantName 1.54%, compliantName 1.19%, ReserveVariable 1.68%); decideBindVarName 5.47%
- new: ReserveColName cumulative 1.77% (ColName.CompliantName 0.51%, ReserveVariable 0.95%, compliantName below the reporting threshold); decideBindVarName 3.54%
- The estimated end-to-end CPU saving is ~1.7% of this path (based on profile share). The remaining ReserveVariable cost is the map insert.

## Gotchas
- Aliasing at other callers: `compliantName` now returns its input when it is already compliant. The other callers are:
  - `engine/insert.go`: Sprintf, so a fresh string.
  - `planbuilder/stream.go` and `vstream.go`: they store `table.Name.CompliantName()` in the MStream/VStream primitive's TableName, which now aliases the SQL of those (rare) plans.
  - `evalengine.FieldResolver`: a transient compare.
  - `normalizer.udvRewrite` (normalizer.go:754): `strings.ToLower(...)` returns its input for lowercase ASCII. The UDV name goes into BindVarNeeds, which is cached with the plan, so it now aliases the query text.
  - `schemadiff` / `onlineddl`: they build new DDL.
  If a reviewer cares, add `strings.Clone` at the stream/vstream/udv sites (all rare paths). The hot path (ReserveColName) clones.
- The uint16 truncation quirk is preserved exactly: fuzzing plus explicit cases for U+10061, U+10031 and U+1005F. All non-ASCII input takes the slow path, whose semantics are identical.
- ReserveVariable now returns the caller's string when it is free, instead of a copy. The other callers pass constants or freshly built strings (`CompliantString` -> `String()`).
- No generated code, no API change, no release-compatibility concerns.

## Tests
New file `go/vt/sqlparser/compliant_name_test.go`:
- The old implementation, kept as `referenceCompliantName`, plus a table equivalence test covering invalid UTF-8, multibyte runes, the >=0x10000 truncation and NUL.
- `FuzzCompliantName`: ran for 60s, ~1.06M execs, no differences.
- `TestCompliantNameNoCopyWhenCompliant`: checks 0 allocs and that the same pointer is returned. It FAILS on main, as required.
- `TestReserveVariableSequence`, `TestReserveColNameSequence` (qualified, prefix, collision, non-compliant) and `TestReserveColNameDoesNotAliasQuery` (guards the clone).
- `BenchmarkCompliantName` and `BenchmarkReserveColName`.

Passing: go/vt/sqlparser/..., go/vt/vtgate/planbuilder/... (bind var names in the expected plans are unchanged), vtgate/evalengine, vtgate/engine, go/vt/vtgate, schemadiff.
