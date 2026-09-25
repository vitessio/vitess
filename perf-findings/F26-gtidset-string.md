# F26: Mysql56GTIDSet.String / EncodePosition / AddGTID on the per-transaction VReplication path

Worktree: /home/user/vitess/.claude/worktrees/agent-addd703517f76fd6f (uncommitted)
Patch: findings/F26-gtidset-string.patch
Raw benchstat: scratchpad/f26_bench*.txt, f26_b1.txt, f26_b34.txt

## 1. Is the finding real?

Mostly yes, with corrections:
- `Mysql56GTIDSet.String()` (mysql56_gtid_set.go:167) already used `strings.Builder`, but it did not pre-size it,
  called `SIDs()` (heap slice + sort), `sid.String()` (1 alloc per SID; the `[]byte("xxx...")` literal stays on
  the stack, so it is 1 alloc and not 2), and `strconv.FormatInt` (1 alloc per bound >= 100). Old allocs:
  3 (1 SID/1 interval), 15 (5 SIDs), 109 (50 SIDs), 310 (50 SIDs x 3 intervals).
- `EncodePosition` added `fmt.Sprintf("%s/%s")`: 3 more allocs and a full copy.
- `AddGTID` (the bigger issue that the finding only hinted at) rebuilt the whole map and copied every SID's
  interval slice for every transaction: 1 map + N slices per GTID event (59 allocs for 50 SIDs).

### Per-transaction call sites (verified)
Source tablet, vstreamer.go (`parseEvent`), for every binlog transaction, including transactions whose tables
the filter does not match (the GTID event is still sent so the target can advance its position):
- GTID event: `vs.pos = replication.AppendGTID(vs.pos, gtid)` -> `Mysql56GTIDSet.AddGTID` (copies the set).
- XID event (and DDL / OTHER statements): `Gtid: replication.EncodePosition(vs.pos)` -> String + Sprintf.
- This runs once per vstreamer, so it is multiplied by the number of streams on the source tablet
  (each workflow x source shard, plus VStream API clients).
Target tablet, vplayer.go:
- `applyEvent` GTID: `binlogplayer.DecodePosition(event.Gtid)` (parse, not changed here; it costs about as much
  as AddGTID used to, see below).
- `updatePos` -> `binlogplayer.GenerateUpdatePos` -> `EncodePosition` (+ `encodeString` + Sprintf) for each commit
  that is saved.
- `getNextPosition` (parallel/batched apply) does Decode + `Position.String()`.
Not hot: `GetTableForPos(... EncodePosition ...)` only when a table plan is built; `Last()` (calls SIDs() 3x,
left as is); `ErrantGTIDsOnReplica`.

## 2. Benefit (measured, 4 vCPU shared machine, -count=8, benchtime=200ms, noisy: +/-15-40%)

### String (old = verbatim copy of previous impl in the _test.go file)
| shape (SIDs x intervals) | old | new | allocs old -> new |
|---|---|---|---|
| 1 x 1   | 301 ns | 256 ns (-15%) | 3 -> 1 |
| 1 x 10  | 1.46 us | 0.70 us (-52%) | 24 -> 1 |
| 5 x 1   | 1.61 us | 0.83 us (-48%) | 15 -> 1 |
| 5 x 10  | 5.97 us | 3.80 us (-36%) | 107 -> 1 |
| 50 x 1  | 22.8 us | 14.5 us (-37%) | 109 -> 2 |
| 50 x 3  | 41.7 us | 17.1 us (-55..-50%) | 310 -> 2 |

### EncodePosition ("MySQL56/" + String)
| shape | old | new | allocs |
|---|---|---|---|
| 1 x 1 | 777 ns | 317 ns (-59%) | 6 -> 1 |
| 5 x 1 | 3.39 us | 1.16 us (-66%) | 18 -> 1 |
| 5 x 10 | 9.33 us | 3.20 us (-66%) | 110 -> 1 |
| 50 x 3 | 32.4 us | 13.7 us (-58%) | 313 -> 4 (-> 2 after the >8 SID fix) |

### AddGTID (share the other SIDs' interval slices instead of copying them)
| shape | old | new | allocs |
|---|---|---|---|
| 1 x 1 | 701 ns | 823 ns (~, p=0.5) | 3 -> 3 |
| 1 x 10 | 952 ns | 1095 ns (~, p=0.13) | 3 -> 3 |
| 5 x 1 | 1.01 us | 0.97 us (~) | 7 -> 3 |
| 5 x 10 | 1.68 us | 1.04 us (-38%) | 7 -> 3 |
| 50 x 1 | 19.4 us | 7.2 us (-63%) | 59 -> 5 |
| 50 x 3 | 18.2 us | 7.4 us (-59%) | 59 -> 5 |

### vstreamer per-transaction cycle: AppendGTID + EncodePosition (both changes)
| shape | old | new | B/op | allocs |
|---|---|---|---|---|
| 1 x 1 | 1.37 us | 0.92 us (-33%) | 640 -> 504 | 10 -> 5 |
| 1 x 10 | 3.03 us | 1.36 us (-55%) | 1760 -> 808 | 31 -> 5 |
| 5 x 1 | 2.70 us | 1.67 us (-38%) | 1856 -> 680 | 26 -> 5 |
| 5 x 10 | 13.2 us | 5.6 us (-58%) | 5.9 Ki -> 1.7 Ki | 118 -> 5 |
| 50 x 1 | 31.0 us | 17.1 us (-45%) | 19.2 Ki -> 6.3 Ki | 172 -> 8 |
| 50 x 3 | 67.8 us | 24.2 us (-64%) | 27.7 Ki -> 7.7 Ki | 373 -> 8 |

Unchanged, for comparison: DecodePosition (the vplayer side) costs 0.4 us (1x1), 1.1 us (5x1), 5.2 us (5x10)
and about 20 us (50 SIDs). This is now the largest remaining position cost per transaction on the target, and it
is a natural next step (for example, skip decoding when the Gtid string equals the previous one, or parse into a
reused set).

### End-to-end (estimated, not measured)
- With 1 server UUID (fresh clusters), the savings are about 0.5 us and 5 allocs per source transaction per
  stream. That is a low single-digit % of vstreamer CPU per small transaction (row event decoding, filtering and
  proto marshalling dominate).
- Long-lived clusters after many reparents typically have gtid_executed with 5-50 UUIDs. There the savings are
  1-44 us and 20-365 allocs per transaction per stream on the source, plus 1-20 us per saved commit on the target
  (updatePos). For small transactions that is likely 10-40% of vstreamer CPU and a large share of its allocation
  rate (GC pressure), multiplied by the number of concurrent streams. Filtered-out transactions pay this cost too,
  so a MoveTables of a small table on a busy keyspace pays it for every transaction in the keyspace.
- No existing package-level vstreamer benchmark runs without MySQL. These numbers come from call-frequency
  reasoning, not from a profile.

## 3. Difficulty
S. About 90 lines of non-test code in 3 files (mysql56_gtid.go, mysql56_gtid_set.go, replication_position.go),
plus about 400 lines of tests and benchmarks. No generated code.

## 4. Prototype (in worktree)
- `SID.appendTo(dst)`: grows dst once and hex-encodes into it; `SID.String` uses a stack [36]byte.
- `Mysql56GTIDSet.textLen()`: exact length (upper bound for negative bounds). `appendTo(dst)`: SIDs are sorted in a
  stack `[8]SID` buffer (one exact-size heap slice if there are more than 8), with `strconv.AppendInt`.
  `String()` = `hack.String(appendTo(make(0, textLen)))`, which is exactly 1 alloc for up to 8 SIDs.
- `EncodePosition`: a fast path for Mysql56GTIDSet builds "MySQL56/" and the set into one buffer (1 alloc). Other
  flavors use `flavor + "/" + String()` (same output as the Sprintf).
- `AddGTID`: builds a new map that shares the unchanged SIDs' interval slices and rebuilds only the target SID's
  slice (same merge logic).

## 5. Gotchas
- Output is byte-identical to the old code, verified by an equivalence test over 5000 random sets. The sets
  include negative and MaxInt64 bounds, end < start, and empty interval lists, which the parser never produces
  but SID blocks can. Parse round-trip tests cover this too.
- `hack.String` on a freshly allocated buffer that nothing retains is safe. `decimalLen` only sizes the buffer, so
  a wrong estimate costs a realloc and never produces wrong output.
- AddGTID aliasing: the new set now shares interval slices with the receiver. Nothing mutates interval elements
  in place: `AddGTIDInPlace` and `UnionInPlace` assign fresh slices. `Union`, `Difference` and `RemoveUUID`
  already share slices today. `AppendGTIDInPlace`/`AddGTIDInPlace` have no callers outside the package.
  One corner case: `AddGTIDInPlace` on a SID whose existing slice is empty but has spare capacity would append into
  a shared backing array. That only writes beyond every holder's len, so nobody can see it. A reviewer may still
  ask for `slices.Clip`, which costs nothing here. A test asserts that the receiver stays unchanged after
  AddGTID + AddGTIDInPlace + UnionInPlace on the result.
- Memory retention: sharing means the old position's slices live as long as the new set. They are the same data,
  so this is not a real retention increase.
- AddGTID with 1 SID: allocs are unchanged. Time is within noise (maybe +10%, not significant). The map is built
  with a size hint instead of lazily.
- No API or wire changes. The helpers are unexported. Compatible across releases.
- The equivalence tests guard a refactor, so they cannot "fail on main". On main they do not compile, because
  they call `textLen`/`appendTo`. A reviewer applying CLAUDE.md strictly may want to trim the test file down to
  the equivalence tests plus the benchmarks.

## 6. Tests
- Added go/mysql/replication/mysql56_gtid_set_string_test.go: `TestSIDStringMatchesReference`,
  `TestMysql56GTIDSetStringMatchesReference` (also covers EncodePosition and textLen),
  `TestMysql56GTIDSetStringRoundTrip` (parse -> String -> parse, Encode -> Decode, exact textLen),
  `TestMysql56GTIDSetAddGTIDMatchesReference` (random AddGTID sequences vs the old impl, including a nil set),
  `TestMysql56GTIDSetAddGTIDDoesNotModifyReceiver`, and benchmarks `BenchmarkMysql56GTIDSetString`,
  `BenchmarkEncodePosition`, `BenchmarkMysql56GTIDSetAddGTID`, `BenchmarkVStreamerGTIDCycle`.
- Pass: go/mysql/replication, go/vt/binlog/binlogplayer, go/mysql, go/vt/vtgate (-run GTID|Position|Binlog|VStream).
- Not run (they need mysqld, which this environment lacks): vstreamer and vreplication packages. go/vt/mysqlctl
  fails for the same reason, on main too.
