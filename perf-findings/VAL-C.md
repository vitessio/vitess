# VAL-C: validation of BUGS #16, #19, #24 and #4

Tree: the worktree HEAD is `e5e0091d44`, which is upstream main and newer than `aa9ccf9`. None of the files involved changed between the two, except `go/mysql/query.go`, where `parseRow` and `FetchNext` are the same in both. The end-to-end cluster (#4) used the base binaries in `/home/vt/bin`. No fix is included. Tests and repro scripts are in `VAL-C.patch`.

| # | Bug | Verdict | Suggested priority |
|---|---|---|---|
| 16 | `Throttler.checkScope` mutates the global `okMetricCheckResult` | **TEST FAILS ON BASE** (`-race` DATA RACE, and the AppName is corrupted deterministically) | P2 by the scale's definition (a data race). The user-visible effect is cosmetic only, so P3 would also be defensible. |
| 19 | Streaming `FetchNext` returns uncapped slices | **TEST FAILS ON BASE** for (a) and (c). **(b): no caller appends** to or mutates the value bytes | P2 (the degraded stream consolidation is real and measured). The corruption itself is latent. |
| 24 | Relay-log stall-flag race | **TEST FAILS ON BASE**: triggered with the real `Send`/`Fetch` and short timeouts | P3 (it needs a 5-minute stall timer to expire within microseconds of a Fetch) |
| 4 | Parallel-insert-worker connections skip the session setup | **REPRODUCED** end to end on a real cluster. The trigger is **wider than stated**: latin1 data is corrupted even on a UTC target | P0, opt-in. Raise the validation level from T to R. |

---

## #16 `Throttler.checkScope` mutates the shared `okMetricCheckResult`

**Verdict: TEST FAILS ON BASE.**

Code (`throttle/throttler.go:1441-1451`): `okMetricCheckResult` is a package-level `*CheckResult`. The exempted-app path runs `result := okMetricCheckResult; result.AppName = matchedApp; return result`, which writes the app's name into the global and returns that same pointer to every caller. The not-running path (`!IsRunning()`) also returns the global.

Tests in `go/vt/vttablet/tabletserver/throttle/throttler_exempt_global_test.go`:
- `TestCheckExemptedAppDoesNotMutateSharedResult` (deterministic, no `-race` needed). It fails on base:
  - `schema-tracker`'s earlier result now reads `messager`, because the same pointer was returned to both callers.
  - `okMetricCheckResult.AppName` expected `""`, actual `"messager"`.
  - After the throttler is disabled, a `vreplication` check returns `AppName="messager"`: "a check on a stopped throttler reports a previous exempted app's name".
- `TestCheckExemptedAppConcurrentRace`: 3 statically exempted apps (schema-tracker, messager, binlog-watcher), 1000 checks each, run concurrently.
  - `go test -race` reports `WARNING: DATA RACE`: a write at `throttler.go:1449` against a write at `throttler.go:1449` from another goroutine, plus reads from the callers.
  - With `-race` the assertion "exempted checks returned another app's name" also fails. Without `-race` it usually passes, because the window is small.
- Bonus: with `result := *okMetricCheckResult; result.AppName = matchedApp; return &result`, both tests pass under `-race`.

**User-visible effect:**
- Only the `AppName` field is corrupted. The global's `ResponseCode` is always OK, and nothing on these paths writes it. The exempted path also skips `check.Check`, so it records no metrics and no `markRecentApp`.
- **No throttling decision can change, and no metric is affected.**
- What does change:
  - The `app_name` and `summary` fields of `CheckThrottler` gRPC responses (`vtctldclient CheckThrottler`), and `AppName` in the `/throttler/check` HTTP JSON.
  - While two exempted apps race, one can receive the other's name.
  - Deterministically: once any exempted app has checked while the throttler ran, every later check against a stopped throttler (disabled via `UpdateThrottlerConfig --disable`, or closed) reports that app's name, e.g. `schema-tracker is granted access` for a `vreplication` check. This lasts until the process restarts.
- Callers of the not-running path that read `AppName` see a stale name. No caller writes to the returned pointer, so there is no second-order corruption.

**Conditions:** the throttler is enabled, and an exempted app checks. The exempted apps are the static ones (schema-tracker with schema tracking, messager with message tables, binlog-watcher with `--watch-replication-stream`) plus any `--throttle-app-exempt` app.

---

## #19 Streaming `FetchNext` returns uncapped sub-slices

**(a) Verdict: TEST FAILS ON BASE.**

`go/mysql/streaming_fetchnext_capacity_test.go` `TestFetchNextValuesHaveCappedCapacity` streams one row (`"aaa","bbb","ccc"`) over a real socket pair through `ExecuteStreamFetch` and `FetchNext`. It fails on base:
- Column 0 has cap 11 and column 1 has cap 7, where both should be 3.
- After `append(row[0].Raw(), "XYZ"...)`, `row[1]` reads **`"YZb"`** instead of `"bbb"`. The append overwrote column 1's length prefix and its first two bytes.

Bonus: with `data[pos : pos+s : pos+s]` in `readLenEncStringAsBytes` (`encoding.go:311`), the test passes.

**(b) Callers: no caller appends to or mutates the row value bytes.** These are all the consumers of `mysql.Conn.FetchNext` rows:
- **vttablet streaming queries (OLAP, and vtgate `StreamExecute`).** The path is `dbconnpool.DBConnection.ExecuteStreamFetch`, then `connpool.Conn.Stream`/`StreamOnce`, then `QueryExecutor` callbacks (optionally through the stream consolidator, which shares `*Result` across followers), then the gRPC send.
  - Along the way, `ReplaceKeyspace` touches only `Fields`. `ResultToProto3`/`RowToProto3` copy the bytes.
  - The only path that skips a proto copy is the in-process vtcombo/local queryservice, where vtgate engines get the same `Value`s. I found no append on `Raw()` there either.
- **schema engine** (`schema/db.go`: views, UDFs, table create times): reads through `ToString` etc.
- **rowstreamer** (VStreamRows: vcopier copy phase, VDiff source and target, `snapshot_conn`):
  - `shouldFilter` only compares.
  - `mapValues` either reuses the `Value` or builds a fresh ksid (`vindexes.Map`). The `binary` vindex returns the `Value`'s own bytes as the ksid, but nothing appends to it.
  - `lastpk` holds references only. Then `RowToProto3Inplace` copies.
  - vreplication (vcopier/vplayer), VDiff and the messager downstream all work on proto-decoded values, not on FetchNext's.
- **resultstreamer** (VStreamResults): `RowToProto3` copies.
- A grep for `append(<x>.Raw()`, `.Raw()[i] =` and similar in non-test code finds only read-only slicing (`mysql/query.go` date parsing, `wrangler/vdiff.go:1488`).

So the column overwrite is **latent**: no current caller is corrupted.

**(c) Stream-consolidator over-count. Verdict: TEST FAILS ON BASE.**

`go/vt/vttablet/tabletserver/stream_consolidator_fetchnext_test.go` `TestStreamConsolidatorAccountsStreamedRowsExactly` does the following:
- Setup: it streams 5000 rows of 20 × 32-byte VARCHAR columns from fakesqldb over the real MySQL protocol, through `dbconnpool.ExecuteStreamFetch`, with the default `StreamBufferSize` of 32 KiB. That produces 97 Results of about 51 rows each.
- It compares `Result.CachedSize(true)` (what `streamInFlight.update` charges) with the same rows copied into exact-capacity values.
- It feeds both sets through `streamInFlight.update` with the default limits: `--consolidator-stream-query-size` = 2 MiB and `--consolidator-stream-total-size` = 128 MiB.

Output on base:

```
97 results of 51 rows x 20 cols x 32 bytes: CachedSize 40026192 vs exact 6586192 (6.1x);
catch-up admits 5 results (2081360 bytes accounted) vs 30 (2054880 bytes)
```

- **Over-count:** 6.1x for this shape. In general it is about (ncols+1)/2 on the value bytes, plus size-class rounding of each cap. It is negligible for 1–2 columns and grows linearly with the column count.
- **Consolidation is abandoned early:**
  - A late follower can join a consolidated stream only while the leader's catch-up buffer is under 2 MiB.
  - On base the buffer is declared full after **5 Results (~255 rows, ~160 KB of row data)** instead of **30 (~1530 rows, ~980 KB)**. The window in which an identical streaming query can piggyback shrinks 6x.
  - The 128 MiB global budget is likewise used up about 6x faster across concurrent streams.
  - Followers that are already attached are not affected, and results stay correct. The cost is only extra MySQL executions of duplicate streaming queries.
- Bonus: with the one-line capped slice, the accounting matches exactly (6586192 = 6586192), 30 = 30, and the test passes.
- The row packet is still retained as a whole, so after the fix the accounting slightly under-counts the length-prefix bytes. That is a minor point.

**Priority:** keep P2. The latent overwrite is P3 on its own. The accounting degradation is real and is on by default (`--enable-consolidator` defaults to true, and that setting also covers streaming SELECTs outside transactions).

---

## #24 Relay-log stall-flag race

**Verdict: TEST FAILS ON BASE.** The race can be triggered with the real `relayLog.Send`/`Fetch` code.

Mechanism (`relaylog.go`):
1. `Send` blocks because the log is full, and its stall timer fires. The timer goroutine has taken `timer.C` and is waiting for `rl.mu`.
2. Meanwhile `Fetch` holds `mu`, drains the items, sets `timedout=false`, broadcasts `canAccept` and releases the lock.
3. If the timer goroutine gets `mu` before the woken `Send`, it sets `timedout=true`.
4. `Send` then checks `rl.timedout` **before** re-checking whether there is room, and returns `relay log I/O stalled` although the log is empty.

`cancelTimer` cannot prevent this, because it only closes `timerDone` and the goroutine is already past its `select`.

Test: `go/vt/vttablet/tabletmanager/vreplication/relaylog_stall_race_test.go` `TestRelayLogSendNoStallAfterFetchDrains`:
- `vplayerProgressDeadline` is set to 20 ms and the log to `maxItems=1`. The log is filled, and a second `Send` blocks.
- The test holds `rl.mu` while a real `Fetch` and the expired stall timer queue up behind it. This is the timer firing while Fetch is in its critical section. The scenario repeats 50 times.
- Result on base, run as `vt` in the MySQL-backed package: `18/50 Sends reported "relay log I/O stalled" although a Fetch had drained the relay log`. A MySQL-free copy gave 16–19 out of 50 over three runs.
- In the "Send wins" ordering, the flag is left stale (`timedout=true`). The next `Fetch` on an empty log then returns immediately with no items (harmless). A ctx cancel while a `Send` waits reports "stalled" instead of EOF (cosmetic).
- Bonus: when `Send` returns the stall error only if the log is still full after waking (`if rl.timedout && (full)`), 0/50 fail, and `TestRelayLogSendStallDeferredWhileThrottled` still passes.

**Real-world conditions:**
- `Send` must have been blocked for the full `vplayerProgressDeadline` (5 min, not a flag), plus any throttle deferral.
- The applier's `Fetch` must drain the log within the microseconds between the timer firing and `Send` re-acquiring the lock.
- The effect is a spurious vplayer error, so the stream retries after `--vreplication-retry-delay`. No data is affected.
- That is extremely unlikely, and a stall at 5 min ± µs would be borderline anyway. **Keep P3.**

---

## #4 Parallel-insert-worker connections skip the session setup

**Verdict: REPRODUCED end to end.**

Setup:
- Cluster: the P4 harness copy (`perf-findings/VAL-C-scripts/`) on BASE 30000 with base binaries. Keyspaces: `src:0 dstoff:0 dston:0 dstflag:0`, all unsharded. MySQL 8.0.46.
- Target mysqlds: `SET GLOBAL time_zone='+05:00'`. The source stays at SYSTEM=UTC.
- Table `tz_t (id PK, ts TIMESTAMP(6), dt DATETIME(6), l1 VARCHAR(32) CHARACTER SET latin1, u8 VARCHAR utf8mb4, vb VARBINARY)`.
  - Rows 1–4 hold handpicked values, including latin1 bytes `E9`, `C3A9` and `E4F6FC`.
  - Rows 101–1000 hold `CONCAT('r', n, X'E9')`.
  - That is 904 rows in total.
- `repro4.sh setup`, then `mt off dstoff`, `mt on dston --config-overrides vreplication-parallel-insert-workers=4`, then `compare`.

Results (`UNIX_TIMESTAMP` and `HEX` read with session tz UTC):

| arm | CHECKSUM | rows differing | row 1 ts (src 1704067200 = 2024-01-01 00:00:00) | l1 `E9` | l1 `C3A9` | l1 `E4F6FC` |
|---|---|---|---|---|---|---|
| workers off (dstoff) | = src | 0 / 904 | 1704067200 | E9 | C3A9 | E4F6FC |
| workers=4 via `--config-overrides` (dston) | ≠ | **904 / 904** | **1704049200 (−5 h: 2023-12-31 19:00:00)** | **'' (truncated)** | **E9 (re-decoded as utf8)** | **3F ('?')** |
| workers=4 via `VTTABLET_EXTRA_FLAGS=--vreplication-parallel-insert-workers=4` on the target vttablet (dstflag) | ≠ | 905 / 905 | −5 h | '' | E9 | 3F |
| same flag, **target at UTC** (`SET GLOBAL time_zone='+00:00'`) | ≠ | **904 / 905** | correct | '' | E9 | 3F |
| same flag, target +05:00, vttablet built with V1's fix (`setDBClientSettings` in `newClientConnection`) | **= src** | 0 / 905 | correct | E9 | C3A9 | E4F6FC |

- **TIMESTAMP:** every copied row is shifted by the target's UTC offset. DATETIME is unaffected. So are utf8mb4 and VARBINARY columns.
- **Non-utf8mb4 strings:** the worker connection's client charset is utf8mb4 (no `set names binary`), so latin1 bytes are decoded as utf8mb4:
  - An invalid sequence is **silently truncated** under vreplication's permissive sql_mode (`E9` → empty, `E4F6FC` → `?`).
  - A valid utf8 sequence is **silently converted** (`C3A9` → `E9`).
  - **This happens on a UTC target too.** The trigger is "parallel insert workers + any latin1 (or other non-utf8mb4) column with non-ASCII data", not only "non-UTC target".
- Only copy-phase rows are affected. A row inserted after the copy is applied by the vplayer on the properly configured connection and is correct (`post`: id 9999 is identical on src and both targets).
- **VDiff detects it:** `VDiff create` on `dston.on` reported `HasMismatch: true`, MatchingRows 1 (the post-copy row) and MismatchedRows 904. So it is silent only for users who skip VDiff before SwitchTraffic.

**Priority:** keep P0 (opt-in). Update the row to validation level **R**, and broaden the trigger to "non-UTC target **or** non-utf8mb4 string columns with non-ASCII bytes". Also mention that VDiff catches it.

---

## Files

In `VAL-C.patch`:
- `go/vt/vttablet/tabletserver/throttle/throttler_exempt_global_test.go` (#16)
- `go/mysql/streaming_fetchnext_capacity_test.go` (#19a)
- `go/vt/vttablet/tabletserver/stream_consolidator_fetchnext_test.go` (#19c)
- `go/vt/vttablet/tabletmanager/vreplication/relaylog_stall_race_test.go` (#24; the package needs MySQL, run it with `runrelay.sh` as vt)
- `perf-findings/VAL-C-scripts/{c.sh,patch_cluster.py,repro4.sh,runrelay.sh}` (#4 repro and the #24 runner)

Cleanup:
- The cluster is down and `/home/vt/VAL-C/c30000` has been removed.
- The orphaned test mysqld from the `vreplication` package run was killed, and its data directory deleted. The test binary and the patched `vttablet` were deleted too.
