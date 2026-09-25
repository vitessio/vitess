# F27: per-row throttler client check (rowstreamer and friends)

## Verification of the finding

- `go/vt/vttablet/tabletserver/vstreamer/rowstreamer.go:443`: `streamQuery` calls
  `rs.vse.throttlerClient.ThrottleCheckOKOrWaitAppName(rs.ctx, throttlerapp.RowStreamerName)` once per
  iteration, and each iteration does one `rs.conn.FetchNext` (one row, streaming query). So one call per row. Confirmed.
- `throttle/client.go` `ThrottleCheckOK`: `lastSuccessfulThrottleMu.Lock(); defer Unlock()`, map lookup
  `c.lastSuccessfulThrottle[checkApp.String()]`, atomic load of `throttleTicks`. `throttlerapp.Name.String()`
  is `string(n)`: no allocation or string building; the cost is mutex + defer + string hash + map probe.
- The client is **shared**: `vstreamer.Engine` has one `throttlerClient` (engine.go:121) used by every
  rowstreamer (per row), resultstreamer (per row), vstreamer (per binlog event, plus heartbeat), and
  `vreplication.Engine` has one client used by every vplayer (per relay-log batch loop iteration) and
  vcopier (per received packet). So concurrent copy streams (VDiff + MoveTables + OnlineDDL, or
  `vreplication-parallel-insert-workers` / several tables copying) all hit the same mutex.
- The doc comments said "The function is not thread safe", which is wrong (it is mutex-protected and is in fact used
  concurrently).
- On a cache miss (every 250 ms tick, per app name) the throttler check runs **while holding the mutex**, so
  all other streams of that client (even other app names with fresh results) block for the duration of
  `throttler.Check` once per tick per app.

## Fix (prototype, in worktree)

`throttle/client.go`:
- `lastSuccessfulThrottle` becomes a `sync.Map` of app name -> `*atomic.Int64` (last successful tick).
- Fast path: `sync.Map.Load` (lock free) + two atomic loads; if the cached tick >= `throttleTicks`, return
  `emptyCheckResult, true` (same result as before for the same tick).
- Slow path: take the mutex, **re-check** (double-checked, so concurrent callers with a stale entry still
  perform a single throttler check, like before), run `throttler.Check`, store the tick on success.
- `clearSuccessfulResultsCache` (test-only) zeroes all entries under the mutex.
- No API change; callers (rowstreamer, vstreamer, vplayer, vcopier, tablegc, querythrottler, rpc_vreplication)
  untouched. ~+30/-10 LOC in one file, plus a new test file.

Semantics: identical results for the same tick. The only difference is that a caller whose entry is fresh no longer waits
behind another caller's in-progress slow-path check (for another app, or one racing a tick boundary); that
is a strict improvement (the old behavior was just head-of-line blocking).

Alternatives considered: checking every N rows / per packet in the rowstreamer would reduce calls further, but
changes throttling responsiveness (a packet can take arbitrarily long if MySQL is slow) and has to be done
per caller; not needed since the fast path is now ~14 ns with no shared writes.

## Benchmarks (measured; shared 4 vCPU box, noisy; interleaved A/B, old impl copied into a _test file)

`BenchmarkThrottleCheckOK` (fresh cache, i.e. the per-row case), `-count 8 -benchtime 300ms -cpu 1,4`:

```
                                   │  old          │  new                          │
ThrottleCheckOK/serial               22.58n ± 112%   15.27n ±  25%  -32.37% (p=0.010)
ThrottleCheckOK/serial-4             21.11n ±  12%   13.38n ±  23%  -36.63% (p=0.000)
ThrottleCheckOK/serial-long-name     20.38n ±  23%   15.47n ±  19%  -24.09% (p=0.000)
ThrottleCheckOK/serial-long-name-4   20.97n ±  10%   16.65n ±  28%  -20.60% (p=0.003)
ThrottleCheckOK/parallel             20.52n ±  15%   14.93n ±  23%  -27.25% (p=0.001)
ThrottleCheckOK/parallel-4           59.59n ±  47%    6.88n ± 170%  -88.45% (p=0.001)
geomean                              25.08n          13.25n         -47.19%
allocs: 0 -> 0
```

(`long-name` = a vplayer-style `vplayer:<workflow>:<uuid>` key.) With 4 goroutines hammering the same
client the old version degrades to ~60 ns/op aggregate (~240 ns latency per call per goroutine: mutex
cache-line ping-pong); the new one scales (read-only shared state).

## End-to-end relevance (estimated, not measured: no mysqld in this environment)

Per row, the rowstreamer loop does FetchNext (packet read + parse + value copies), shouldFilter, mapValues,
RowToProto3Inplace, plus amortized gRPC marshal/send: order of 0.5-2 us per row for typical rows. The old
uncontended check (~20 ns) is ~1-4% of that; the fix saves ~5-8 ns/row uncontended (~0.5-1%). Under several
concurrent copy/vstream streams on the same tablet, contention adds real cost (tens to a few hundred ns
per call in the tight-loop benchmark; in practice lower since streams spend ~98% of time outside the
critical section) and, more importantly, removes head-of-line blocking of every stream behind the
once-per-250ms `throttler.Check` of any stream. Small but free.

## Tests

New `throttle/client_test.go`:
- `TestThrottleCheckOKFreshResultDoesNotLock`: with a fresh cached success and the client mutex held,
  `ThrottleCheckOK` must return. On main it blocks (the test fails after 30 s; also does not compile on main because it
  uses the new `lastSuccessfulTick` helper to pin the cache fresh deterministically against the 250 ms ticker).
- `TestThrottleCheckOKStaleResultChecksThrottler`: fast path is per app and per tick: another app, the same app
  after `throttleTicks` advances, and after `clearSuccessfulResultsCache`, all go through the mutex-serialized check.
- `BenchmarkThrottleCheckOK` (serial, serial-long-name, parallel).

Results: `go test -race ./go/vt/vttablet/tabletserver/throttle/...` passes (count=3 and count=2 with -race, count=2 without).
One earlier full-package -race run reported FAIL, but I did not capture which test, and 7 later runs all passed. That
suggests a pre-existing timing-sensitive test under machine load, but it is not proven. vstreamer and vreplication tests
need mysqld (testenv), which is not available here, so for them only `go vet` (which compiles the tests) was run, and it passed.

## Gotchas

- `sync.Map` never shrinks, and neither did the old map. App names include workflow names, so the entry count grows with
  the number of distinct workflows over the tablet lifetime (same as before; each entry is small).
- `clearSuccessfulResultsCache` now zeroes entries instead of replacing the map; a concurrent slow-path
  caller could re-store a tick right after the clear. That is harmless and the function is only used in tests.
- Uses `sync/atomic` 64-bit ops through `atomic.Int64` (aligned on all platforms incl. 32-bit/arm).
- Release compatibility: internal only, no wire/flag changes.
- Side finding (pre-existing, not fixed): `Throttler.checkScope` exempted-app path does
  `result := okMetricCheckResult; result.AppName = matchedApp`, where `okMetricCheckResult` is a shared
  `*CheckResult` global, so this is a data race and it corrupts the global's AppName. Separate small fix: copy the struct.
