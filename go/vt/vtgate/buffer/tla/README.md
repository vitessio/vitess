# TLA+ model of the vtgate failover buffer

`ShardBuffer.tla` models the concurrency of one `shardBuffer`
(`shard_buffer.go`, `timeout_thread.go`, `buffer.go`) and is checked with the
TLC model checker. It covers request goroutines, the timeout thread, the drain
goroutine and its workers, the async `waitForRequestFinish` goroutines,
keyspace events, client cancellation, the max-failover-duration timer and
`Buffer.Shutdown()`.

Each critical section under `sb.mu` is one atomic step, because none of them
block. Lock-free reads (the atomic state fast path, channel selects, WaitGroup
waits) are separate steps, so TLC explores every interleaving between them.

## Properties

| Property | Kind | Meaning |
| --- | --- | --- |
| `NoRuntimeErrors` | invariant | no double `close(e.done)`, no semaphore over-release, no `sync.WaitGroup.Add` racing `Wait` |
| `SlotConservation` | invariant | every buffer slot is free, queued, or pending release: no leaks |
| `QueueWellFormed` | invariant | the queue only holds blocked requests and is empty unless buffering |
| `BufferingHasTimeoutThread` | invariant | a live timeout thread exists while buffering |
| `TimeoutThreadNotBlind` | invariant | the timeout thread never sleeps on a stale `queueNotEmpty` channel while entries are queued |
| `ShutdownIsFinal` | invariant | after `Shutdown()` returns nothing runs and buffering never restarts |
| `AllRequestsComplete`, `ShutdownCompletes` | liveness | every request and `Shutdown()` eventually finish |
| `WindowEnforced` | liveness | an entry is evicted after its window without relying on the max-failover-duration timer |

## Running

```sh
curl -sSLO https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
java -cp tla2tools.jar tlc2.TLC -workers auto -deadlock -config Safety.cfg   ShardBuffer.tla
java -cp tla2tools.jar tlc2.TLC -workers auto -deadlock -config Liveness.cfg ShardBuffer.tla
java -cp tla2tools.jar tlc2.TLC -workers auto -deadlock -config Window.cfg   ShardBuffer.tla
```

`-deadlock` disables deadlock checking. The model terminates once all requests
finish, and TLC would otherwise report that as a deadlock. Stuck states are
caught by the liveness properties instead.

Raise `Req`, `MaxAttempts`, `Size` or `DrainConcurrency` in a config to check
larger instances. State space grows quickly; three requests with one attempt
each takes a few minutes on four cores.

## Checking that the model catches known bugs

Setting one of the `Bug*` constants to `TRUE` re-introduces a past bug:

* `BugLostWakeup`: the timeout thread read `tt.queueNotEmpty` after
  `oldestEntry()` had released `sb.mu`. A request buffered in between was not
  evicted after `--buffer-window`. It stayed buffered until the failover ended
  or `--buffer-max-failover-duration` passed. Violates `TimeoutThreadNotBlind`
  and `WindowEnforced`.
* `BugNoStoppedRecheck`: no `Buffer.stopped` check under `sb.mu` (#19954).
  Violates `ShutdownIsFinal`.
* `BugRemoveNoRelease`: `remove()` does not release the buffer slot of a
  canceled request. Violates `SlotConservation`.

Keep the model in sync when changing the concurrency of the buffer.
