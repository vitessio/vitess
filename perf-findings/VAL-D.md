# VAL-D: validation of bugs #12, #13 and #14

Base: worktree HEAD (contains aa9ccf9). Base binaries in `/home/vt/bin` (vcs.revision eacb38da, which contains the
10 s health-check backoff cap f92fcc1847). Cluster BASE 40000, data in `/home/vt/VAL-D` (removed afterwards).
`VAL-D.patch` has only tests and repro tools:

| file | purpose |
|---|---|
| `go/vt/vttablet/grpctmclient/fullstatus_failfast_test.go` | #13 unit tests (fail on base) |
| `go/vt/vttablet/tabletmanager/vdiff/stop_lock_wait_test.go` | #14 test (fails on base, MySQL-backed package) |
| `go/test/valrepro/vtgaterestart/main.go` | #12 repro: load through vtgate, kill/restart one vttablet, measure |
| `go/test/valrepro/scripts/vtorc_failover.sh` | #13 end to end: 3-tablet semi_sync shard + VTOrc, kill -9 the primary host |
| `go/test/valrepro/scripts/c.sh`, `runvt.sh` | cluster wrapper (BASE 40000, uses P5's `cluster.sh`), run a test binary as `vt` |

---

## #13 VTOrc `FullStatus` hangs ~15 s on a refused connection: **TEST FAILS ON BASE**; user impact is mostly after the failover, not detection

**Cause (confirmed).** `grpctmclient` dials every tablet-manager connection with `grpcclient.FailFast(false)`, so every
call uses `grpc.WaitForReady(true)`. An RPC to a port that refuses connections waits for the caller's deadline instead
of failing with `Unavailable`. VTOrc's `inst.fullStatus` uses `topo.RemoteOperationTimeout` (15 s).

**Tests** (both FAIL on base; pure Go, no MySQL; `go test ./go/vt/vttablet/grpctmclient -run TestFullStatusFailsFast`):
- `TestFullStatusFailsFastOnRefusedConnection`: a fresh dial to a closed port.
- `TestFullStatusFailsFastAfterTabletDies`: a pooled connection to a live fake tablet manager, then the server stops (the VTOrc case).

```
Error: "15.003151876s" is not less than "2s"
Messages: FullStatus to a refused port took 15.003151876s (err: Code: DEADLINE_EXCEEDED
  rpc error: code = DeadlineExceeded desc = latest balancer error: connection error: desc = "transport: Error while
  dialing: dial tcp 127.0.0.1:32821: connect: connection refused")
--- FAIL: TestFullStatusFailsFastAfterTabletDies (15.00s)   "15.000390857s" is not less than "2s"
```
Bonus: with `FailFast(true)` in `grpctmclient` (a scratch change, reverted, not a proposed fix) both tests pass in 0.04 s.

**End to end** (`vtorc_failover.sh`: 3 tablets, semi_sync, default VTOrc flags, kill -9 of vttablet+mysqld_safe+mysqld,
random phase; 3 trials t1–t3; times relative to the kill):

| | t1 | t2 | t3 |
|---|---|---|---|
| DeadPrimary analysis | 4.17 s | 6.62 s | 6.88 s |
| ERS finished | 4.30 s | 6.76 s | 7.02 s |
| shard lock held after ERS by the **DeadPrimary** recovery | until 17.20 s (**+12.9 s**) | until 19.63 s (**+12.9 s**) | until 19.88 s (**+12.9 s**) |
| shard lock then taken by **StaleTopoPrimary** (demote old primary) | 18.16 s → (>34 s, end of log) | 20.66 → 50.83 s (**30 s**) | 35.86 → 66.31 s (**30 s**) |

- **Detection:** not delayed much, as P5 said. The 1 s `lastAttemptedCheckTimer` marks the check invalid while
  `FullStatus` hangs, so fail-fast would only save up to ~1 s per failover. The detection times match P5's model
  (U(0, 6 s) + 1 s + U(0, 1 s) + 0.2 s).
- **ERS is not delayed by `recheckPrimaryHealth`:** it runs only for non-shard-wide recoveries (`!isShardWideRecovery`),
  so not for DeadPrimary. It does hang for 15 s in the replica-side `ReplicationStopped` recovery, which then aborts
  (t1: "aborting ReplicationStopped, primary mitigation is required" at +17.1 s). That runs in parallel and does not
  hold the shard lock. The pre-ERS refresh of the DeadPrimary recovery skips the dead primary
  (`shardWideRecoveryIgnoredTablets`).
- **The real cost comes after the failover.** The post-ERS `forceRefreshAllTabletsInShard` calls
  `DiscoverInstance(deadPrimary, true)` inside `executeCheckAndRecoverFunction`, before the deferred shard unlock, so
  the lock stays held for ~13 s after ERS. Then `StaleTopoPrimary` holds it for 30 s: `DiscoverInstance` takes 15 s and
  `forceDemotePrimary`→`DemotePrimary` takes another 15 s, both against the dead tablet. Result: the shard is locked for
  ~43 of the 45 s after ERS in every trial.
- **User-visible example (t3):** `PlannedReparentShard` issued 1 s after the failover waited ~11.6 s for the shard lock
  (lock keys=2 from 8.25 to 19.88 s). It then failed after **26.7 s** in total with
  `DeadlineExceeded ... dial tcp 127.0.0.1:40200: connect: connection refused`: vtctld's tmclient has the same
  WaitForReady behaviour. A second failure in the shard during this window would also be delayed, because VTOrc's
  recovery `TryLock` fails with "lock already exists" every second.
- **By code reading (not run):** the opt-in quorum path `PrimaryTabletUnreachableByQuorum` deliberately refreshes the
  primary **before** ERS. Its comment says "A still-dead vttablet fails the refresh quickly (connection refused)", which
  the tests above disprove. On that path, **ERS is delayed by 15 s**.

**Conditions:** the vttablet process is dead but its host is up (connection refused). This covers a process crash,
kill -9 and OOM kill. A hung host with packets dropped would time out anyway.

**Suggested priority:** keep it, but rewrite it as "tmclient WaitForReady: 15 s hangs on dead tablets hold the shard lock
~43 s after every VTOrc failover (blocks PRS/second failover); +15 s before ERS on the quorum path; ≤1 s on detection".
**P2** by default (P1 if quorum ERS is commonly enabled).

---

## #12 vtgate took ~40 s to route to a restarted tablet: **CANNOT REPRODUCE** (downgrade)

**Mechanism (code).** vtgate's per-tablet health check (`discovery/tablet_health_check.go: checkConn`) retries the
`StreamHealth` stream with a backoff that starts at `--healthcheck-retry-delay` (2 ms). The backoff doubles and is
capped at `maxHealthCheckRetryDelay` = 10 s (#19967, which is in base and in the test binaries). Each retry opens a new
connection (`closeConnection` sets `thc.Conn = nil`), so gRPC's own reconnect backoff does not apply. A received message
resets the backoff. The retry instants after a failure are therefore 2 ms, 6 ms, …, 4.1, 8.2, 16.4, 26.4, 36.4, 46.4 s,
…. After the tablet is serving again, vtgate reconnects at the next retry instant: **at most ~10 s later**. The topology
watcher's 1 min `--tablet-refresh-interval` matters only if the tablet's address or ports change.
`--healthcheck-timeout` (1 min) matters only for a stream that stays open but silent. Before #19967 the backoff grew up
to the 1 min health-check timeout, which could produce ~30–60 s delays. That is a plausible explanation for the original
observation, if it ran on an older build.

**Measurements** (`vtgaterestart`, 4 threads × 5 ms queries through vtgate, shard with 1 primary + 1 replica, 31
restarts). "route" is the time from the tablet reporting `SERVING` to the first successful query through vtgate.

| scenario | trials | down time | route delay | total outage |
|---|---|---|---|---|
| replica, kill -9, immediate restart | 6 | 0 | −0.01 … 0.11 s | 0.14–0.29 s |
| replica, kill -9 | 3+1+1+1+3+1+1+3+1 | 3/5/12/15/20/25/30/45/52 s | 0.99, 3.0×3, 4.24, 1.24×3, 6.24, 1.22, 6.22×4, 1.23×3, 4.23 s | ends exactly at the retry instants 4.1/8.2/16.4/26.4/36.4/46.4/56.4 s |
| primary (writes), kill -9, immediate | 3 | 0 | 0.10–0.13 s | 0.27–0.29 s |
| primary (writes), SIGTERM, immediate | 3 | (10 s shutdown) | 0.09–0.11 s | **10.27 s** |
| replica, SIGTERM, immediate | 2 | (10 s shutdown) | 0.11 s | **10.27 s** |
| 2 replicas, restart one (term, or kill -9 + 20 s down) | 3 | – | – | 0 errors (vtgate retries the other replica) |

Maximum measured vtgate-side delay: **6.3 s**, in line with the ≤10 s bound. The error during the outage is
`no healthy tablet available` once the stream breaks. The tablet itself serves again 0.15–0.22 s after it starts.

**Related new finding (reproduced, deterministic):** a **graceful** vttablet shutdown (SIGTERM) causes **10 s** of
errors for a single-tablet target (the primary, or the only replica), whereas kill -9 causes 0.3 s. The log shows
`Initiated graceful stop of gRPC server` → 10 s later `OnTermSync hooks timed out`. `GRPCServer.GracefulStop` (an
OnTermSync hook) waits for open streams such as vtgate's `StreamHealth`, and the tablet does not go non-serving until
OnClose. During those 10 s vtgate still considers the tablet healthy, but new RPCs fail with
`connection error: desc = "transport: Error while dialing: dial tcp …: connect: connection refused"` (8.2k errors per
trial). The P4 harness restarted tablets with SIGTERM, so this adds 10 s per restart, but that is still not 40 s. The
same 10 s OnTermSync timeout is what lets V3 #1's self-RPC run out of time.

**Suggested priority:** #12 → **P3 / close as not reproducible** (the vtgate delay is bounded at ≤10 s by design). Add a
new **P2** entry: "graceful vttablet SIGTERM: 10 s of connection-refused errors while vtgate still routes to it
(GracefulStop waits on StreamHealth streams until `--onterm-timeout`)".

---

## #14 `VDiff stop/delete` blocked ~7 min waiting for the workflow lock: **TEST FAILS ON BASE**, with a corrected mechanism

**The claim as written is inaccurate.** A controller that is itself waiting in `LockName` **does** abort on
`ct.cancel`:
- `tableDiffer.initialize` passes the controller ctx to `ts.LockName`;
- etcd2's `waitOnLastRev` selects on `ctx.Done()`;
- the retry loop selects on `ctx.Done()` and `ct.done`.

The test asserts this too, and that part passes.

**Actual cause.** `tableDiffer.initialize` first takes the **engine-wide** `vdiffEngine.snapshotMu` with a plain
`Lock()`, and holds it through the whole lock-retry loop. The loop backs off up to `topo.LockTimeout`, and
`LockName` waits up to 45 s per attempt. Any other VDiff controller on the same tablet, for the same workflow **or any
other workflow**, blocks in `snapshotMu.Lock()`, which ignores its context. Then:
- `handleStopAction` and `handleDeleteAction` call `controller.Stop()` (cancel, then `<-ct.done`) **while holding
  `vde.mu`**, so stopping a queued VDiff blocks until the lock-waiting VDiff gets its workflow lock or is stopped
  itself. With an orphaned lock (24 h lease, #6/VAL-A) that never happens.
- `VDiff delete all` stops controllers in row order. If it reaches a queued controller before the one holding
  `snapshotMu`, it hangs. This matches V3: "blocked about 7 minutes, until I deleted the orphaned key".
- While `vde.mu` is held, every other VDiff create/stop/delete on that tablet blocks too.

**Test:** `TestVDiffStopWhileWaitingForWorkflowLock` (package `vdiff`, which is MySQL-backed only because of its
`TestMain`; built as root with `GOFLAGS=-trimpath` and run as `vt`). It uses a memorytopo and holds the first
workflow's named lock. VDiff 1's `initialize` waits for that lock while holding `snapshotMu`, and VDiff 2 queues behind
it. `ct.Stop()` of VDiff 2 must return within 10 s. The test has two subtests: VDiff 2 on the same workflow, and on
another workflow whose lock is free.
```
--- FAIL: TestVDiffStopWhileWaitingForWorkflowLock (20.02s)
    --- FAIL: .../same_workflow (10.01s)   VDiff stop of a VDiff queued behind another VDiff's workflow-lock wait did not return within 10s
    --- FAIL: .../other_workflow (10.01s)  (same message)
```
The second assertion, "stopping the VDiff that waits in `LockName` is prompt", passes on base.

Bonus: a scratch change (reverted) that acquires `snapshotMu` with `TryLock` in a loop that watches `ctx.Done()` and
`ct.done` makes both subtests pass in 0.02 s.

**Conditions:** two or more VDiffs initializing on the same target tablet (any workflows) while one of them cannot get
its workflow lock. Examples: an orphaned lock (#6), a long SwitchTraffic, or many target shards queuing on the same
`ks/wf` named lock. A single VDiff can always be stopped promptly.

**Suggested priority:** **P2**, validated **T**. It is tied to #6: fixing #6 removes the 24 h trigger, but a 45 s lock
wait still blocks stop and delete, and it also blocks VDiffs of unrelated workflows.
