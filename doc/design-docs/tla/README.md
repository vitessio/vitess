# TLA+ model of atomic distributed transactions

`TwoPC.tla` models the two-phase commit protocol behind
`transaction_mode = TWOPC`, as described in
[AtomicDistributedTransaction.md](../AtomicDistributedTransaction.md) and
implemented in:

| Component | Code |
|---|---|
| Coordinator and resolver | `go/vt/vtgate/tx_conn.go` (`commit2PC`, `errActionAndLogWarn`, `resolveTx`) |
| Tablet RPC handlers | `go/vt/vttablet/tabletserver/dt_executor.go` |
| Prepared pool | `go/vt/vttablet/tabletserver/tx_prep_pool.go` |
| Redo of prepared transactions | `go/vt/vttablet/tabletserver/tx_engine.go` (`prepareFromRedo`) |

The model covers a single distributed transaction. It includes:

- the coordinator VTGate and resolver VTGates, which may act concurrently;
- asynchronous RPCs whose callers may time out while the handler keeps running;
- handlers split into steps wherever the code uses separate database transactions;
- tablet restarts that lose connections and the prepared pool but keep the redo
  log and the transaction record, followed by redo;
- the transaction killer;
- commit, StartCommit and redo outcomes, including errors after MySQL applied
  the commit.

The model leaves out isolation, conflicting writes from other transactions,
the content of redo statements, query rules (Online DDL, MoveTables) and
operator actions.

## Properties

| Property | Meaning |
|---|---|
| `CommitNeedsDecision` | An RM commits only after the MM committed the decision. |
| `DecisionConsistent` | The transaction record agrees with the MM's own transaction. |
| `CommitDurable` | After a commit decision, every RM is committed or still recoverable. |
| `NoLostCommit` | A committed transaction is not concluded while an RM is uncommitted. |
| `NoOrphanPrepare` | No prepared transaction is left behind once the record is concluded. |
| `LocksHeld` | A serving RM with a prepared redo log holds the prepared connection. |

## Running

Download `tla2tools.jar` from the [TLA+ releases](https://github.com/tlaplus/tlaplus/releases)
(v1.8.0 was used), then run from this directory:

```sh
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -workers auto -config MCTwoPC.cfg MCTwoPC.tla
```

The constants `FixRedoPending`, `FixDTIDLock` and `FixKeepLocks` switch the
fixes below on or off, so that each config without one shows the problem it
fixes. `RetryableLockLoss` allows the retryable failures that still release
row locks (see the last finding).

| Config | Bounds | Expected result |
|---|---|---|
| `MCTwoPC.cfg` | 2 RMs, 1 restart, 1 resolution | Invariants hold (566K states, under a minute on 4 cores) |
| `MCTwoPCLarge.cfg` | 2 RMs, 2 restarts, 2 resolutions | Invariants hold (6.7M states, about 2.5 minutes on 4 cores) |
| `MCTwoPCDeep.cfg` | 1 RM, 2 resolvers, 2 restarts, 2 resolutions | Invariants hold (973K states) |
| `MCTwoPCLocks.cfg` | as Deep, `RetryableLockLoss = FALSE` | Invariants hold, including `LocksHeld` |
| `MCTwoPCNoRedoFix.cfg` | as Deep, `FixRedoPending = FALSE` | `NoLostCommit` is violated |
| `MCTwoPCNoDTIDLock.cfg` | as Deep, `FixDTIDLock = FALSE` | `NoOrphanPrepare` is violated |
| `MCTwoPCNoKeepLocks.cfg` | as Locks, `FixKeepLocks = FALSE` | `LocksHeld` is violated |
| `MCTwoPCLockLoss.cfg` | as Deep | `LocksHeld` is violated |

The first three configs check `CommitNeedsDecision`, `DecisionConsistent`,
`CommitDurable`, `NoLostCommit` and `NoOrphanPrepare`. The state space grows
quickly with the bounds: with two RMs, each further resolution attempt
multiplies it, so larger bounds call for a single RM or a longer run.

### Assumptions

- Semi-sync replication: a commit acknowledged by a primary survives a
  reparent, so a reparent is modeled as a tablet restart.
- The abandon age (`--twopc-abandon-age`, 15 minutes by default) is longer than
  the transaction timeout (`--queryserver-config-transaction-timeout`, 30
  seconds by default). A resolver therefore acts only after the transaction
  killer has rolled back every unprepared participant, so a Prepare that
  arrives late cannot prepare a transaction that a resolver already rolled
  back. With a shorter abandon age this is not guaranteed.

## Findings

### Lost commit after a retryable redo failure (fixed)

Found by reading the code and confirmed by `MCTwoPCNoRedoFix.cfg`:

1. All RMs prepare and the MM commits the decision.
2. An RM restarts and its redo fails with a retryable error. The redo log stays
   prepared, but the DTID was in neither the pool's connections nor its
   reservations.
3. `CommitPrepared` found no entry, treated the transaction as already
   committed and returned success.
4. The coordinator concluded the transaction without the RM's writes.

`prepareFromRedo` now reserves the DTID with a redo-pending error, so
`CommitPrepared` fails until a later redo prepares the transaction again.
Tests: `TestTabletServerCommitPreparedAfterRetryableRedoFailure` and
`TestPrepRedoPending`.

### Orphaned prepared transaction after a timed-out Prepare (fixed)

Found by the model and confirmed by `MCTwoPCNoDTIDLock.cfg`:

1. Prepare puts the connection in the prepared pool.
2. The coordinator times out on Prepare, sets the record to ROLLBACK and sends
   RollbackPrepared.
3. RollbackPrepared deletes the redo log, which does not exist yet.
4. The timed-out Prepare saves the redo log.
5. RollbackPrepared rolls back the pooled connection.
6. The record is concluded.

The redo log was left prepared with nothing to resolve it. The next redo
prepared it again, and it held its row locks until an operator concluded it.

Prepare, CommitPrepared and RollbackPrepared now take a per-DTID lock on the
tablet (`dtidLocks`), so a RollbackPrepared waits for a running Prepare.
Tests: `TestTabletServerRollbackPreparedWaitsForPrepare` and `TestDTIDLocks`.

### Released locks after a failed redo log deletion (fixed)

Found by the model and confirmed by `MCTwoPCNoKeepLocks.cfg`: when
RollbackPrepared failed to delete the redo log, it still rolled back the
prepared connection. Other transactions could then change the rows, and the
next redo prepared the transaction again over them.

RollbackPrepared now keeps the prepared transaction when the deletion fails,
and the retry releases it. Test: `TestTxExecutorRollbackRedoFailKeepsPrepared`.

### Released locks after retryable redo and commit failures (open)

`MCTwoPCLockLoss.cfg` shows that a serving RM can still have a prepared redo
log with no connection holding its row locks:

- A redo that fails with a retryable error: the tablet starts serving anyway,
  and the transaction is prepared again only by a later redo.
- A `CommitPrepared` whose commit fails with a retryable error without being
  applied: the connection is rolled back and the redo log stays prepared.

Other transactions can change the rows in the meantime, and the later redo
applies the statements over them. The design accepts this for availability
and alerts on it (see "Data Guarantees" in the design document).
`MCTwoPCLocks.cfg` checks that no other path releases the locks.
