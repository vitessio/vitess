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

| Config | Bounds | Expected result |
|---|---|---|
| `MCTwoPC.cfg` | 2 RMs, 1 restart, 1 resolution | Atomicity holds (2.3M states, about 1 minute on 4 cores) |
| `MCTwoPCDeep.cfg` | 1 RM, 2 resolvers, 2 restarts, 2 resolutions | Atomicity holds (2.1M states) |
| `MCTwoPCNoRedoFix.cfg` | as Deep, without the redo-pending fix | `NoLostCommit` is violated |
| `MCTwoPCOrphan.cfg` | as Deep, no non-retryable failures | `NoOrphanPrepare` is violated |
| `MCTwoPCLocks.cfg` | as Orphan | `LocksHeld` is violated |

The atomicity configs check `CommitNeedsDecision`, `DecisionConsistent`,
`CommitDurable` and `NoLostCommit`.

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
The tests are `TestTabletServerCommitPreparedAfterRetryableRedoFailure` and
`TestPrepRedoPending`.

### Orphaned prepared transaction after a timed-out Prepare (open)

`MCTwoPCOrphan.cfg` finds this interleaving:

1. Prepare puts the connection in the prepared pool.
2. The coordinator times out on Prepare, sets the record to ROLLBACK and sends
   RollbackPrepared.
3. RollbackPrepared deletes the redo log, which does not exist yet.
4. The timed-out Prepare saves the redo log.
5. RollbackPrepared's deferred step rolls back the pooled connection.
6. The record is concluded.

The redo log is left prepared with nothing to resolve it. The next redo
prepares it again, and it holds its row locks until an operator concludes it.
It is reported by the `Unresolved` `ResourceManager` gauge.

This needs Prepare to save its redo log after its caller timed out, which the
cancelled RPC context makes unlikely. It does not break atomicity.

### Unlocked rows behind a prepared redo log (accepted by design)

`MCTwoPCLocks.cfg` shows that a serving RM can have a prepared redo log with no
connection holding its locks, for example after a retryable redo failure or a
retryable `CommitPrepared` failure. Other transactions can then change the rows
before the redo is applied. The design accepts this for availability and
alerts on it (see "Data Guarantees" in the design document).
