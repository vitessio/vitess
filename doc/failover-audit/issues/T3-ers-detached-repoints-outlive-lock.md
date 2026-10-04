<!-- Draft for vitessio/vitess, Bug Report template. Not filed. -->
# Bug Report: ERS's SetReplicationSource RPCs keep running after it releases the shard lock

**Suggested labels:** Type: Bug, Component: Cluster management, Component: VTOrc
**Tracker task:** T3

## Overview of the Issue

`reparentReplicas` (`go/vt/vtctl/reparentutil/emergency_reparenter.go`) sends `SetReplicationSource` to every tablet with `replCtx := context.WithTimeout(context.WithoutCancel(ctx), opts.WaitReplicasTimeout)` and returns as soon as one replica succeeds. The other RPCs keep running for up to `--wait-replicas-timeout` (30s in VTOrc) after ERS returned and the shard lock was released.

A late RPC can land after a later reparent has already stopped that replica's replication to revoke the primary being replaced. It points the replica back at that primary (the first ERS's choice) with semi-sync ACKs on. If that primary is still serving (the later ERS cancelled its demotion, see T8), the replica ACKs its blocked writes, and the later ERS promotes a tablet that lacks them.

### Proposed fix

ERS should not leave work running after it releases the lock:
- After the promotion, wait for the remaining replica RPCs, bounded by what is left of `WaitReplicasTimeout`, and cancel the rest before returning. Tablets that were not repointed are fixed later by VTOrc under the lock (`fixReplica`, and `StaleTopoPrimary` for the old primary, which force-demotes first).
- Alternatively, keep the RPCs running but make them carry a fencing token (the reparent's term) that tablets compare against the highest term they have seen, refusing older ones. This is the more general fix and also closes T2 and T4, but it needs a proto change (see the note on replacing `PrimaryTermStartTime` with a term number).

The model configuration `current_fixed` (ERS cancels its RPCs at unlock) passes exhaustively.

### How to test the fix

- Unit test in `emergency_reparenter_test.go`: with a fake TMC whose `SetReplicationSource` to one tablet blocks, ERS must return only after that call was cancelled (assert the call's context is done when `ReparentShard` returns).

## Reproduction Steps

Model only (no E2E yet; the window needs two reparents within 30s and an RPC to an unreachable tablet that becomes reachable):
1. `cd doc/design-docs/semi_sync_tla && ./run.sh detached_repoint` violates `NoLostAck` (24 steps, about 5 min).
2. `./trace.py out/detached_repoint.out`: ERS #1 promotes t2 and its repoint of t3 is still pending; ERS #2 stops t3 and picks t1; the stale RPC repoints t3 to t2; t3 ACKs t2's write; ERS #2 promotes t1.

## Binary Version

```sh
main at fb653f2 (version 25.0.0-SNAPSHOT)
```

## Operating System and Environment details

TLA+ model `doc/design-docs/semi_sync_tla/SemiSyncFailover.tla`, TLC v1.8.0: 3 tablets, `semi_sync`, one or two VTOrcs. The model follows the code on `main`; the README maps each action to its Go function.

## Log Fragments

```sh
n/a (model counterexample; see the reproduction steps)
```

Full context: `doc/failover-audit/SemiSyncFailover.md`.
