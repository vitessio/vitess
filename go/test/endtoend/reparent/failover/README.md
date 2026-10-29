# failover

End-to-end tests of failover scenarios: reparents interleaved with restarts, locks and
replication states that a test arranges. VTOrc does not run unless a test starts it.

The controls pass on main and show that the fixture behaves as scenarios rely on:

- `TestRestartReplicaVttabletKeepsMysqld`: restart only a replica's vttablet; its mysqld and
  replication state stay.
- `TestShardLockControl`: hold a test-owned shard lock past its etcd lease TTL by checking it
  periodically, and detect a lost lease.
- `TestERSHeldByApplierBarriers`: hold a real EmergencyReparentShard after it stops the
  replicas and before it promotes, using read locks that block the appliers of the two most
  advanced replicas while a third replicates through one of them. Release the locks and verify
  the promotion. ERS holds the shard lock through the hold, and nothing renews the lock's lease
  there, so the hold has a budget well within the lease TTL. An attempt that exceeds it, or
  whose lease is about to expire at the release, fails as an invalid control.

The regressions protect replication initialization under a held shard lock (#21549). A
replica that starts or finishes a restore while another operation holds the lock answers
RPCs, leaves replication unchanged and stays out of rotation until replication runs from the
shard primary:

- `TestERSRestartedReplicaStaysStopped`: a replica's vttablet restarts during the held ERS;
  its receiver stays stopped until ERS repoints it.
- `TestStoppedReplicaRestartedUnderShardLock`: a replica with stopped replication restarts
  under a test-owned lock; its wait for the lock times out without leaving a request queued,
  and replication stays stopped after the release, until VTOrc starts it.
- `TestSiblingReplicasRestartTogether`: all replicas restart at once with stopped replication;
  each waits briefly for the others' initialization, and all resume replication without VTOrc.
- `TestERSRepointsRestartingReplica`: a replica restarts during the held ERS, which needs its
  RPCs to repoint it while it waits for the lock that ERS holds; ERS repoints it.
- `TestRestoreUnderShardLock`: `RestoreFromBackup` through vtctld under a test-owned lock;
  vtctld's follow-up, which sets the replica's source, waits for the lock, then starts
  replication from the primary.
- `TestRestoreDuringERS`: `RestoreFromBackup` through vtctld during the held ERS; the
  follow-up does not repoint the replica while ERS holds the lock, and after ERS it reads the
  new primary, once the new primary has published itself in the shard record.

Run the package through the CI entry point, keeping the data of every run:

```
go run test.go -docker=false -keep-data -print-log reparent_failover
```

Or directly, with the binaries of the tree under test first in `PATH`:

```
go test -count=1 -v ./go/test/endtoend/reparent/failover/
```

The shard lock control needs the etcd version that CI installs: `TryLock` blocks instead of
failing fast on etcd 3.7 and later (#21548).

A failed test prints the last lines of each component log, which is the only failure data
that CI keeps. It leaves its `VTDATAROOT` in place, but `test.go` removes the data of a run
unless it runs with `-keep-data`.
