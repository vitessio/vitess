<!-- Draft for follow-up work on the chaos harness, not for vitessio/vitess. Not filed. -->
# Fix the remaining chaos harness artifacts

**Tracker task:** T23
**Owner area:** Failover validation (chaos harness)

## Problem

Some checks of the harness reported problems that are not Vitess bugs. Fixed on this branch:
- The profile's my.cnf replaced `EXTRA_MY_CNF`, so S13 ran with `relay_log_recovery=1` and reported HARNESS violations. The profile's file, `EXTRA_MY_CNF` and `CHAOS_RELAY_LOG_SAFE=1` are now joined, the later winning.
- S13's tear check expected the cut on an event boundary, which compressed transactions (`Transaction_payload`) defeat; it now reads mysqlbinlog's "truncated in the middle of event".
- S11 and S11b removed their injected applier delay with `CHANGE REPLICATION SOURCE TO SOURCE_DELAY=0` while both threads were stopped, which itself purges the relay log (S11b: 764 "lost" writes; S11 with `relay_log_recovery=0`: 527). `delayApplier` now starts the receiver around the change when it is stopped. S11 was rerun; S11b was not.

Still open:
- The split-brain check flags any two tablets that are writable and report PRIMARY, including an isolated old primary that cannot commit because no ACK can reach it.

## Evidence

`go/test/endtoend/vtorc/chaos/cluster.go`, `s11_test.go`, `s13_test.go`, `observer.go`, `invariants.go`; reports in `results/fixed-relaylog-safe-semisync-3vtorc`.

## Steps

1. Rerun S11b with the fixed `delayApplier`.
2. Split brain: report "writable and able to ACK" (semi-sync off, or a connected semi-sync replica) as the violation, and plain dual-writable as a note.

Full context: `doc/failover-audit/SemiSyncFailover.md`.
