<!-- Draft for follow-up work on the chaos harness, not for vitessio/vitess. Not filed. -->
# Fix the remaining chaos harness artifacts

**Tracker task:** T23
**Owner area:** Failover validation (chaos harness)

## Problem

Three checks of the harness report problems that are not Vitess bugs:
- S11b removes its injected applier delay with `CHANGE REPLICATION SOURCE TO SOURCE_DELAY=0` while both threads are stopped, which itself discards the relay log (764 "lost" writes).
- The S13 scenarios assert `relay_log_recovery=0` and `sync_relay_log=1`, so under the `semisync-*` profiles they report HARNESS violations.
- The split-brain check flags any two tablets that are writable and report PRIMARY, including an isolated old primary that cannot commit because no ACK can reach it.

## Evidence

`go/test/endtoend/vtorc/chaos/s11_test.go`, `s13_test.go`, `observer.go`, `invariants.go`.

## Proposed change

See steps.

## Steps

1. S11b: remove the delay with a receiver-only change (applier running), or restart only the SQL thread.
2. S13: give it its own profile that sets the relay-log settings it needs.
3. Split brain: report "writable and able to ACK" (semi-sync off, or a connected semi-sync replica) as the violation, and plain dual-writable as a note.

Full context: `doc/failover-audit/SemiSyncFailover.md`.
