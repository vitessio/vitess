# EmergencyReparentShard (ERS)

The goals, rules and limitations of ERS. Changes to `go/vt/vtctl/reparentutil` must uphold these; they are review requirements, not aspirations.

- ERS must prioritize **certainty** that we picked the most-advanced candidate
- ERS must error when the most-advanced candidate is not clear, and/or a split-brain is suspected
- ERS must avoid introducing errant GTIDs on replicas. This includes writes that are considered unacknowledged to the client as MySQL cannot rewind GTIDs of any kind
- ERS runs while the shard cannot accept writes: it must resolve the availability loss, and every stage is time-bound (`--wait-replicas-timeout` for replica waits, `topo.RemoteOperationTimeout` for primary RPCs) — each added stage extends the total outage. Changes should prioritize reducing points of failure - avoid new RPCs or work that may delay or make ERS more brittle
- ERS holds the shard lock for its entire run and re-checks it at every phase boundary (`CheckShardLocked`), aborting on loss; it is the only guard against two concurrent reparents. Any new phase must re-check the lock before acting
- ERS must error if a shard contains a mix of GTID-based and non-GTID-based replication. Their position semantics differ (`Combined` = retrieved+executed for GTID vs. executed-only for non-GTID), so a unified split-brain / most-advanced check across both is unsafe
- ERS must never promote a tablet with received-but-unapplied transactions. Replica candidates establish this by completing a relay-log apply wait; a demoted former primary is exempt, since it was not replicating its received and executed positions are the same
- ERS must only promote a candidate that can make forward progress once promoted: under a semi-sync durability policy the new primary needs enough reachable ackers, or it wedges waiting for an ACK that will never arrive. `canEstablishForTablet` guards this and any change to candidacy must keep it truthful
- All promotion rules, acker counts and per-replica semi-sync flags flow from the `policy.Durabler`; never hardcode semi-sync behaviour outside the durability policy
- During the stop-replication phase, a single error from the known PRIMARY tablet is expected and tolerated (we are abandoning a dead primary). On any other partial failure, `haveRevoked` must return true before ERS proceeds, guaranteeing no further writes can be accepted by any reachable tablet
- `NewPrimaryAlias` is not a bypass for safety checks. An explicitly-requested primary must still pass every guard: no errant GTIDs, no `MustNot` promotion rule, in-cell if `PreventCrossCellPromotion`, and able to establish forward progress with reachable tablets. It does not need to lead the election: a requested candidate that is behind replicates from the intermediate source, and must reach the winning position before it is promoted
- The most-advanced tablet becomes the intermediate source, which is a replication source and not an automatic winner: the final primary must come from the filtered valid candidates, and catch up to the intermediate source before the switch
- `PopulateReparentJournal` on the promoted primary is the system of record for promotions: errant GTID detection counts journal rows to decide which tablets can serve as evidence, so every promotion must write it
- The reparent sorter (via `ElectNewPrimary`) and durability helpers like `canEstablishForTablet` are shared with `PlannedReparentShard`: changes to candidate ordering or semi-sync accounting affect PRS too
- Any new pipeline step that stops replication on a tablet must add that tablet to `replicasToRestart`, so the deferred cleanup can recover it if ERS aborts. The code can't enforce this — review carefully
