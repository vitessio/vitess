# EmergencyReparentShard (ERS)

- ERS must prioritize **certainty** that we picked the most-advanced candidate
- ERS must error when the most-advanced candidate is not clear, and/or a split-brain is suspected
- ERS must avoid introducing errant GTIDs on replicas. This includes writes that are considered unacknowledged to the client as MySQL cannot rewind GTIDs of any kind
- Changes should prioritize reducing points of failure - avoid new RPCs or work that may delay or make ERS more brittle
- ERS must error if a shard contains a mix of GTID-based and non-GTID-based replication. Their position semantics differ (`Combined` = retrieved+executed for GTID vs. executed-only for non-GTID), so a unified split-brain / most-advanced check across both is unsafe
- For non-GTID flavors, ERS must wait on every candidate and fail on any error. The "filter to leading group + short-circuit on first success" optimization is only safe for GTID-based flavors, where `Combined` is distinct from the executed position
- Candidates at the same received (`Combined`) position converge to identical state once their relay logs are applied, which is what makes short-circuiting the relay-log wait on the first success safe. Leading candidates with incomparable positions are a suspected split brain and must all be waited on — a failed one must not be dropped before the split-brain check sees it
- Tablets that fail the relay-log wait are removed from candidacy, but only ever from a uniform leading group, and their received positions must still corroborate errant GTID detection — dropping that evidence can falsely flag a surviving leader as errant and elect a tablet that is behind
- During the stop-replication phase, a single error from the known PRIMARY tablet is expected and tolerated (we are abandoning a dead primary). On any other partial failure, `haveRevoked` must return true before ERS proceeds, guaranteeing no further writes can be accepted by any reachable tablet
- `NewPrimaryAlias` is not a bypass for safety checks. An explicitly-requested primary must still pass every guard: no errant GTIDs, at least as advanced as the winning position, no `MustNot` promotion rule, in-cell if `PreventCrossCellPromotion`, and able to establish forward progress with reachable tablets
- The promoted primary must have completed a relay-log apply wait. If errant-GTID detection eliminates every tablet that completed the first wait, ERS must re-wait on the surviving candidates before promotion — otherwise we risk promoting a tablet with received-but-unapplied transactions
- Any new pipeline step that stops replication on a tablet must add that tablet to `replicasToRestart`, so the deferred cleanup can recover it if ERS aborts. The code can't enforce this — review carefully
