/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package chaos

import (
	"testing"
	"time"
)

// Scenarios for the findings of the semi-sync TLA+ model (doc/design-docs/semi_sync_tla).

// P1: the primary is isolated (from the replicas, VTOrcs, topo and vtgate) while clients wait on
// commits that semi-sync blocks. ERS promotes a replica and sends the old primary a
// SetReplicationSource RPC that outlives the ERS. When the partition heals, the RPC changes the
// old primary to REPLICA without killing the waiting sessions and disables source-side semi-sync,
// which completes the blocked commits: their clients (the write probe waits up to 60s) get an OK
// for rows that only the old primary has (model: b1_srs_ack).
func TestP1IsolatedPrimaryAcksAfterFailover(t *testing.T) {
	runScenario(t, "P1-isolated-primary-acks-after-failover", Options{WriteProbe: true}, func(s *Scenario) {
		s.MarkFault()
		s.Isolate(s.OldPrimary.Group)
		d, ok := s.WaitFor("new primary in topo", 120*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover happened=%v after %.1fs", ok, d.Seconds())
		// Heal while ERS's SetReplicationSource to the old primary is still retrying
		// (--wait-replicas-timeout, 30s by default).
		s.Sleep(5*time.Second, "writes on the new primary while the old one is isolated")
		s.Heal()
		s.Sleep(40*time.Second, "let the repoint land and the probes return")
	})
}

// P2: the primary is cut from both replicas only (vtgate, VTOrc and topo still reach it), so its
// commits wait for ACKs while their clients stay connected. VTOrc fails over (with a single VTOrc,
// after its replica repairs time out) and ERS's SetReplicationSource lands on the old primary
// while it is still PRIMARY: the write probe's transactions, which wait up to 60s, show whether the
// released commits are acknowledged to their clients (model: b1_srs_ack).
func TestP2ReplicationPartitionAcksOnDeposedPrimary(t *testing.T) {
	runScenario(t, "P2-replication-partition-acks-on-deposed-primary", Options{WriteProbe: true}, func(s *Scenario) {
		s.MarkFault()
		for _, r := range s.Replicas() {
			s.Partition(s.OldPrimary.Group, r.Group)
		}
		d, ok := s.WaitFor("new primary in topo", 120*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover happened=%v after %.1fs", ok, d.Seconds())
		s.Sleep(20*time.Second, "let the old primary be repointed and the probes return")
		s.Heal()
		s.Sleep(25*time.Second, "healed; let things converge")
	})
}
