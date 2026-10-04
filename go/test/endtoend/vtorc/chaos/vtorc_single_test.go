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

// Single-VTOrc deployments: only one VTOrc, in one of the three cells. The primary starts in
// zone1; the VTOrc runs in zone2 (a replica's cell) or zone1 (the primary's cell).

// V1: the only VTOrc runs in a replica cell; that cell (VTOrc + replica) is partitioned from
// the other two cells' tablets, VTOrcs and etcds. No failover is expected; writes continue
// with the other replica acking.
func TestV1SingleVTOrcReplicaCellPartitioned(t *testing.T) {
	runScenario(t, "V1-single-vtorc-replica-cell-partitioned", Options{OrcCells: []string{"zone2"}, PrimaryCell: "zone1"}, func(s *Scenario) {
		r1, r2 := s.Nodes[1], s.Nodes[2]
		s.check.ExpectNoFailover = true
		s.MarkFault()
		for _, in := range []string{r1.Group, r1.OrcGroup, r1.EtcdGroup} {
			for _, out := range []string{s.OldPrimary.Group, s.OldPrimary.OrcGroup, s.OldPrimary.EtcdGroup, r2.Group, r2.OrcGroup, r2.EtcdGroup} {
				s.Partition(in, out)
			}
		}
		s.WaitFor("new primary in topo", 60*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.Heal()
		s.Sleep(20*time.Second, "healed")
	})
}

// V2: the only VTOrc is co-located with the primary; the primary's tablet and VTOrc are cut
// from both replicas' tablets (etcd and vtgate stay reachable).
func TestV2SingleVTOrcWithPrimaryCutFromReplicas(t *testing.T) {
	runScenario(t, "V2-single-vtorc-with-primary-cut-from-replicas", Options{OrcCells: []string{"zone1"}, PrimaryCell: "zone1"}, func(s *Scenario) {
		p := s.OldPrimary
		s.MarkFault()
		for _, in := range []string{p.Group, p.OrcGroup} {
			for _, r := range s.Replicas() {
				s.Partition(in, r.Group)
			}
		}
		s.WaitFor("new primary in topo", 60*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.Sleep(30*time.Second, "observe write availability / flapping")
		s.Heal()
		s.Sleep(25*time.Second, "healed")
	})
}

// V3: the only VTOrc runs in a replica cell; the primary's mysqld is killed.
func TestV3SingleVTOrcKillPrimary(t *testing.T) {
	runScenario(t, "V3-single-vtorc-kill-primary", Options{OrcCells: []string{"zone2"}, PrimaryCell: "zone1"}, func(s *Scenario) {
		s.MarkFault()
		s.KillMysqld(s.OldPrimary, true)
		d, ok := s.WaitFor("new primary in topo", 90*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover happened=%v after %.1fs", ok, d.Seconds())
		s.Sleep(10*time.Second, "writes")
		_ = s.RestartMysqld(s.OldPrimary)
		s.Sleep(20*time.Second, "old primary rejoins")
	})
}

// V4: the only VTOrc runs in the primary's cell and the whole cell fails (tablet and VTOrc
// killed). Nothing can fail over until the cell comes back; the scenario measures how long writes
// stay down and checks that nothing is lost when the cell returns.
func TestV4SingleVTOrcPrimaryCellOutage(t *testing.T) {
	runScenario(t, "V4-single-vtorc-primary-cell-outage", Options{OrcCells: []string{"zone1"}, PrimaryCell: "zone1"}, func(s *Scenario) {
		s.MarkFault()
		s.KillCell(s.OldPrimary)
		d, ok := s.WaitFor("new primary in topo", 60*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover with the only VTOrc down happened=%v after %.1fs", ok, d.Seconds())
		_ = s.RestartEtcd(s.OldPrimary)
		_ = s.RestartMysqld(s.OldPrimary)
		_ = s.RestartVttablet(s.OldPrimary)
		_ = s.RestartOrc(s.OldPrimary)
		s.Sleep(30*time.Second, "cell restored")
	})
}
