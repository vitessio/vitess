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

// Single-VTOrc deployments: only one VTOrc, in one of the three cells.

// V1: the only VTOrc runs in a replica cell; that cell (VTOrc + replica) is partitioned from
// the other two cells' tablets, VTOrcs and etcds. No failover is expected; writes continue
// with the other replica acking.
func TestV1SingleVTOrcReplicaCellPartitioned(t *testing.T) {
	runScenario(t, "V1-single-vtorc-replica-cell-partitioned", Options{}, func(s *Scenario) {
		r1, r2 := s.Replicas()[0], s.Replicas()[1]
		s.KeepOnlyOrc(r1)
		s.check.ExpectNoFailover = true
		s.Sleep(5*time.Second, "single vtorc")
		s.MarkFault()
		for _, in := range []string{r1.Group, r1.OrcGroup} {
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
	runScenario(t, "V2-single-vtorc-with-primary-cut-from-replicas", Options{}, func(s *Scenario) {
		p := s.OldPrimary
		s.KeepOnlyOrc(p)
		s.Sleep(5*time.Second, "single vtorc")
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
	runScenario(t, "V3-single-vtorc-kill-primary", Options{}, func(s *Scenario) {
		s.KeepOnlyOrc(s.Replicas()[0])
		s.Sleep(5*time.Second, "single vtorc")
		s.MarkFault()
		s.KillMysqld(s.OldPrimary, true)
		d, ok := s.WaitFor("new primary in topo", 90*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover happened=%v after %.1fs", ok, d.Seconds())
		s.Sleep(10*time.Second, "writes")
		_ = s.RestartMysqld(s.OldPrimary)
		s.Sleep(20*time.Second, "old primary rejoins")
	})
}
