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
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"
)

// Scenario is the context handed to a scenario body.
type Scenario struct {
	*Chaos
	R          *Report
	W          *Workload
	O          *Observer
	OldPrimary *Node
	Fault      time.Time
	check      CheckOptions
}

// Replicas returns the non-primary nodes (at scenario start).
func (s *Scenario) Replicas() []*Node {
	var r []*Node
	for _, n := range s.Nodes {
		if n != s.OldPrimary {
			r = append(r, n)
		}
	}
	return r
}

// MarkFault records the fault injection time.
func (s *Scenario) MarkFault() { s.Fault = time.Now() }

// Sleep sleeps while logging.
func (s *Scenario) Sleep(d time.Duration, why string) {
	s.Log.Add("sleep", fmt.Sprintf("%v: %s", d, why))
	time.Sleep(d)
}

// runScenario builds a fresh cluster, starts the observer and the writers, runs body (which must
// inject faults and heal them), then stops the writers and checks the invariants.
func runScenario(t *testing.T, name string, opts Options, body func(s *Scenario)) {
	if os.Getenv("CHAOS_E2E") == "" {
		t.Skip("set CHAOS_E2E=1 (and run through chaos_run.sh) to run the chaos scenarios")
	}
	c := NewChaos(t, name, opts)
	s := &Scenario{Chaos: c, R: &Report{Name: name}}
	s.OldPrimary = c.topoPrimary()
	c.Log.Add("scenario", "start; primary="+s.OldPrimary.Tablet.Alias)
	s.O = c.StartObserver()
	s.W = c.StartWorkload(4, 40*time.Millisecond)
	s.Sleep(5*time.Second, "baseline load")

	body(s)

	s.W.Stop()
	c.Log.Add("scenario", "writers stopped")
	s.check.Fault = s.Fault
	s.check.OldPrimary = s.OldPrimary
	c.CheckInvariants(s.R, s.W, s.O, s.check)
	for _, l := range c.VTOrcSummary() {
		s.R.note("%s", l)
	}
	s.O.Stop()
	s.R.Save()
	fmt.Println(s.R.String())
	t.Log("\n" + s.R.String())
	if len(s.R.Violations) > 0 {
		t.Errorf("scenario %s: %d invariant violations", name, len(s.R.Violations))
	}
}

// S1: kill -9 the primary's mysqld (and mysqld_safe so it stays down) under write load.
// After the failover, restart it and verify it rejoins as a replica.
func TestS1KillPrimaryMysqld(t *testing.T) {
	runScenario(t, "S1-kill9-primary-mysqld", Options{}, func(s *Scenario) {
		s.MarkFault()
		s.KillMysqld(s.OldPrimary, true)
		d, ok := s.WaitFor("new primary in topo", 90*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover happened=%v after %.1fs", ok, d.Seconds())
		s.Sleep(10*time.Second, "writes on new primary")
		_ = s.RestartMysqld(s.OldPrimary)
		s.Sleep(5*time.Second, "let old primary rejoin")
	})
}

// S1b: kill -9 only mysqld; mysqld_safe restarts it right away (crash + fast restart).
func TestS1bKillPrimaryMysqldAutoRestart(t *testing.T) {
	runScenario(t, "S1b-kill9-primary-mysqld-autorestart", Options{}, func(s *Scenario) {
		s.MarkFault()
		s.KillMysqld(s.OldPrimary, false)
		d, ok := s.WaitFor("new primary in topo", 60*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover happened=%v after %.1fs", ok, d.Seconds())
		s.Sleep(15*time.Second, "writes after crash/restart")
	})
}

// S2: SIGSTOP the primary's mysqld and vttablet (hung host) under load, CONT it after failover.
func TestS2HangPrimary(t *testing.T) {
	runScenario(t, "S2-sigstop-primary", Options{}, func(s *Scenario) {
		s.MarkFault()
		s.StopNode(s.OldPrimary)
		d, ok := s.WaitFor("new primary in topo", 120*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover happened=%v after %.1fs", ok, d.Seconds())
		s.Sleep(10*time.Second, "writes on new primary")
		s.ResumeNode(s.OldPrimary)
		s.Sleep(25*time.Second, "old primary resumed; let it be fenced and rejoin")
	})
}

// S3: full network isolation of the primary node (mysql, grpc, http unreachable from everyone
// except the observer; it can't reach topo either) under load, then heal.
func TestS3IsolatePrimary(t *testing.T) {
	runScenario(t, "S3-isolate-primary", Options{}, func(s *Scenario) {
		s.MarkFault()
		s.Isolate(s.OldPrimary.Group)
		d, ok := s.WaitFor("new primary in topo", 120*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover happened=%v after %.1fs", ok, d.Seconds())
		s.Sleep(10*time.Second, "writes on new primary while old primary still isolated")
		s.Heal()
		s.Sleep(25*time.Second, "let old primary rejoin")
	})
}

// S4: partition the primary from the replicas only (replication broken both ways), while VTOrcs
// and vtgate can still reach everything.
func TestS4ReplicationPartition(t *testing.T) {
	runScenario(t, "S4-replication-partition", Options{}, func(s *Scenario) {
		s.MarkFault()
		for _, r := range s.Replicas() {
			s.Partition(s.OldPrimary.Group, r.Group)
		}
		s.Sleep(12*time.Second, "let replication notice the partition")
		for _, r := range s.Replicas() {
			rs, _ := r.replicaStatus()
			s.Log.Add("check", fmt.Sprintf("%s io=%s sql=%s io_err=%q", r.Tablet.Alias, rs["Replica_IO_Running"], rs["Replica_SQL_Running"], rs["Last_IO_Error"]))
		}
		d, ok := s.WaitFor("new primary in topo", 60*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover happened=%v after %.1fs (+12s)", ok, d.Seconds())
		s.Sleep(10*time.Second, "writes")
		s.Heal()
		s.Sleep(25*time.Second, "healed; let things converge")
	})
}

// S5: the primary crashes while replica R1 is hung (SIGSTOP) and R2 is behind because it was
// partitioned from the primary, so the last acked writes exist only on the primary and R1.
// Does VTOrc promote R2 (losing acked writes) or refuse until R1 is back?
func TestS5PrimaryCrashWithAckerDown(t *testing.T) {
	runScenario(t, "S5-primary-crash-acker-down", Options{}, func(s *Scenario) {
		r1, r2 := s.Replicas()[0], s.Replicas()[1]
		s.Partition(s.OldPrimary.Group, r2.Group)
		s.Sleep(6*time.Second, "writes acked by R1 only; R2 falls behind")
		s.StopNode(r1)
		s.MarkFault()
		s.KillMysqld(s.OldPrimary, true)
		s.Heal()
		d, ok := s.WaitFor("new primary in topo (R1 hung)", 120*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover while R1 hung happened=%v after %.1fs", ok, d.Seconds())
		if ok {
			p := s.topoPrimary()
			ids, err := p.ids()
			missing := 0
			for _, rec := range s.W.Records() {
				if _, ok := ids[rec.ID]; rec.Acked && !ok && err == nil {
					missing++
				}
			}
			s.R.outcome("promoted %s while R1 hung; acked writes missing on it at that point: %d (err=%v)", p.Tablet.Alias, missing, err)
		}
		s.ResumeNode(r1)
		d, ok = s.WaitFor("new primary in topo (R1 resumed)", 90*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover after R1 resumed happened=%v after %.1fs", ok, d.Seconds())
		s.Sleep(5*time.Second, "writes")
		_ = s.RestartMysqld(s.OldPrimary)
		s.Sleep(20*time.Second, "let old primary rejoin")
	})
}

// S5b: like S5 but R1 is network-isolated instead of hung.
func TestS5bPrimaryCrashWithAckerIsolated(t *testing.T) {
	runScenario(t, "S5b-primary-crash-acker-isolated", Options{}, func(s *Scenario) {
		r1, r2 := s.Replicas()[0], s.Replicas()[1]
		s.Partition(s.OldPrimary.Group, r2.Group)
		s.Sleep(6*time.Second, "writes acked by R1 only; R2 falls behind")
		s.MarkFault()
		s.Isolate(r1.Group)
		s.KillMysqld(s.OldPrimary, true)
		// Keep R1 isolated but let R2 talk to everyone.
		_ = s.Net.HealAll()
		s.Isolate(r1.Group)
		d, ok := s.WaitFor("new primary in topo (R1 isolated)", 120*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover while R1 isolated happened=%v after %.1fs", ok, d.Seconds())
		s.Heal()
		d, ok = s.WaitFor("new primary in topo (R1 healed)", 90*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover after R1 healed happened=%v after %.1fs", ok, d.Seconds())
		s.Sleep(5*time.Second, "writes")
		_ = s.RestartMysqld(s.OldPrimary)
		s.Sleep(20*time.Second, "let old primary rejoin")
	})
}

// S6: one VTOrc (in a replica's cell) cannot reach the primary; everything else is fine.
// No failover must happen.
func TestS6OneVTOrcCutFromPrimary(t *testing.T) {
	runScenario(t, "S6-one-vtorc-cut-from-primary", Options{}, func(s *Scenario) {
		s.check.ExpectNoFailover = true
		s.MarkFault()
		s.Block(s.Replicas()[0].OrcGroup, s.OldPrimary.Group)
		s.WaitFor("new primary in topo", 45*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.Heal()
		s.Sleep(5*time.Second, "healed")
	})
}

// S6b: all VTOrcs cannot reach the primary's vttablet; replication and vtgate are fine.
func TestS6bAllVTOrcsCutFromPrimary(t *testing.T) {
	runScenario(t, "S6b-all-vtorcs-cut-from-primary", Options{}, func(s *Scenario) {
		s.check.ExpectNoFailover = true
		s.MarkFault()
		for _, n := range s.Nodes {
			s.Block(n.OrcGroup, s.OldPrimary.Group)
		}
		s.WaitFor("new primary in topo", 45*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.Heal()
		s.Sleep(5*time.Second, "healed")
	})
}

// S7: double failure: the new primary's mysqld is killed right after the first failover.
func TestS7DoubleFailure(t *testing.T) {
	runScenario(t, "S7-double-failure", Options{}, func(s *Scenario) {
		s.MarkFault()
		s.KillMysqld(s.OldPrimary, true)
		d, ok := s.WaitFor("first failover", 90*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("first failover happened=%v after %.1fs", ok, d.Seconds())
		p2 := s.topoPrimary()
		if !ok || p2 == nil {
			return
		}
		s.KillMysqld(p2, true)
		d, ok = s.WaitFor("second failover", 90*time.Second, s.PrimaryChanged(p2))
		p3 := s.topoPrimary()
		s.R.outcome("second failover (away from %s) happened=%v after %.1fs, primary now %v", p2.Tablet.Alias, ok, d.Seconds(), aliasOf(p3))
		s.Sleep(5*time.Second, "writes")
		_ = s.RestartMysqld(s.OldPrimary)
		_ = s.RestartMysqld(p2)
		s.Sleep(25*time.Second, "let both old primaries rejoin")
	})
}

// S7b: the replica being promoted is killed while the ERS is in progress (as soon as its
// vttablet reports PRIMARY, before topo is updated).
func TestS7bKillDuringFailover(t *testing.T) {
	runScenario(t, "S7b-kill-during-failover", Options{}, func(s *Scenario) {
		s.MarkFault()
		s.KillMysqld(s.OldPrimary, true)
		var victim *Node
		deadline := time.Now().Add(60 * time.Second)
		for victim == nil && time.Now().Before(deadline) {
			for _, r := range s.Replicas() {
				if typ := s.tabletTypeHTTP(r); typ == "PRIMARY" {
					victim = r
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		if victim == nil {
			s.R.outcome("no replica became PRIMARY within 60s")
		} else {
			s.KillMysqld(victim, true)
			s.R.outcome("killed %s mysqld as soon as its vttablet reported PRIMARY (topo primary then: %v)", victim.Tablet.Alias, aliasOf(s.topoPrimary()))
			d, ok := s.WaitFor("failover to the last replica", 90*time.Second, func() bool {
				p := s.topoPrimary()
				return p != nil && p != s.OldPrimary && p != victim
			})
			s.R.outcome("failover to last replica happened=%v after %.1fs", ok, d.Seconds())
		}
		s.Sleep(5*time.Second, "writes")
		_ = s.RestartMysqld(s.OldPrimary)
		if victim != nil {
			_ = s.RestartMysqld(victim)
		}
		s.Sleep(25*time.Second, "let everything rejoin")
	})
}

// S7c: flapping primary: isolate/heal the original primary repeatedly.
func TestS7cFlappingPrimary(t *testing.T) {
	runScenario(t, "S7c-flapping-primary", Options{}, func(s *Scenario) {
		s.MarkFault()
		for range 6 {
			s.Isolate(s.OldPrimary.Group)
			time.Sleep(5 * time.Second)
			s.Heal()
			time.Sleep(4 * time.Second)
		}
		s.Sleep(20*time.Second, "settle")
	})
}

// S7d: flapping with isolation periods long enough to trigger ERS (~10s isolated, 5s healed).
func TestS7dFlappingPrimaryLong(t *testing.T) {
	runScenario(t, "S7d-flapping-primary-long", Options{}, func(s *Scenario) {
		s.MarkFault()
		for range 4 {
			p := s.topoPrimary()
			if p == nil {
				p = s.OldPrimary
			}
			s.Isolate(p.Group)
			time.Sleep(11 * time.Second)
			s.Heal()
			time.Sleep(5 * time.Second)
		}
		s.Sleep(20*time.Second, "settle")
	})
}

// S8: one VTOrc is partitioned from all topo servers during a failover: the VTOrc that starts
// the ERS is cut from topo right after it takes the shard lock.
func TestS8VTOrcCutFromTopoDuringERS(t *testing.T) {
	runScenario(t, "S8-vtorc-cut-from-topo-during-ers", Options{}, func(s *Scenario) {
		s.MarkFault()
		s.KillMysqld(s.OldPrimary, true)
		n := s.WaitForOrcLog(`Locking shard ks/0 for action VTOrc Recovery for DeadPrimary`, s.Fault, 60*time.Second)
		if n != nil {
			s.Block(n.OrcGroup, "infra")
			for _, m := range s.Nodes {
				s.Block(n.OrcGroup, m.EtcdGroup)
			}
			s.R.outcome("cut vtorc-%s (running ERS) from all topo servers", n.Cell)
		}
		d, ok := s.WaitFor("new primary in topo", 120*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover happened=%v after %.1fs", ok, d.Seconds())
		s.Sleep(10*time.Second, "writes")
		s.Heal()
		_ = s.RestartMysqld(s.OldPrimary)
		s.Sleep(20*time.Second, "let old primary rejoin")
	})
}

// S8b: one VTOrc (in a replica cell) is partitioned from topo before the primary dies.
func TestS8bVTOrcCutFromTopoBeforeFailure(t *testing.T) {
	runScenario(t, "S8b-vtorc-cut-from-topo-before", Options{}, func(s *Scenario) {
		n := s.Replicas()[0]
		s.Block(n.OrcGroup, "infra")
		for _, m := range s.Nodes {
			s.Block(n.OrcGroup, m.EtcdGroup)
		}
		s.Sleep(5*time.Second, "vtorc cut from topo")
		s.MarkFault()
		s.KillMysqld(s.OldPrimary, true)
		d, ok := s.WaitFor("new primary in topo", 120*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover happened=%v after %.1fs", ok, d.Seconds())
		s.Sleep(10*time.Second, "writes")
		s.Heal()
		_ = s.RestartMysqld(s.OldPrimary)
		s.Sleep(20*time.Second, "let old primary rejoin")
	})
}

// S9: whole-cell outage of the primary's cell: its tablet, VTOrc and cell-local etcd are
// killed. (vtgate stays up; it is considered to be outside the failed cell for measurement.)
func TestS9PrimaryCellOutage(t *testing.T) {
	runScenario(t, "S9-primary-cell-outage-kill", Options{}, func(s *Scenario) {
		s.MarkFault()
		s.KillCell(s.OldPrimary)
		d, ok := s.WaitFor("new primary in topo", 120*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover happened=%v after %.1fs", ok, d.Seconds())
		s.Sleep(10*time.Second, "writes")
		_ = s.RestartEtcd(s.OldPrimary)
		_ = s.RestartMysqld(s.OldPrimary)
		_ = s.RestartVttablet(s.OldPrimary)
		_ = s.RestartOrc(s.OldPrimary)
		if !ok {
			d, ok = s.WaitFor("new primary in topo after cell restored", 60*time.Second, s.PrimaryChanged(s.OldPrimary))
			s.R.outcome("failover after cell restored happened=%v after %.1fs", ok, d.Seconds())
		}
		s.Sleep(25*time.Second, "let the cell rejoin")
	})
}

// S9i: whole-cell network partition of the primary's cell (tablet, VTOrc and cell etcd alive
// but unreachable from the other cells), then heal.
func TestS9iPrimaryCellPartition(t *testing.T) {
	runScenario(t, "S9i-primary-cell-partition", Options{}, func(s *Scenario) {
		p := s.OldPrimary
		cell := []string{p.Group, p.OrcGroup, p.EtcdGroup}
		s.MarkFault()
		// Cut every group of the cell from every group outside the cell (inside the cell
		// they can still talk). vtgate is treated as outside the cell.
		for _, in := range cell {
			for name := range s.Net.groups {
				if name == harnessGroup || name == p.Group || name == p.OrcGroup || name == p.EtcdGroup {
					continue
				}
				s.Partition(in, name)
			}
		}
		d, ok := s.WaitFor("new primary in topo", 120*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover happened=%v after %.1fs", ok, d.Seconds())
		s.Sleep(10*time.Second, "writes")
		s.Heal()
		s.Sleep(25*time.Second, "let the cell rejoin")
	})
}

// S9b: the primary's mysqld dies while a NON-primary cell's local etcd is down.
func TestS9bPrimaryDiesWhileOtherCellTopoDown(t *testing.T) {
	runScenario(t, "S9b-primary-dies-other-cell-topo-down", Options{}, func(s *Scenario) {
		other := s.Replicas()[0]
		killGroup(other.EtcdGroup)
		s.Log.Add("fault", "kill -9 etcd of "+other.Cell)
		s.Sleep(5*time.Second, "cell topo down")
		s.MarkFault()
		s.KillMysqld(s.OldPrimary, true)
		d, ok := s.WaitFor("new primary in topo", 120*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover with %s topo down happened=%v after %.1fs", other.Cell, ok, d.Seconds())
		_ = s.RestartEtcd(other)
		if !ok {
			d, ok = s.WaitFor("new primary in topo after etcd restored", 90*time.Second, s.PrimaryChanged(s.OldPrimary))
			s.R.outcome("failover after %s topo restored happened=%v after %.1fs", other.Cell, ok, d.Seconds())
		}
		s.Sleep(5*time.Second, "writes")
		_ = s.RestartMysqld(s.OldPrimary)
		s.Sleep(20*time.Second, "let old primary rejoin")
	})
}

// S10: the global etcd hangs (SIGSTOP, 10s) right after VTOrc starts the ERS.
func TestS10GlobalTopoHangDuringERS(t *testing.T) {
	runScenario(t, "S10-global-topo-hang-during-ers", Options{}, func(s *Scenario) {
		s.MarkFault()
		s.KillMysqld(s.OldPrimary, true)
		n := s.WaitForOrcLog(`Locking shard ks/0 for action VTOrc Recovery for DeadPrimary`, s.Fault, 60*time.Second)
		p := signalGroup("infra", syscall.SIGSTOP, "etcd")
		s.Log.Add("fault", fmt.Sprintf("SIGSTOP global etcd pids=%v (ers by %v)", p, n != nil))
		time.Sleep(10 * time.Second)
		p = signalGroup("infra", syscall.SIGCONT, "etcd")
		s.Log.Add("heal", fmt.Sprintf("SIGCONT global etcd pids=%v", p))
		d, ok := s.WaitFor("new primary in topo", 120*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover happened=%v after %.1fs (after etcd resumed)", ok, d.Seconds())
		s.Sleep(5*time.Second, "writes")
		_ = s.RestartMysqld(s.OldPrimary)
		s.Sleep(20*time.Second, "let old primary rejoin")
	})
}

func aliasOf(n *Node) string {
	if n == nil {
		return "<none>"
	}
	return n.Tablet.Alias
}
