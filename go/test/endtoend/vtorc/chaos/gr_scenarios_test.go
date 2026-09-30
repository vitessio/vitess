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
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

// Scenarios for the Group Replication mode (CHAOS_DURABILITY=group_replication_cross_cell).
// Unless noted otherwise they skip in semi-sync mode.

func requireGR(t *testing.T) {
	if !GRMode() {
		t.Skip("Group Replication scenario; set CHAOS_DURABILITY=group_replication_cross_cell")
	}
}

// grRelayState logs a member's executed and received-from-the-group GTID sets.
func (s *Scenario) grRelayState(n *Node, label string) (received, executed, unapplied string) {
	g := n.grState()
	executed, _ = n.gtidExecuted()
	received = g.Received
	if received != "" {
		unapplied, _ = n.scalar("select gtid_subtract(?, @@global.gtid_executed)", received)
		unapplied = strings.ReplaceAll(unapplied, "\n", "")
	}
	msg := fmt.Sprintf("%s %s: %s Received=%q Executed=%q unapplied=%q", label, n.Tablet.Alias, g, received, executed, unapplied)
	s.Log.Add("relay", msg)
	s.R.note("%s", msg)
	return
}

// onlyOn returns the GTIDs in set that none of the other nodes has executed.
func (s *Scenario) onlyOn(n *Node, others ...*Node) string {
	set, err := n.gtidExecuted()
	if err != nil {
		return "?"
	}
	for _, o := range others {
		g, err := o.gtidExecuted()
		if err != nil {
			continue
		}
		set, _ = n.scalar("select gtid_subtract(?, ?)", set, g)
		set = strings.ReplaceAll(set, "\n", "")
	}
	return set
}

// g11 is the Group Replication counterpart of S11 (relay-log discard): R2 is cut from both other
// tablets until the group expels it, so the group is {P, R1}. Then R1 leaves (how: "graceful"
// mysqld shutdown, "stopreplication" = vtctldclient StopReplication, "kill9" with its applier
// blocked), writes continue for a while, and P's host dies (mysqld and vttablet killed). The
// question is whether acked writes can be lost, or whether the system fails closed; and whether
// P goes on committing as a group of one.
func g11(t *testing.T, name, how string) {
	requireGR(t)
	runScenario(t, name, Options{}, func(s *Scenario) {
		p := s.OldPrimary
		r1, r2 := s.Replicas()[0], s.Replicas()[1]
		s.R.outcome("P=%s R1=%s (leaves: %s) R2=%s (expelled)", p.Tablet.Alias, r1.Tablet.Alias, how, r2.Tablet.Alias)
		s.Partition(p.Group, r2.Group)
		s.Partition(r1.Group, r2.Group)
		d, ok := s.WaitFor("R2 expelled: P's view has 2 members", 40*time.Second, func() bool {
			g := p.grState()
			return g.Members == 2 && g.Online == 2
		})
		s.R.timing("R2 expelled from P's view %.1fs after the partition (ok=%v)", d.Seconds(), ok)
		var lock *sql.Conn
		if how == "kill9" {
			lock = s.lockTable(r1)
		}
		s.Sleep(4*time.Second, "writes certified by the group {P, R1}")
		s.grRelayState(r1, "before-leave")
		s.grRelayState(r2, "before-leave")

		leaveAt := time.Now()
		switch how {
		case "graceful":
			s.Log.Add("fault", "mysqlctl shutdown R1 "+r1.Tablet.Alias)
			err := r1.Tablet.MysqlctlProcess.Stop()
			s.Log.Add("fault", fmt.Sprintf("R1 mysqld stopped err=%v", err))
		case "stopreplication":
			out, err := s.CI.VtctldClientProcess.ExecuteCommandWithOutput("StopReplication", r1.Tablet.Alias)
			s.Log.Add("fault", fmt.Sprintf("vtctldclient StopReplication %s err=%v %s", r1.Tablet.Alias, err, strings.TrimSpace(out)))
		case "kill9":
			s.KillMysqld(r1, true)
		}
		s.R.timing("R1 left %.2fs after the leave was started", time.Since(leaveAt).Seconds())
		alone, _ := s.WaitFor("P is the only member of its group (view 1/1)", 12*time.Second, func() bool {
			g := p.grState()
			return g.OK && g.State == "ONLINE" && g.Members == 1
		})
		s.Log.Add("check", "P: "+p.grState().String())
		s.Sleep(6*time.Second, "P with R1 gone")
		soloEnd := time.Now()
		solo := s.ackedBetween(leaveAt, soloEnd)
		soloFrom := leaveAt.Add(alone)
		s.R.outcome("P after R1 left: %s; writes acked between R1 leaving and P's host dying: %d", p.grState(), len(solo))
		if pv, _ := p.scalar("select @@global.super_read_only"); pv != "" {
			s.R.note("P super_read_only=%s at the end of the solo period (from ~+%.1fs after leave)", pv, soloFrom.Sub(leaveAt).Seconds())
		}
		others := []*Node{r2}
		if how == "stopreplication" {
			others = append(others, r1)
		}
		only := s.onlyOn(p, others...)
		s.R.outcome("GTIDs that exist ONLY on P right before its host dies (not on %d other reachable member(s)): %q", len(others), only)

		s.MarkFault()
		s.KillNode(p)
		if lock != nil {
			_ = lock.Close()
		}
		if how != "stopreplication" {
			_ = s.RestartMysqld(r1)
		}
		s.Heal()
		s.Sleep(2*time.Second, "R1 back, R2 reconnected")
		s.grRelayState(r1, "after-restart")
		s.grRelayState(r2, "after-heal")
		d, ok = s.WaitFor("new primary in topo while P's host is down", 80*time.Second, s.PrimaryChanged(p))
		s.R.outcome("failover while P's host is down happened=%v after %.1fs", ok, d.Seconds())
		s.R.outcome("members while P is down: R1 %s, R2 %s", r1.grState(), r2.grState())
		if ok {
			s.reportTagged(solo)
		}
		lastAck := time.Time{}
		for _, r := range s.W.Records() {
			if r.Acked && r.End.After(lastAck) {
				lastAck = r.End
			}
		}
		s.R.note("last acked write before P came back: +%.2fs after the fault", lastAck.Sub(s.Fault).Seconds())

		restartAt := time.Now()
		_ = s.RestartMysqld(p)
		_ = s.RestartVttablet(p)
		if how == "stopreplication" {
			out, err := s.CI.VtctldClientProcess.ExecuteCommandWithOutput("StartReplication", r1.Tablet.Alias)
			s.Log.Add("heal", fmt.Sprintf("vtctldclient StartReplication %s err=%v %s", r1.Tablet.Alias, err, strings.TrimSpace(out)))
		}
		d, ok = s.WaitFor("writes succeed again after P came back", 120*time.Second, func() bool {
			for _, r := range s.W.Records() {
				if r.Acked && r.Start.After(restartAt) {
					return true
				}
			}
			return false
		})
		s.R.timing("writes succeeded again %.1fs after P's mysqld+vttablet restart (ok=%v)", d.Seconds(), ok)
		s.Sleep(20*time.Second, "members rejoin")
		s.reportTagged(solo)
		s.R.note("VTOrc: %s", strings.ReplaceAll(s.GrepLogs(`Bootstrap|GroupNotBootstrapped|bootstrapping|no reachable member of the replication group has quorum`, 12), "\n", " || "))
	})
}

// G11: R1 restarts gracefully (clean mysqld shutdown) while R2 is expelled, then P's host dies.
func TestG11GracefulLeaveThenPrimaryDies(t *testing.T) {
	g11(t, "G11-gr-graceful-leave-then-primary-dies", "graceful")
}

// G11s: R1 leaves the group through vtctldclient StopReplication while R2 is expelled, then P's
// host dies.
func TestG11sStopReplicationThenPrimaryDies(t *testing.T) {
	g11(t, "G11s-gr-stopreplication-then-primary-dies", "stopreplication")
}

// G11k: R1's mysqld is killed (applier blocked, so it holds certified but unapplied
// transactions) while R2 is expelled, then P's host dies.
func TestG11kKill9ThenPrimaryDies(t *testing.T) {
	g11(t, "G11k-gr-kill9-then-primary-dies", "kill9")
}

// G3D: a stale SetReplicationSource (as sent by a VTOrc fixReplica, or by an ERS acting on an old
// view) reaches the current primary's tablet, pointing it at a replica. Runs in both modes.
func TestG3DStaleSetReplicationSourceOnPrimary(t *testing.T) {
	runScenario(t, "G3D-stale-setreplicationsource-on-primary", Options{}, func(s *Scenario) {
		p, r := s.OldPrimary, s.Replicas()[0]
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		ti, err := s.Ts.GetTablet(ctx, p.Tablet.GetAlias())
		if err != nil {
			s.t.Fatal(err)
		}
		tmc := grpctmclient.NewClient()
		defer tmc.Close()
		s.MarkFault()
		err = tmc.SetReplicationSource(ctx, ti.Tablet, r.Tablet.GetAlias(), 0, "", false, true, 0)
		s.Log.Add("fault", fmt.Sprintf("SetReplicationSource(%s -> parent %s) on the primary's tablet err=%v", p.Tablet.Alias, r.Tablet.Alias, err))
		s.R.outcome("stale SetReplicationSource on primary %s (parent %s): err=%v", p.Tablet.Alias, r.Tablet.Alias, err)
		back, ok := s.WaitFor("old primary's vttablet PRIMARY again", 60*time.Second, func() bool { return s.tabletTypeHTTP(p) == "PRIMARY" })
		s.R.outcome("primary's vttablet PRIMARY again=%v after %.2fs", ok, back.Seconds())
		s.Sleep(15*time.Second, "writes")
	})
}

// G3E: the primary is cut from the other tablets, every topo server and every VTOrc, but vtgate
// (and the observer) can still reach it. How long does vtgate keep sending primary traffic to
// it? Runs in both modes.
func TestG3EPrimaryIsolatedButReachableByVtgate(t *testing.T) {
	runScenario(t, "G3E-primary-isolated-reachable-by-vtgate", Options{}, func(s *Scenario) {
		p := s.OldPrimary
		s.MarkFault()
		for _, n := range s.Nodes {
			if n != p {
				s.Partition(p.Group, n.Group)
			}
			s.Partition(p.Group, n.EtcdGroup)
			s.Partition(p.Group, n.OrcGroup)
		}
		s.Partition(p.Group, "infra")
		d, ok := s.WaitFor("new primary in topo", 90*time.Second, s.PrimaryChanged(p))
		s.R.outcome("failover happened=%v after %.1fs", ok, d.Seconds())
		s.Sleep(20*time.Second, "old primary still isolated (reachable by vtgate)")
		s.Heal()
		s.Sleep(25*time.Second, "healed")
	})
}

// nextGroupPrimary returns the member the group elects when p fails: lowest version (all equal
// here), then highest member weight, then lowest server_uuid.
func (s *Scenario) nextGroupPrimary(p *Node) *Node {
	type cand struct {
		n      *Node
		weight int
		uuid   string
	}
	var cs []cand
	for _, n := range s.Nodes {
		if n == p {
			continue
		}
		w, _ := strconv.Atoi(n.variables("group_replication_member_weight")["group_replication_member_weight"])
		cs = append(cs, cand{n, w, n.serverUUID()})
	}
	slices.SortFunc(cs, func(a, b cand) int {
		if a.weight != b.weight {
			return b.weight - a.weight
		}
		return strings.Compare(a.uuid, b.uuid)
	})
	return cs[0].n
}

// G9b: the primary's mysqld dies while the cell-local topo of the member that the group will
// elect is down (S9b with the unlucky cell chosen deterministically).
func TestG9bPrimaryDiesWhileElectedMembersCellTopoDown(t *testing.T) {
	requireGR(t)
	runScenario(t, "G9b-gr-primary-dies-elected-members-cell-topo-down", Options{}, func(s *Scenario) {
		next := s.nextGroupPrimary(s.OldPrimary)
		s.R.outcome("the group is expected to elect %s; killing the etcd of %s", next.Tablet.Alias, next.Cell)
		killGroup(next.EtcdGroup)
		s.Log.Add("fault", "kill -9 etcd of "+next.Cell)
		s.Sleep(5*time.Second, "cell topo down")
		s.MarkFault()
		s.KillMysqld(s.OldPrimary, true)
		groupAt, _ := s.WaitFor("group elected a new primary", 30*time.Second, func() bool {
			for _, n := range s.Replicas() {
				if g := n.grState(); g.State == "ONLINE" && g.Role == "PRIMARY" {
					return true
				}
			}
			return false
		})
		for _, n := range s.Replicas() {
			s.R.outcome("%s after election: %s", n.Tablet.Alias, n.grState())
		}
		s.R.timing("group elected a new primary at about +%.1fs", groupAt.Seconds())
		d, ok := s.WaitFor("new primary in topo", 90*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover with %s topo down happened=%v after %.1fs", next.Cell, ok, d.Seconds())
		_ = s.RestartEtcd(next)
		if !ok {
			d, ok = s.WaitFor("new primary in topo after etcd restored", 90*time.Second, s.PrimaryChanged(s.OldPrimary))
			s.R.outcome("failover after %s topo restored happened=%v after %.1fs", next.Cell, ok, d.Seconds())
		}
		s.Sleep(5*time.Second, "writes")
		_ = s.RestartMysqld(s.OldPrimary)
		s.Sleep(20*time.Second, "let old primary rejoin")
	})
}

// G1x: S1 with VTOrc's --prevent-cross-cell-failover. Every tablet is in its own cell, so no
// failover would be allowed.
func TestG1xKillPrimaryPreventCrossCell(t *testing.T) {
	runScenario(t, "G1x-kill-primary-prevent-cross-cell", Options{OrcExtraArgs: []string{"--prevent-cross-cell-failover"}}, func(s *Scenario) {
		s.MarkFault()
		s.KillMysqld(s.OldPrimary, true)
		d, ok := s.WaitFor("new primary in topo", 60*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover happened=%v after %.1fs, new primary %s (old primary cell %s)", ok, d.Seconds(), aliasOf(s.topoPrimary()), s.OldPrimary.Cell)
		s.Sleep(5*time.Second, "writes")
		_ = s.RestartMysqld(s.OldPrimary)
		s.Sleep(20*time.Second, "old primary rejoins")
	})
}
