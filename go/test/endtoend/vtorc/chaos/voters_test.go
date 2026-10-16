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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Scenarios of VTOrc's changes of the voter list: one voter per cell, a failed voter swapped for a
// spare REPLICA of its cell after --group-replication-voter-replacement-grace-period (1 minute by
// default), and a voter removed only once the operator deleted its tablet record.

// aliasesOf returns the tablet aliases of nodes.
func aliasesOf(nodes []*Node) []string {
	var a []string
	for _, n := range nodes {
		a = append(a, n.Tablet.Alias)
	}
	return a
}

// reportVoterChanges reports VTOrc's changes of the voter list and its bootstraps since the fault,
// as their recovery audit lines.
func (s *Scenario) reportVoterChanges() {
	for _, n := range s.Nodes[:len(cells)] {
		for _, l := range s.orcLogSince(n, s.Fault) {
			if i := strings.Index(l.text, "topology_recovery: "); i >= 0 &&
				(strings.Contains(l.text, "changed the voters of") || strings.Contains(l.text, "bootstrapping the replication group on")) {
				s.R.timing("vtorc-%s at +%.2fs: %s", n.Cell, l.at.Sub(s.Fault.UTC()).Seconds(), l.text[i+len("topology_recovery: "):])
			}
		}
	}
}

// waitVoters waits until the shard record lists exactly the given voters, and reports when.
func (s *Scenario) waitVoters(what string, timeout time.Duration, want ...*Node) bool {
	_, ok := s.WaitFor(what, timeout, func() bool {
		voters, err := s.shardVoters()
		if err != nil || len(voters) != len(want) {
			return false
		}
		for _, n := range want {
			if !slices.Contains(voters, n) {
				return false
			}
		}
		return true
	})
	s.R.outcome("%s: happened=%v at +%.1fs", what, ok, time.Since(s.Fault).Seconds())
	return ok
}

// VS: the host of a secondary voter dies for good, in a cell that has a spare REPLICA. After the
// grace period VTOrc swaps the spare in (SwapVoter); the spare joins the group, the list keeps
// three voters, and the primary is not touched.
func TestVSSwapFailedVoterForSpare(t *testing.T) {
	requireGR(t)
	runScenario(t, "VS-swap-failed-voter-for-spare", Options{ExtraTabletCells: []string{"zone2"}}, func(s *Scenario) {
		voters, err := s.shardVoters()
		require.NoError(t, err)
		var victim, spare *Node
		for _, n := range s.Nodes {
			if n.Cell != "zone2" {
				continue
			}
			if slices.Contains(voters, n) {
				victim = n
			} else {
				spare = n
			}
		}
		require.NotNil(t, victim, "no voter in zone2: %v", aliasesOf(voters))
		require.NotNil(t, spare, "no spare in zone2: %v", aliasesOf(voters))
		if victim == s.OldPrimary {
			// The scenario kills a secondary voter: move the primary out of the spare's cell first.
			for _, v := range voters {
				if v.Cell != victim.Cell {
					s.ensurePrimary(v)
					break
				}
			}
			s.check.OldPrimaryUUID = s.OldPrimary.serverUUID()
		}
		s.check.ExpectNoFailover = true
		s.R.outcome("voters %v, primary %s, spare %s; the host of voter %s dies for good",
			aliasesOf(voters), s.OldPrimary.Tablet.Alias, spare.Tablet.Alias, victim.Tablet.Alias)

		s.MarkGone(victim)
		s.MarkFault()
		s.KillNode(victim)
		d, ok := s.WaitFor("the group expelled the dead voter", 60*time.Second, func() bool {
			g := s.OldPrimary.grState()
			return g.State == "ONLINE" && g.Members == 2 && g.Online == 2
		})
		s.R.outcome("dead voter expelled: happened=%v after %.1fs", ok, d.Seconds())

		var want []*Node
		for _, v := range voters {
			if v != victim {
				want = append(want, v)
			}
		}
		want = append(want, spare)
		s.waitVoters("spare swapped in for the dead voter", 240*time.Second, want...)
		_, ok = s.WaitFor("the spare is an ONLINE member", 90*time.Second, func() bool {
			g := spare.grState()
			return g.State == "ONLINE" && g.Online == 3 && g.Members == 3
		})
		s.R.outcome("spare ONLINE in a view of 3: happened=%v at +%.1fs", ok, time.Since(s.Fault).Seconds())
		s.reportVoterChanges()
		s.Sleep(10*time.Second, "writes with the new voter")
	})
}

// VD: the host of a voter dies for good, then a second voter's mysqld crashes and restarts, so the
// group loses its majority and stops. VTOrc does not bootstrap it while the dead voter is listed.
// The operator deletes the dead voter's tablet record: VTOrc removes it from the list
// (RemoveVoterNoGroup) and bootstraps the group from the two remaining voters, and the shard serves
// again. Every write acknowledged before the loss that the survivors held must be there; a write
// that is missing is reported with where it still exists (the accepted loss is a write that only the
// dead voter held).
func TestVDDeleteDeadVoterAfterMajorityLoss(t *testing.T) {
	requireGR(t)
	runScenario(t, "VD-delete-dead-voter-after-majority-loss", Options{}, func(s *Scenario) {
		replicas := s.Replicas()
		dead, crashed := replicas[0], replicas[1]
		s.wantVoters = 2
		s.R.outcome("primary %s; the host of voter %s dies for good, then the mysqld of voter %s crashes and restarts",
			s.OldPrimary.Tablet.Alias, dead.Tablet.Alias, crashed.Tablet.Alias)

		s.MarkGone(dead)
		s.MarkFault()
		s.KillNode(dead)
		d, ok := s.WaitFor("the group expelled the dead voter", 60*time.Second, func() bool {
			g := s.OldPrimary.grState()
			return g.State == "ONLINE" && g.Members == 2 && g.Online == 2
		})
		s.R.outcome("dead voter expelled: happened=%v after %.1fs", ok, d.Seconds())
		s.Sleep(5*time.Second, "writes on the two remaining voters")

		lossAt := time.Now()
		s.KillMysqld(crashed, false)
		d, ok = s.WaitFor("the group lost its majority", 60*time.Second, func() bool {
			return s.OldPrimary.grState().State != "ONLINE"
		})
		s.R.outcome("primary left its group: happened=%v %.1fs after the crash", ok, d.Seconds())
		s.Sleep(20*time.Second, "the group stays down while the dead voter is listed")
		if voters, err := s.shardVoters(); err == nil {
			s.R.outcome("voters before the deletion: %v", aliasesOf(voters))
		}
		ackedBefore := s.ackedBetween(time.Time{}, lossAt)

		deleteAt := time.Now()
		out, err := s.CI.VtctldClientProcess.ExecuteCommandWithOutput("DeleteTablets", dead.Tablet.Alias)
		s.Log.Add("fault", fmt.Sprintf("DeleteTablets %s err=%v %s", dead.Tablet.Alias, err, strings.TrimSpace(out)))
		s.R.outcome("DeleteTablets %s at +%.1fs: err=%v", dead.Tablet.Alias, deleteAt.Sub(s.Fault).Seconds(), err)

		s.waitVoters("dead voter removed from the list", 120*time.Second, s.OldPrimary, crashed)
		var p *Node
		_, ok = s.WaitFor("the shard serves again", 180*time.Second, func() bool {
			p = s.topoPrimary()
			if p == nil || p == dead || s.tabletTypeHTTP(p) != "PRIMARY" {
				return false
			}
			g := p.grState()
			return g.State == "ONLINE" && g.Role == "PRIMARY" && g.Online == 2
		})
		s.R.outcome("shard serving again on %s: happened=%v %.1fs after the deletion", aliasOf(p), ok, time.Since(deleteAt).Seconds())
		s.reportVoterChanges()

		// Acknowledged writes from before the loss of the majority that the new primary lacks.
		if p != nil {
			if pids, err := p.ids(); err == nil {
				var missing []int64
				for _, id := range ackedBefore {
					if _, ok := pids[id]; !ok {
						missing = append(missing, id)
					}
				}
				s.R.outcome("%d writes acknowledged before the loss of the majority, %d missing on %s", len(ackedBefore), len(missing), p.Tablet.Alias)
				if len(missing) > 0 {
					s.classifyMissing(dead, crashed, missing)
				}
			}
		}
		s.Sleep(10*time.Second, "writes after the bootstrap")
	})
}

// classifyMissing reports where acknowledged writes that the new primary lacks still exist: on the
// surviving voter, or only on the dead one, whose mysqld it starts alone (no vttablet; its group
// replication does not start on boot) to look.
func (s *Scenario) classifyMissing(dead, survivor *Node, missing []int64) {
	onSurvivor := map[int64]bool{}
	if ids, err := survivor.ids(); err == nil {
		for _, id := range missing {
			if _, ok := ids[id]; ok {
				onSurvivor[id] = true
			}
		}
	}
	onDead := map[int64]bool{}
	if err := s.RestartMysqld(dead); err == nil {
		if ids, err := dead.ids(); err == nil {
			for _, id := range missing {
				if _, ok := ids[id]; ok {
					onDead[id] = true
				}
			}
		}
	}
	var onlyDead, survivorToo, nowhere []int64
	for _, id := range missing {
		switch {
		case onSurvivor[id]:
			survivorToo = append(survivorToo, id)
		case onDead[id]:
			onlyDead = append(onlyDead, id)
		default:
			nowhere = append(nowhere, id)
		}
	}
	s.R.outcome("missing acknowledged writes: %d only on the dead voter (the accepted loss) %v; %d also on the surviving voter %v; %d on neither %v",
		len(onlyDead), firstN(onlyDead, 10), len(survivorToo), firstN(survivorToo, 10), len(nowhere), firstN(nowhere, 10))
	if len(survivorToo) > 0 || len(nowhere) > 0 {
		s.R.violation("LOST: %d acknowledged writes missing that the dead voter did not hold alone", len(survivorToo)+len(nowhere))
	}
}
