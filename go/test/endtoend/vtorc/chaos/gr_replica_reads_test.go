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
	"testing"
	"time"
)

// replicaReadStats summarizes the replica reads of one mysqld in a time window.
type replicaReadStats struct {
	Answered int
	// Stale counts the answers that missed a write acknowledged at least StaleAge before the read
	// started; Missing those that missed the last write acknowledged before it.
	Stale, Missing int
	First, Last    time.Time
	FirstStale     time.Time
	LastStale      time.Time
}

// replicaReadsBy returns the replica read statistics of every mysqld (by server_uuid) for the
// reads that started in [from, to), and how many reads failed.
func replicaReadsBy(reads []ReplicaReadRecord, from, to time.Time) (map[string]*replicaReadStats, int) {
	stats := make(map[string]*replicaReadStats)
	failed := 0
	for _, r := range reads {
		if r.Start.Before(from) || !r.Start.Before(to) {
			continue
		}
		if r.Err != "" {
			failed++
			continue
		}
		st := stats[r.UUID]
		if st == nil {
			st = &replicaReadStats{First: r.Start}
			stats[r.UUID] = st
		}
		st.Answered++
		st.Last = r.Start
		if r.Missing {
			st.Missing++
		}
		if r.MissingOld {
			st.Stale++
			if st.FirstStale.IsZero() {
				st.FirstStale = r.Start
			}
			st.LastStale = r.Start
		}
	}
	return stats, failed
}

// reportReplicaReads reports, for the window [from, to), which mysqld answered the replica reads
// and how stale they were, with times relative to the fault.
func (s *Scenario) reportReplicaReads(label string, from, to time.Time) map[string]*replicaReadStats {
	stats, failed := replicaReadsBy(s.W.ReplicaReads(), from, to)
	rel := func(t time.Time) string {
		if t.IsZero() {
			return "-"
		}
		return fmt.Sprintf("%+.1fs", t.Sub(s.Fault).Seconds())
	}
	for _, n := range s.Nodes {
		st := stats[n.serverUUID()]
		if st == nil {
			s.R.outcome("%s: %s answered no replica read", label, n.Tablet.Alias)
			continue
		}
		s.R.outcome("%s: %s answered %d replica reads (%s .. %s), %d missed the last acked write, %d stale by >%v (first %s, last %s)",
			label, n.Tablet.Alias, st.Answered, rel(st.First), rel(st.Last), st.Missing, st.Stale, StaleAge, rel(st.FirstStale), rel(st.LastStale))
	}
	s.R.outcome("%s: %d replica reads failed", label, failed)
	return stats
}

// replicaReadOptions makes vtgate route replica reads to the REPLICA tablets of every cell and
// take a tablet out of them once its lag exceeds 5s, even if it is the only other one
// (--min-number-serving-vttablets 1; with the default of 2 and two secondaries, vtgate would keep
// a lagging secondary until the tablet's own unhealthy threshold, 2h by default). The tablets
// report their health every second. Heartbeat mode is selected with CHAOS_VTTABLET_HEARTBEAT=1;
// otherwise the cluster framework's --enable-replication-reporter (polling) applies.
func replicaReadOptions() Options {
	return Options{
		CellsAlias:      true,
		VtgateExtraArgs: []string{"--discovery-low-replication-lag", "5s", "--min-number-serving-vttablets", "1"},
		TabletExtraArgs: []string{"--health-check-interval", "1s"},
	}
}

// G13: a secondary is cut off from the other members of its group (the group's traffic goes
// through the MySQL port of its tablet group), while vtgate, VTOrc and the topology servers can
// still reach it. MySQL's READ_ONLY exit state action keeps it readable once it leaves the group.
// The secondary cut off is the one that answers replica reads. Does it leave replica reads once
// its replication lag passes vtgate's threshold, does the other secondary take them over, and does
// it come back after the partition heals?
func TestG13SecondaryCutOffFromGroupReplicaReads(t *testing.T) {
	requireGR(t)
	runScenario(t, "G13-secondary-cut-off-replica-reads", replicaReadOptions(), func(s *Scenario) {
		mode := "polling (--enable-replication-reporter)"
		if os.Getenv("CHAOS_VTTABLET_HEARTBEAT") == "1" {
			mode = "heartbeat (--heartbeat-enable --heartbeat-interval 1s)"
		}
		s.R.outcome("lag tracking: %s; vtgate --discovery-low-replication-lag 5s --min-number-serving-vttablets 1; vttablet --health-check-interval 1s", mode)
		s.W.StartReplicaReader(s.CI.VtgateProcess.MySQLServerPort, 100*time.Millisecond)
		replicas := s.Replicas()
		// vtgate sends replica reads to the REPLICA tablets of its own cell first, even within a
		// cell alias: cut off the secondary that answers them, the other one must take over.
		readsFrom := time.Now()
		var cut, other *Node
		_, ok := s.WaitFor("a secondary answers replica reads", 60*time.Second, func() bool {
			stats, _ := replicaReadsBy(s.W.ReplicaReads(), readsFrom, time.Now())
			for i, n := range replicas {
				if stats[n.serverUUID()] != nil {
					cut, other = n, replicas[1-i]
					return true
				}
			}
			return false
		})
		if !ok {
			s.R.violation("no secondary answered replica reads before the fault")
			return
		}
		s.Sleep(5*time.Second, "replica reads before the fault")

		s.MarkFault()
		for _, n := range s.Nodes {
			if n != cut {
				s.Partition(cut.Group, n.Group)
			}
		}
		s.Log.Add("fault", cut.Tablet.Alias+" cut off from the other members; vtgate, VTOrc and topo still reach it")
		left, ok := s.WaitFor(cut.Tablet.Alias+" left its group", 30*time.Second, func() bool {
			return cut.grState().State != "ONLINE"
		})
		s.R.timing("%s left its group (left=%v) at +%.1fs: %s", cut.Tablet.Alias, ok, left.Seconds(), cut.grState())
		s.Sleep(40*time.Second, "secondary cut off")
		healAt := time.Now()
		s.Heal()
		back, ok := s.WaitFor(cut.Tablet.Alias+" ONLINE again", 90*time.Second, func() bool {
			return cut.grState().State == "ONLINE"
		})
		s.R.timing("%s ONLINE again=%v %.1fs after the partition healed", cut.Tablet.Alias, ok, back.Seconds())
		s.Sleep(20*time.Second, "healed")

		s.reportReplicaReads("before the fault", readsFrom, s.Fault)
		during := s.reportReplicaReads("partitioned", s.Fault, healAt)
		s.reportReplicaReads("healed", healAt, time.Now())

		cutStats, otherStats := during[cut.serverUUID()], during[other.serverUUID()]
		if cutStats != nil {
			s.R.timing("cut-off secondary %s answered its last replica read at +%.1fs; its last stale answer at %s",
				cut.Tablet.Alias, cutStats.Last.Sub(s.Fault).Seconds(), relOrNone(cutStats.LastStale, s.Fault))
			if healAt.Sub(cutStats.Last) < 10*time.Second {
				s.R.violation("cut-off secondary %s still answered replica reads %.1fs before the partition healed",
					cut.Tablet.Alias, healAt.Sub(cutStats.Last).Seconds())
			}
		}
		if otherStats == nil || healAt.Sub(otherStats.Last) > 5*time.Second {
			s.R.violation("the other secondary %s did not answer replica reads at the end of the partition", other.Tablet.Alias)
		} else if otherStats.Stale > 0 {
			s.R.violation("the other secondary %s answered %d stale replica reads during the partition", other.Tablet.Alias, otherStats.Stale)
		}
	})
}

func relOrNone(t, fault time.Time) string {
	if t.IsZero() {
		return "none"
	}
	return fmt.Sprintf("+%.1fs", t.Sub(fault).Seconds())
}
