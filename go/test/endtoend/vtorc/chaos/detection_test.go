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
	"bufio"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Scenarios that measure how long VTOrc takes to detect a primary failure and to start its
// recovery, with every topology server healthy and while the topology server of another cell
// hangs (SIGSTOP). While a cell's topology server hangs, each topology refresh of VTOrc's main
// loop blocks it for --topo-information-refresh-duration (3s here), and the loop also drives
// discovery and analysis. They run in both modes.

var (
	orcLogTime     = regexp.MustCompile(`^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3})`)
	orcAnalysis    = regexp.MustCompile(`Recovery for (\w+) on ks/0: Starting checkAndRecover`)
	orcLocking     = regexp.MustCompile(`Locking shard ks/0 for action VTOrc Recovery for (\w+) on`)
	orcRefreshFail = regexp.MustCompile(`failed to refresh topo information|Failed to load tablets from cell`)
	orcAudit       = regexp.MustCompile(`topology_recovery: (.*)`)
)

// primaryFailureAnalyses are the analyses VTOrc reports for a dead or unreachable primary.
var primaryFailureAnalyses = []string{
	"DeadPrimary", "DeadPrimaryWithoutReplicas", "DeadPrimaryAndReplicas",
	"DeadPrimaryAndSomeReplicas", "PrimaryTabletDeleted", "UnreachablePrimary", "UnreachablePrimaryWithLaggingReplicas",
	"PrimaryHasPrimary", "PrimaryIsReadOnly", "IncapacitatedPrimary", "ClusterHasNoPrimary",
}

type orcLogLine struct {
	at   time.Time
	text string
}

// orcLogSince returns the lines of n's VTOrc log logged at or after since.
func (s *Scenario) orcLogSince(n *Node, since time.Time) []orcLogLine {
	f, err := os.Open(fmt.Sprintf("%s/%s", s.CI.TmpDirectory, n.Orc.LogFileName))
	if err != nil {
		return nil
	}
	defer f.Close()
	var lines []orcLogLine
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		m := orcLogTime.FindStringSubmatch(sc.Text())
		if m == nil {
			continue
		}
		at, err := time.Parse("2006-01-02 15:04:05.000", m[1])
		if err != nil || at.Before(since.UTC()) {
			continue
		}
		lines = append(lines, orcLogLine{at: at, text: sc.Text()})
	}
	return lines
}

// reportDetection reports, for every VTOrc, when it first analyzed a primary failure after the
// fault, when it first took the shard lock for a recovery, its first recovery audit step, the
// spacing of its analysis passes and the topology refreshes that failed. watch lists other
// analyses of interest (GroupPrimaryNotInTopo); from is the time they are measured from.
func (s *Scenario) reportDetection(watch []string, from time.Time, fromWhat string) {
	for _, n := range s.Nodes[:len(cells)] {
		lines := s.orcLogSince(n, s.Fault)
		rel := func(t time.Time) string { return fmt.Sprintf("+%.2fs", t.Sub(s.Fault.UTC()).Seconds()) }
		var (
			firstAny, firstPrimary, firstLock, firstAudit orcLogLine
			anyName, primaryName, lockName                string
			watched                                       = make(map[string]orcLogLine)
			passes                                        []time.Time
			refreshFails                                  []time.Time
		)
		for _, l := range lines {
			if m := orcAnalysis.FindStringSubmatch(l.text); m != nil {
				if firstAny.at.IsZero() {
					firstAny, anyName = l, m[1]
				}
				if firstPrimary.at.IsZero() && slices.Contains(primaryFailureAnalyses, m[1]) {
					firstPrimary, primaryName = l, m[1]
				}
				if slices.Contains(watch, m[1]) {
					if _, ok := watched[m[1]]; !ok {
						watched[m[1]] = l
					}
				}
				if primaryName != "" && m[1] == primaryName && len(passes) < 20 {
					passes = append(passes, l.at)
				}
			}
			if m := orcLocking.FindStringSubmatch(l.text); m != nil && firstLock.at.IsZero() {
				firstLock, lockName = l, m[1]
			}
			if m := orcAudit.FindStringSubmatch(l.text); m != nil && firstAudit.at.IsZero() {
				firstAudit = orcLogLine{at: l.at, text: m[1]}
			}
			if orcRefreshFail.MatchString(l.text) {
				refreshFails = append(refreshFails, l.at)
			}
		}
		var b strings.Builder
		fmt.Fprintf(&b, "detection vtorc-%s:", n.Cell)
		if !firstAny.at.IsZero() {
			fmt.Fprintf(&b, " first analysis %s at %s;", anyName, rel(firstAny.at))
		}
		if !firstPrimary.at.IsZero() {
			fmt.Fprintf(&b, " primary failure %s at %s;", primaryName, rel(firstPrimary.at))
		} else {
			b.WriteString(" no primary failure analyzed;")
		}
		for _, w := range watch {
			if l, ok := watched[w]; ok {
				fmt.Fprintf(&b, " %s at %s (%.2fs after %s);", w, rel(l.at), l.at.Sub(from.UTC()).Seconds(), fromWhat)
			}
		}
		if !firstLock.at.IsZero() {
			fmt.Fprintf(&b, " shard locked for %s at %s;", lockName, rel(firstLock.at))
		}
		if !firstAudit.at.IsZero() {
			fmt.Fprintf(&b, " first recovery step at %s (%s);", rel(firstAudit.at), firstAudit.text[:min(len(firstAudit.text), 120)])
		}
		if len(passes) > 1 {
			var gaps []string
			for i := 1; i < len(passes) && i < 8; i++ {
				gaps = append(gaps, fmt.Sprintf("%.1f", passes[i].Sub(passes[i-1]).Seconds()))
			}
			fmt.Fprintf(&b, " %s analyzed every [%s]s;", primaryName, strings.Join(gaps, " "))
		}
		if len(refreshFails) > 0 {
			fmt.Fprintf(&b, " %d failed topo refreshes from %s to %s", len(refreshFails), rel(refreshFails[0]), rel(refreshFails[len(refreshFails)-1]))
		} else {
			b.WriteString(" 0 failed topo refreshes")
		}
		s.R.timing("%s", b.String())
	}
}

// otherReplica returns the replica that the group would not elect if p failed: in GR mode, its
// cell's topology can hang without delaying the new primary's own promotion.
func (s *Scenario) otherReplica(p *Node) *Node {
	next := s.nextGroupPrimary(p)
	for _, n := range s.Replicas() {
		if n != next {
			return n
		}
	}
	return nil
}

// detectionScenario kills the primary's mysqld (and mysqld_safe), optionally while the
// topology server of hung's cell is stopped (SIGSTOP), and reports VTOrc's detection.
func detectionScenario(t *testing.T, name string, pickHung func(s *Scenario) *Node) {
	runScenario(t, name, Options{}, func(s *Scenario) {
		var hung *Node
		if pickHung != nil {
			hung = pickHung(s)
			pids := signalGroup(hung.EtcdGroup, syscall.SIGSTOP, "etcd")
			s.Log.Add("fault", fmt.Sprintf("SIGSTOP etcd of %s pids=%v", hung.Cell, pids))
			s.R.outcome("etcd of %s hung (SIGSTOP) 10s before the primary's mysqld was killed", hung.Cell)
			s.Sleep(10*time.Second, "cell topo hung")
		}
		s.MarkFault()
		s.KillMysqld(s.OldPrimary, true)
		electedAt := time.Time{}
		if GRMode() {
			d, ok := s.WaitFor("group elected a new primary", 30*time.Second, func() bool {
				for _, n := range s.Replicas() {
					if g := n.grState(); g.State == "ONLINE" && g.Role == "PRIMARY" {
						return true
					}
				}
				return false
			})
			if ok {
				electedAt = s.Fault.Add(d)
				s.R.timing("group elected a new primary at about +%.1fs", d.Seconds())
			}
		}
		d, ok := s.WaitFor("new primary in topo", 90*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover happened=%v after %.1fs, new primary %v", ok, d.Seconds(), aliasOf(s.topoPrimary()))
		if hung != nil {
			pids := signalGroup(hung.EtcdGroup, syscall.SIGCONT, "etcd")
			s.Log.Add("heal", fmt.Sprintf("SIGCONT etcd of %s pids=%v", hung.Cell, pids))
			if !ok {
				d, ok = s.WaitFor("new primary in topo after etcd resumed", 90*time.Second, s.PrimaryChanged(s.OldPrimary))
				s.R.outcome("failover after etcd resumed happened=%v after %.1fs", ok, d.Seconds())
			}
		}
		if electedAt.IsZero() {
			electedAt = s.Fault
		}
		s.reportDetection([]string{"GroupPrimaryNotInTopo"}, electedAt, "the election")
		s.Sleep(5*time.Second, "writes")
		_ = s.RestartMysqld(s.OldPrimary)
		s.Sleep(20*time.Second, "let old primary rejoin")
	})
}

// D1: the primary's mysqld dies with every topology server healthy.
func TestD1KillPrimaryDetection(t *testing.T) {
	detectionScenario(t, "D1-kill-primary-detection", nil)
}

// D1h: the primary's mysqld dies while the topology server of another cell hangs. In GR mode the
// hung cell is that of the member the group does not elect, so that the new primary's own
// promotion does not depend on it; the semi-sync run hangs the same replica's cell.
func TestD1hKillPrimaryWhileOtherCellTopoHangs(t *testing.T) {
	detectionScenario(t, "D1h-kill-primary-other-cell-topo-hung", func(s *Scenario) *Node { return s.otherReplica(s.OldPrimary) })
}

// G9h: G9b with the topology server of the elected member's cell hung (SIGSTOP) instead of
// killed: the elected member's tablet cannot become PRIMARY, and VTOrc must detect
// GroupPrimaryNotInTopo and move the group primary.
func TestG9hPrimaryDiesWhileElectedMembersCellTopoHangs(t *testing.T) {
	requireGR(t)
	detectionScenario(t, "G9h-gr-primary-dies-elected-members-cell-topo-hung", func(s *Scenario) *Node { return s.nextGroupPrimary(s.OldPrimary) })
}
