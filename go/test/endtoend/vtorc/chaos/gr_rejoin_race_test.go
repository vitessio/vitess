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
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// G12: a rolling restart meets a partition. A secondary voter's mysqld is restarted (a clean
// shutdown, as in a rolling restart, so the group shrinks to the primary and the other secondary)
// and its vttablet starts it rejoining the group. A controlled offset after the joiner's START
// GROUP_REPLICATION, the primary's cell is isolated. The two members left can only keep a majority
// if the group expels the primary, with the joiner's vote, before the remaining secondary's
// group_replication_unreachable_majority_timeout; otherwise it leaves and the joiner is alone.
// The network is healed and the cluster converges before the next cycle.
//
// CHAOS_RACE_CYCLES (default 6) is the number of cycles, CHAOS_RACE_OFFSETS (default
// "0.5s,1.5s,2.5s") the offsets after the joiner's START, used in turn.
func TestG12VoterRejoinsWhilePrimaryCellIsolated(t *testing.T) {
	requireGR(t)
	cycles := 6
	if v := os.Getenv("CHAOS_RACE_CYCLES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			t.Fatalf("CHAOS_RACE_CYCLES=%q: want a positive number", v)
		}
		cycles = n
	}
	offsets := []time.Duration{500 * time.Millisecond, 1500 * time.Millisecond, 2500 * time.Millisecond}
	if v := os.Getenv("CHAOS_RACE_OFFSETS"); v != "" {
		offsets = nil
		for f := range strings.SplitSeq(v, ",") {
			d, err := time.ParseDuration(strings.TrimSpace(f))
			if err != nil {
				t.Fatalf("CHAOS_RACE_OFFSETS=%q: %v", v, err)
			}
			offsets = append(offsets, d)
		}
	}
	runScenario(t, "G12-voter-rejoin-primary-cell-isolated", Options{}, func(s *Scenario) {
		for i := range cycles {
			if !s.rejoinRaceCycle(i+1, offsets[i%len(offsets)]) {
				break
			}
		}
		s.Sleep(5*time.Second, "settle")
	})
}

// MySQL error log codes of Group Replication.
const (
	myGRStarting      = "MY-013587" // Plugin 'group_replication' is starting (START GROUP_REPLICATION)
	myGRViewChanged   = "MY-011503" // Group membership changed to ...
	myGRMajorityLeave = "MY-011711" // leaves after group_replication_unreachable_majority_timeout
	myGRNoDonor       = "MY-015084" // No donor available to provide the certification information
)

// errLogLine is one line of a MySQL error log.
type errLogLine struct {
	T    time.Time
	Code string
	Msg  string
}

var errLogRe = regexp.MustCompile(`^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+Z) \S+ \[\w+\] \[(MY-\d+)\] (.*)$`)

func parseErrLogLine(l string) (errLogLine, bool) {
	m := errLogRe.FindStringSubmatch(l)
	if m == nil {
		return errLogLine{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, m[1])
	if err != nil {
		return errLogLine{}, false
	}
	return errLogLine{T: t, Code: m[2], Msg: m[3]}, true
}

// readErrLog returns the parsed lines of a MySQL error log from byte offset off, and the offset of
// the end of the last complete line.
func readErrLog(file string, off int64) ([]errLogLine, int64) {
	f, err := os.Open(file)
	if err != nil {
		return nil, off
	}
	defer f.Close()
	if st, err := f.Stat(); err == nil && st.Size() < off {
		off = 0 // rotated or truncated
	}
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return nil, off
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, off
	}
	end := bytes.LastIndexByte(b, '\n')
	if end < 0 {
		return nil, off
	}
	var res []errLogLine
	for l := range strings.SplitSeq(string(b[:end]), "\n") {
		if e, ok := parseErrLogLine(l); ok {
			res = append(res, e)
		}
	}
	return res, off + int64(end) + 1
}

func errLogSize(file string) int64 {
	st, err := os.Stat(file)
	if err != nil {
		return 0
	}
	return st.Size()
}

// waitErrLog polls the error log from offset off until a line with code appears.
func waitErrLog(file string, off int64, code string, timeout time.Duration) (errLogLine, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var lines []errLogLine
		lines, off = readErrLog(file, off)
		for _, l := range lines {
			if l.Code == code {
				return l, true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errLogLine{}, false
}

// viewSize returns the number of members of a "Group membership changed to" message.
func viewSize(msg string) int {
	_, after, ok := strings.Cut(msg, "changed to ")
	if !ok {
		return 0
	}
	members, _, _ := strings.Cut(after, " on view")
	return len(strings.Split(members, ","))
}

// windowOutage returns the longest interval without an acked write that overlaps [from, to], and
// the total length of such intervals of at least outageThreshold. An interval still open at `to`
// ends there.
func windowOutage(recs []WriteRecord, from, to time.Time) (time.Duration, time.Duration) {
	var ends []time.Time
	for _, r := range recs {
		if r.Acked {
			ends = append(ends, r.End)
		}
	}
	sort.Slice(ends, func(i, j int) bool { return ends[i].Before(ends[j]) })
	var longest, total time.Duration
	add := func(a, b time.Time) {
		if !b.After(from) || !a.Before(to) {
			return
		}
		d := b.Sub(a)
		if d > longest {
			longest = d
		}
		if d >= outageThreshold {
			total += d
		}
	}
	last := time.Time{}
	for _, e := range ends {
		if !last.IsZero() {
			add(last, e)
		}
		last = e
		if e.After(to) {
			return longest, total
		}
	}
	if !last.IsZero() && last.Before(to) {
		add(last, to)
	}
	return longest, total
}

// rejoinRaceCycle runs one cycle of G12 with the given offset. It returns false when the cluster
// did not converge, and the scenario should stop.
func (s *Scenario) rejoinRaceCycle(k int, offset time.Duration) bool {
	_, probs, _ := s.WaitConverged(240 * time.Second)
	if len(probs) > 0 {
		s.R.violation("cycle %d: cluster not converged before the cycle: %s", k, strings.Join(probs, "; "))
		return false
	}
	p := s.topoPrimary()
	var secondaries []*Node
	for _, n := range s.Nodes {
		if n != p {
			secondaries = append(secondaries, n)
		}
	}
	joiner, other := secondaries[k%2], secondaries[(k+1)%2]
	tag := fmt.Sprintf("cycle %d", k)
	s.Log.Add("race", fmt.Sprintf("%s: offset %v, primary %s, joiner %s, other %s", tag, offset, p.Tablet.Alias, joiner.Tablet.Alias, other.Tablet.Alias))
	logs := map[*Node]string{}
	offs := map[*Node]int64{}
	for _, n := range s.Nodes {
		logs[n] = path.Join(n.DataDir(), "error.log")
		offs[n] = errLogSize(logs[n])
	}

	// Rolling restart of the joiner's mysqld.
	err := joiner.Tablet.MysqlctlProcess.Stop()
	s.Log.Add("race", fmt.Sprintf("%s: mysqld of %s shut down err=%v", tag, joiner.Tablet.Alias, err))
	if err := s.RestartMysqld(joiner); err != nil {
		s.R.violation("%s: cannot restart mysqld of %s: %v", tag, joiner.Tablet.Alias, err)
		return false
	}
	start, ok := waitErrLog(logs[joiner], offs[joiner], myGRStarting, 120*time.Second)
	if !ok {
		s.R.violation("%s: %s did not start group replication within 120s of its restart", tag, joiner.Tablet.Alias)
		return false
	}
	s.Log.Add("race", fmt.Sprintf("%s: %s START GROUP_REPLICATION at %s", tag, joiner.Tablet.Alias, start.T.Local().Format("15:04:05.000000")))

	// Isolate the primary's cell: its tablet, VTOrc and etcd keep reaching each other, nothing
	// outside the cell (vtgate included) reaches them. The tablets are cut first, so the group's
	// partition starts at the offset.
	time.Sleep(time.Until(start.T.Add(offset)))
	iso := time.Now()
	if s.Fault.IsZero() {
		s.MarkFault()
	}
	cell := []string{p.Group, p.OrcGroup, p.EtcdGroup}
	for _, n := range s.Nodes {
		if n != p {
			s.Partition(p.Group, n.Group)
		}
	}
	cut := time.Now()
	for _, in := range cell {
		for name := range s.Net.groups {
			if name == harnessGroup || name == p.Group || name == p.OrcGroup || name == p.EtcdGroup {
				continue
			}
			if in == p.Group && strings.HasPrefix(name, "tablet") {
				continue // already cut
			}
			s.Partition(in, name)
		}
	}
	s.Log.Add("race", fmt.Sprintf("%s: cell %s isolated at START+%.3fs (tablets cut at START+%.3fs, cell at START+%.3fs)", tag, p.Cell,
		iso.Sub(start.T).Seconds(), cut.Sub(start.T).Seconds(), time.Since(start.T).Seconds()))

	s.Sleep(20*time.Second, tag+": primary's cell isolated")
	heal := time.Now()
	s.Heal()
	_, probs, took := s.WaitConverged(240 * time.Second)
	converged := time.Now()
	s.Sleep(5*time.Second, tag+": converged; settle")

	// Outcome, from the error logs and the observer.
	byNode := map[*Node][]errLogLine{}
	for _, n := range s.Nodes {
		byNode[n], _ = readErrLog(logs[n], offs[n])
	}
	count := func(n *Node, code string, from, to time.Time) int {
		k := 0
		for _, l := range byNode[n] {
			if l.Code == code && !l.T.Before(from) && !l.T.After(to) {
				k++
			}
		}
		return k
	}
	admitted := "not before the heal"
	for _, l := range byNode[joiner] {
		if l.Code == myGRViewChanged && l.T.After(start.T) && viewSize(l.Msg) > 1 {
			if l.T.Before(heal) {
				admitted = fmt.Sprintf("START+%.2fs", l.T.Sub(start.T).Seconds())
			}
			break
		}
	}
	alone := 0
	for _, l := range byNode[joiner] {
		if l.Code == myGRViewChanged && l.T.After(iso) && l.T.Before(heal) && viewSize(l.Msg) == 1 {
			alone++
		}
	}
	otherLeft := count(other, myGRMajorityLeave, iso, heal)
	joinerLeft := count(joiner, myGRMajorityLeave, iso, heal)
	noDonor := 0
	for _, n := range s.Nodes {
		noDonor += count(n, myGRNoDonor, iso, converged)
	}
	// The majority was kept if a member outside the isolated cell was the ONLINE primary of a
	// view of at least two members with a majority, before the heal.
	kept := time.Time{}
	var errState []string
	for _, n := range s.Nodes {
		var errFrom, errTo time.Time
		hungFrom, hungMax := time.Time{}, time.Duration(0)
		s.O.mu.Lock()
		for _, smp := range s.O.samples[n.Idx] {
			if smp.T.Before(iso) || smp.T.After(converged) {
				continue
			}
			if n != p && smp.T.Before(heal) && smp.GR.OK && smp.GRCanCommit() && smp.GR.Online >= 2 && (kept.IsZero() || smp.T.Before(kept)) {
				kept = smp.T
			}
			if smp.GR.OK && smp.GR.State == "ERROR" {
				if errFrom.IsZero() {
					errFrom = smp.T
				}
				errTo = smp.T
			}
			if !smp.MySQLOK {
				if hungFrom.IsZero() {
					hungFrom = smp.T
				}
				if d := smp.T.Sub(hungFrom); d > hungMax {
					hungMax = d
				}
			} else {
				hungFrom = time.Time{}
			}
		}
		s.O.mu.Unlock()
		if !errFrom.IsZero() {
			errState = append(errState, fmt.Sprintf("%s ERROR from +%.1fs to +%.1fs", n.Tablet.Alias, errFrom.Sub(iso).Seconds(), errTo.Sub(iso).Seconds()))
		}
		if hungMax >= 5*time.Second {
			errState = append(errState, fmt.Sprintf("%s status unanswered for %.1fs", n.Tablet.Alias, hungMax.Seconds()))
		}
	}
	keptStr := "LOST"
	if !kept.IsZero() {
		keptStr = fmt.Sprintf("kept (new primary at +%.2fs)", kept.Sub(iso).Seconds())
	}
	longest, total := windowOutage(s.W.Records(), iso, converged)
	convStr := fmt.Sprintf("%.1fs after heal", took.Seconds())
	if len(probs) > 0 {
		convStr = "NOT converged after 240s: " + strings.Join(probs, "; ")
		s.R.violation("%s: not converged after the heal: %s", tag, strings.Join(probs, "; "))
	}
	if len(errState) == 0 {
		errState = []string{"none"}
	}
	line := fmt.Sprintf("CYCLE %d offset=%v actual=START+%.3fs primary=%s joiner=%s other=%s joiner_admitted=%s majority=%s other_left=%d joiner_left=%d joiner_alone_views=%d no_donor=%d error_states=[%s] outage_longest=%.2fs outage_total=%.2fs converged=%s",
		k, offset, iso.Sub(start.T).Seconds(), p.Tablet.Alias, joiner.Tablet.Alias, other.Tablet.Alias, admitted, keptStr,
		otherLeft, joinerLeft, alone, noDonor, strings.Join(errState, "; "), longest.Seconds(), total.Seconds(), convStr)
	s.Log.Add("race", line)
	s.R.outcome("%s", line)
	return len(probs) == 0
}
