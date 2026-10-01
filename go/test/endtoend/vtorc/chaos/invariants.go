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
	"os"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// ---- mysql helpers ----

func (n *Node) query(q string, args ...any) ([]map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return n.queryCtx(ctx, q, args...)
}

func (n *Node) queryCtx(ctx context.Context, q string, args ...any) ([]map[string]string, error) {
	rows, err := n.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var res []map[string]string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		m := map[string]string{}
		for i, c := range cols {
			m[c] = vals[i].String
		}
		res = append(res, m)
	}
	return res, rows.Err()
}

func (n *Node) scalar(q string, args ...any) (string, error) {
	r, err := n.query(q, args...)
	if err != nil {
		return "", err
	}
	if len(r) == 0 {
		return "", nil
	}
	for _, v := range r[0] {
		return v, nil
	}
	return "", nil
}

func (n *Node) serverUUID() string {
	v, _ := n.scalar("select @@global.server_uuid")
	return v
}

func (n *Node) gtidExecuted() (string, error) {
	v, err := n.scalar("select @@global.gtid_executed")
	return strings.ReplaceAll(v, "\n", ""), err
}

func (n *Node) replicaStatus() (map[string]string, error) {
	r, err := n.query("show replica status")
	if err != nil || len(r) == 0 {
		return nil, err
	}
	return r[0], nil
}

func (n *Node) variables(like string) map[string]string {
	r, _ := n.query("show global variables like ?", like)
	m := map[string]string{}
	for _, row := range r {
		m[row["Variable_name"]] = row["Value"]
	}
	return m
}

func (n *Node) status(like string) map[string]string {
	r, _ := n.query("show global status like ?", like)
	m := map[string]string{}
	for _, row := range r {
		m[row["Variable_name"]] = row["Value"]
	}
	return m
}

func (n *Node) ids() (map[int64]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := n.db.QueryContext(ctx, fmt.Sprintf("select id, coalesce(src,'') from vt_%s.%s", keyspaceName, tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := map[int64]string{}
	for rows.Next() {
		var id int64
		var src string
		if err := rows.Scan(&id, &src); err != nil {
			return nil, err
		}
		m[id] = src
	}
	return m, rows.Err()
}

func (c *Chaos) tabletTypeHTTP(n *Node) string {
	o := &Observer{client: httpClient}
	t, ok := o.tabletType(n)
	if !ok {
		return "DOWN"
	}
	return t
}

// ---- convergence ----

// convergenceProblems returns why the cluster is not (yet) in the healthy steady state:
// exactly one primary (topo shard record, tablet record, vttablet and mysqld agree), every other
// tablet a read-only REPLICA replicating from it.
func (c *Chaos) convergenceProblems() (*Node, []string) {
	var probs []string
	p := c.topoPrimary()
	if p == nil {
		return nil, []string{"no shard primary in topo"}
	}
	if typ := c.tabletTypeHTTP(p); typ != "PRIMARY" {
		probs = append(probs, fmt.Sprintf("primary %s vttablet type %s", p.Tablet.Alias, typ))
	}
	if ro, err := p.scalar("select @@global.read_only"); err != nil || ro != "0" {
		probs = append(probs, fmt.Sprintf("primary %s read_only=%s err=%v", p.Tablet.Alias, ro, err))
	}
	for _, n := range c.Nodes {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ti, err := c.Ts.GetTablet(ctx, n.Tablet.GetAlias())
		cancel()
		want := "REPLICA"
		if n == p {
			want = "PRIMARY"
		}
		if err != nil {
			probs = append(probs, fmt.Sprintf("%s: topo tablet read: %v", n.Tablet.Alias, err))
		} else if ti.Type.String() != want {
			probs = append(probs, fmt.Sprintf("%s: topo type %s want %s", n.Tablet.Alias, ti.Type, want))
		}
		if n == p {
			continue
		}
		if typ := c.tabletTypeHTTP(n); typ != "REPLICA" {
			probs = append(probs, fmt.Sprintf("%s vttablet type %s", n.Tablet.Alias, typ))
		}
		if sro, err := n.scalar("select @@global.super_read_only"); err != nil || sro != "1" {
			probs = append(probs, fmt.Sprintf("%s super_read_only=%s err=%v", n.Tablet.Alias, sro, err))
		}
		if c.gr {
			// Group members replicate through the group's channels, not the default one.
			continue
		}
		rs, err := n.replicaStatus()
		if err != nil || rs == nil {
			probs = append(probs, fmt.Sprintf("%s: no replica status (%v)", n.Tablet.Alias, err))
			continue
		}
		if rs["Source_Port"] != strconv.Itoa(p.Tablet.MySQLPort) || rs["Replica_IO_Running"] != "Yes" || rs["Replica_SQL_Running"] != "Yes" {
			probs = append(probs, fmt.Sprintf("%s: replicating from port %s io=%s sql=%s io_err=%q sql_err=%q", n.Tablet.Alias,
				rs["Source_Port"], rs["Replica_IO_Running"], rs["Replica_SQL_Running"], rs["Last_IO_Error"], rs["Last_SQL_Error"]))
		}
	}
	if c.gr {
		probs = append(probs, c.grConvergenceProblems(p)...)
	}
	return p, probs
}

// WaitConverged waits until the cluster is in the healthy steady state.
func (c *Chaos) WaitConverged(timeout time.Duration) (*Node, []string, time.Duration) {
	start := time.Now()
	var p *Node
	var probs []string
	last := ""
	for time.Since(start) < timeout {
		p, probs = c.convergenceProblems()
		if len(probs) == 0 {
			return p, nil, time.Since(start)
		}
		if s := strings.Join(probs, "; "); s != last {
			c.Log.Add("converge", "waiting: "+s)
			last = s
		}
		time.Sleep(500 * time.Millisecond)
	}
	return p, probs, time.Since(start)
}

// WaitHealthy is used during setup; it fails the test if the cluster does not become healthy.
func (c *Chaos) WaitHealthy(timeout time.Duration) {
	p, probs, _ := c.WaitConverged(timeout)
	if len(probs) > 0 {
		c.t.Fatalf("cluster not healthy: %v", probs)
	}
	if c.gr {
		return
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if p.status("Rpl_semi_sync_source_clients")["Rpl_semi_sync_source_clients"] == strconv.Itoa(c.semiSyncAckers(p)) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.t.Fatalf("primary %s does not have %d semi-sync clients", p.Tablet.Alias, c.semiSyncAckers(p))
}

// ---- report ----

// Report is the outcome of a scenario.
type Report struct {
	Name       string
	Outcome    []string
	Timings    []string
	Violations []string
	Notes      []string
}

func (r *Report) violation(f string, a ...any) {
	r.Violations = append(r.Violations, fmt.Sprintf(f, a...))
}
func (r *Report) note(f string, a ...any)    { r.Notes = append(r.Notes, fmt.Sprintf(f, a...)) }
func (r *Report) outcome(f string, a ...any) { r.Outcome = append(r.Outcome, fmt.Sprintf(f, a...)) }

func (r *Report) timing(f string, a ...any) { r.Timings = append(r.Timings, fmt.Sprintf(f, a...)) }

func (r *Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "==== SCENARIO %s ====\n", r.Name)
	sec := func(title string, l []string) {
		fmt.Fprintf(&b, "-- %s (%d)\n", title, len(l))
		for _, s := range l {
			fmt.Fprintf(&b, "   %s\n", s)
		}
	}
	sec("OUTCOME", r.Outcome)
	sec("TIMINGS", r.Timings)
	sec("VIOLATIONS", r.Violations)
	sec("NOTES", r.Notes)
	return b.String()
}

func (r *Report) Save() {
	d := path.Join(resultsDir(), r.Name)
	_ = os.MkdirAll(d, 0o755)
	_ = os.WriteFile(path.Join(d, "report.txt"), []byte(r.String()), 0o644)
}

// CheckOptions tunes the final invariant checks.
type CheckOptions struct {
	ConvergeTimeout time.Duration
	Fault           time.Time // when the fault was injected (for timings)
	OldPrimary      *Node
	// ExpectNoFailover makes a primary change a violation.
	ExpectNoFailover bool
	// OldPrimaryUUID is the old primary's server_uuid, read before the fault.
	OldPrimaryUUID string
}

// reportOldPrimaryServing reports how long the old primary kept acting as the primary after
// the fault: its vttablet reporting PRIMARY, its mysqld writable, and vtgate routing primary
// reads to it.
func (c *Chaos) reportOldPrimaryServing(r *Report, w *Workload, o *Observer, opts CheckOptions) {
	old := opts.OldPrimary
	isPrimary := func(s Sample) bool { return s.TypeOK && s.Type == "PRIMARY" }
	writable := func(s Sample) bool { return s.MySQLOK && !s.ReadOnly }
	if s, ok := o.FirstAfter(old.Idx, opts.Fault, func(s Sample) bool { return !isPrimary(s) }); ok {
		r.timing("old primary %s vttablet stopped reporting PRIMARY at +%.2fs (%s)", old.Tablet.Alias, s.T.Sub(opts.Fault).Seconds(), describeSample(s))
	} else {
		r.timing("old primary %s vttablet reported PRIMARY for the whole scenario", old.Tablet.Alias)
	}
	if s, ok := o.FirstAfter(old.Idx, opts.Fault, func(s Sample) bool { return !writable(s) }); ok {
		r.timing("old primary %s mysqld stopped being writable (read_only or down) at +%.2fs", old.Tablet.Alias, s.T.Sub(opts.Fault).Seconds())
	}
	if s, ok := o.FirstAfter(old.Idx, opts.Fault, func(s Sample) bool { return s.MySQLOK && s.SuperReadOnly }); ok {
		r.timing("old primary %s mysqld super_read_only ON at +%.2fs", old.Tablet.Alias, s.T.Sub(opts.Fault).Seconds())
	} else {
		r.timing("old primary %s mysqld super_read_only never ON after the fault", old.Tablet.Alias)
	}
	if s, ok := o.FirstAfter(old.Idx, opts.Fault, func(s Sample) bool { return s.MySQLOK && s.OfflineMode }); ok {
		r.timing("old primary %s mysqld offline_mode ON (app connections refused) at +%.2fs", old.Tablet.Alias, s.T.Sub(opts.Fault).Seconds())
	}
	if opts.OldPrimaryUUID == "" {
		return
	}
	changeAt, changed := time.Time{}, false
	if _, at, ok := o.FirstTopoChange(old.Tablet.Alias, opts.Fault); ok {
		changeAt, changed = at, true
	}
	backAt, back := time.Time{}, false
	if changed {
		backAt, back = o.FirstTopoPrimary(old.Tablet.Alias, changeAt)
	}
	// In Group Replication mode, the old primary's data is stale once another member is the
	// primary of a view with a majority: that member commits writes the old primary never sees.
	electedAt, elected := time.Time{}, false
	if c.gr {
		electedAt, elected = o.FirstOtherGroupPrimary(old.Idx, opts.Fault)
		if elected {
			r.timing("another member became the primary of a majority view at +%.2fs", electedAt.Sub(opts.Fault).Seconds())
		}
	}
	c.reportWriteProbe(r, w, opts, backAt, back)
	rs := oldPrimaryReads(w.Reads(), opts.OldPrimaryUUID, opts.Fault, changeAt, changed, electedAt, elected, backAt, back)
	if !rs.LastOld.IsZero() {
		r.timing("vtgate primary reads answered by the old primary until +%.2fs after the fault", rs.LastOld.Sub(opts.Fault).Seconds())
	} else {
		r.timing("vtgate primary reads: none answered by the old primary after the fault")
	}
	if !rs.LastStale.IsZero() {
		r.timing("stale primary reads (old primary, after another member was elected): last at +%.2fs", rs.LastStale.Sub(opts.Fault).Seconds())
	}
	r.outcome("vtgate primary reads after fault: %d, failed %d, answered by old primary: %d, after another member was elected: %d, after the topo primary changed: %d",
		rs.Total, rs.Failed, rs.Old, rs.Stale, rs.AfterTopoChange)
}

// probeErrno matches the MySQL error number in an error message: the one MySQL returned to
// vttablet, else the one vtgate returned.
var probeErrno = []*regexp.Regexp{regexp.MustCompile(`errno (\d+)`), regexp.MustCompile(`Error (\d+) \(`)}

// probeErrorKind shortens a write probe's error to a category.
func probeErrorKind(e string) string {
	for _, re := range probeErrno {
		if m := re.FindStringSubmatch(e); m != nil {
			return "errno " + m[1]
		}
	}
	for _, k := range []string{"context deadline exceeded", "i/o timeout", "invalid connection", "connection refused", "bad connection", "EOF"} {
		if strings.Contains(e, k) {
			return k
		}
	}
	if len(e) > 60 {
		e = e[:60]
	}
	return e
}

// probeStats summarizes the write probe's transactions that vtgate routed to the old primary after
// a fault, until the old primary became the topo primary again (back).
type probeStats struct {
	Started, Routed, Inferred, Acked, Failed int
	// Waits are how long the failed transactions routed to the old primary waited for their error.
	Waits []time.Duration
	// MaxWaitFirstSecond is the longest wait of such a transaction started in the first second.
	MaxWaitFirstSecond time.Duration
	LastStart, LastEnd time.Time
	Kinds              map[string]int
}

// oldPrimaryProbes counts the write probe's transactions routed to the old primary. A transaction
// whose first statement failed has no server_uuid: it counts as routed to the old primary
// (Inferred) when the last primary read that started before it was answered by the old primary.
func oldPrimaryProbes(probes []ProbeRecord, reads []ReadRecord, oldUUID string, fault, backAt time.Time, back bool) probeStats {
	st := probeStats{Kinds: map[string]int{}}
	for _, p := range probes {
		if !p.Start.After(fault) || (back && !p.Start.Before(backAt)) {
			continue
		}
		st.Started++
		routed := p.UUID == oldUUID
		if p.UUID == "" {
			last := ""
			for _, rd := range reads {
				if rd.Start.After(p.Start) {
					break
				}
				if rd.Err == "" {
					last = rd.UUID
				}
			}
			if last == oldUUID {
				routed = true
				st.Inferred++
			}
		}
		if !routed {
			continue
		}
		st.Routed++
		st.LastStart = p.Start
		if p.End.After(st.LastEnd) {
			st.LastEnd = p.End
		}
		if p.Acked {
			st.Acked++
			continue
		}
		st.Failed++
		wait := p.End.Sub(p.Start)
		st.Waits = append(st.Waits, wait)
		if p.Start.Sub(fault) < time.Second && wait > st.MaxWaitFirstSecond {
			st.MaxWaitFirstSecond = wait
		}
		st.Kinds[probeErrorKind(p.Err)]++
	}
	slices.Sort(st.Waits)
	return st
}

// reportWriteProbe reports how long the write probe's transactions that vtgate routed to the old
// primary waited for their outcome, and saves every probe transaction to probes.txt.
func (c *Chaos) reportWriteProbe(r *Report, w *Workload, opts CheckOptions, backAt time.Time, back bool) {
	probes := w.Probes()
	if len(probes) == 0 {
		return
	}
	reads := w.Reads()
	st := oldPrimaryProbes(probes, reads, opts.OldPrimaryUUID, opts.Fault, backAt, back)
	var b strings.Builder
	b.WriteString("# start(+s after fault) wait(s) server acked error\n")
	for _, p := range probes {
		fmt.Fprintf(&b, "%+.3f %.3f %s %v %s\n", p.Start.Sub(opts.Fault).Seconds(), p.End.Sub(p.Start).Seconds(),
			aliasOf(c.nodeByUUIDCached(p.UUID)), p.Acked, strings.ReplaceAll(p.Err, "\n", " "))
	}
	_ = os.MkdirAll(path.Join(resultsDir(), r.Name), 0o755)
	_ = os.WriteFile(path.Join(resultsDir(), r.Name, "probes.txt"), []byte(b.String()), 0o644)
	if st.Routed == 0 {
		r.timing("write probe: %d transactions after the fault, none routed to the old primary", st.Started)
		return
	}
	var kinds []string
	for k, n := range st.Kinds {
		kinds = append(kinds, fmt.Sprintf("%s x%d", k, n))
	}
	slices.Sort(kinds)
	r.timing("write probe: %d transactions after the fault, %d routed to the old primary (%d inferred from reads), last one started at +%.2fs: acked %d, failed %d [%s]",
		st.Started, st.Routed, st.Inferred, st.LastStart.Sub(opts.Fault).Seconds(), st.Acked, st.Failed, strings.Join(kinds, ", "))
	if len(st.Waits) > 0 {
		r.timing("write probe: failed transactions routed to the old primary waited min %.2fs, median %.2fs, max %.2fs (max %.2fs for those started in the first second); the last returned at +%.2fs",
			st.Waits[0].Seconds(), st.Waits[len(st.Waits)/2].Seconds(), st.Waits[len(st.Waits)-1].Seconds(),
			st.MaxWaitFirstSecond.Seconds(), st.LastEnd.Sub(opts.Fault).Seconds())
	}
}

// primaryReadStats counts the primary reads of the workload after a fault.
type primaryReadStats struct {
	Total, Failed int
	// Old reads were answered by the old primary; Stale ones started after another member was
	// elected, AfterTopoChange ones after the shard record named another primary.
	Old, Stale, AfterTopoChange int
	LastOld, LastStale          time.Time
}

// oldPrimaryReads counts the reads that started after the fault, and before the old primary became
// the topo primary again (back), by who answered them.
func oldPrimaryReads(reads []ReadRecord, oldUUID string, fault, changeAt time.Time, changed bool, electedAt time.Time, elected bool, backAt time.Time, back bool) primaryReadStats {
	var st primaryReadStats
	for _, rd := range reads {
		if !rd.Start.After(fault) || (back && !rd.Start.Before(backAt)) {
			continue
		}
		st.Total++
		if rd.Err != "" {
			st.Failed++
			continue
		}
		if rd.UUID != oldUUID {
			continue
		}
		st.Old++
		st.LastOld = rd.End
		if elected && rd.Start.After(electedAt) {
			st.Stale++
			st.LastStale = rd.End
		}
		if changed && rd.Start.After(changeAt) {
			st.AfterTopoChange++
		}
	}
	return st
}

// CheckInvariants stops nothing; the caller must have stopped the workload and healed faults.
func (c *Chaos) CheckInvariants(r *Report, w *Workload, o *Observer, opts CheckOptions) {
	if opts.ConvergeTimeout == 0 {
		opts.ConvergeTimeout = 120 * time.Second
	}
	// Workload/availability timings.
	st := w.Stats(opts.Fault)
	r.outcome("writes: total=%d acked=%d failed/unknown=%d", st.Total, st.Acked, st.Failed)
	if !opts.Fault.IsZero() {
		ongoing := ""
		if st.GapOngoing {
			ongoing = ", still ongoing when the writers stopped"
		}
		r.timing("longest write gap: %.2fs (from +%.2fs to +%.2fs relative to fault%s)", st.LongestGap.Seconds(),
			st.GapFrom.Sub(opts.Fault).Seconds(), st.GapTo.Sub(opts.Fault).Seconds(), ongoing)
		var outages []string
		for _, o := range st.Outages {
			s := fmt.Sprintf("%.1fs at +%.1fs", o.Duration().Seconds(), o.From.Sub(opts.Fault).Seconds())
			if o.Ongoing {
				s += " (ongoing at stop)"
			}
			outages = append(outages, s)
		}
		r.timing("unavailable (no acked write for >= %v): %.2fs in total, %d outages [%s]; writers stopped at +%.2fs",
			outageThreshold, st.Unavailable.Seconds(), len(st.Outages), strings.Join(outages, ", "), st.Stopped.Sub(opts.Fault).Seconds())
		if opts.OldPrimary != nil {
			if np, at, ok := o.FirstTopoChange(opts.OldPrimary.Tablet.Alias, opts.Fault); ok {
				r.timing("topo shard primary changed to %s at +%.2fs", np, at.Sub(opts.Fault).Seconds())
				if opts.ExpectNoFailover {
					r.violation("unexpected failover to %s at +%.2fs", np, at.Sub(opts.Fault).Seconds())
				}
			} else {
				r.timing("topo shard primary never changed from %s", opts.OldPrimary.Tablet.Alias)
			}
			c.reportOldPrimaryServing(r, w, o, opts)
		}
		if !st.FirstFailAfter.IsZero() {
			r.timing("first failed write started at +%.2fs", st.FirstFailAfter.Sub(opts.Fault).Seconds())
		}
	}

	// Split brain during the scenario.
	for _, sb := range o.SplitBrains() {
		kind := "TWO WRITABLE PRIMARIES (read_only=OFF and vttablet type PRIMARY)"
		if sb.MySQLOnly {
			kind = "two mysqld with read_only=OFF"
		}
		msg := fmt.Sprintf("%s: %s & %s from %s to %s (%d samples)", kind, sb.A, sb.B, sb.From.Format("15:04:05.000"), sb.To.Format("15:04:05.000"), sb.Samples)
		if sb.MySQLOnly {
			r.note("%s", msg)
		} else {
			r.violation("SPLIT BRAIN %s", msg)
		}
	}

	// Convergence.
	p, probs, took := c.WaitConverged(opts.ConvergeTimeout)
	if len(probs) > 0 {
		r.violation("CONVERGENCE: not converged after %v: %s", took.Round(time.Second), strings.Join(probs, "; "))
	} else {
		r.timing("converged %.1fs after heal/stop-writes; primary=%s", took.Seconds(), p.Tablet.Alias)
	}
	if p == nil {
		r.violation("no primary; skipping data checks")
		return
	}
	if opts.OldPrimary != nil {
		r.outcome("old primary %s -> new primary %s", opts.OldPrimary.Tablet.Alias, p.Tablet.Alias)
	}

	// Let replicas catch up on the final primary before data checks.
	pg, err := p.gtidExecuted()
	if err != nil {
		r.violation("cannot read primary gtid_executed: %v", err)
		return
	}
	for _, n := range c.Nodes {
		if n == p {
			continue
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			ok, err := n.scalar("select gtid_subset(?, @@global.gtid_executed)", pg)
			if err == nil && ok == "1" {
				break
			}
			if time.Now().After(deadline) {
				r.violation("%s did not catch up to primary's gtid_executed within 30s (err=%v)", n.Tablet.Alias, err)
				break
			}
			time.Sleep(300 * time.Millisecond)
		}
	}

	uuids := map[string]string{}
	for _, n := range c.Nodes {
		uuids[n.serverUUID()] = n.Tablet.Alias
	}

	// Errant GTIDs.
	for _, n := range c.Nodes {
		if n == p {
			continue
		}
		g, err := n.gtidExecuted()
		if err != nil {
			r.violation("%s: cannot read gtid_executed: %v", n.Tablet.Alias, err)
			continue
		}
		errant, err := p.scalar("select gtid_subtract(?, @@global.gtid_executed)", g)
		if err != nil {
			r.violation("%s: gtid_subtract failed: %v", n.Tablet.Alias, err)
			continue
		}
		errant = strings.ReplaceAll(errant, "\n", "")
		if errant != "" {
			var origin []string
			for part := range strings.SplitSeq(errant, ",") {
				u, _, _ := strings.Cut(part, ":")
				origin = append(origin, fmt.Sprintf("%s(from %s)", part, uuids[u]))
			}
			r.violation("ERRANT GTIDs on %s (not on primary %s): %s", n.Tablet.Alias, p.Tablet.Alias, strings.Join(origin, ", "))
			if at, ok := o.FirstContaining(n.Idx, errant); ok {
				r.note("errant GTIDs %s first observed on %s at %s (+%.2fs after fault)", errant, n.Tablet.Alias, at.Format("15:04:05.000"), at.Sub(opts.Fault).Seconds())
			}
		}
	}

	// Durability.
	pids, err := p.ids()
	if err != nil {
		r.violation("cannot read rows on primary: %v", err)
		return
	}
	others := map[string]map[int64]string{}
	for _, n := range c.Nodes {
		if n != p {
			if m, err := n.ids(); err == nil {
				others[n.Tablet.Alias] = m
			}
		}
	}
	var missing []string
	acked := 0
	for _, rec := range w.Records() {
		if !rec.Acked {
			continue
		}
		acked++
		if _, ok := pids[rec.ID]; ok {
			continue
		}
		var where []string
		for a, m := range others {
			if src, ok := m[rec.ID]; ok {
				where = append(where, fmt.Sprintf("%s(src=%s)", a, uuids[src]))
			}
		}
		missing = append(missing, fmt.Sprintf("id=%d acked at %s present-on=%v", rec.ID, rec.End.Format("15:04:05.000"), where))
	}
	if len(missing) > 0 {
		r.violation("DURABILITY: %d/%d acked writes missing on new primary %s: %s", len(missing), acked, p.Tablet.Alias, strings.Join(firstN(missing, 20), "; "))
	} else {
		r.outcome("durability: all %d acked writes present on primary %s", acked, p.Tablet.Alias)
	}
	// Rows present on a replica but not on the primary (committed, maybe unacked, "phantom" rows).
	for a, m := range others {
		var extra []int64
		for id := range m {
			if _, ok := pids[id]; !ok {
				extra = append(extra, id)
			}
		}
		if len(extra) > 0 {
			slices.Sort(extra)
			r.note("%s has %d rows not on primary (unacked commits): %v", a, len(extra), firstN(extra, 10))
		}
	}
	// Writes acked after the new primary took over in topo but committed by the deposed primary.
	if opts.OldPrimary != nil {
		if _, at, ok := o.FirstTopoChange(opts.OldPrimary.Tablet.Alias, opts.Fault); ok {
			oldUUID := opts.OldPrimary.serverUUID()
			// The old primary may legitimately become primary again later (e.g. after a
			// double failure); only look at writes issued before that.
			backAt, back := o.FirstTopoPrimary(opts.OldPrimary.Tablet.Alias, at)
			// Its vttablet reports the promotion before the shard record names it.
			if promotedAt, ok := o.Repromoted(opts.OldPrimary.Idx, at); ok && (!back || promotedAt.Before(backAt)) {
				backAt, back = promotedAt, true
			}
			var late []string
			for _, rec := range w.Records() {
				if !rec.Acked || !rec.Start.After(at) || (back && !rec.End.Before(backAt)) {
					continue
				}
				src, ok := pids[rec.ID]
				if !ok {
					for _, m := range others {
						if s, ok2 := m[rec.ID]; ok2 {
							src = s
						}
					}
				}
				if src == oldUUID {
					late = append(late, strconv.FormatInt(rec.ID, 10))
				}
			}
			if len(late) > 0 {
				r.violation("%d writes issued after the topo primary changed were acked by the DEPOSED primary %s: %v", len(late), opts.OldPrimary.Tablet.Alias, firstN(late, 10))
			} else {
				r.outcome("no write issued after the topo primary change was committed by the deposed primary")
			}
		}
	}
	// Which server committed the acked writes (src column).
	srcCount := map[string]int{}
	for _, src := range pids {
		srcCount[uuids[src]]++
	}
	r.note("rows on primary by committing server: %v", srcCount)

	if c.gr {
		c.grCheckInvariants(r, p)
		return
	}
	// Semi-sync configuration for cross_cell (every tablet is in a different cell, so both
	// replicas must be semi-sync ackers and the primary must require an ack).
	pv := p.variables("rpl_semi_sync_%enabled")
	if pv["rpl_semi_sync_source_enabled"] != "ON" {
		r.violation("SEMISYNC: primary %s rpl_semi_sync_source_enabled=%s", p.Tablet.Alias, pv["rpl_semi_sync_source_enabled"])
	}
	for _, n := range c.Nodes {
		if n == p {
			continue
		}
		v := n.variables("rpl_semi_sync_%enabled")
		wantReplica := "ON"
		if n.Cell == p.Cell {
			wantReplica = "OFF" // same-cell replicas are not cross_cell ackers
		}
		if v["rpl_semi_sync_replica_enabled"] != wantReplica || v["rpl_semi_sync_source_enabled"] != "OFF" {
			r.violation("SEMISYNC: replica %s replica_enabled=%s source_enabled=%s", n.Tablet.Alias, v["rpl_semi_sync_replica_enabled"], v["rpl_semi_sync_source_enabled"])
		}
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		cl := p.status("Rpl_semi_sync_source_clients")["Rpl_semi_sync_source_clients"]
		if cl == strconv.Itoa(c.semiSyncAckers(p)) {
			break
		}
		if time.Now().After(deadline) {
			r.violation("SEMISYNC: primary %s has %s semi-sync clients, want %d", p.Tablet.Alias, cl, c.semiSyncAckers(p))
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func firstN[T any](l []T, n int) []T {
	if len(l) > n {
		return l[:n]
	}
	return l
}
