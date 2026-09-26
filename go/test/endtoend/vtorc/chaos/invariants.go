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
	"slices"
	"strconv"
	"strings"
	"time"
)

// ---- mysql helpers ----

func (n *Node) query(q string, args ...any) ([]map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
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
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if p.status("Rpl_semi_sync_source_clients")["Rpl_semi_sync_source_clients"] == "2" {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.t.Fatalf("primary %s does not have 2 semi-sync clients", p.Tablet.Alias)
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
		r.timing("longest write gap: %.2fs (from +%.2fs to +%.2fs relative to fault)", st.LongestGap.Seconds(),
			st.GapFrom.Sub(opts.Fault).Seconds(), st.GapTo.Sub(opts.Fault).Seconds())
		if opts.OldPrimary != nil {
			if np, at, ok := o.FirstTopoChange(opts.OldPrimary.Tablet.Alias, opts.Fault); ok {
				r.timing("topo shard primary changed to %s at +%.2fs", np, at.Sub(opts.Fault).Seconds())
				if opts.ExpectNoFailover {
					r.violation("unexpected failover to %s at +%.2fs", np, at.Sub(opts.Fault).Seconds())
				}
			} else {
				r.timing("topo shard primary never changed from %s", opts.OldPrimary.Tablet.Alias)
			}
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
		if v["rpl_semi_sync_replica_enabled"] != "ON" || v["rpl_semi_sync_source_enabled"] != "OFF" {
			r.violation("SEMISYNC: replica %s replica_enabled=%s source_enabled=%s", n.Tablet.Alias, v["rpl_semi_sync_replica_enabled"], v["rpl_semi_sync_source_enabled"])
		}
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		cl := p.status("Rpl_semi_sync_source_clients")["Rpl_semi_sync_source_clients"]
		if cl == "2" {
			break
		}
		if time.Now().After(deadline) {
			r.violation("SEMISYNC: primary %s has %s semi-sync clients, want 2", p.Tablet.Alias, cl)
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
