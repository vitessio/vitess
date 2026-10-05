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
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql/replication"
)

// S13: VTOrc healing a replica whose applier is stuck on a torn relay log.
//
// With relay_log_recovery=0 (so MySQL does not discard unapplied relay log events at startup)
// and sync_relay_log=1, a crash (power loss, full disk) can leave the newest relay log truncated
// in the middle of an event. The applier then stops for good with "Relay log read failure"
// (MY-013121). VTOrc analyses that as ReplicationStopped and runs fixReplica, which calls
// SetReplicationSource on the replica.
//
// The tear is simulated: R1's applier is held with LOCK TABLES so its relay log holds a backlog
// of received (and semi-sync acknowledged) but unapplied transactions, R1's mysqld is kill -9'ed,
// its newest relay log is truncated inside a rows event, and mysqld is started again.
//
// Every S13 test runs its cluster with relay_log_recovery=0 and sync_relay_log=1 (via
// EXTRA_MY_CNF, which mysqlctl appends to my.cnf), unless EXTRA_MY_CNF is already set.

const relayLogSafeCnf = "# S13: keep unapplied relay log events across restarts\nrelay_log_recovery = 0\nsync_relay_log = 1\n"

// useRelayLogSafeConfig makes the mysqlds of the next cluster run with relay_log_recovery=0 and
// sync_relay_log=1, after the profile's own settings (see NewChaos).
func useRelayLogSafeConfig(t *testing.T) {
	t.Setenv("CHAOS_RELAY_LOG_SAFE", "1")
}

// checkRelayLogConfig reports the relay log settings of every mysqld.
func (s *Scenario) checkRelayLogConfig() {
	for _, n := range s.Nodes {
		r, err := n.query("select @@global.relay_log_recovery rlr, @@global.sync_relay_log srl, @@global.sync_binlog sb, @@global.skip_replica_start srs")
		if err != nil || len(r) == 0 {
			s.R.violation("HARNESS: cannot read relay log settings of %s: %v", n.Tablet.Alias, err)
			continue
		}
		s.R.note("%s relay_log_recovery=%s sync_relay_log=%s sync_binlog=%s skip_replica_start=%s", n.Tablet.Alias, r[0]["rlr"], r[0]["srl"], r[0]["sb"], r[0]["srs"])
		if want := os.Getenv("S13_EXPECT_RELAY_LOG_RECOVERY"); want == "" && (r[0]["rlr"] != "0" || r[0]["srl"] != "1") {
			s.R.violation("HARNESS: %s runs relay_log_recovery=%s sync_relay_log=%s, want 0 and 1", n.Tablet.Alias, r[0]["rlr"], r[0]["srl"])
		}
	}
}

// ---- relay log parsing ----

type relayEvent struct {
	Pos, End int64
	Type     string
	GTID     string
	IDs      []int64
}

type relayTrx struct {
	GTID       string
	Start, End int64
	Complete   bool
	Rows       []relayEvent
	IDs        []int64
}

var (
	reAt       = regexp.MustCompile(`^# at (\d+)$`)
	reHeader   = regexp.MustCompile(`server id \d+\s+end_log_pos \d+(?: CRC32 0x[0-9a-f]+)?\s+([A-Za-z_-]+)`)
	reGTIDNext = regexp.MustCompile(`GTID_NEXT= '([0-9a-f-]+:\d+)'`)
	reRowID    = regexp.MustCompile(`^###   @1=(-?\d+)`)
)

// newestRelayLog returns the path of the node's newest relay log file.
func newestRelayLog(n *Node) (string, error) {
	files, err := filepath.Glob(path.Join(n.DataDir(), "relay-logs", fmt.Sprintf("vt-%010d-relay-bin.[0-9]*", n.Tablet.TabletUID)))
	if err != nil {
		return "", err
	}
	if len(files) == 0 {
		return "", fmt.Errorf("no relay logs in %s", path.Join(n.DataDir(), "relay-logs"))
	}
	sort.Strings(files)
	return files[len(files)-1], nil
}

// parseRelayLog decodes a relay log file with mysqlbinlog and returns its events and
// transactions, plus what mysqlbinlog wrote to stderr.
func parseRelayLog(file string) ([]relayEvent, []relayTrx, string, error) {
	st, err := os.Stat(file)
	if err != nil {
		return nil, nil, "", err
	}
	cmd := exec.Command("mysqlbinlog", "--base64-output=decode-rows", "-v", file)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil && len(out) == 0 {
		return nil, nil, stderr.String(), fmt.Errorf("mysqlbinlog %s: %v: %s", file, err, stderr.String())
	}
	var evs []relayEvent
	wantHeader := false
	for l := range strings.SplitSeq(string(out), "\n") {
		if m := reAt.FindStringSubmatch(l); m != nil {
			pos, _ := strconv.ParseInt(m[1], 10, 64)
			if len(evs) > 0 {
				evs[len(evs)-1].End = pos
			}
			evs = append(evs, relayEvent{Pos: pos})
			wantHeader = true
			continue
		}
		if len(evs) == 0 {
			continue
		}
		e := &evs[len(evs)-1]
		if wantHeader {
			if m := reHeader.FindStringSubmatch(l); m != nil {
				e.Type = strings.TrimSuffix(m[1], ":")
				wantHeader = false
			}
			continue
		}
		if m := reGTIDNext.FindStringSubmatch(l); m != nil {
			e.GTID = m[1]
		}
		if m := reRowID.FindStringSubmatch(l); m != nil {
			id, _ := strconv.ParseInt(m[1], 10, 64)
			e.IDs = append(e.IDs, id)
		}
	}
	if len(evs) > 0 {
		evs[len(evs)-1].End = st.Size()
	}
	var trxs []relayTrx
	var cur *relayTrx
	for _, e := range evs {
		switch e.Type {
		case "GTID":
			if cur != nil {
				// The previous transaction had no Xid (DDL): it ended before this GTID.
				cur.End, cur.Complete = e.Pos, true
				trxs = append(trxs, *cur)
			}
			cur = &relayTrx{GTID: e.GTID, Start: e.Pos}
		case "Write_rows", "Update_rows", "Delete_rows":
			if cur != nil {
				cur.Rows = append(cur.Rows, e)
				cur.IDs = append(cur.IDs, e.IDs...)
			}
		case "Xid":
			if cur != nil {
				cur.End, cur.Complete = e.End, true
				trxs = append(trxs, *cur)
				cur = nil
			}
		}
		if cur != nil && e.Type == "GTID" && cur.GTID == "" {
			cur.GTID = e.GTID
		}
	}
	if cur != nil {
		cur.End = st.Size()
		trxs = append(trxs, *cur)
	}
	return evs, trxs, stderr.String(), nil
}

// tearResult describes a truncation of a relay log.
type tearResult struct {
	File       string
	Offset     int64
	OldSize    int64
	Torn       relayTrx   // the transaction the cut lands in
	Dropped    []relayTrx // complete transactions after the torn one, dropped entirely
	DroppedIDs []int64    // row ids of the torn and dropped transactions
	Verified   bool       // the cut is not on an event boundary and mysqlbinlog fails to read the tail
}

// tearRelayLog truncates the node's newest relay log (mysqld must be down) inside the first rows
// event of a transaction chosen by pick among the complete transactions that were not applied
// (not in executed). It returns what was cut.
func (s *Scenario) tearRelayLog(n *Node, executed string, pick func(unapplied []relayTrx) int) (*tearResult, error) {
	file, err := newestRelayLog(n)
	if err != nil {
		return nil, err
	}
	// Keep a copy of the untouched file for later inspection.
	d := path.Join(resultsDir(), s.R.Name)
	_ = os.MkdirAll(d, 0o755)
	_ = exec.Command("cp", file, path.Join(d, "relaylog-before-tear-"+path.Base(file))).Run()
	evs, trxs, _, err := parseRelayLog(file)
	if err != nil {
		return nil, err
	}
	exec_, err := replication.ParseMysql56GTIDSet(executed)
	if err != nil {
		return nil, fmt.Errorf("parse executed %q: %v", executed, err)
	}
	var unapplied []relayTrx
	for _, tr := range trxs {
		if !tr.Complete || len(tr.Rows) == 0 || tr.GTID == "" {
			continue
		}
		g, err := replication.ParseMysql56GTIDSet(tr.GTID)
		if err != nil || exec_.Contains(g) {
			continue
		}
		unapplied = append(unapplied, tr)
	}
	s.R.note("relay log %s: %d events, %d transactions, %d complete unapplied (first %s, last %s)", path.Base(file), len(evs), len(trxs), len(unapplied),
		func() string {
			if len(unapplied) == 0 {
				return "-"
			}
			return unapplied[0].GTID
		}(), func() string {
			if len(unapplied) == 0 {
				return "-"
			}
			return unapplied[len(unapplied)-1].GTID
		}())
	if len(unapplied) < 2 {
		return nil, fmt.Errorf("relay log %s holds only %d complete unapplied transactions", file, len(unapplied))
	}
	i := pick(unapplied)
	target := unapplied[i]
	rows := target.Rows[0]
	offset := rows.Pos + (rows.End-rows.Pos)/2
	res := &tearResult{File: file, Offset: offset, Torn: target}
	st, _ := os.Stat(file)
	res.OldSize = st.Size()
	res.DroppedIDs = append(res.DroppedIDs, target.IDs...)
	for _, tr := range trxs {
		if tr.Start > target.Start {
			res.Dropped = append(res.Dropped, tr)
			res.DroppedIDs = append(res.DroppedIDs, tr.IDs...)
		}
	}
	if err := os.Truncate(file, offset); err != nil {
		return nil, err
	}
	// mysqlbinlog decides whether the cut is inside an event. The decoded positions cannot: with
	// binlog_transaction_compression, the rows events are inside a Transaction_payload event and
	// mysqlbinlog prints them at the payload's position, so they look like empty events.
	_, _, stderr, _ := parseRelayLog(file)
	res.Verified = strings.Contains(stderr, "truncated in the middle of event") || strings.Contains(stderr, "Could not read entry")
	s.Log.Add("fault", fmt.Sprintf("TORE relay log %s of %s at offset %d (size was %d) inside %s event [%d,%d) of %s; %d later transactions dropped; mysqlbinlog: %s",
		path.Base(file), n.Tablet.Alias, offset, res.OldSize, rows.Type, rows.Pos, rows.End, target.GTID, len(res.Dropped), strings.TrimSpace(stderr)))
	s.R.outcome("tear: %s cut at %d/%d inside %s [%d,%d) of trx %s (%d/%d unapplied); %d transactions after it dropped; verified torn=%v (mysqlbinlog: %q)",
		path.Base(file), offset, res.OldSize, rows.Type, rows.Pos, rows.End, target.GTID, i+1, len(unapplied), len(res.Dropped), res.Verified, strings.TrimSpace(stderr))
	if !res.Verified {
		s.R.violation("HARNESS: tear not verified (mysqlbinlog stderr %q)", stderr)
	}
	return res, nil
}

// waitApplierStuck waits until the node's applier stopped with an error and reports it.
func (s *Scenario) waitApplierStuck(n *Node, since time.Time, timeout time.Duration) (errno, errmsg string, ok bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rs, err := n.replicaStatus()
		if err == nil && rs != nil && rs["Replica_SQL_Running"] == "No" && rs["Last_SQL_Error"] != "" {
			errno, errmsg = rs["Last_SQL_Errno"], rs["Last_SQL_Error"]
			ok = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	s.R.outcome("applier stuck=%v Last_SQL_Errno=%s Last_SQL_Error=%q", ok, errno, errmsg)
	if l := s.grepMysqlLog(n, `MY-013121|MY-010586|MY-013146|Relay log read failure|relay log|Relay log|Error reading|MY-010584|partial`, since, 12); l != "" {
		s.R.note("mysqld error log of %s: %s", n.Tablet.Alias, l)
	}
	return
}

// healedCondition returns a condition that is true once n replicates from p with both threads
// running and without errors, and has applied every GTID in want.
func (s *Scenario) healedCondition(n, p *Node, want string) func() bool {
	return func() bool {
		rs, err := n.replicaStatus()
		if err != nil || rs == nil || rs["Replica_IO_Running"] != "Yes" || rs["Replica_SQL_Running"] != "Yes" || rs["Last_SQL_Error"] != "" ||
			rs["Source_Port"] != strconv.Itoa(p.Tablet.MySQLPort) {
			return false
		}
		if want == "" {
			return true
		}
		ok, err := n.scalar("select gtid_subset(?, @@global.gtid_executed)", want)
		return err == nil && ok == "1"
	}
}

// orcLinesFor returns VTOrc log lines about node n since `since`.
func (s *Scenario) orcLinesFor(n *Node, pattern string, since time.Time, max int) []string {
	type line struct{ key, text string }
	var lines []line
	for _, m := range s.orcNodes() {
		f := fmt.Sprintf("%s/%s", s.CI.TmpDirectory, m.Orc.LogFileName)
		out, _ := exec.Command("sh", "-c", fmt.Sprintf("grep -h -E %q %s | grep -F %q | cut -c1-420", pattern, f, n.Tablet.Alias)).Output()
		for l := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
			if len(l) < 23 {
				continue
			}
			if ts, err := time.Parse("2006-01-02 15:04:05.000", l[:23]); err == nil && ts.Before(since.UTC()) {
				continue
			}
			lines = append(lines, line{key: l, text: "vtorc-" + m.Cell + ": " + l})
		}
	}
	sort.SliceStable(lines, func(i, j int) bool { return lines[i].key < lines[j].key })
	var res []string
	for _, l := range lines {
		res = append(res, l.text)
	}
	if len(res) > max {
		res = append(res[:max/2], res[len(res)-max/2:]...)
	}
	return res
}

// tabletLinesSince returns the vttablet log lines matching pattern since `since`.
func (s *Scenario) tabletLinesSince(n *Node, pattern string, since time.Time, max int) []string {
	out, _ := exec.Command("sh", "-c", fmt.Sprintf("grep -h -E %q %s | cut -c1-600", pattern, n.Tablet.VttabletProcess.ErrorLog)).Output()
	var res []string
	for l := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		if len(l) < 23 {
			continue
		}
		if ts, err := time.Parse("2006-01-02 15:04:05.000", l[:23]); err == nil && ts.Before(since.UTC()) {
			continue
		}
		res = append(res, l)
	}
	if len(res) > max {
		res = append(res[:max/2], res[len(res)-max/2:]...)
	}
	return res
}

const (
	orcFixPattern    = `Analysis: .*(FixReplica|will fix)|proceeding with recovery on|Unlocking shard ks/0 for .*(error|successful)`
	tabletRepointPat = `relay log|refusing|Errant|SetReplicationSource:|exec (STOP|START|CHANGE|RESET)|recoverable replication|replication applier`
)

// reportHealLogs adds the VTOrc and vttablet lines about the heal of n to the report.
func (s *Scenario) reportHealLogs(n *Node, since time.Time) {
	for _, l := range s.orcLinesFor(n, orcFixPattern, since, 16) {
		s.R.note("%s", l)
	}
	for _, l := range s.tabletLinesSince(n, tabletRepointPat, since, 24) {
		s.R.note("vttablet %s: %s", n.Tablet.Alias, l)
	}
	if l := s.grepMysqlLog(n, `MY-010597|MY-013121|MY-010596|MY-010818`, since, 8); l != "" {
		s.R.note("mysqld %s: %s", n.Tablet.Alias, l)
	}
}

// compareData stops the writers, waits for every replica to catch up and compares
// gtid_executed and CHECKSUM TABLE of the chaos table on every tablet with the primary's.
func (s *Scenario) compareData() {
	s.W.Stop()
	s.Log.Add("scenario", "writers stopped for the data comparison")
	p, probs, _ := s.WaitConverged(90 * time.Second)
	if p == nil || len(probs) > 0 {
		s.R.outcome("data comparison skipped: not converged: %v", probs)
		return
	}
	pg, _ := p.gtidExecuted()
	for _, n := range s.Nodes {
		if n != p {
			s.WaitFor(n.Tablet.Alias+" caught up", 30*time.Second, func() bool {
				ok, err := n.scalar("select gtid_subset(?, @@global.gtid_executed)", pg)
				return err == nil && ok == "1"
			})
		}
	}
	sum := func(n *Node) string {
		r, err := n.query(fmt.Sprintf("checksum table vt_%s.%s", keyspaceName, tableName))
		if err != nil || len(r) == 0 {
			return fmt.Sprintf("err=%v", err)
		}
		return r[0]["Checksum"]
	}
	ps := sum(p)
	for _, n := range s.Nodes {
		if n == p {
			continue
		}
		g, _ := n.gtidExecuted()
		c := sum(n)
		s.R.outcome("data %s vs primary %s: gtid_executed equal=%v checksum %s vs %s equal=%v", n.Tablet.Alias, p.Tablet.Alias, g == pg, c, ps, c == ps)
		if c != ps {
			s.R.violation("DATA: checksum of %s (%s) differs from primary %s (%s)", n.Tablet.Alias, c, p.Tablet.Alias, ps)
		}
	}
}

// reportIDs reports how many of ids were acked by the workload and how many of those are
// missing on the (current) primary.
func (s *Scenario) reportIDs(label string, ids []int64) {
	acked := map[int64]bool{}
	for _, r := range s.W.Records() {
		if r.Acked {
			acked[r.ID] = true
		}
	}
	var ack []int64
	for _, id := range ids {
		if acked[id] {
			ack = append(ack, id)
		}
	}
	p := s.topoPrimary()
	if p == nil {
		s.R.outcome("%s: %d rows, %d acked; no primary", label, len(ids), len(ack))
		return
	}
	have, err := p.ids()
	if err != nil {
		s.R.outcome("%s: cannot read primary: %v", label, err)
		return
	}
	var lost []int64
	for _, id := range ack {
		if _, ok := have[id]; !ok {
			lost = append(lost, id)
		}
	}
	s.R.outcome("%s: %d rows cut from R1's relay log, %d of them acked to the client, %d of those LOST on primary %s %v", label, len(ids), len(ack), len(lost), p.Tablet.Alias, firstN(lost, 10))
}

// s13Opts configures an S13 torn relay log scenario.
type s13Opts struct {
	// pickAcked cuts into the middle of the unapplied backlog (complete, acknowledged
	// transactions); otherwise the cut lands in the last complete transaction.
	pickAcked bool
	// confirmStuck disables VTOrc recoveries while R1's mysqld restarts, starts R1's applier by
	// hand and waits for it to fail on the tear before recoveries are enabled again. Otherwise
	// VTOrc finds R1 with both replication threads stopped (skip_replica_start) and acts first.
	confirmStuck bool
	// soleAckerPrimaryDies partitions R2 from P before the backlog builds up (R1 is the only
	// acker), and kills P's mysqld before R1 comes back.
	soleAckerPrimaryDies bool
}

func s13(t *testing.T, name string, o s13Opts) {
	useRelayLogSafeConfig(t)
	runScenario(t, name, Options{}, func(s *Scenario) {
		s.checkRelayLogConfig()
		p := s.OldPrimary
		r1, r2 := s.Replicas()[0], s.Replicas()[1]
		s.R.outcome("P=%s R1=%s R2=%s opts=%+v", p.Tablet.Alias, r1.Tablet.Alias, r2.Tablet.Alias, o)
		if o.soleAckerPrimaryDies {
			s.Partition(p.Group, r2.Group)
			s.Sleep(3*time.Second, "R2 cut from P")
		}
		lockConn := s.lockTable(r1)
		lockAt := time.Now()
		s.Sleep(4*time.Second, "backlog of acked but unapplied transactions in R1's relay log")
		tagged := s.ackedBetween(lockAt, time.Now())
		s.R.outcome("writes acked while R1's applier was blocked: %d", len(tagged))
		_, executed, unapplied := s.relayState(r1, "before-kill")
		if unapplied == "" {
			s.R.violation("HARNESS: R1 has no unapplied relay log transactions")
		}
		if o.confirmStuck || o.soleAckerPrimaryDies {
			s.setRecoveries(false)
		}
		s.MarkFault()
		s.KillMysqld(r1, true)
		_ = lockConn.Close()
		pick := func(u []relayTrx) int { return len(u) - 1 }
		if o.pickAcked {
			pick = func(u []relayTrx) int { return len(u) / 2 }
		}
		tear, err := s.tearRelayLog(r1, executed, pick)
		if err != nil {
			s.R.violation("HARNESS: cannot tear R1's relay log: %v", err)
		}
		if o.soleAckerPrimaryDies {
			s.KillMysqld(p, true)
		}
		start := time.Now()
		if err := s.RestartMysqld(r1); err != nil {
			s.R.violation("R1 mysqld did not start: %v", err)
		}
		s.R.timing("R1 mysqld up %.1fs after start", time.Since(start).Seconds())
		s.relayState(r1, "after-restart")
		if o.confirmStuck {
			_, err := r1.db.Exec("start replica sql_thread")
			s.Log.Add("fault", fmt.Sprintf("START REPLICA SQL_THREAD on R1 err=%v", err))
			s.waitApplierStuck(r1, start, 30*time.Second)
			s.relayState(r1, "stuck")
		}
		if o.soleAckerPrimaryDies {
			s.Heal()
		}
		enabledAt := time.Now()
		if o.confirmStuck || o.soleAckerPrimaryDies {
			s.setRecoveries(true)
		}
		if !o.soleAckerPrimaryDies {
			want := ""
			if tear != nil {
				gtids := []string{tear.Torn.GTID}
				for _, tr := range tear.Dropped {
					if tr.GTID != "" {
						gtids = append(gtids, tr.GTID)
					}
				}
				want = strings.Join(gtids, ",")
			}
			_, ok := s.WaitFor("R1 healed", 120*time.Second, s.healedCondition(r1, p, want))
			s.R.outcome("R1 healed=%v %.1fs after its mysqld start, %.1fs after VTOrc recoveries were (re)enabled (replicating from P, applier running, has the cut GTIDs)",
				ok, time.Since(start).Seconds(), time.Since(enabledAt).Seconds())
			if !ok {
				s.relayState(r1, "not-healed")
			}
			s.relayState(r1, "healed")
		} else {
			d, ok := s.WaitFor("new primary in topo", 120*time.Second, s.PrimaryChanged(p))
			s.R.outcome("failover happened=%v after %.1fs; new primary %v", ok, d.Seconds(), func() string {
				if np := s.topoPrimary(); np != nil {
					return np.Tablet.Alias
				}
				return "-"
			}())
			if ok {
				s.reportLostTagged("TAGGED (acked while only P and R1's relay log had them)", tagged)
				if tear != nil {
					s.reportIDs("CUT", tear.DroppedIDs)
				}
				s.R.note("ERS log: %s", strings.ReplaceAll(s.GrepLogs(`ERS - |Recovery for DeadPrimary.* on ks/0: (Analysis|ERS)|Error running ERS`, 30), "\n", " || "))
			}
			s.Sleep(8*time.Second, "writes on the new primary while P is down")
			_ = s.RestartMysqld(p)
			s.Sleep(30*time.Second, "old primary back")
			s.reportErrantDetail("after old primary restart")
		}
		s.reportHealLogs(r1, start)
		s.Sleep(3*time.Second, "writes")
		if !o.soleAckerPrimaryDies {
			if tear != nil {
				s.reportIDs("CUT", tear.DroppedIDs)
			}
			s.reportLostTagged("TAGGED (acked while R1's applier was blocked)", tagged)
			s.compareData()
		}
	})
}

// T1: tear inside the last transaction of the backlog, P alive. R1's applier is started by hand
// and fails on the tear before VTOrc may act.
func TestS13T1TornLastTrx(t *testing.T) {
	s13(t, "S13-T1-torn-last-trx", s13Opts{confirmStuck: true})
}

// T1b: like T1, but VTOrc acts first: R1 comes back with both replication threads stopped
// (skip_replica_start) and nothing starts its applier before VTOrc's fixReplica.
func TestS13T1bTornLastTrxVTOrcFirst(t *testing.T) {
	s13(t, "S13-T1b-torn-last-trx-vtorc-first", s13Opts{})
}

// T2: tear in the middle of the backlog, cutting away complete, acknowledged transactions (what
// sync_relay_log > 1 plus a power loss can do), P alive.
func TestS13T2TornAckedTrx(t *testing.T) {
	s13(t, "S13-T2-torn-acked-trx", s13Opts{pickAcked: true, confirmStuck: true})
}

// T2b: like T2, but VTOrc acts first.
func TestS13T2bTornAckedTrxVTOrcFirst(t *testing.T) {
	s13(t, "S13-T2b-torn-acked-trx-vtorc-first", s13Opts{pickAcked: true})
}

// T3: like T2, but R2 is partitioned from P while the backlog builds up (R1 is the only acker)
// and P's mysqld is killed before R1 comes back, so VTOrc fails over.
func TestS13T3TornAckedTrxPrimaryDies(t *testing.T) {
	s13(t, "S13-T3-torn-acked-trx-primary-dies", s13Opts{pickAcked: true, confirmStuck: true, soleAckerPrimaryDies: true})
}

// T3b: like T3 without starting R1's applier by hand.
func TestS13T3bTornAckedTrxPrimaryDiesVTOrcFirst(t *testing.T) {
	s13(t, "S13-T3b-torn-acked-trx-primary-dies-vtorc-first", s13Opts{pickAcked: true, soleAckerPrimaryDies: true})
}

// T4: R1's applier stops on a data error (duplicate key): a row inserted on R1 without binary
// logging conflicts with a row written later on P. Observe VTOrc's fixReplica attempts for a
// minute, then remove the conflicting row on R1 and let replication heal.
func TestS13T4ApplierDuplicateKey(t *testing.T) {
	useRelayLogSafeConfig(t)
	runScenario(t, "S13-T4-applier-duplicate-key", Options{}, func(s *Scenario) {
		s.checkRelayLogConfig()
		p := s.OldPrimary
		r1 := s.Replicas()[0]
		s.R.outcome("P=%s R1=%s", p.Tablet.Alias, r1.Tablet.Alias)
		const conflictID = -4242
		ctx := context.Background()
		conn, err := r1.db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range []string{
			"set session sql_log_bin = 0",
			"set global super_read_only = 0",
			fmt.Sprintf("insert into vt_%s.%s (id, worker, src) values (%d, -1, 'r1-local')", keyspaceName, tableName, conflictID),
			"set global super_read_only = 1",
		} {
			if _, err := conn.ExecContext(ctx, q); err != nil {
				t.Fatalf("%s on R1: %v", q, err)
			}
		}
		_ = conn.Close()
		s.Log.Add("fault", fmt.Sprintf("inserted id=%d on R1 without binary logging", conflictID))
		s.MarkFault()
		start := time.Now()
		if _, err := s.vtgateDB.Exec(fmt.Sprintf("insert into %s (id, worker, src) values (%d, -2, @@global.server_uuid)", tableName, conflictID)); err != nil {
			t.Fatalf("conflicting insert on P: %v", err)
		}
		s.Log.Add("fault", fmt.Sprintf("inserted id=%d on P through vtgate", conflictID))
		s.waitApplierStuck(r1, start, 20*time.Second)
		s.relayState(r1, "stuck")
		// Sample R1's replication state for a minute while VTOrc acts.
		var samples []string
		prev := ""
		for range 60 {
			rs, err := r1.replicaStatus()
			cur := "?"
			if err == nil && rs != nil {
				cur = fmt.Sprintf("io=%s sql=%s errno=%s relay=%s:%s retrieved=%s", rs["Replica_IO_Running"], rs["Replica_SQL_Running"], rs["Last_SQL_Errno"],
					rs["Relay_Log_File"], rs["Relay_Log_Pos"], strings.ReplaceAll(rs["Retrieved_Gtid_Set"], "\n", ""))
			}
			if cur != prev {
				samples = append(samples, fmt.Sprintf("+%.0fs %s", time.Since(start).Seconds(), cur))
				prev = cur
			}
			time.Sleep(time.Second)
		}
		s.R.note("R1 replication state changes during 60s: %d", len(samples))
		for _, l := range firstN(samples, 30) {
			s.R.note("  %s", l)
		}
		fixes := s.orcLinesFor(r1, `will fix replica`, start, 1000)
		recov := s.orcLinesFor(r1, `Unlocking shard ks/0 for .*ReplicationStopped`, start, 1000)
		s.R.outcome("VTOrc in the first %.0fs: %d 'will fix replica' lines, %d finished ReplicationStopped recoveries on R1", time.Since(start).Seconds(), len(fixes), len(recov))
		s.reportHealLogs(r1, start)
		discards := s.tabletLinesSince(r1, `discarding the relay log`, start, 1000)
		s.R.outcome("vttablet 'discarding the relay log' lines on R1: %d", len(discards))
		conn, err = r1.db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range []string{
			"set session sql_log_bin = 0",
			"set global super_read_only = 0",
			fmt.Sprintf("delete from vt_%s.%s where id = %d", keyspaceName, tableName, conflictID),
			"set global super_read_only = 1",
		} {
			if _, err := conn.ExecContext(ctx, q); err != nil {
				t.Fatalf("%s on R1: %v", q, err)
			}
		}
		_ = conn.Close()
		healStart := time.Now()
		s.Log.Add("heal", "deleted the conflicting row on R1")
		pg, _ := p.gtidExecuted()
		_, ok := s.WaitFor("R1 healed", 90*time.Second, s.healedCondition(r1, p, pg))
		s.R.outcome("R1 healed=%v %.1fs after the conflicting row was removed", ok, time.Since(healStart).Seconds())
		s.compareData()
	})
}
