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
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/topo"
)

// S12: a replica's vttablet restarts in the middle of an EmergencyReparentShard. On startup,
// initializeReplication reads the (still old) shard record primary without a shard lock and
// repoints the replica at it with semi-sync ACKs enabled, although ERS already stopped that
// replica's IO thread to revoke the old primary's ackers.
//
// Layout: P = zone1-100 (made primary with PRS if needed), R1 = zone2-200, R2 = zone3-300,
// optionally R3 = zone2-400. VTOrc recoveries are disabled; the only failover is a manual ERS
// through vtctld. P is unreachable from vtctld and the VTOrcs only; vtgate and the tablets can
// still reach it.

// Unblock removes a rule added by BlockPorts.
func (n *NetFault) Unblock(from string, ports ...int) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	f, ok := n.groups[from]
	if !ok {
		return fmt.Errorf("unknown group %s", from)
	}
	var ps []string
	for _, p := range ports {
		ps = append(ps, strconv.Itoa(p))
	}
	if err := n.run("-D", chaosDropChain, "-p", "tcp", "-m", "connmark", "--mark", strconv.Itoa(f.ID),
		"-m", "multiport", "--ports", strings.Join(ps, ","), "-j", "DROP"); err != nil {
		return err
	}
	want := fmt.Sprintf("%s -> ports %v", from, ports)
	for i, a := range n.active {
		if a == want {
			n.active = append(n.active[:i], n.active[i+1:]...)
			break
		}
	}
	return nil
}

func (s *Scenario) setRecoveries(enable bool) {
	api := "/api/disable-global-recoveries"
	if enable {
		api = "/api/enable-global-recoveries"
	}
	for _, n := range s.orcNodes() {
		n.Orc.MakeAPICallRetry(s.t, api)
	}
	s.Log.Add("setup", fmt.Sprintf("VTOrc global recoveries enabled=%v", enable))
}

func (s *Scenario) node(alias string) *Node {
	for _, n := range s.Nodes {
		if n.Tablet.Alias == alias {
			return n
		}
	}
	s.t.Fatalf("no tablet %s", alias)
	return nil
}

// ensurePrimary makes n the primary with PRS (if it is not already) and waits for health.
func (s *Scenario) ensurePrimary(n *Node) {
	if s.topoPrimary() != n {
		out, err := s.CI.VtctldClientProcess.ExecuteCommandWithOutput("PlannedReparentShard", keyspaceName+"/"+shardName, "--new-primary", n.Tablet.Alias)
		if err != nil {
			s.t.Fatalf("PRS to %s: %v\n%s", n.Tablet.Alias, err, out)
		}
		s.Log.Add("setup", "PRS to "+n.Tablet.Alias)
	}
	s.WaitHealthy(90 * time.Second)
	s.OldPrimary = n
}

// cutPrimaryFromERSCaller makes p unreachable from vtctld (infra group) and every VTOrc.
func (s *Scenario) cutPrimaryFromERSCaller(p *Node) {
	s.Block("infra", p.Group)
	for _, n := range s.Nodes[:len(cells)] {
		s.Block(n.OrcGroup, p.Group)
	}
}

type ersResult struct {
	out        string
	err        error
	start, end time.Time
}

func (s *Scenario) startERS(extra ...string) <-chan ersResult {
	args := append([]string{
		"--server", s.CI.VtctldClientProcess.Server, "EmergencyReparentShard", keyspaceName + "/" + shardName,
		"--wait-replicas-timeout", "20s",
	}, extra...)
	ch := make(chan ersResult, 1)
	start := time.Now()
	s.Log.Add("ers", "START vtctldclient "+strings.Join(args[2:], " "))
	go func() {
		out, err := exec.Command(s.CI.VtctldClientProcess.Binary, args...).CombinedOutput()
		r := ersResult{out: string(out), err: err, start: start, end: time.Now()}
		s.Log.Add("ers", fmt.Sprintf("DONE after %.1fs err=%v", r.end.Sub(start).Seconds(), err))
		d := path.Join(resultsDir(), s.R.Name)
		_ = os.MkdirAll(d, 0o755)
		_ = os.WriteFile(path.Join(d, "ers-output.txt"), out, 0o644)
		ch <- r
	}()
	return ch
}

func (s *Scenario) ioRunning(n *Node) string {
	rs, err := n.replicaStatus()
	if err != nil || rs == nil {
		return "?"
	}
	return rs["Replica_IO_Running"]
}

// replicatingFrom reports whether n's IO thread is connected to src.
func (s *Scenario) replicatingFrom(n, src *Node) bool {
	rs, err := n.replicaStatus()
	return err == nil && rs != nil && rs["Source_Port"] == strconv.Itoa(src.Tablet.MySQLPort) && rs["Replica_IO_Running"] == "Yes"
}

func (s *Scenario) semiSync(p *Node) string {
	st := p.status("Rpl_semi_sync_source_%")
	return fmt.Sprintf("clients=%s yes_tx=%s no_tx=%s status=%s wait_sessions=%s", st["Rpl_semi_sync_source_clients"], st["Rpl_semi_sync_source_yes_tx"],
		st["Rpl_semi_sync_source_no_tx"], st["Rpl_semi_sync_source_status"], st["Rpl_semi_sync_source_wait_sessions"])
}

// restartVttablet kill -9's n's vttablet and starts it again with the same flags, keeping a
// copy of the previous log (Setup truncates it). The harness starts vttablets with
// --restore-from-backup, and with that flag a restart of a tablet that has data takes the
// restore path, which leaves replication alone and never calls initializeReplication. Unless
// S12_RESTORE_FROM_BACKUP=1, the restart drops that flag (a vttablet deployed without
// --restore-from-backup/--restore-with-clone), so startup runs initializeReplication.
func (s *Scenario) restartVttablet(n *Node) {
	vp := n.Tablet.VttabletProcess
	vp.SupportsBackup = os.Getenv("S12_RESTORE_FROM_BACKUP") == "1"
	s.R.note("restarting %s vttablet with --restore-from-backup=%v", n.Tablet.Alias, vp.SupportsBackup)
	if vp.ErrorLog != "" {
		b, _ := os.ReadFile(vp.ErrorLog)
		_ = os.WriteFile(strings.TrimSuffix(vp.ErrorLog, ".txt")+"-before-restart.txt", b, 0o644)
	}
	_ = vp.Kill()
	s.Log.Add("fault", "kill -9 vttablet of "+n.Tablet.Alias)
	vp.ServingStatus = ""
	err := vp.Setup()
	s.Log.Add("fault", fmt.Sprintf("vttablet of %s started again err=%v", n.Tablet.Alias, err))
}

func (s *Scenario) grepTabletLog(n *Node, pattern string, max int) string {
	out, _ := exec.Command("sh", "-c", fmt.Sprintf("grep -h -E %q %s | cut -c1-400 | head -n %d", pattern, n.Tablet.VttabletProcess.ErrorLog, max)).CombinedOutput()
	return strings.TrimSpace(string(out))
}

func (s *Scenario) grepMysqlLog(n *Node, pattern string, since time.Time, max int) string {
	out, _ := exec.Command("sh", "-c", fmt.Sprintf("grep -h -E %q %s/error.log | tail -n %d", pattern, n.DataDir(), max)).CombinedOutput()
	var keep []string
	for l := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		if len(l) >= 19 {
			if ts, err := time.Parse("2006-01-02T15:04:05", l[:19]); err == nil && ts.Before(since.UTC().Add(-time.Second)) {
				continue
			}
		}
		if l != "" {
			keep = append(keep, l)
		}
	}
	return strings.Join(keep, " || ")
}

// reportAckedSince reports the writes acked after `since` and which of them are missing on the
// final primary (after the invariant checks ran, this is the converged primary).
func (s *Scenario) reportAckedSince(label string, since, until time.Time) []int64 {
	ids := s.ackedBetween(since, until)
	s.R.outcome("%s: %d writes acked in [%s, %s]", label, len(ids), since.Format("15:04:05.000"), until.Format("15:04:05.000"))
	return ids
}

func (s *Scenario) reportLostTagged(label string, tagged []int64) {
	p := s.topoPrimary()
	if p == nil {
		s.R.outcome("%s: no primary", label)
		return
	}
	ids, err := p.ids()
	if err != nil {
		s.R.outcome("%s: cannot read primary rows: %v", label, err)
		return
	}
	var lost []int64
	for _, id := range tagged {
		if _, ok := ids[id]; !ok {
			lost = append(lost, id)
		}
	}
	s.R.outcome("%s: %d tagged acked writes, %d LOST on primary %s: %v", label, len(tagged), len(lost), p.Tablet.Alias, firstN(lost, 30))
}

func (s *Scenario) waitERS(ch <-chan ersResult, timeout time.Duration) (ersResult, bool) {
	select {
	case r := <-ch:
		return r, true
	case <-time.After(timeout):
		return ersResult{}, false
	}
}

func (s *Scenario) reportERS(r ersResult) {
	lines := strings.Split(strings.TrimSpace(r.out), "\n")
	var keep []string
	for _, l := range lines {
		if strings.Contains(l, "rror") || strings.Contains(l, "intermediate source") || strings.Contains(l, "promotion") ||
			strings.Contains(l, "rrant") || strings.Contains(l, "failed") || strings.Contains(l, "acker") || strings.Contains(l, "new primary") {
			keep = append(keep, strings.TrimSpace(l))
		}
	}
	s.R.outcome("ERS took %.1fs, err=%v", r.end.Sub(r.start).Seconds(), r.err)
	out, _ := exec.Command("sh", "-c", fmt.Sprintf("grep -h -E %q %s/vtctld-stderr.txt | grep -E ' (WRN|ERR) |syslogger|intermediate|candidate|promot' | cut -c1-450 | tail -n 30",
		"EmergencyReparent|reparent|rrant|acker|intermediate source|candidate|promot", s.CI.TmpDirectory)).CombinedOutput()
	for l := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		if len(l) >= 23 {
			if ts, err := time.Parse("2006-01-02 15:04:05.000", l[:23]); err == nil && ts.Before(r.start.UTC().Add(-time.Second)) {
				continue
			}
		}
		if l != "" {
			s.R.note("vtctld: %s", l)
		}
	}
	for _, l := range firstN(keep, 25) {
		s.R.note("ERS: %s", l[:min(len(l), 500)])
	}
}

// afterERS heals everything, re-enables VTOrc and lets the cluster settle; the caller's
// runScenario then stops the writers and checks the invariants.
func (s *Scenario) afterERS(extra func()) {
	s.Heal()
	for _, n := range s.Nodes {
		signalGroup(n.Group, syscall.SIGCONT)
	}
	if extra != nil {
		extra()
	}
	// Let VTOrc rediscover P before recoveries are back on (a stale "unreachable" analysis
	// would otherwise trigger an unrelated DeadPrimary ERS), and restart IO threads that an
	// aborted ERS left stopped on replicas still pointing at the topo primary.
	s.Sleep(12*time.Second, "healed; VTOrc rediscovery")
	if p := s.topoPrimary(); p != nil {
		for _, n := range s.Nodes {
			if n == p {
				continue
			}
			rs, err := n.replicaStatus()
			if err == nil && rs != nil && rs["Source_Port"] == strconv.Itoa(p.Tablet.MySQLPort) && rs["Replica_IO_Running"] == "No" {
				_, err := n.db.Exec("start replica")
				s.Log.Add("heal", fmt.Sprintf("START REPLICA on %s (IO stopped, source is topo primary) err=%v", n.Tablet.Alias, err))
			}
		}
	}
	s.setRecoveries(true)
	s.Sleep(25*time.Second, "VTOrc recoveries on")
}

// s12Restart implements variants (a) and (c): R1 lags in receipt (its IO link to P is cut) so
// ERS only waits on the leading candidate(s), whose appliers are held with LOCK TABLES. While
// ERS waits, R1's vttablet is restarted and repoints R1 to P.
func s12Restart(t *testing.T, name string, opts Options) {
	runScenario(t, name, opts, func(s *Scenario) {
		p := s.node("zone1-0000000100")
		r1 := s.node("zone2-0000000200")
		s.ensurePrimary(p)
		s.setRecoveries(false)
		var held []*Node
		for _, n := range s.Nodes {
			if n != p && n != r1 {
				held = append(held, n)
			}
		}
		s.R.outcome("P=%s R1=%s (restarted) held=%v", p.Tablet.Alias, r1.Tablet.Alias, aliases(held))
		s.Sleep(3*time.Second, "baseline")

		// R1 falls behind in receipt, the others keep receiving but their appliers stop.
		var locks []*sql.Conn
		for _, n := range held {
			locks = append(locks, s.lockTable(n))
		}
		if err := s.Net.BlockPorts(r1.Group, p.Tablet.MySQLPort); err != nil {
			t.Fatal(err)
		}
		s.Log.Add("fault", "cut R1 -> P mysql (R1 stops receiving)")
		s.Sleep(3*time.Second, "R1 behind; held tablets ack")
		s.cutPrimaryFromERSCaller(p)
		s.MarkFault()
		ers := s.startERS()

		// Wait until ERS stopped the IO threads of all replicas (P's ackers revoked).
		_, ok := s.WaitFor("replica IO threads stopped by ERS", 30*time.Second, func() bool {
			for _, n := range s.Nodes {
				if n != p && s.ioRunning(n) != "No" {
					return false
				}
			}
			return true
		})
		if !ok {
			s.R.violation("HARNESS: ERS did not stop all replica IO threads")
		}
		revokedAt := time.Now()
		s.R.note("P semi-sync after revocation: %s", s.semiSync(p))
		for _, n := range s.Nodes {
			if n != p {
				s.relayState(n, "after-ERS-stop")
			}
		}

		// Restart R1's vttablet with R1 -> P reachable again.
		if err := s.Net.Unblock(r1.Group, p.Tablet.MySQLPort); err != nil {
			t.Fatal(err)
		}
		s.Log.Add("heal", "R1 -> P mysql reachable again")
		restartAt := time.Now()
		s.restartVttablet(r1)
		d, ok := s.WaitFor("R1 replicating from P after vttablet restart", 15*time.Second, func() bool { return s.replicatingFrom(r1, p) })
		s.R.outcome("R1 repointed to P by its restarted vttablet: %v (after %.1fs)", ok, d.Seconds())
		yes0 := s.semiSync(p)
		s.Sleep(5*time.Second, "R1 acking P while ERS waits")
		s.R.note("P semi-sync: 5s apart: [%s] -> [%s]", yes0, s.semiSync(p))
		s.relayState(r1, "R1-after-restart")
		reconnectedAckedUntil := time.Now()

		// Let ERS continue.
		for _, c := range locks {
			// Close alone would return the session, and its table lock, to the pool.
			if _, err := c.ExecContext(t.Context(), "unlock tables"); err != nil {
				s.R.violation("HARNESS: unlock tables: %v", err)
			}
			_ = c.Close()
		}
		s.Log.Add("heal", "released applier locks")
		r, done := s.waitERS(ers, 240*time.Second)
		if !done {
			s.R.violation("HARNESS: ERS did not finish in 240s")
		} else {
			s.reportERS(r)
		}
		s.R.outcome("topo primary right after ERS: %v; vttablet types: %s", aliasOf(s.topoPrimary()), s.types())
		s.R.note("R1 vttablet log: %s", strings.ReplaceAll(s.grepTabletLog(r1, "durability policy|rrant|SetReplicationSource|replication source|initializeReplication|cannot start replication", 20), "\n", " || "))
		s.R.note("P mysqld log (dump threads since fault): %s", s.grepMysqlLog(p, "binlog_dump|semi-sync", s.Fault, 12))
		tagged := s.reportAckedSince("acked after revocation", revokedAt, time.Now())
		s.R.outcome("acked between R1 restart and lock release: %d", len(s.ackedBetween(restartAt, reconnectedAckedUntil)))
		s.Sleep(5*time.Second, "writes after ERS")
		s.afterERS(nil)
		s.reportErrantDetail("after settle")
		s.reportLostTagged("acked after revocation (before invariant checks)", tagged)
		s.W.Stop()
		s.reportLostTagged("acked after revocation (writers stopped)", tagged)
	})
}

func aliases(ns []*Node) []string {
	var r []string
	for _, n := range ns {
		r = append(r, n.Tablet.Alias)
	}
	return r
}

func (s *Scenario) types() string {
	var r []string
	for _, n := range s.Nodes {
		r = append(r, n.Tablet.Alias+"="+s.tabletTypeHTTP(n))
	}
	return strings.Join(r, " ")
}

// S12a: 3 tablets; ERS elects R2 (the only candidate it waits on) while R1 got repointed to P.
func TestS12aERSReplicaRestartToOldPrimary(t *testing.T) {
	s12Restart(t, "S12a-ers-replica-restart-3tablets", Options{})
}

// S12c: 4 tablets (extra REPLICA zone2-400); ERS can get its acker quorum without R1.
func TestS12cERSReplicaRestartFourTablets(t *testing.T) {
	s12Restart(t, "S12c-ers-replica-restart-4tablets", Options{ExtraTabletCells: []string{"zone2"}})
}

// s12b: ERS promotes R1 itself. R2 lags in receipt (its IO link to P is cut) and its vttablet is
// SIGSTOPed so ERS's stop-replication phase waits for it; meanwhile R1's vttablet restarts and
// repoints R1 to P, acking P's writes. With lagApplier R1's applier is held back with
// SOURCE_DELAY after the restart, so what R1 ACKs is only in its relay log.
func s12b(t *testing.T, name string, lagApplier bool) {
	runScenario(t, name, Options{}, func(s *Scenario) {
		p := s.node("zone1-0000000100")
		r1 := s.node("zone2-0000000200")
		r2 := s.node("zone3-0000000300")
		s.ensurePrimary(p)
		s.setRecoveries(false)
		s.R.outcome("P=%s R1=%s (restarted, promoted) R2=%s (behind, vttablet stopped during ERS stop phase) lagApplier=%v", p.Tablet.Alias, r1.Tablet.Alias, r2.Tablet.Alias, lagApplier)
		s.Sleep(3*time.Second, "baseline")
		if err := s.Net.BlockPorts(r2.Group, p.Tablet.MySQLPort); err != nil {
			t.Fatal(err)
		}
		s.Log.Add("fault", "cut R2 -> P mysql (R2 stops receiving)")
		s.Sleep(3*time.Second, "R2 behind; R1 acks")
		pids := signalGroup(r2.Group, syscall.SIGSTOP, "vttablet")
		s.Log.Add("fault", fmt.Sprintf("SIGSTOP vttablet of R2 pids=%v", pids))
		s.cutPrimaryFromERSCaller(p)
		s.MarkFault()
		ers := s.startERS("--new-primary", r1.Tablet.Alias)
		_, ok := s.WaitFor("R1 IO thread stopped by ERS", 10*time.Second, func() bool { return s.ioRunning(r1) == "No" })
		if !ok {
			s.R.violation("HARNESS: ERS did not stop R1's IO thread")
		}
		revokedAt := time.Now()
		s.relayState(r1, "after-ERS-stop")
		s.R.note("P semi-sync after revocation: %s", s.semiSync(p))
		restartAt := time.Now()
		s.restartVttablet(r1)
		d, ok := s.WaitFor("R1 replicating from P after vttablet restart", 10*time.Second, func() bool { return s.replicatingFrom(r1, p) })
		s.R.outcome("R1 repointed to P by its restarted vttablet: %v (after %.1fs)", ok, d.Seconds())
		if lagApplier {
			s.delayApplier(r1, 3600)
		}
		// Keep R2's stop RPC pending, but finish well within ERS's 15s stop-replication timeout.
		yes0 := s.semiSync(p)
		for time.Since(s.Fault) < 11*time.Second {
			time.Sleep(100 * time.Millisecond)
		}
		s.R.note("P semi-sync while R1 was acking: [%s] -> [%s]", yes0, s.semiSync(p))
		s.relayState(r1, "R1-before-R2-resume")
		pids = signalGroup(r2.Group, syscall.SIGCONT, "vttablet")
		s.Log.Add("heal", fmt.Sprintf("SIGCONT vttablet of R2 pids=%v (%.1fs after ERS start)", pids, time.Since(s.Fault).Seconds()))
		r, done := s.waitERS(ers, 240*time.Second)
		if !done {
			s.R.violation("HARNESS: ERS did not finish in 240s")
		} else {
			s.reportERS(r)
		}
		s.R.outcome("topo primary right after ERS: %v; vttablet types: %s", aliasOf(s.topoPrimary()), s.types())
		s.R.note("R1 vttablet log: %s", strings.ReplaceAll(s.grepTabletLog(r1, "durability policy|rrant|PromoteReplica|replication source|cannot start replication", 20), "\n", " || "))
		s.R.note("P mysqld log (dump threads since fault): %s", s.grepMysqlLog(p, "binlog_dump|semi-sync", s.Fault, 12))
		tagged := s.reportAckedSince("acked after revocation", revokedAt, time.Now())
		s.R.outcome("acked after R1 restart until ERS end: %d", len(s.ackedBetween(restartAt, r.end)))
		s.Sleep(5*time.Second, "writes after ERS")
		s.afterERS(nil)
		s.reportErrantDetail("after settle")
		s.reportLostTagged("acked after revocation (before invariant checks)", tagged)
		s.W.Stop()
		s.reportLostTagged("acked after revocation (writers stopped)", tagged)
	})
}

// S12b: ERS promotes the restarted replica R1; R1's applier keeps up.
func TestS12bERSPromotesRestartedReplica(t *testing.T) {
	s12b(t, "S12b-ers-promotes-restarted-replica", false)
}

// S12b2: as S12b but R1's applier lags (SOURCE_DELAY) after the restart.
func TestS12b2ERSPromotesRestartedReplicaLaggingApplier(t *testing.T) {
	s12b(t, "S12b2-ers-promotes-restarted-replica-lagging", true)
}

// s12d covers the window after a successful ERS but before the new primary's shard_sync has
// written the shard record. That window cannot be widened by cutting the new primary from the
// global topo (its PromoteReplica then fails: the tablet record update reads CellInfo from the
// global topo), so it is emulated: P's vttablet is cut from the global topo (so it does not
// learn about the new primary and self-demote), ERS promotes R2, then R2 is cut from the global
// topo and the harness writes the pre-ERS shard record (P and P's term) back. R1's vttablet
// then restarts and reads that stale record. With lagR1, R1's applier runs with SOURCE_DELAY,
// so R2's reparent journal entry is in R1's relay log but not yet executed.
func s12d(t *testing.T, name string, lagR1 bool) {
	runScenario(t, name, Options{}, func(s *Scenario) {
		p := s.node("zone1-0000000100")
		r1 := s.node("zone2-0000000200")
		r2 := s.node("zone3-0000000300")
		s.ensurePrimary(p)
		s.setRecoveries(false)
		s.R.outcome("P=%s R1=%s (restarted) R2=%s (ERS target) lagR1=%v", p.Tablet.Alias, r1.Tablet.Alias, r2.Tablet.Alias, lagR1)
		s.Sleep(3*time.Second, "baseline")
		if lagR1 {
			s.delayApplier(r1, 3600)
			s.Sleep(2*time.Second, "R1 applier lagging")
		}
		ctx := t.Context()
		before, err := s.Ts.GetShard(ctx, keyspaceName, shardName)
		if err != nil {
			t.Fatal(err)
		}
		s.Block(p.Group, "infra") // P's vttablet cannot see the shard record change
		s.cutPrimaryFromERSCaller(p)
		s.MarkFault()
		r, done := s.waitERS(s.startERS("--new-primary", r2.Tablet.Alias), 240*time.Second)
		if !done {
			s.R.violation("HARNESS: ERS did not finish in 240s")
		} else {
			s.reportERS(r)
		}
		s.WaitFor("shard record names R2", 20*time.Second, func() bool { return s.topoPrimary() == r2 })
		s.R.outcome("after ERS: shard record primary=%v; vttablet types: %s; R1 replicating from R2=%v", aliasOf(s.topoPrimary()), s.types(), s.replicatingFrom(r1, r2))
		s.Sleep(3*time.Second, "writes on R2")
		s.relayState(r1, "R1-before-restart")
		g, _ := r1.gtidExecuted()
		s.R.note("R1 gtid_executed before restart: %s", g)
		// Emulate the window: R2 can no longer update the shard record, which still names P.
		s.Block(r2.Group, "infra")
		_, err = s.Ts.UpdateShardFields(ctx, keyspaceName, shardName, func(si *topo.ShardInfo) error {
			si.PrimaryAlias = before.PrimaryAlias
			si.PrimaryTermStartTime = before.PrimaryTermStartTime
			return nil
		})
		s.Log.Add("fault", fmt.Sprintf("shard record rewritten to pre-ERS value (primary %s) err=%v", aliasOf(s.nodeByAlias(before.PrimaryAlias)), err))
		restartAt := time.Now()
		s.restartVttablet(r1)
		s.Sleep(10*time.Second, "R1 vttablet startup")
		rs, _ := r1.replicaStatus()
		s.R.outcome("after R1 restart: shard record primary=%v; R1 source port=%s io=%s (P=%d R2=%d); R1 vttablet=%s", aliasOf(s.topoPrimary()),
			rs["Source_Port"], rs["Replica_IO_Running"], p.Tablet.MySQLPort, r2.Tablet.MySQLPort, s.tabletTypeHTTP(r1))
		s.R.note("R1 vttablet log: %s", strings.ReplaceAll(s.grepTabletLog(r1, "durability policy|rrant|replication source|cannot start replication|failed|Exit|exit", 20), "\n", " || "))
		s.R.note("P semi-sync: %s; R2 semi-sync: %s", s.semiSync(p), s.semiSync(r2))
		s.relayState(r1, "R1-after-restart")
		tagged := s.reportAckedSince("acked after R1 restart", restartAt, time.Now())
		s.afterERS(func() {
			// What R2's shard_sync would have written.
			_, err := s.Ts.UpdateShardFields(ctx, keyspaceName, shardName, func(si *topo.ShardInfo) error {
				ti, err := s.Ts.GetTablet(ctx, r2.Tablet.GetAlias())
				if err != nil {
					return err
				}
				si.PrimaryAlias = r2.Tablet.GetAlias()
				si.PrimaryTermStartTime = ti.PrimaryTermStartTime
				return nil
			})
			s.Log.Add("heal", fmt.Sprintf("shard record set back to R2 err=%v", err))
			if lagR1 {
				s.delayApplier(r1, 0)
			}
			if s.tabletTypeHTTP(r1) == "DOWN" {
				s.restartVttablet(r1)
			}
		})
		s.reportLostTagged("acked after R1 restart", tagged)
	})
}

// S12d: stale shard record after ERS; R1's applier is caught up.
func TestS12dPostERSStaleShardRecord(t *testing.T) {
	s12d(t, "S12d-post-ers-stale-shard-record", false)
}

// S12d2: as S12d, but R1's applier lags so the new primary's journal entry is not executed yet.
func TestS12d2PostERSStaleShardRecordLaggingApplier(t *testing.T) {
	s12d(t, "S12d2-post-ers-stale-shard-record-lagging", true)
}

// reportErrantDetail reports, for every tablet other than the topo primary, its errant GTIDs
// (not on the primary), which tables those transactions touched (from its binlogs), whether it
// is replicating from the primary anyway, and its super_read_only.
func (s *Scenario) reportErrantDetail(label string) {
	p := s.topoPrimary()
	if p == nil {
		s.R.outcome("%s: no topo primary", label)
		return
	}
	pg, err := p.gtidExecuted()
	if err != nil {
		s.R.outcome("%s: cannot read primary gtid_executed: %v", label, err)
		return
	}
	hb, hbErr := p.scalar("select count(*) from _vt.heartbeat")
	s.R.note("%s: primary %s _vt.heartbeat rows=%s err=%v (vttablet heartbeat env=%q)", label, p.Tablet.Alias, hb, hbErr, os.Getenv("CHAOS_VTTABLET_HEARTBEAT"))
	for _, n := range s.Nodes {
		if n == p {
			continue
		}
		errant, err := n.scalar("select gtid_subtract(@@global.gtid_executed, ?)", pg)
		errant = strings.ReplaceAll(errant, "\n", "")
		sro, _ := n.scalar("select @@global.super_read_only")
		rs, _ := n.replicaStatus()
		repl := "no replica status"
		if rs != nil {
			repl = fmt.Sprintf("source_port=%s (primary %d) io=%s sql=%s io_err=%q", rs["Source_Port"], p.Tablet.MySQLPort, rs["Replica_IO_Running"], rs["Replica_SQL_Running"], rs["Last_IO_Error"])
		}
		tables := ""
		count := 0
		if errant != "" {
			for part := range strings.SplitSeq(errant, ",") {
				_, rng, _ := strings.Cut(part, ":")
				for r := range strings.SplitSeq(rng, ":") {
					a, b, ok := strings.Cut(r, "-")
					lo, _ := strconv.Atoi(a)
					hi := lo
					if ok {
						hi, _ = strconv.Atoi(b)
					}
					count += hi - lo + 1
				}
			}
			base, _ := n.scalar("select @@global.log_bin_basename")
			script := fmt.Sprintf(`mysqlbinlog --include-gtids=%q -v --base64-output=DECODE-ROWS %s.[0-9]* 2>&1 | grep -oiE '(Table_map: [^ ]+|^(truncate|insert into|update|delete from) +[^ (]+)' | sed -E 's/Table_map: //' | sort | uniq -c | sort -rn | head -10 | tr '\n' ';'`, errant, base)
			out, _ := exec.Command("sh", "-c", script).CombinedOutput()
			tables = strings.TrimSpace(string(out))
		}
		s.R.outcome("%s: %s errant=%q (%d trx) err=%v tables=[%s] super_read_only=%s %s", label, n.Tablet.Alias, errant, count, err, tables, sro, repl)
	}
}

// S3hb: like S3 (isolate the primary), with the client workload stopped before the fault so
// errant GTIDs on the old primary can only come from Vitess-internal writes. Run it with
// CHAOS_VTTABLET_HEARTBEAT=1 to have vttablet replication heartbeats on.
func TestS3hbIsolatePrimaryNoWorkload(t *testing.T) {
	runScenario(t, "S3hb-isolate-primary-no-workload", Options{}, func(s *Scenario) {
		s.W.Stop()
		s.Log.Add("scenario", "client writers stopped before the fault")
		s.Sleep(3*time.Second, "idle")
		s.MarkFault()
		s.Isolate(s.OldPrimary.Group)
		d, ok := s.WaitFor("new primary in topo", 120*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover happened=%v after %.1fs", ok, d.Seconds())
		s.Sleep(10*time.Second, "old primary still isolated")
		s.Heal()
		s.Sleep(40*time.Second, "let old primary rejoin")
		s.reportErrantDetail("after rejoin")
	})
}
