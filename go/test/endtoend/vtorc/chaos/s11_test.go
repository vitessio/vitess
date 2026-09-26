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
	"strings"
	"testing"
	"time"
)

// relayState captures a replica's received vs. applied GTID sets.
func (s *Scenario) relayState(n *Node, label string) (retrieved, executed, unapplied string) {
	rs, err := n.replicaStatus()
	if err != nil || rs == nil {
		s.Log.Add("relay", fmt.Sprintf("%s %s: no replica status (%v)", label, n.Tablet.Alias, err))
		return
	}
	retrieved = strings.ReplaceAll(rs["Retrieved_Gtid_Set"], "\n", "")
	executed = strings.ReplaceAll(rs["Executed_Gtid_Set"], "\n", "")
	unapplied, _ = n.scalar("select gtid_subtract(?, @@global.gtid_executed)", retrieved)
	unapplied = strings.ReplaceAll(unapplied, "\n", "")
	msg := fmt.Sprintf("%s %s: io=%s sql=%s Retrieved=%q Executed=%q unapplied=%q relay_log_recovery=%s",
		label, n.Tablet.Alias, rs["Replica_IO_Running"], rs["Replica_SQL_Running"], retrieved, executed, unapplied,
		n.variables("relay_log_recovery")["relay_log_recovery"])
	s.Log.Add("relay", msg)
	s.R.note("%s", msg)
	return
}

// lockTable holds LOCK TABLES ... READ on the node's chaos table in a dedicated session, which
// blocks the replication applier (it waits on the metadata lock) while the receiver keeps
// writing the relay log (and sending semi-sync ACKs).
func (s *Scenario) lockTable(n *Node) *sql.Conn {
	conn, err := n.db.Conn(context.Background())
	if err != nil {
		s.t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), fmt.Sprintf("lock tables vt_%s.%s read", keyspaceName, tableName)); err != nil {
		s.t.Fatal(err)
	}
	s.Log.Add("fault", "LOCK TABLES chaos_t READ on "+n.Tablet.Alias+" (applier blocked)")
	return conn
}

// ackedBetween returns the ids acked by writes that completed in (from, to].
func (s *Scenario) ackedBetween(from, to time.Time) []int64 {
	var ids []int64
	for _, r := range s.W.Records() {
		if r.Acked && r.End.After(from) && !r.End.After(to) {
			ids = append(ids, r.ID)
		}
	}
	return ids
}

// reportTagged reports which of the tagged acked ids exist on the (new) primary.
func (s *Scenario) reportTagged(tagged []int64) {
	p := s.topoPrimary()
	if p == nil {
		s.R.outcome("tagged: no primary")
		return
	}
	ids, err := p.ids()
	if err != nil {
		s.R.outcome("tagged: cannot read primary rows: %v", err)
		return
	}
	var lost []int64
	for _, id := range tagged {
		if _, ok := ids[id]; !ok {
			lost = append(lost, id)
		}
	}
	s.R.outcome("TAGGED acked writes (acked while only in R1's relay log): %d, LOST on new primary %s: %d (first: %v)", len(tagged), p.Tablet.Alias, len(lost), firstN(lost, 10))
}

// delayApplier makes the node's applier lag by an hour (SOURCE_DELAY) while its receiver keeps
// writing (and acking) the relay log. Unlike a table lock it does not block STOP REPLICA or a
// graceful mysqld shutdown.
func (s *Scenario) delayApplier(n *Node, seconds int) {
	for _, q := range []string{"stop replica sql_thread", fmt.Sprintf("change replication source to source_delay = %d", seconds), "start replica sql_thread"} {
		if _, err := n.db.Exec(q); err != nil {
			s.t.Fatalf("%s on %s: %v", q, n.Tablet.Alias, err)
		}
	}
	s.Log.Add("fault", fmt.Sprintf("SOURCE_DELAY=%d on %s", seconds, n.Tablet.Alias))
}

// s11 runs the relay-log-discard scenario. restart is how R1's mysqld is restarted. With
// useDelay the applier is held back with SOURCE_DELAY, otherwise with LOCK TABLES.
func s11(t *testing.T, name string, useDelay bool, restart func(s *Scenario, r1 *Node)) {
	runScenario(t, name, Options{}, func(s *Scenario) {
		r1, r2 := s.Replicas()[0], s.Replicas()[1]
		s.R.outcome("P=%s R1=%s (sole acker, applier blocked) R2=%s (not receiving)", s.OldPrimary.Tablet.Alias, r1.Tablet.Alias, r2.Tablet.Alias)
		// 1. R2 stops receiving: partition R2 <-> P.
		s.Partition(s.OldPrimary.Group, r2.Group)
		s.Sleep(3*time.Second, "R2 cut from P")
		// 2. R1's applier blocked, receiver running.
		var lockConn *sql.Conn
		if useDelay {
			s.delayApplier(r1, 3600)
		} else {
			lockConn = s.lockTable(r1)
		}
		lockAt := time.Now()
		// 3. Writes are now acked by R1 (relay log) only.
		s.Sleep(5*time.Second, "writes acked with only R1's relay log holding them")
		tagEnd := time.Now()
		tagged := s.ackedBetween(lockAt, tagEnd)
		s.R.outcome("acked writes while R1's applier was blocked: %d", len(tagged))
		_, _, unapplied := s.relayState(r1, "before-restart")
		s.relayState(r2, "before-restart")
		pg, _ := s.OldPrimary.gtidExecuted()
		s.R.note("P gtid_executed before fault: %s", pg)
		if unapplied == "" {
			s.R.violation("HARNESS: R1 has no unapplied relay-log transactions; applier block did not work")
		}
		// 4. Restart R1's mysqld and kill P.
		s.MarkFault()
		restart(s, r1)
		if lockConn != nil {
			_ = lockConn.Close()
		}
		s.relayState(r1, "after-restart")
		// 5. Restore R2's connectivity (P is dead anyway) and let VTOrc run ERS.
		s.Heal()
		d, ok := s.WaitFor("new primary in topo", 120*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover happened=%v after %.1fs", ok, d.Seconds())
		if useDelay && s.topoPrimary() != r1 {
			s.delayApplier(r1, 0)
		}
		if ok {
			s.reportTagged(tagged)
			s.R.note("ERS log: %s", strings.ReplaceAll(s.GrepLogs(`ERS - |Recovery for DeadPrimary on ks/0: (Analysis|ERS)`, 30), "\n", " || "))
		}
		s.Sleep(5*time.Second, "writes")
		_ = s.RestartMysqld(s.OldPrimary)
		s.Sleep(20*time.Second, "old primary rejoins")
	})
}

// S11: graceful mysqld restart of R1 (mysqlctl shutdown + start) right before P is killed.
func TestS11RelayLogDiscardGraceful(t *testing.T) {
	s11(t, "S11-relaylog-discard-graceful-restart", true, func(s *Scenario, r1 *Node) {
		s.Log.Add("fault", "mysqlctl shutdown R1 "+r1.Tablet.Alias)
		err := r1.Tablet.MysqlctlProcess.Stop()
		s.Log.Add("fault", fmt.Sprintf("R1 mysqld stopped err=%v", err))
		s.KillMysqld(s.OldPrimary, true)
		_ = s.RestartMysqld(r1)
	})
}

// S11k: kill -9 of R1's mysqld (then start) right before P is killed.
func TestS11kRelayLogDiscardKill9(t *testing.T) {
	s11(t, "S11k-relaylog-discard-kill9", false, func(s *Scenario, r1 *Node) {
		s.KillMysqld(r1, true)
		s.KillMysqld(s.OldPrimary, true)
		time.Sleep(time.Second)
		_ = s.RestartMysqld(r1)
	})
}

// s11b: no restart. With transactions unapplied on R1 (SOURCE_DELAY) and R2 not receiving,
// make VTOrc run fixReplica on R1 (by turning off its replica semi-sync), then kill P.
// With cutR1 the replication connection R1 -> P mysql is cut just before fixReplica runs (so
// the IO thread cannot re-download what the CHANGE REPLICATION SOURCE purges).
func s11b(t *testing.T, name string, cutR1 bool) {
	runScenario(t, name, Options{}, func(s *Scenario) {
		r1, r2 := s.Replicas()[0], s.Replicas()[1]
		s.R.outcome("P=%s R1=%s R2=%s cutR1=%v", s.OldPrimary.Tablet.Alias, r1.Tablet.Alias, r2.Tablet.Alias, cutR1)
		s.Partition(s.OldPrimary.Group, r2.Group)
		s.Sleep(3*time.Second, "R2 cut from P")
		s.delayApplier(r1, 3600)
		lockAt := time.Now()
		s.Sleep(5*time.Second, "writes acked with only R1's relay log holding them")
		tagged := s.ackedBetween(lockAt, time.Now())
		s.R.outcome("acked writes while R1's applier was delayed: %d", len(tagged))
		s.relayState(r1, "before-fixreplica")
		if cutR1 {
			if err := s.Net.BlockPorts(r1.Group, s.OldPrimary.Tablet.MySQLPort); err != nil {
				t.Fatal(err)
			}
			s.Log.Add("fault", "cut R1 -> P mysql port (replication only)")
		}
		start := time.Now()
		if _, err := r1.db.Exec("set global rpl_semi_sync_replica_enabled = 0"); err != nil {
			s.R.note("set semi-sync off failed: %v", err)
		}
		s.Log.Add("fault", "SET GLOBAL rpl_semi_sync_replica_enabled=0 on R1 (expect VTOrc fixReplica)")
		n := s.WaitForOrcLog(`Unlocking shard ks/0 for .*ReplicaSemiSyncMustBeSet on `+r1.Tablet.Alias, start, 60*time.Second)
		s.R.outcome("VTOrc ReplicaSemiSyncMustBeSet recovery finished=%v", n != nil)
		s.Sleep(2*time.Second, "let R1's IO thread reconnect if it can")
		s.relayState(r1, "after-fixreplica")
		s.MarkFault()
		s.KillMysqld(s.OldPrimary, true)
		s.Heal()
		d, ok := s.WaitFor("new primary in topo", 120*time.Second, s.PrimaryChanged(s.OldPrimary))
		s.R.outcome("failover happened=%v after %.1fs", ok, d.Seconds())
		if s.topoPrimary() != r1 {
			s.delayApplier(r1, 0)
		}
		if ok {
			s.reportTagged(tagged)
		}
		s.Sleep(5*time.Second, "writes")
		_ = s.RestartMysqld(s.OldPrimary)
		s.Sleep(20*time.Second, "old primary rejoins")
	})
}

// S11b: fixReplica on R1 while P is healthy, then P dies (P reachable during fixReplica).
func TestS11bRelayLogDiscardFixReplica(t *testing.T) {
	s11b(t, "S11b-relaylog-fixreplica-p-reachable", false)
}

// S11c: fixReplica on R1 while R1 cannot reach P's mysql, then P dies.
func TestS11cRelayLogDiscardFixReplicaCut(t *testing.T) {
	s11b(t, "S11c-relaylog-fixreplica-r1-cut", true)
}
