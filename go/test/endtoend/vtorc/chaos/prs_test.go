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
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"
)

// PlannedReparentShard scenarios. PRS demotes the primary, waits for the primary-elect to catch up,
// then promotes it and repoints the other tablets. It holds the shard lock throughout, but the lock
// is an etcd lease that nothing keeps alive: it lasts --topo-etcd-lease-ttl (30s) and only
// CheckShardLocked, at PRS's phase boundaries, renews it.

// startPRS runs PlannedReparentShard to newPrimary in the background.
func (s *Scenario) startPRS(newPrimary *Node, waitReplicasTimeout string) <-chan ersResult {
	args := []string{
		"--server", s.CI.VtctldClientProcess.Server, "PlannedReparentShard", keyspaceName + "/" + shardName,
		"--new-primary", newPrimary.Tablet.Alias, "--wait-replicas-timeout", waitReplicasTimeout,
	}
	ch := make(chan ersResult, 1)
	start := time.Now()
	s.Log.Add("prs", "START vtctldclient "+strings.Join(args[2:], " "))
	go func() {
		out, err := exec.Command(s.CI.VtctldClientProcess.Binary, args...).CombinedOutput()
		r := ersResult{out: string(out), err: err, start: start, end: time.Now()}
		s.Log.Add("prs", fmt.Sprintf("DONE after %.1fs err=%v", r.end.Sub(start).Seconds(), err))
		d := path.Join(resultsDir(), s.R.Name)
		_ = os.MkdirAll(d, 0o755)
		_ = os.WriteFile(path.Join(d, "prs-output.txt"), out, 0o644)
		ch <- r
	}()
	return ch
}

// reportPRS records the outcome of a PRS and the tablets' state right after it.
func (s *Scenario) reportPRS(r ersResult) {
	errLine := "none"
	if r.err != nil {
		lines := strings.Split(strings.TrimSpace(r.out), "\n")
		errLine = lines[len(lines)-1]
	}
	s.R.outcome("PRS returned after %.1fs, error: %s", r.end.Sub(r.start).Seconds(), errLine)
	for _, n := range s.Nodes {
		v := n.variables("super_read_only")
		s.R.note("after PRS %s: tablet record %s, super_read_only=%s", n.Tablet.Alias, s.tabletRecordType(n), v["super_read_only"])
	}
}

// tabletRecordType returns the type in n's tablet record.
func (s *Scenario) tabletRecordType(n *Node) string {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ti, err := s.Ts.GetTablet(ctx, n.Tablet.GetAlias())
	if err != nil {
		return "?"
	}
	return ti.Type.String()
}

// superReadOnly reports whether n's mysqld has super_read_only on.
func (s *Scenario) superReadOnly(n *Node) bool {
	v, err := n.scalar("select @@global.super_read_only")
	return err == nil && v == "1"
}

// setSourceDelay sets SOURCE_DELAY on a replica with only the applier stopped, so that the
// receiver keeps its relay log (a CHANGE with both threads stopped discards it).
func (s *Scenario) setSourceDelay(n *Node, seconds int) {
	for _, q := range []string{
		"stop replica sql_thread",
		fmt.Sprintf("change replication source to source_delay = %d", seconds),
		"start replica sql_thread",
	} {
		if _, err := n.db.Exec(q); err != nil {
			s.R.note("SOURCE_DELAY=%d on %s: %q failed: %v", seconds, n.Tablet.Alias, q, err)
			return
		}
	}
	s.Log.Add("fault", fmt.Sprintf("SOURCE_DELAY=%d on %s", seconds, n.Tablet.Alias))
}

// heldTx is a transaction held open on the primary through vtgate; it has inserted the row id.
type heldTx struct {
	*sql.Tx
	db *sql.DB
	id int64
}

// holdTransaction opens a transaction on the primary through vtgate, inserts a row and leaves it
// open. While it is open, DemotePrimary waits for it (up to the shutdown grace period).
func (s *Scenario) holdTransaction() *heldTx {
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%d)/%s", s.CI.VtgateProcess.MySQLServerPort, keyspaceName))
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(func() { _ = db.Close() })
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		s.t.Fatal(err)
	}
	id := s.W.next.Add(1)
	if _, err := tx.Exec(fmt.Sprintf("insert into %s (id, worker, src) values (?, -3, @@global.server_uuid)", tableName), id); err != nil {
		s.t.Fatal(err)
	}
	s.Log.Add("fault", fmt.Sprintf("transaction with id %d held open on the primary", id))
	return &heldTx{Tx: tx, db: db, id: id}
}

// orcFixedPrimary reports the VTOrc log lines in which fixPrimary undid a demotion.
func (s *Scenario) orcFixedPrimary() {
	if l := s.GrepLogs("will fix primary to read-write", 5); l != "" {
		s.R.outcome("VTOrc fixPrimary (UndoDemotePrimary) ran: %s", strings.ReplaceAll(strings.TrimSpace(l), "\n", " || "))
	}
}

// R1: PRS under write load, with nothing else going on. The baseline: how long writes stop, and
// that no acknowledged write is lost.
func TestR1PRSUnderLoad(t *testing.T) {
	runScenario(t, "R1-prs-under-load", Options{WriteProbe: true}, func(s *Scenario) {
		cand := s.Replicas()[0]
		s.MarkFault()
		r := <-s.startPRS(cand, "15s")
		s.reportPRS(r)
		s.Sleep(10*time.Second, "writes on the new primary")
	})
}

// R2: PRS's wait for the primary-elect outlives the shard lock. A client holds a transaction open
// on the primary. PRS's catch-up returns at once, then DemotePrimary stops serving and waits for
// in-flight transactions (the shutdown grace period). Meanwhile the primary-elect gets
// SOURCE_DELAY=35 and the client commits: the demoted position includes that commit, which the
// primary-elect applies 35s later. PRS renews the 30s lease only at its lock checks, the last
// one just before the demotion, so the lease expires while it waits. PRS then fails without
// UndoDemotePrimary, and the old primary stays demoted, unless VTOrc's fixPrimary, which gets the
// lock once the lease is gone, undoes the demotion first (while PRS still waits).
func TestR2PRSOutlivesShardLock(t *testing.T) {
	runScenario(t, "R2-prs-outlives-shard-lock", Options{}, func(s *Scenario) {
		cand := s.Replicas()[0]
		tx := s.holdTransaction()
		s.MarkFault()
		ch := s.startPRS(cand, "60s")
		d, ok := s.WaitFor("demotion started (writes fail)", 30*time.Second, func() bool { return s.W.failing.Load() })
		s.R.timing("writes started failing after %.1fs (seen=%v)", d.Seconds(), ok)
		s.setSourceDelay(cand, 35)
		cerr := tx.Commit()
		s.Log.Add("fault", fmt.Sprintf("held transaction committed: err=%v", cerr))
		s.R.outcome("held transaction id %d commit during the demotion: err=%v", tx.id, cerr)
		r := <-ch
		s.reportPRS(r)
		s.orcFixedPrimary()
		if s.tabletRecordType(cand) != "PRIMARY" {
			s.setSourceDelay(cand, 0)
		}
		s.Sleep(20*time.Second, "recovery")
		if p := s.topoPrimary(); p != nil {
			ids, err := p.ids()
			_, present := ids[tx.id]
			s.R.outcome("held transaction id %d on the final primary %s: %v (err=%v)", tx.id, p.Tablet.Alias, present, err)
		}
	})
}

// R2b: the primary-elect applies 35s behind (SOURCE_DELAY) and PRS runs with
// --wait-replicas-timeout 60s. Every snapshot includes heartbeat transactions, so PRS's catch-up
// itself waits about 35s, and its lock check before the demotion finds the lease expired: PRS
// fails before it changes anything. The lease, not --wait-replicas-timeout, bounds each phase.
func TestR2bPRSCatchupOutlivesShardLock(t *testing.T) {
	runScenario(t, "R2b-prs-catchup-outlives-shard-lock", Options{}, func(s *Scenario) {
		cand := s.Replicas()[0]
		s.setSourceDelay(cand, 35)
		s.MarkFault()
		r := <-s.startPRS(cand, "60s")
		s.reportPRS(r)
		if s.tabletRecordType(cand) != "PRIMARY" {
			s.setSourceDelay(cand, 0)
		}
		s.Sleep(20*time.Second, "recovery")
	})
}

// R3: the response to PRS's DemotePrimary is lost. A client holds a transaction open on the
// primary, so DemotePrimary waits for it (the shutdown grace period) after it stopped serving.
// While it waits, vtctld is cut off from the primary, and the transaction is rolled back: the
// tablet finishes demoting itself, but its response never reaches PRS, which sees DemotePrimary
// time out. PRS returns that error without UndoDemotePrimary, which it only runs when the wait for
// the primary-elect fails, so the primary stays demoted until VTOrc's fixPrimary undoes it.
func TestR3PRSDemoteResponseLost(t *testing.T) {
	runScenario(t, "R3-prs-demote-response-lost", Options{WriteProbe: true}, func(s *Scenario) {
		cand := s.Replicas()[0]
		tx := s.holdTransaction()
		ch := s.startPRS(cand, "15s")
		d, ok := s.WaitFor("demotion started (writes fail)", 30*time.Second, func() bool { return s.W.failing.Load() })
		s.R.timing("writes started failing after %.1fs (seen=%v)", d.Seconds(), ok)
		s.MarkFault()
		s.Block("infra", s.OldPrimary.Group)
		s.R.outcome("held transaction rolled back during the demotion: err=%v", tx.Rollback())
		r := <-ch
		s.reportPRS(r)
		s.Sleep(20*time.Second, "VTOrc reacts while vtctld cannot reach the old primary")
		s.orcFixedPrimary()
		s.Heal()
		s.Sleep(15*time.Second, "recovery after heal")
	})
}

// R5: a replica's vttablet restarts while PRS runs. On startup it repoints the replica to the
// shard record's primary without the shard lock (initializeReplication), which may be the primary
// PRS is replacing.
func TestR5PRSReplicaRestart(t *testing.T) {
	runScenario(t, "R5-prs-replica-vttablet-restart", Options{}, func(s *Scenario) {
		cand, other := s.Replicas()[0], s.Replicas()[1]
		ch := s.startPRS(cand, "15s")
		_, ok := s.WaitFor("old primary demoted", 40*time.Second, func() bool { return s.superReadOnly(s.OldPrimary) })
		if ok {
			s.MarkFault()
			if err := s.RestartVttablet(other); err != nil {
				s.R.note("vttablet restart of %s: %v", other.Tablet.Alias, err)
			}
		}
		r := <-ch
		s.reportPRS(r)
		s.Sleep(20*time.Second, "recovery")
	})
}
