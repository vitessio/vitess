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

package grouprepl

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// failoverBenchEnv enables TestUnplannedFailoverTimes, which takes several minutes, and sets
// the number of trials per mode.
const failoverBenchEnv = "VT_UNPLANNED_FAILOVER_TRIALS"

// ackedWrite is an insert that vtgate acknowledged.
type ackedWrite struct {
	id              uint64
	start, finished time.Time
}

// ackingWriter inserts rows through vtgate and records every acknowledged insert.
type ackingWriter struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	acked  []ackedWrite
}

func startAckingWriter(t *testing.T, tc *testCluster) *ackingWriter {
	ctx, cancel := context.WithCancel(t.Context())
	w := &ackingWriter{cancel: cancel}
	params := mysql.ConnParams{Host: tc.Hostname, Port: tc.VtgateMySQLPort}
	w.wg.Go(func() {
		var conn *mysql.Conn
		for ctx.Err() == nil {
			if conn == nil {
				var err error
				if conn, err = mysql.Connect(ctx, &params); err != nil {
					time.Sleep(20 * time.Millisecond)
					continue
				}
			}
			start := time.Now()
			qr, err := conn.ExecuteFetch("insert into writes (val) values ('x')", 0, false)
			if err != nil {
				conn.Close()
				conn = nil
				time.Sleep(20 * time.Millisecond)
				continue
			}
			w.mu.Lock()
			w.acked = append(w.acked, ackedWrite{id: qr.InsertID, start: start, finished: time.Now()})
			w.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
		if conn != nil {
			conn.Close()
		}
	})
	return w
}

func (w *ackingWriter) stop() []ackedWrite {
	w.cancel()
	w.wg.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.acked
}

// firstWriteStartedAfter returns the first acknowledged write that was sent after t.
func (w *ackingWriter) firstWriteStartedAfter(t time.Time) (ackedWrite, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, a := range w.acked {
		if a.start.After(t) {
			return a, true
		}
	}
	return ackedWrite{}, false
}

// killHost simulates the crash of the host of a tablet: mysqld_safe, mysqld and vttablet die
// at once and nothing restarts them.
func killHost(t *testing.T, tablet *cluster.Vttablet) {
	dir := path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("vt_%010d", tablet.TabletUID))
	// Kill mysqld_safe first, so that it does not restart mysqld.
	_ = exec.Command("pkill", "-9", "-f", "mysqld_safe.*"+dir+"/").Run()
	data, err := os.ReadFile(path.Join(dir, "mysql.pid"))
	require.NoError(t, err)
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	require.NoError(t, err)
	require.NoError(t, syscall.Kill(pid, syscall.SIGKILL))
	// Kill returns the exit error of the killed process, "signal: killed".
	_ = tablet.VttabletProcess.Kill()
}

// freezeMysqld stops the tablet's mysqld without closing its connections, the way a hung disk
// or a stuck kernel would: vttablet stays up, but MySQL answers nothing.
func freezeMysqld(t *testing.T, tablet *cluster.Vttablet) {
	dir := path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("vt_%010d", tablet.TabletUID))
	data, err := os.ReadFile(path.Join(dir, "mysql.pid"))
	require.NoError(t, err)
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	require.NoError(t, err)
	require.NoError(t, syscall.Kill(pid, syscall.SIGSTOP))
	// Let the teardown stop mysqld.
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGCONT) })
}

type failoverResult struct {
	scenario       string
	mode           string
	topo, write    time.Duration
	ackedLost      int
	ackedBeforeCnt int
}

// TestUnplannedFailoverTimes compares how long an unplanned failover takes with cross-cell
// semi-sync and VTOrc, and with Group Replication. It crashes the primary's host and measures
// the time until the new primary is in the topology, and until a write sent through vtgate
// after the crash succeeds. It also checks that no acknowledged write was lost. It only runs
// when VT_UNPLANNED_FAILOVER_TRIALS is set to the number of trials per mode.
func TestUnplannedFailoverTimes(t *testing.T) {
	trials, _ := strconv.Atoi(os.Getenv(failoverBenchEnv))
	if trials <= 0 {
		t.Skipf("set %s to the number of trials per mode to run this benchmark", failoverBenchEnv)
	}

	vtorcDefaultPoll := vtorcConfig
	vtorcDefaultPoll.InstancePollTime = "" // VTOrc's default, 5s
	modes := []struct {
		name             string
		vtorc            cluster.VTOrcConfiguration
		groupReplication bool
	}{
		{"semi-sync cross_cell, VTOrc default poll (5s)", vtorcDefaultPoll, false},
		{"semi-sync cross_cell, VTOrc poll 1s", vtorcConfig, false},
		{"group_replication_cross_cell", vtorcConfig, true},
	}

	scenarios := []struct {
		name  string
		crash func(t *testing.T, tablet *cluster.Vttablet)
	}{
		{"host crash", killHost},
		{"mysqld freeze", freezeMysqld},
	}

	var results []failoverResult
	for _, scenario := range scenarios {
		for _, mode := range modes {
			for trial := 1; trial <= trials; trial++ {
				t.Run(fmt.Sprintf("%s/%s/%d", scenario.name, mode.name, trial), func(t *testing.T) {
					opts := defaultClusterOptions()
					opts.vtorc = mode.vtorc
					tc := setupCluster(t, opts)
					primary := tc.replicas[0]
					if mode.groupReplication {
						out, err := tc.VtctldClientProcess.ExecuteCommandWithOutput("MigrateReplicationMode",
							"--durability-policy", policy.DurabilityGroupReplicationCrossCell, keyspaceName)
						require.NoError(t, err, out)
						waitForGroup(t, tc, primary, tc.replicas)
					}

					w := startAckingWriter(t, tc)
					// Let VTOrc discover the healthy shard and the writes reach a steady state.
					time.Sleep(10 * time.Second)

					scenario.crash(t, primary)
					// Measure from the moment the crash is complete: writes sent before may still
					// have reached the old primary.
					killed := time.Now()

					var newPrimary string
					require.Eventually(t, func() bool {
						newPrimary = shardPrimary(t, tc)
						return newPrimary != "" && newPrimary != primary.Alias
					}, 2*waitTimeout, 50*time.Millisecond)
					topoTime := time.Since(killed)

					var first ackedWrite
					require.Eventually(t, func() bool {
						var ok bool
						first, ok = w.firstWriteStartedAfter(killed)
						return ok
					}, 2*waitTimeout, 50*time.Millisecond)
					acked := w.stop()

					// Every write acknowledged before the crash must be on the new primary.
					var newPrimaryTablet *cluster.Vttablet
					for _, v := range tc.replicas {
						if v.Alias == newPrimary {
							newPrimaryTablet = v
						}
					}
					require.NotNil(t, newPrimaryTablet)
					qr, err := newPrimaryTablet.VttabletProcess.QueryTablet("select id from writes", keyspaceName, true)
					require.NoError(t, err)
					present := make(map[uint64]bool, len(qr.Rows))
					for _, row := range qr.Rows {
						id, err := row[0].ToCastUint64()
						require.NoError(t, err)
						present[id] = true
					}
					result := failoverResult{scenario: scenario.name, mode: mode.name, topo: topoTime, write: first.finished.Sub(killed)}
					for _, a := range acked {
						if a.finished.Before(killed) {
							result.ackedBeforeCnt++
							if !present[a.id] {
								result.ackedLost++
							}
						}
					}
					assert.Zero(t, result.ackedLost, "acknowledged writes were lost")
					t.Logf("%s, %s: new primary %s in topo after %v, first write sent after the crash acknowledged after %v, %d/%d writes acknowledged before the crash lost",
						scenario.name, mode.name, newPrimary, result.topo, result.write, result.ackedLost, result.ackedBeforeCnt)
					results = append(results, result)
				})
			}
		}
	}

	sort.SliceStable(results, func(i, j int) bool {
		if results[i].scenario != results[j].scenario {
			return results[i].scenario < results[j].scenario
		}
		return results[i].mode < results[j].mode
	})
	var b strings.Builder
	b.WriteString("\n| Scenario | Mode | New primary in topo | First write after crash acknowledged | Acknowledged writes lost |\n|---|---|---|---|---|\n")
	for _, r := range results {
		fmt.Fprintf(&b, "| %s | %s | %.1fs | %.1fs | %d of %d |\n", r.scenario, r.mode, r.topo.Seconds(), r.write.Seconds(), r.ackedLost, r.ackedBeforeCnt)
	}
	t.Log(b.String())
}
