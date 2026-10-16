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
	"slices"
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

// failoverBufferEnv selects vtgate's buffering in TestUnplannedFailoverTimes: "on" (the default,
// --enable-buffer), "off", or "both".
const failoverBufferEnv = "VT_UNPLANNED_FAILOVER_BUFFER"

const (
	// clientWriteInterval is how often the client sends a write, whether or not the earlier ones
	// returned.
	clientWriteInterval = 50 * time.Millisecond
	// clientWriteTimeout is how long the client waits for a write before it gives up on it, longer
	// than a buffered failover takes: a write to a frozen primary never returns.
	clientWriteTimeout = 20 * time.Second
)

// clientWrite is an insert sent through vtgate, and its outcome.
type clientWrite struct {
	id         uint64
	start, end time.Time
	err        error
}

func (w clientWrite) acked() bool { return w.err == nil }

// clientWriter sends an insert through vtgate every clientWriteInterval, each in its own
// goroutine, so that writes stuck on a failed primary do not hold back the next ones, as
// independent clients would. Each write gives up after clientWriteTimeout.
type clientWriter struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	writes []clientWrite
	idle   chan *mysql.Conn
}

func startClientWriter(t *testing.T, tc *testCluster) *clientWriter {
	ctx, cancel := context.WithCancel(t.Context())
	w := &clientWriter{cancel: cancel, idle: make(chan *mysql.Conn, 1000)}
	params := mysql.ConnParams{Host: tc.Hostname, Port: tc.VtgateMySQLPort}
	// The writes in flight outlive the stop of the writer, until their own timeout.
	writeCtx := context.WithoutCancel(ctx)
	w.wg.Go(func() {
		ticker := time.NewTicker(clientWriteInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.wg.Go(func() { w.write(writeCtx, &params) })
			}
		}
	})
	return w
}

func (w *clientWriter) write(ctx context.Context, params *mysql.ConnParams) {
	start := time.Now()
	ctx, cancel := context.WithDeadline(ctx, start.Add(clientWriteTimeout))
	defer cancel()
	record := func(id uint64, err error) {
		w.mu.Lock()
		w.writes = append(w.writes, clientWrite{id: id, start: start, end: time.Now(), err: err})
		w.mu.Unlock()
	}
	var conn *mysql.Conn
	select {
	case conn = <-w.idle:
	default:
		var err error
		if conn, err = mysql.Connect(ctx, params); err != nil {
			record(0, err)
			return
		}
	}
	timeout := context.AfterFunc(ctx, conn.Close)
	qr, err := conn.ExecuteFetch("insert into writes (val) values ('x')", 0, false)
	if !timeout() || err != nil {
		if err == nil {
			err = ctx.Err()
		}
		conn.Close()
		record(0, err)
		return
	}
	record(qr.InsertID, nil)
	select {
	case w.idle <- conn:
	default:
		conn.Close()
	}
}

// stop stops sending writes, waits for the writes in flight, and returns every write.
func (w *clientWriter) stop() []clientWrite {
	w.cancel()
	w.wg.Wait()
	close(w.idle)
	for conn := range w.idle {
		conn.Close()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	slices.SortFunc(w.writes, func(a, b clientWrite) int { return a.start.Compare(b.start) })
	return w.writes
}

// firstAckedAfter returns the end of the first acknowledged write that was sent after t.
func (w *clientWriter) firstAckedAfter(t time.Time) (time.Time, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var first time.Time
	for _, wr := range w.writes {
		if wr.acked() && wr.start.After(t) && (first.IsZero() || wr.end.Before(first)) {
			first = wr.end
		}
	}
	return first, !first.IsZero()
}

// bufferVars returns vtgate's buffer counters, flattened: one value per variable and key.
func bufferVars(tc *testCluster) map[string]float64 {
	vars := tc.VtgateProcess.GetVars()
	out := make(map[string]float64)
	for name, v := range vars {
		if !strings.HasPrefix(name, "Buffer") {
			continue
		}
		switch v := v.(type) {
		case float64:
			out[name] = v
		case map[string]any:
			for k, x := range v {
				if f, ok := x.(float64); ok {
					out[name+"."+k] = f
				}
			}
		}
	}
	return out
}

// bufferDelta returns the counters that changed between two readings of bufferVars.
func bufferDelta(before, after map[string]float64) map[string]float64 {
	d := make(map[string]float64)
	for k, v := range after {
		if v != before[k] && !strings.Contains(k, "Size") && !strings.Contains(k, "Window") && !strings.Contains(k, "Duration") {
			d[k] = v - before[k]
		}
	}
	return d
}

// sumVars sums the counters of a variable over its keys.
func sumVars(d map[string]float64, name string) float64 {
	var s float64
	for k, v := range d {
		if k == name || strings.HasPrefix(k, name+".") {
			s += v
		}
	}
	return s
}

// vtgateBufferLog returns vtgate's log lines about its buffer and keyspace events logged at or
// after since.
func vtgateBufferLog(tc *testCluster, since time.Time) []string {
	data, err := os.ReadFile(tc.VtgateProcess.ErrorLog)
	if err != nil {
		return []string{err.Error()}
	}
	from := since.UTC().Format("2006-01-02 15:04:05.000")
	var lines []string
	for line := range strings.SplitSeq(string(data), "\n") {
		i := strings.Index(line, "20")
		if i < 0 || len(line) < i+23 || line[i:i+23] < from {
			continue
		}
		for _, pat := range []string{"buffering for shard", "Stopping buffering", "Draining finished", "CausedByFailover", "Keyspace Event received", "is now consistent", "not buffering"} {
			if strings.Contains(line, pat) {
				lines = append(lines, strings.TrimSpace(line[i:]))
				break
			}
		}
	}
	return lines
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
	scenario, mode, buffer string
	// topo is the time until the new primary is in the topology, firstAck the time until the
	// first write sent after the crash is acknowledged.
	topo, firstAck time.Duration
	// The writes sent from 1s before the crash until the measurement ended, and how they ended.
	sent, failed, timedOut int
	// buffered is how many requests vtgate buffered (BufferRequestsBuffered), bufferStarts how
	// many bufferings it started.
	buffered, bufferStarts int
	// longestAcked is the longest time an acknowledged write took, longestFailed the longest
	// time a write took to fail.
	longestAcked, longestFailed time.Duration
	ackedLost, acked            int
}

// TestUnplannedFailoverTimes compares how long an unplanned failover takes with cross-cell
// semi-sync and VTOrc, and with Group Replication, and what clients see meanwhile. It crashes the
// primary's host, or freezes its mysqld, while a client sends a write through vtgate every 50ms,
// each giving up after 20s. It measures the time until the new primary is in the topology and
// until a write sent after the crash is acknowledged, the writes that failed, the writes vtgate
// buffered, and the longest write; and checks that no acknowledged write was lost. vtgate runs
// with or without --enable-buffer (VT_UNPLANNED_FAILOVER_BUFFER). It only runs when
// VT_UNPLANNED_FAILOVER_TRIALS is set to the number of trials per mode.
func TestUnplannedFailoverTimes(t *testing.T) {
	trials, _ := strconv.Atoi(os.Getenv(failoverBenchEnv))
	if trials <= 0 {
		t.Skipf("set %s to the number of trials per mode to run this benchmark", failoverBenchEnv)
	}
	buffers := []string{"buffer on"}
	switch os.Getenv(failoverBufferEnv) {
	case "", "on":
	case "off":
		buffers = []string{"buffer off"}
	case "both":
		buffers = []string{"buffer on", "buffer off"}
	default:
		t.Fatalf("%s must be on, off or both", failoverBufferEnv)
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
			for _, buffer := range buffers {
				for trial := 1; trial <= trials; trial++ {
					t.Run(fmt.Sprintf("%s/%s/%s/%d", scenario.name, mode.name, buffer, trial), func(t *testing.T) {
						opts := defaultClusterOptions()
						opts.vtorc = mode.vtorc
						opts.noBuffer = buffer == "buffer off"
						tc := setupCluster(t, opts)
						primary := tc.replicas[0]
						if mode.groupReplication {
							out, err := tc.VtctldClientProcess.ExecuteCommandWithOutput("MigrateReplicationMode",
								"--durability-policy", policy.DurabilityGroupReplicationCrossCell, keyspaceName)
							require.NoError(t, err, out)
							waitForGroup(t, tc, primary, tc.replicas)
						}

						w := startClientWriter(t, tc)
						// Let VTOrc discover the healthy shard, the writes reach a steady state, and
						// vtgate's buffer forget the migration's pause.
						time.Sleep(10 * time.Second)
						varsBefore := bufferVars(tc)

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

						var firstAck time.Time
						require.Eventually(t, func() bool {
							var ok bool
							firstAck, ok = w.firstAckedAfter(killed)
							return ok
						}, 2*waitTimeout, 50*time.Millisecond)
						// Keep writing for 5s after the failover, then let the writes in flight end.
						time.Sleep(5 * time.Second)
						measured := time.Now()
						writes := w.stop()
						delta := bufferDelta(varsBefore, bufferVars(tc))

						// Every acknowledged write must be on the new primary.
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
						result := failoverResult{
							scenario: scenario.name, mode: mode.name, buffer: buffer,
							topo: topoTime, firstAck: firstAck.Sub(killed),
							buffered:     int(sumVars(delta, "BufferRequestsBuffered")),
							bufferStarts: int(sumVars(delta, "BufferStarts")),
						}
						errs := make(map[string]int)
						for _, wr := range writes {
							if wr.acked() {
								result.acked++
								if !present[wr.id] {
									result.ackedLost++
								}
							}
							if wr.start.Before(killed.Add(-time.Second)) || wr.start.After(measured) {
								continue
							}
							result.sent++
							d := wr.end.Sub(wr.start)
							if wr.acked() {
								result.longestAcked = max(result.longestAcked, d)
								continue
							}
							result.failed++
							result.longestFailed = max(result.longestFailed, d)
							if d >= clientWriteTimeout {
								result.timedOut++
							}
							msg := wr.err.Error()
							if len(msg) > 160 {
								msg = msg[:160]
							}
							errs[msg]++
						}
						assert.Zero(t, result.ackedLost, "acknowledged writes were lost")
						t.Logf("%s, %s, %s: new primary %s in topo after %v, first write sent after the crash acknowledged after %v; "+
							"%d writes sent from 1s before the crash, %d failed (%d timed out after %v), longest acknowledged write %v, longest failed write %v; "+
							"vtgate buffered %d requests in %d bufferings; %d/%d acknowledged writes lost",
							scenario.name, mode.name, buffer, newPrimary, result.topo, result.firstAck,
							result.sent, result.failed, result.timedOut, clientWriteTimeout, result.longestAcked, result.longestFailed,
							result.buffered, result.bufferStarts, result.ackedLost, result.acked)
						t.Logf("vtgate buffer counters that changed: %v", delta)
						for msg, n := range errs {
							t.Logf("failed writes: %d x %s", n, msg)
						}
						for _, line := range vtgateBufferLog(tc, killed.Add(-time.Second)) {
							t.Logf("vtgate: %s", line)
						}
						results = append(results, result)
					})
				}
			}
		}
	}

	sort.SliceStable(results, func(i, j int) bool {
		if results[i].scenario != results[j].scenario {
			return results[i].scenario < results[j].scenario
		}
		if results[i].mode != results[j].mode {
			return results[i].mode < results[j].mode
		}
		return results[i].buffer < results[j].buffer
	})
	var b strings.Builder
	b.WriteString("\n| Scenario | Mode | Buffer | New primary in topo | First write after crash acknowledged | Writes sent / failed (timed out) | Buffered (bufferings) | Longest acknowledged write | Longest failed write | Acknowledged writes lost |\n|---|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range results {
		fmt.Fprintf(&b, "| %s | %s | %s | %.1fs | %.1fs | %d / %d (%d) | %d (%d) | %.2fs | %.2fs | %d of %d |\n", r.scenario, r.mode, r.buffer,
			r.topo.Seconds(), r.firstAck.Seconds(), r.sent, r.failed, r.timedOut, r.buffered, r.bufferStarts,
			r.longestAcked.Seconds(), r.longestFailed.Seconds(), r.ackedLost, r.acked)
	}
	t.Log(b.String())
}
