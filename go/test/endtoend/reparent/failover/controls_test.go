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

// Package failover holds end-to-end tests of failover scenarios: reparents interleaved with
// restarts, locks and replication states that a test arranges. Its controls show that the
// fixture behaves as the scenarios rely on, and pass on main.
package failover

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/reparent/utils"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// teardownOnCleanup stops the cluster when the test ends. A failed test writes the tails of
// the component logs to the test output, which is all that CI retains, and leaves its
// VTDATAROOT in place; test.go still removes it unless it runs with -keep-data.
func teardownOnCleanup(t *testing.T, ci *cluster.LocalProcessCluster) {
	t.Cleanup(func() {
		if !t.Failed() {
			utils.TeardownCluster(ci)
			return
		}
		logs, _ := filepath.Glob(path.Join(ci.TmpDirectory, "*stderr.txt*"))
		for _, l := range logs {
			b, _ := os.ReadFile(l)
			lines := strings.Split(string(b), "\n")
			t.Logf("==== last lines of %s\n%s", l, strings.Join(lines[max(0, len(lines)-40):], "\n"))
		}
		ci.Teardown()
		t.Logf("left %s in place for the failed test", ci.CurrentVTDATAROOT)
	})
}

// restartVttablet restarts only tab's vttablet: its mysqld and replication state stay. The
// log of the stopped process is kept as <log>.gen<gen>.
func restartVttablet(t *testing.T, tab *cluster.Vttablet, gen int) {
	vp := tab.VttabletProcess
	require.NoError(t, vp.TearDownWithTimeout(30*time.Second))
	require.NoError(t, os.Rename(vp.ErrorLog, fmt.Sprintf("%s.gen%d", vp.ErrorLog, gen)))
	// The caller waits for the state it expects.
	vp.ServingStatus = ""
	require.NoError(t, vp.Setup())
}

// vtctldclient runs one vtctldclient command, without the retries of the cluster's client,
// and kills it when ctx ends.
func vtctldclient(ctx context.Context, ci *cluster.LocalProcessCluster, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, ci.VtctldClientProcess.Binary, append([]string{"--server", ci.VtctldClientProcess.Server}, args...)...)
	cmd.WaitDelay = 5 * time.Second
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// waitForTabletManager waits until tab answers tablet manager RPCs. Each attempt is killed
// after two seconds or at the deadline, and returns before the next one starts.
func waitForTabletManager(t *testing.T, ci *cluster.LocalProcessCluster, tab *cluster.Vttablet, deadline time.Time) {
	for {
		ctx, cancel := context.WithDeadline(t.Context(), deadline)
		ctx, cancelAttempt := context.WithTimeout(ctx, 2*time.Second)
		out, err := vtctldclient(ctx, ci, "GetFullStatus", tab.Alias)
		cancelAttempt()
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			require.FailNow(t, "the vttablet does not answer tablet manager RPCs", "%v: %s", err, out)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func replicaStatus(t *testing.T, tab *cluster.Vttablet) map[string]string {
	qr := utils.RunSQL(t.Context(), t, "show replica status", tab)
	require.Len(t, qr.Rows, 1)
	st := map[string]string{}
	for i, f := range qr.Fields {
		st[f.Name] = qr.Rows[0][i].ToString()
	}
	return st
}

func mysqldPid(t *testing.T, tab *cluster.Vttablet) string {
	b, err := os.ReadFile(path.Join(tab.MysqlctlProcess.BasePath(), "mysql.pid"))
	require.NoError(t, err)
	return strings.TrimSpace(string(b))
}

// TestRestartReplicaVttabletKeepsMysqld restarts a replica's vttablet without its mysqld.
// The new process answers tablet manager RPCs, replicates from the primary, serves, and
// replicates a later write.
func TestRestartReplicaVttabletKeepsMysqld(t *testing.T) {
	ci := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	teardownOnCleanup(t, ci)
	tablets := ci.Keyspaces[0].Shards[0].Vttablets
	primary, replica := tablets[0], tablets[1]

	before := utils.ConfirmReplication(t, primary, tablets[1:])
	pid := mysqldPid(t, replica)
	gen0Log := replica.VttabletProcess.ErrorLog

	restartVttablet(t, replica, 0)

	require.Equal(t, pid, mysqldPid(t, replica), "mysqld was restarted")
	require.FileExists(t, gen0Log+".gen0")
	waitForTabletManager(t, ci, replica, time.Now().Add(30*time.Second))
	utils.WaitForTabletToBeServing(t.Context(), t, ci, replica, 30*time.Second)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		st := replicaStatus(t, replica)
		assert.Equal(c, "Yes", st["Replica_IO_Running"])
		assert.Equal(c, "Yes", st["Replica_SQL_Running"])
		assert.Equal(c, strconv.Itoa(primary.MySQLPort), st["Source_Port"])
	}, 30*time.Second, 100*time.Millisecond)
	require.NoError(t, utils.CheckInsertedValues(t.Context(), t, replica, before))
	utils.ConfirmReplication(t, primary, tablets[1:])
}

// heldLock is a test-owned shard lock. Its etcd lease is renewed only when the lock is
// checked, so a checker calls topo.CheckShardLocked every interval, each check bounded by
// the interval; the first failed check ends the checker and is its result.
type heldLock struct {
	ctx    context.Context
	unlock func(*error)
	stop   context.CancelFunc
	done   chan struct{}
	err    error
}

func holdShardLock(t *testing.T, ts *topo.Server, action string, ttl, interval time.Duration) *heldLock {
	lockCtx, unlock, err := ts.LockShard(t.Context(), utils.KeyspaceName, utils.ShardName, action, topo.WithTTL(ttl))
	require.NoError(t, err)
	// Derived from lockCtx, which carries the lock that CheckShardLocked looks up.
	checkerCtx, stop := context.WithCancel(lockCtx)
	l := &heldLock{ctx: lockCtx, unlock: unlock, stop: stop, done: make(chan struct{})}
	go func() {
		defer close(l.done)
		for {
			select {
			case <-checkerCtx.Done():
				return
			case <-time.After(interval):
			}
			ctx, cancel := context.WithTimeout(checkerCtx, interval)
			err := topo.CheckShardLocked(ctx, utils.KeyspaceName, utils.ShardName)
			cancel()
			if err != nil && checkerCtx.Err() == nil {
				l.err = err
				return
			}
		}
	}()
	t.Cleanup(func() {
		// Releases a lock the test did not release: an unlock error is expected after a failure.
		_ = l.stopChecker()
		var uerr error
		l.unlock(&uerr)
	})
	return l
}

// stopChecker stops the checker and returns its result.
func (l *heldLock) stopChecker() error {
	l.stop()
	select {
	case <-l.done:
		return l.err
	case <-time.After(30 * time.Second):
		return errors.New("the lock checker did not stop")
	}
}

// release stops the checker, checks ownership one last time and unlocks.
func (l *heldLock) release(t *testing.T) {
	require.NoError(t, l.stopChecker(), "the lock was lost while held")
	ctx, cancel := context.WithTimeout(l.ctx, topo.RemoteOperationTimeout)
	defer cancel()
	require.NoError(t, topo.CheckShardLocked(ctx, utils.KeyspaceName, utils.ShardName))
	var err error
	l.unlock(&err)
	l.unlock = func(*error) {}
	require.NoError(t, err)
}

// shardLockLease returns the etcd lease of the shard's lock, failing if the shard is not locked.
// ctx bounds the lookup: the client's DialTimeout does not bound requests.
func shardLockLease(ctx context.Context, t *testing.T, ci *cluster.LocalProcessCluster) (*clientv3.Client, clientv3.LeaseID) {
	require.Equal(t, "etcd2", *ci.TopoFlavorString(), "shard lock leases are etcd specific")
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{net.JoinHostPort(ci.TopoProcess.Host, strconv.Itoa(ci.TopoProcess.Port))}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	locks := path.Join(cluster.TopoGlobalRoot("etcd2"), "keyspaces", utils.KeyspaceName, "shards", utils.ShardName, "locks")
	resp, err := cli.Get(ctx, locks, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1, "the shard is not locked")
	return cli, clientv3.LeaseID(resp.Kvs[0].Lease)
}

func tryLockShard(t *testing.T, ts *topo.Server) error {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, unlock, err := ts.TryLockShard(ctx, utils.KeyspaceName, utils.ShardName, "contender")
	if err == nil {
		var uerr error
		unlock(&uerr)
		require.NoError(t, uerr)
	}
	return err
}

// TestShardLockControl holds a test-owned shard lock past its lease TTL with periodic checks.
// A non-blocking lock attempt fails while it is held and succeeds after release, and the
// checker reports a lease that etcd revoked.
func TestShardLockControl(t *testing.T) {
	ci := cluster.NewCluster("zone1", "localhost")
	teardownOnCleanup(t, ci)
	require.NoError(t, ci.StartTopo())
	require.Equal(t, "etcd2", *ci.TopoFlavorString(), "the lease checks below are etcd specific")
	ts := ci.TopoProcess.Server
	require.NoError(t, ts.CreateKeyspace(t.Context(), utils.KeyspaceName, &topodatapb.Keyspace{}))
	require.NoError(t, ts.CreateShard(t.Context(), utils.KeyspaceName, utils.ShardName))
	const ttl, interval = 3 * time.Second, time.Second

	l := holdShardLock(t, ts, "control hold", ttl, interval)
	err := tryLockShard(t, ts)
	require.True(t, topo.IsErrType(err, topo.NodeExists), "a second lock attempt while held: %v", err)
	require.Never(t, func() bool {
		select {
		case <-l.done:
			return true
		default:
			return false
		}
	}, 2*ttl, 100*time.Millisecond, "the checker failed while the lock was held")
	l.release(t)
	require.NoError(t, tryLockShard(t, ts), "the lock was not released")

	// A lost lease: revoke the lock's lease in etcd and expect the checker to fail.
	l = holdShardLock(t, ts, "control lost lease", ttl, interval)
	ctx, cancel := context.WithTimeout(t.Context(), topo.RemoteOperationTimeout)
	defer cancel()
	cli, lease := shardLockLease(ctx, t, ci)
	_, err = cli.Revoke(ctx, lease)
	require.NoError(t, err)
	select {
	case <-l.done:
		require.Error(t, l.err)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the checker did not report the revoked lease")
	}
}
