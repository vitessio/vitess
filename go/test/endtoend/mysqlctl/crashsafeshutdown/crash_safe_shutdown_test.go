//go:build !windows

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

package crashsafeshutdown

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

const (
	keyspaceName = "crash_safe_shutdown"
	databaseName = "vt_" + keyspaceName
)

// TestReplicaShutdownRelayDurabilityFence verifies that Vitess fences a
// replica's relay log for crash safety BEFORE handing control to the
// mysqld_shutdown hook (and mysqladmin): it sets sync_relay_log=1, flushes the
// relay logs, and stops the receiver (I/O) thread. Without that fence, an
// interrupted shutdown can leave an unsynced relay-log tail while the receiver
// is still writing, which is the durability gap that lets a subsequently
// crashed replica re-fetch and re-apply already-applied transactions.
//
// The fence runs inside mysqld shutdown, after the preflight hook and before
// the mysqld_shutdown hook. Blocking that hook lets us observe the replica's
// live state at exactly that boundary: a fixed build has already stopped the
// receiver and set sync_relay_log=1; an unfixed build has not. The test is
// therefore red on a build without the fix and green on one with it.
func TestReplicaShutdownRelayDurabilityFence(t *testing.T) {
	localCluster := cluster.NewCluster("zone1", "localhost")
	localCluster.TmpDirectory = t.TempDir()
	t.Cleanup(localCluster.Teardown)

	// Start with sync_relay_log at a non-default value so the fence setting it
	// to 1 is observable.
	extraMyCnf := filepath.Join(t.TempDir(), "crash-safe-shutdown.cnf")
	require.NoError(t, os.WriteFile(extraMyCnf, []byte("[mysqld]\nsync_relay_log=10000\n"), 0o600))
	t.Setenv("EXTRA_MY_CNF", extraMyCnf)

	require.NoError(t, localCluster.StartTopo())
	require.NoError(t, localCluster.StartUnshardedKeyspace(cluster.Keyspace{
		Name:      keyspaceName,
		SchemaSQL: "CREATE TABLE messages (id BIGINT PRIMARY KEY) ENGINE=InnoDB",
	}, 1, false, localCluster.Cell))

	shard := &localCluster.Keyspaces[0].Shards[0]
	primary := shard.PrimaryTablet()
	replica := shard.Replica()
	require.NotNil(t, replica)

	replicaConn := connectMySQL(t, &replica.MysqlctlProcess)
	t.Cleanup(replicaConn.Close)

	// The replica must be actively replicating with the elevated
	// sync_relay_log before we exercise shutdown.
	require.Eventually(t, func() bool {
		status, err := replicaConn.ShowReplicationStatus()
		return err == nil && status.Running()
	}, 45*time.Second, 100*time.Millisecond, "replica did not start replicating")
	assertGlobalVariable(t, replicaConn, "sync_relay_log", "10000")

	// Give the receiver a live relay log by generating a little traffic.
	primaryConn := connectMySQL(t, &primary.MysqlctlProcess)
	t.Cleanup(primaryConn.Close)
	for id := 1; id <= 200; id++ {
		execQuery(t, primaryConn, fmt.Sprintf("INSERT INTO %s.messages (id) VALUES (%d)", databaseName, id))
	}

	// Block the replica's mysqld_shutdown hook. The relay durability fence runs
	// before this hook, so at the block the fence's effects are already visible
	// on a fixed build.
	require.NoError(t, replica.VttabletProcess.TearDown())
	shutdown := startBlockedShutdown(t, &replica.MysqlctlProcess)
	require.Eventually(t, func() bool {
		_, err := os.Stat(shutdown.entered)
		return err == nil
	}, 45*time.Second, 10*time.Millisecond, "mysqlctl did not enter the blocked mysqld_shutdown hook")

	status, err := replicaConn.ShowReplicationStatus()
	require.NoError(t, err)
	syncRelayLog := queryString(t, replicaConn, "SELECT @@global.sync_relay_log")
	t.Logf("at blocked mysqld_shutdown hook: Replica_IO_Running=%v Replica_SQL_Running=%v sync_relay_log=%s",
		status.IOState, status.SQLState, syncRelayLog)

	assert.Equalf(t, replication.ReplicationStateStopped, status.IOState,
		"the receiver (I/O thread) was not stopped before the mysqld_shutdown hook; the relay durability fence did not run")
	assert.Equalf(t, replication.ReplicationStateStopped, status.SQLState,
		"the applier (SQL thread) was not stopped before the mysqld_shutdown hook; the relay durability fence did not run")
	assert.Equalf(t, "1", syncRelayLog,
		"sync_relay_log was not set to 1 before the mysqld_shutdown hook; the relay durability fence did not run")

	// The observation above is the assertion. Release the blocked hook so
	// mysqlctl unwinds; the blocking hook stands in for the real mysqld
	// shutdown, so the cluster teardown performs the final process cleanup.
	shutdown.release(t)
}

type blockedShutdown struct {
	cmd         *exec.Cmd
	done        chan error
	entered     string
	releasePath string
}

func startBlockedShutdown(t *testing.T, replica *cluster.MysqlctlProcess) *blockedShutdown {
	t.Helper()

	hookRoot := t.TempDir()
	hookDir := filepath.Join(hookRoot, "vthook")
	require.NoError(t, os.Mkdir(hookDir, 0o700))
	entered := filepath.Join(hookRoot, "entered")
	releasePath := filepath.Join(hookRoot, "release")
	hookPath := filepath.Join(hookDir, "mysqld_shutdown")
	require.NoError(t, os.WriteFile(hookPath, []byte(`#!/bin/sh
: > "$MYSQLCTL_SHUTDOWN_HOOK_ENTERED"
while [ ! -e "$MYSQLCTL_SHUTDOWN_HOOK_RELEASE" ]; do
  sleep 0.1
done
`), 0o700))

	cmd := exec.Command("mysqlctl", "--tablet-uid", strconv.Itoa(replica.TabletUID), "--log-format", "text", "shutdown")
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	cmd.Env = append(os.Environ(),
		"VTROOT="+hookRoot,
		"MYSQLCTL_SHUTDOWN_HOOK_ENTERED="+entered,
		"MYSQLCTL_SHUTDOWN_HOOK_RELEASE="+releasePath,
	)
	require.NoError(t, cmd.Start())

	shutdown := &blockedShutdown{
		cmd:         cmd,
		done:        make(chan error, 1),
		entered:     entered,
		releasePath: releasePath,
	}
	go func() {
		shutdown.done <- cmd.Wait()
	}()
	t.Cleanup(func() {
		_ = os.WriteFile(releasePath, nil, 0o600)
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			return
		}
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		select {
		case <-shutdown.done:
		case <-time.After(10 * time.Second):
			t.Logf("mysqlctl shutdown did not exit; output:\n%s", output.String())
		}
	})

	return shutdown
}

func (s *blockedShutdown) release(t *testing.T) {
	t.Helper()
	require.NoError(t, os.WriteFile(s.releasePath, nil, 0o600))
}

func connectMySQL(t *testing.T, process *cluster.MysqlctlProcess) *mysql.Conn {
	t.Helper()

	conn, err := mysql.Connect(t.Context(), &mysql.ConnParams{
		Uname:      "root",
		UnixSocket: filepath.Join(process.BasePath(), "mysql.sock"),
	})
	require.NoError(t, err)
	return conn
}

func execQuery(t *testing.T, conn *mysql.Conn, query string) {
	t.Helper()
	_, err := conn.ExecuteFetch(query, 1000, false)
	require.NoError(t, err, "query: %s", query)
}

func queryString(t *testing.T, conn *mysql.Conn, query string) string {
	t.Helper()
	result, err := conn.ExecuteFetch(query, 1, false)
	require.NoError(t, err, "query: %s", query)
	require.Len(t, result.Rows, 1)
	return result.Rows[0][0].ToString()
}

func assertGlobalVariable(t *testing.T, conn *mysql.Conn, name, expected string) {
	t.Helper()
	result, err := conn.ExecuteFetch("SELECT @@global."+name, 1, false)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, expected, result.Rows[0][0].ToString())
}
