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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

const (
	databaseName        = "crash_safe_shutdown"
	initialRows         = 200
	finalRows           = 250
	shutdownGracePeriod = time.Second
)

func TestReplicaCrashSafeShutdown(t *testing.T) {
	localCluster := cluster.NewCluster("zone1", "localhost")
	localCluster.TmpDirectory = t.TempDir()
	t.Cleanup(localCluster.Teardown)

	extraMyCnf := filepath.Join(t.TempDir(), "crash-safe-shutdown.cnf")
	require.NoError(t, os.WriteFile(extraMyCnf, []byte(`[mysqld]
replica_parallel_workers=4
replica_preserve_commit_order=OFF
sync_relay_log=10000
`), 0o600))
	t.Setenv("EXTRA_MY_CNF", extraMyCnf)

	primary := startMySQL(t, localCluster)
	replica := startMySQL(t, localCluster)
	primaryConn := connectMySQL(t, primary)
	t.Cleanup(primaryConn.Close)
	replicaConn := connectMySQL(t, replica)
	t.Cleanup(replicaConn.Close)

	execQuery(t, replicaConn, fmt.Sprintf(`CHANGE REPLICATION SOURCE TO
		SOURCE_HOST = 'localhost',
		SOURCE_PORT = %d,
		SOURCE_USER = 'vt_repl',
		GET_SOURCE_PUBLIC_KEY = 1,
		SOURCE_AUTO_POSITION = 1`, primary.MySQLPort))
	execQuery(t, replicaConn, "START REPLICA")
	require.Eventually(t, func() bool {
		status, err := replicaConn.ShowReplicationStatus()
		return err == nil && status.Running()
	}, 30*time.Second, 100*time.Millisecond)

	assertGlobalVariable(t, replicaConn, "replica_parallel_workers", "4")
	assertGlobalVariable(t, replicaConn, "sync_relay_log", "10000")

	execQuery(t, primaryConn, "SET GLOBAL super_read_only = OFF")
	execQuery(t, primaryConn, "CREATE DATABASE "+databaseName)
	execQuery(t, primaryConn, "CREATE TABLE "+databaseName+".messages (id BIGINT PRIMARY KEY, value BIGINT NOT NULL) ENGINE=InnoDB")
	for id := 1; id <= initialRows; id++ {
		execQuery(t, primaryConn, fmt.Sprintf("INSERT INTO %s.messages VALUES (%d, %d)", databaseName, id, id))
	}
	waitForRowCount(t, replicaConn, initialRows)

	relayLogBeforeShutdown := currentRelayLog(t, replica)
	shutdown := startBlockedShutdown(t, replica)
	require.Eventually(t, func() bool {
		_, err := os.Stat(shutdown.entered)
		return err == nil
	}, 30*time.Second, 100*time.Millisecond)

	assertGlobalVariable(t, replicaConn, "sync_relay_log", "1")
	assert.NotEqual(t, relayLogBeforeShutdown, currentRelayLog(t, replica))
	status, err := replicaConn.ShowReplicationStatus()
	require.NoError(t, err)
	assert.Equal(t, replication.ReplicationStateStopped, status.IOState)

	for id := initialRows + 1; id <= finalRows; id++ {
		execQuery(t, primaryConn, fmt.Sprintf("INSERT INTO %s.messages VALUES (%d, %d)", databaseName, id, id))
	}

	select {
	case err := <-shutdown.done:
		require.FailNow(t, "mysqlctl shutdown exited before the grace period", "%v", err)
	case <-time.After(shutdownGracePeriod):
	}
	killMySQL(t, replica.TabletUID)
	shutdown.release(t)
	require.NoError(t, shutdown.wait(t))

	require.NoError(t, replica.StartProvideInit(false))
	replicaConn.Close()
	replicaConn = connectMySQL(t, replica)
	t.Cleanup(replicaConn.Close)
	assertGlobalVariable(t, replicaConn, "sync_relay_log", "10000")
	execQuery(t, replicaConn, "START REPLICA")
	waitForRowCount(t, replicaConn, finalRows)

	status, err = replicaConn.ShowReplicationStatus()
	require.NoError(t, err)
	assert.True(t, status.Running())
	assert.Empty(t, status.LastIOError)
	assert.Empty(t, status.LastSQLError)
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

func (s *blockedShutdown) wait(t *testing.T) error {
	t.Helper()
	select {
	case err := <-s.done:
		return err
	case <-time.After(30 * time.Second):
		return errors.New("mysqlctl shutdown did not exit")
	}
}

func startMySQL(t *testing.T, localCluster *cluster.LocalProcessCluster) *cluster.MysqlctlProcess {
	t.Helper()

	process, err := cluster.MysqlCtlProcessInstance(
		localCluster.GetAndReserveTabletUID(),
		localCluster.GetAndReservePort(),
		localCluster.TmpDirectory,
	)
	require.NoError(t, err)
	require.NoError(t, process.Start())
	t.Cleanup(func() {
		assert.NoError(t, process.Stop())
	})
	return process
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

func waitForRowCount(t *testing.T, conn *mysql.Conn, expected int) {
	t.Helper()
	require.Eventually(t, func() bool {
		result, err := conn.ExecuteFetch("SELECT COUNT(*) FROM "+databaseName+".messages", 1, false)
		return err == nil && len(result.Rows) == 1 && result.Rows[0][0].ToString() == strconv.Itoa(expected)
	}, 30*time.Second, 100*time.Millisecond)
}

func assertGlobalVariable(t *testing.T, conn *mysql.Conn, name, expected string) {
	t.Helper()
	result, err := conn.ExecuteFetch("SELECT @@global."+name, 1, false)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, expected, result.Rows[0][0].ToString())
}

func currentRelayLog(t *testing.T, process *cluster.MysqlctlProcess) string {
	t.Helper()
	indexPath := filepath.Join(
		process.BasePath(),
		"relay-logs",
		fmt.Sprintf("vt-%010d-relay-bin.index", process.TabletUID),
	)
	contents, err := os.ReadFile(indexPath)
	require.NoError(t, err)
	relayLogs := strings.Fields(string(contents))
	require.NotEmpty(t, relayLogs)
	return relayLogs[len(relayLogs)-1]
}

func killMySQL(t *testing.T, tabletUID int) {
	t.Helper()

	output, err := exec.Command("ps", "-axo", "pid=,command=").Output()
	require.NoError(t, err)
	marker := fmt.Sprintf("vt_%010d", tabletUID)
	type process struct {
		pid        int
		mysqldSafe bool
	}
	processes := make([]process, 0, 2)
	for line := range strings.SplitSeq(string(output), "\n") {
		if !strings.Contains(line, marker) || (!strings.Contains(line, "mysqld") && !strings.Contains(line, "mariadbd")) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		require.NoError(t, err)
		processes = append(processes, process{
			pid:        pid,
			mysqldSafe: strings.Contains(line, "mysqld_safe") || strings.Contains(line, "mariadbd-safe"),
		})
	}
	require.NotEmpty(t, processes)
	sort.Slice(processes, func(i, j int) bool {
		return processes[i].mysqldSafe && !processes[j].mysqldSafe
	})
	for _, process := range processes {
		require.NoError(t, syscall.Kill(process.pid, syscall.SIGKILL))
	}

	basePath := filepath.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("vt_%010d", tabletUID))
	require.Eventually(t, func() bool {
		output, err := exec.Command("ps", "-axo", "command=").Output()
		return err == nil && !strings.Contains(string(output), marker)
	}, 30*time.Second, 100*time.Millisecond)
	if err := os.Remove(filepath.Join(basePath, "mysql.pid")); err != nil {
		require.ErrorIs(t, err, os.ErrNotExist)
	}
	_ = os.Remove(filepath.Join(basePath, "mysql.sock"))
}
