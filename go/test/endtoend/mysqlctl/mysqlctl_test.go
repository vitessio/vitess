/*
Copyright 2019 The Vitess Authors.

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

package mysqlctl

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	primaryTablet   cluster.Vttablet
	replicaTablet   cluster.Vttablet
	hostname        = "localhost"
	keyspaceName    = "test_keyspace"
	shardName       = "0"
	cell            = "zone1"
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		if err := clusterInstance.VtctldClientProcess.CreateKeyspace(keyspaceName, sidecar.DefaultName, ""); err != nil {
			return 1
		}

		initCluster([]string{"0"}, 2)

		// Collect tablet paths and ports
		tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
		for _, tablet := range tablets {
			if tablet.Type == "primary" {
				primaryTablet = *tablet
			} else if tablet.Type != "rdonly" {
				replicaTablet = *tablet
			}
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func initCluster(shardNames []string, totalTabletsRequired int) {
	keyspace := cluster.Keyspace{
		Name: keyspaceName,
	}
	for _, shardName := range shardNames {
		shard := &cluster.Shard{
			Name: shardName,
		}
		var mysqlCtlProcessList []*exec.Cmd
		for i := range totalTabletsRequired {
			// instantiate vttablet object with reserved ports
			tabletUID := clusterInstance.GetAndReserveTabletUID()
			tablet := &cluster.Vttablet{
				TabletUID: tabletUID,
				HTTPPort:  clusterInstance.GetAndReservePort(),
				GrpcPort:  clusterInstance.GetAndReservePort(),
				MySQLPort: clusterInstance.GetAndReservePort(),
				Alias:     fmt.Sprintf("%s-%010d", clusterInstance.Cell, tabletUID),
			}
			if i == 0 { // Make the first one as primary
				tablet.Type = "primary"
			}
			// Start Mysqlctl process
			mysqlctlProcess, err := cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
			if err != nil {
				return
			}
			tablet.MysqlctlProcess = *mysqlctlProcess
			proc, err := tablet.MysqlctlProcess.StartProcess()
			if err != nil {
				return
			}
			mysqlCtlProcessList = append(mysqlCtlProcessList, proc)

			// start vttablet process
			tablet.VttabletProcess = cluster.VttabletProcessInstance(
				tablet.HTTPPort,
				tablet.GrpcPort,
				tablet.TabletUID,
				clusterInstance.Cell,
				shardName,
				keyspaceName,
				clusterInstance.VtctldProcess.Port,
				tablet.Type,
				clusterInstance.TopoProcess.Port,
				clusterInstance.Hostname,
				clusterInstance.TmpDirectory,
				clusterInstance.VtTabletExtraArgs,
				clusterInstance.DefaultCharset)
			tablet.Alias = tablet.VttabletProcess.TabletPath

			shard.Vttablets = append(shard.Vttablets, tablet)
		}
		for _, proc := range mysqlCtlProcessList {
			if err := proc.Wait(); err != nil {
				return
			}
		}

		keyspace.Shards = append(keyspace.Shards, *shard)
	}
	clusterInstance.Keyspaces = append(clusterInstance.Keyspaces, keyspace)
}

func TestRestart(t *testing.T) {
	err := primaryTablet.MysqlctlProcess.Stop()
	require.NoError(t, err)
	primaryTablet.MysqlctlProcess.CleanupFiles(primaryTablet.TabletUID)
	err = primaryTablet.MysqlctlProcess.Start()
	require.NoError(t, err)
}

func TestAutoDetect(t *testing.T) {
	err := clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].VttabletProcess.Setup()
	require.NoError(t, err)
	err = clusterInstance.Keyspaces[0].Shards[0].Vttablets[1].VttabletProcess.Setup()
	require.NoError(t, err)

	// Reparent tablets, which requires flavor detection
	err = clusterInstance.VtctldClientProcess.InitializeShard(keyspaceName, shardName, cell, primaryTablet.TabletUID)
	require.NoError(t, err)
}

func TestIsLocalMySQLDown(t *testing.T) {
	tabletDir := path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("vt_%010d", replicaTablet.TabletUID))
	socketFile := path.Join(tabletDir, "mysql.sock")
	pidFile := path.Join(tabletDir, "mysql.pid")

	connParams := mysql.ConnParams{
		Uname:      "root",
		UnixSocket: socketFile,
	}
	dbcfgs := dbconfigs.NewTestDBConfigs(connParams, connParams, "")
	mysqld := mysqlctl.NewMysqld(dbcfgs)
	defer mysqld.Close()

	t.Run("mysql is alive", func(t *testing.T) {
		assert.False(t, mysqld.IsLocalMySQLDown(t.Context()))
	})

	t.Run("mysql killed with SIGKILL", func(t *testing.T) {
		// Restore MySQL after the test so subsequent tests are not affected.
		t.Cleanup(func() {
			require.NoError(t, replicaTablet.MysqlctlProcess.StartProvideInit(false))
		})

		pidBytes, err := os.ReadFile(pidFile)
		require.NoError(t, err)

		pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
		require.NoError(t, err)

		require.NoError(t, syscall.Kill(pid, syscall.SIGKILL))

		// Wait for MySQL to be reported as down. We check IsLocalMySQLDown rather
		// than waiting for the socket file to disappear, because SIGKILL bypasses
		// cleanup and the socket file may persist on disk.
		require.Eventually(t, func() bool {
			return mysqld.IsLocalMySQLDown(t.Context())
		}, 30*time.Second, 100*time.Millisecond, "MySQL was not reported down after SIGKILL")
	})

	t.Run("fd exhaustion", func(t *testing.T) {
		// Lower the fd limit so we can exhaust fds without opening thousands.
		var original syscall.Rlimit
		require.NoError(t, syscall.Getrlimit(syscall.RLIMIT_NOFILE, &original))

		low := syscall.Rlimit{Cur: 32, Max: original.Max}
		require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_NOFILE, &low))
		t.Cleanup(func() {
			require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_NOFILE, &original))
		})

		// Consume all remaining fds via Dup.
		var fds []int
		t.Cleanup(func() {
			for _, fd := range fds {
				syscall.Close(fd)
			}
		})
		for {
			fd, err := syscall.Dup(0)
			if err != nil {
				break
			}
			fds = append(fds, fd)
		}

		assert.False(t, mysqld.IsLocalMySQLDown(t.Context()), "should not report MySQL as down when fds are exhausted")
	})
}
