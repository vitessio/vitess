/*
Copyright 2020 The Vitess Authors.

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

package unsharded

import (
	"context"
	"flag"
	"os"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	cell            = "zone1"
	hostname        = "localhost"
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name: keyspaceName,
		}
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 0, false); err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGateExtraArgs = []string{"--dbddl_plugin", "noop", "--mysql_server_query_timeout", "60s"}
		vtgateProcess := clusterInstance.NewVtgateInstance()
		vtgateProcess.SysVarSetEnabled = true
		if err := vtgateProcess.Setup(); err != nil {
			return 1
		}

		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestDBDDLPlugin(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	createAndDrop := func(t *testing.T) {
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			qr := utils.Exec(t, conn, `create database aaa`)
			require.EqualValues(t, 1, qr.RowsAffected)
		}()
		time.Sleep(300 * time.Millisecond)
		start(t, "aaa")

		// wait until the create database query has returned
		wg.Wait()

		utils.Exec(t, conn, `use aaa`)
		utils.Exec(t, conn, `create table t (id bigint primary key)`)
		utils.Exec(t, conn, `insert into t(id) values (1),(2),(3),(4),(5)`)
		utils.AssertMatches(t, conn, "select count(*) from t", `[[INT64(5)]]`)

		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = utils.Exec(t, conn, `drop database aaa`)
		}()
		time.Sleep(300 * time.Millisecond)
		shutdown(t, "aaa")

		// wait until the drop database query has returned
		wg.Wait()

		_, err = conn.ExecuteFetch(`select count(*) from t`, 1000, true)
		require.Error(t, err)
	}
	t.Run("first try", func(t *testing.T) {
		createAndDrop(t)
	})
	if !t.Failed() {
		t.Run("second try", func(t *testing.T) {
			createAndDrop(t)
		})
	}
}

func start(t *testing.T, ksName string) {
	keyspace := &cluster.Keyspace{
		Name: ksName,
	}
	require.NoError(t,
		clusterInstance.StartUnshardedKeyspace(*keyspace, 0, false),
		"new database creation failed")
}

func shutdown(t *testing.T, ksName string) {
	for _, ks := range clusterInstance.Keyspaces {
		if ks.Name != ksName {
			continue
		}
		for _, shard := range ks.Shards {
			for _, tablet := range shard.Vttablets {
				if tablet.MysqlctlProcess.TabletUID > 0 {
					_, err := tablet.MysqlctlProcess.StopProcess()
					assert.NoError(t, err)
				}
				if tablet.MysqlctldProcess.TabletUID > 0 {
					err := tablet.MysqlctldProcess.Stop()
					assert.NoError(t, err)
				}
				_ = tablet.VttabletProcess.TearDown()
			}
		}
	}

	require.NoError(t,
		clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteKeyspace", "--", "--recursive", ksName))

	require.NoError(t,
		clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildVSchemaGraph"))
}
