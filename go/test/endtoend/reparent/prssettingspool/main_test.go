/*
Copyright 2023 The Vitess Authors.

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

package misc

import (
	"context"
	_ "embed"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	rutils "vitess.io/vitess/go/test/endtoend/reparent/utils"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	cell            = "test"

	//go:embed schema.sql
	schemaSQL string
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: schemaSQL,
		}
		clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs,
			"--queryserver-enable-settings-pool")
		err = clusterInstance.StartUnshardedKeyspace(*keyspace, 2, false)
		if err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs,
			"--planner-version", "gen4")
		err = clusterInstance.StartVtgate()
		if err != nil {
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

func TestSettingsPoolWithTXAndPRS(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// set a system settings that will trigger reserved connection usage.
	utils.Exec(t, conn, "set default_week_format = 5")

	// have transaction on the session
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "select id1, id2 from t1")
	utils.Exec(t, conn, "commit")

	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	// prs should happen without any error.
	text, err := rutils.Prs(t, clusterInstance, tablets[1])
	require.NoError(t, err, text)
	rutils.WaitForTabletToBeServing(t, clusterInstance, tablets[0], 1*time.Minute)

	defer func() {
		// reset state
		text, err = rutils.Prs(t, clusterInstance, tablets[0])
		require.NoError(t, err, text)
		rutils.WaitForTabletToBeServing(t, clusterInstance, tablets[1], 1*time.Minute)
	}()

	// no error should occur and it should go to the right tablet.
	utils.Exec(t, conn, "select id1, id2 from t1")
}

func TestSettingsPoolWithoutTXAndPRS(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// set a system settings that will trigger reserved connection usage.
	utils.Exec(t, conn, "set default_week_format = 5")

	// execute non-tx query
	utils.Exec(t, conn, "select id1, id2 from t1")

	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	// prs should happen without any error.
	text, err := rutils.Prs(t, clusterInstance, tablets[1])
	require.NoError(t, err, text)
	rutils.WaitForTabletToBeServing(t, clusterInstance, tablets[0], 1*time.Minute)
	defer func() {
		// reset state
		text, err = rutils.Prs(t, clusterInstance, tablets[0])
		require.NoError(t, err, text)
		rutils.WaitForTabletToBeServing(t, clusterInstance, tablets[1], 1*time.Minute)
	}()

	// no error should occur and it should go to the right tablet.
	utils.Exec(t, conn, "select id1, id2 from t1")

}
