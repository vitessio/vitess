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

package vtgate

import (
	"context"
	_ "embed"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/onlineddl"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/schema"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	KeyspaceName    = "ks"
	Cell            = "test"

	//go:embed schema.sql
	SchemaSQL string

	//go:embed vschema.json
	VSchema string
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()
	exitCode := func() int {
		clusterInstance = cluster.NewCluster(Cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      KeyspaceName,
			SchemaSQL: SchemaSQL,
			VSchema:   VSchema,
		}
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 0, false)
		if err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs,
			"--buffer_size", "10000",
			"--buffer_window", "20s",
			"--enable_buffer",
			"--buffer_drain_concurrency", "100",
			"--buffer_max_failover_duration", "30s",
			"--buffer_min_time_between_failovers", "1m0s",
			"--planner-version", "gen4")
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		vtParams = clusterInstance.GetVTParams(KeyspaceName)

		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestOnlineDDLWithQueryServing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	for i := 0; i < 1000; i++ {
		utils.Exec(t, conn, `INSERT INTO users (id, team_id) VALUES ("`+uuid.New().String()+`", "`+uuid.New().String()+`")`)
		utils.Exec(t, conn, `INSERT INTO music (id, music_id) VALUES ("`+uuid.New().String()+`", "`+uuid.New().String()+`")`)
	}

	var errCount int
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				_, err = utils.ExecAllowError(t, conn, `SELECT id, team_id from users LIMIT 1`)
				if err != nil {
					errCount++
				}
			}
		}
	}()

	ddlConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer ddlConn.Close()

	utils.Exec(t, ddlConn, `SET @@ddl_strategy='vitess'`)
	qr := utils.Exec(t, ddlConn, `ALTER TABLE users ADD INDEX team_id (team_id)`)

	migrationID := qr.Rows[0][0].ToString()
	onlineddl.WaitForMigrationStatus(t, &vtParams, clusterInstance.Keyspaces[0].Shards, migrationID, 1*time.Minute, schema.OnlineDDLStatusComplete)

	cancel()
	require.Zero(t, errCount, "errCount: %d", errCount)
}
