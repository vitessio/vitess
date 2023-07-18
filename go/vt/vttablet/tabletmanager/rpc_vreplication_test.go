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

package tabletmanager

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
)

func TestUpdateVRWorkflow(t *testing.T) {
	ctx := context.Background()
	cells := []string{"zone1"}
	tabletTypes := []string{"replica"}
	workflow := "testwf"
	dbName := "test"
	vreplID := 1
	shortCircuitErr := fmt.Errorf("short circuiting test")
	cp := mysql.ConnParams{}
	db := fakesqldb.New(t)
	ts := memorytopo.NewServer(cells[0])
	mysqld := mysqlctl.NewFakeMysqlDaemon(db)
	dbClient := binlogplayer.NewMockDBClient(t)
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	// Intentionally using Sprintf here as the query matching is exact
	// and the engine uses `db_name=` w/o any spacing and the parser
	// will add spacing.
	dbClient.ExpectRequest(fmt.Sprintf("select * from _vt.vreplication where db_name='%s'", dbName),
		&sqltypes.Result{}, nil)
	vre := vreplication.NewSimpleTestEngine(ts, cells[0], mysqld, dbClientFactory, dbClientFactory, dbName, nil)
	vre.Open(context.Background())
	tm := &TabletManager{
		MysqlDaemon:         mysqld,
		DBConfigs:           dbconfigs.NewTestDBConfigs(cp, cp, dbName),
		QueryServiceControl: tabletservermock.NewController(),
		VREngine:            vre,
	}
	defer func() {
		vre.Close()
		dbClient.Close()
		mysqld.Close()
		db.Close()
	}()
	parsed := sqlparser.BuildParsedQuery(sqlSelectVRWorkflowConfig, sidecardb.DefaultName, ":wf")
	bindVars := map[string]*querypb.BindVariable{
		"wf": sqltypes.StringBindVariable(workflow),
	}
	selectQuery, err := parsed.GenerateQuery(bindVars, nil)
	require.NoError(t, err)
	blsStr := fmt.Sprintf(`keyspace:"%s" shard:"%s" filter:{rules:{match:"customer" filter:"select * from customer"} rules:{match:"corder" filter:"select * from corder"}}`,
		keyspace, shard)
	selectRes := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|source|cell|tablet_types",
			"int64|varchar|varchar|varchar",
		),
		fmt.Sprintf("%d|%s|%s|%s", vreplID, blsStr, cells[0], tabletTypes[0]),
	)
	idQuery, err := sqlparser.ParseAndBind("select id from _vt.vreplication where id = %a",
		sqltypes.Int64BindVariable(int64(vreplID)))
	require.NoError(t, err)
	idRes := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id",
			"int64",
		),
		fmt.Sprintf("%d", vreplID),
	)

	tests := []struct {
		name    string
		request *tabletmanagerdatapb.UpdateVRWorkflowRequest
		query   string
	}{
		{
			name: "update cells",
			request: &tabletmanagerdatapb.UpdateVRWorkflowRequest{
				Workflow: workflow,
				Cells:    []string{"zone2"},
				// TabletTypes is an empty value, so the current value should be cleared
			},
			query: fmt.Sprintf(`update _vt.vreplication set source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}}', cell = '%s', tablet_types = '' where id in (%d)`,
				keyspace, shard, "zone2", vreplID),
		},
		{
			name: "update cells, NULL tablet_types",
			request: &tabletmanagerdatapb.UpdateVRWorkflowRequest{
				Workflow:    workflow,
				Cells:       []string{"zone3"},
				TabletTypes: textutil.SimulatedNullStringSlice, // So keep the current value of replica
			},
			query: fmt.Sprintf(`update _vt.vreplication set source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}}', cell = '%s', tablet_types = '%s' where id in (%d)`,
				keyspace, shard, "zone3", tabletTypes[0], vreplID),
		},
		{
			name: "update tablet_types",
			request: &tabletmanagerdatapb.UpdateVRWorkflowRequest{
				Workflow:    workflow,
				TabletTypes: []string{"in_order:rdonly", "replica"},
			},
			query: fmt.Sprintf(`update _vt.vreplication set source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}}', cell = '', tablet_types = '%s' where id in (%d)`,
				keyspace, shard, "in_order:rdonly,replica", vreplID),
		},
		{
			name: "update tablet_types, NULL cells",
			request: &tabletmanagerdatapb.UpdateVRWorkflowRequest{
				Workflow:    workflow,
				Cells:       textutil.SimulatedNullStringSlice, // So keep the current value of zone1
				TabletTypes: []string{"rdonly"},
			},
			query: fmt.Sprintf(`update _vt.vreplication set source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}}', cell = '%s', tablet_types = '%s' where id in (%d)`,
				keyspace, shard, cells[0], "rdonly", vreplID),
		},
		{
			name: "update on_ddl",
			request: &tabletmanagerdatapb.UpdateVRWorkflowRequest{
				Workflow: workflow,
				OnDdl:    binlogdatapb.OnDDLAction_EXEC,
			},
			query: fmt.Sprintf(`update _vt.vreplication set source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}} on_ddl:%s', cell = '', tablet_types = '' where id in (%d)`,
				keyspace, shard, binlogdatapb.OnDDLAction_EXEC.String(), vreplID),
		},
		{
			name: "update cell,tablet_types,on_ddl",
			request: &tabletmanagerdatapb.UpdateVRWorkflowRequest{
				Workflow:    workflow,
				Cells:       []string{"zone1", "zone2", "zone3"},
				TabletTypes: []string{"rdonly", "replica", "primary"},
				OnDdl:       binlogdatapb.OnDDLAction_EXEC_IGNORE,
			},
			query: fmt.Sprintf(`update _vt.vreplication set source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}} on_ddl:%s', cell = '%s', tablet_types = '%s' where id in (%d)`,
				keyspace, shard, binlogdatapb.OnDDLAction_EXEC_IGNORE.String(), "zone1,zone2,zone3", "rdonly,replica,primary", vreplID),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This is needed because MockDBClient uses t.Fatal()
			// which doesn't play well with subtests.
			defer func() {
				if err := recover(); err != nil {
					t.Errorf("Recovered from panic: %v", err)
				}
			}()

			require.NotNil(t, tt.request, "No request provided")
			require.NotEqual(t, "", tt.query, "No expected query provided")

			// These are the same for each RPC call.
			dbClient.ExpectRequest(fmt.Sprintf("use %s", sidecardb.DefaultName), &sqltypes.Result{}, nil)
			dbClient.ExpectRequest(selectQuery, selectRes, nil)
			dbClient.ExpectRequest(fmt.Sprintf("use %s", sidecardb.DefaultName), &sqltypes.Result{}, nil)
			dbClient.ExpectRequest(idQuery, idRes, nil)

			// This is our expected query, which will also short circuit
			// the test with an error as at this point we've tested what
			// we wanted to test.
			dbClient.ExpectRequest(tt.query, &sqltypes.Result{RowsAffected: 1}, shortCircuitErr)
			_, err = tm.UpdateVRWorkflow(ctx, tt.request)
			dbClient.Wait()
			require.ErrorIs(t, err, shortCircuitErr)
		})
	}
}
