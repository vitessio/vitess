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
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
)

func TestUpdateVRWorkflow(t *testing.T) {
	ctx := context.Background()
	cell := "cell1"
	tabletTypes := "replica"
	workflow := "testwf"
	dbName := "test"
	shortCircuitErr := fmt.Errorf("Short circuiting test")
	cp := mysql.ConnParams{}
	db := fakesqldb.New(t)
	ts := memorytopo.NewServer(cell)
	mysqld := mysqlctl.NewFakeMysqlDaemon(db)
	dbClient := binlogplayer.NewMockDBClient(t)
	defer func() {
		dbClient.Close()
		mysqld.Close()
		db.Close()
	}()
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	selectQuery, err := sqlparser.ParseAndBind("select id, source, cell, tablet_types from _vt.vreplication where workflow = %a",
		sqltypes.StringBindVariable(workflow))
	require.NoError(t, err)
	blsStr := fmt.Sprintf(`keyspace:"%s" shard:"%s" filter:{rules:{match:"customer" filter:"select * from customer"} rules:{match:"corder" filter:"select * from corder"}}`,
		keyspace, shard)
	id := 1
	selectRes := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|source|cell|tablet_types",
			"int64|varchar|varchar|varchar",
		),
		fmt.Sprintf("%d|%s|%s|%s", id, blsStr, cell, tabletTypes),
	)
	idQuery := fmt.Sprintf("select id from _vt.vreplication where id = %d", id)
	idRes := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id",
			"int64",
		),
		fmt.Sprintf("%d", id),
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
				Cells:    cell,
			},
			query: fmt.Sprintf(`update _vt.vreplication set source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}}', cell = '%s', tablet_types = '' where id in (%d)`,
				keyspace, shard, cell, id),
		},
		{
			name: "update tablet_types",
			request: &tabletmanagerdatapb.UpdateVRWorkflowRequest{
				Workflow:    workflow,
				TabletTypes: tabletTypes,
			},
			query: fmt.Sprintf(`update _vt.vreplication set source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}}', cell = '', tablet_types = '%s' where id in (%d)`,
				keyspace, shard, tabletTypes, id),
		},
		{
			name: "update on_ddl",
			request: &tabletmanagerdatapb.UpdateVRWorkflowRequest{
				Workflow: workflow,
				OnDdl:    binlogdatapb.OnDDLAction_EXEC,
			},
			query: fmt.Sprintf(`update _vt.vreplication set source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}} on_ddl:%s', cell = '', tablet_types = '' where id in (%d)`,
				keyspace, shard, binlogdatapb.OnDDLAction_name[int32(binlogdatapb.OnDDLAction_EXEC)], id),
		},
		{
			name: "update cell,tablet_types,on_ddl",
			request: &tabletmanagerdatapb.UpdateVRWorkflowRequest{
				Workflow:    workflow,
				Cells:       cell,
				TabletTypes: tabletTypes,
				OnDdl:       binlogdatapb.OnDDLAction_EXEC,
			},
			query: fmt.Sprintf(`update _vt.vreplication set source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}} on_ddl:%s', cell = '%s', tablet_types = '%s' where id in (%d)`,
				keyspace, shard, binlogdatapb.OnDDLAction_name[int32(binlogdatapb.OnDDLAction_EXEC)], cell, tabletTypes, id),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NotNil(t, tt.request, "No request provided")
			require.NotEqual(t, "", tt.query, "No expected query provided")

			dbClient.ExpectRequest(fmt.Sprintf("select * from _vt.vreplication where db_name='%s'", dbName), &sqltypes.Result{}, nil)
			dbClient.ExpectRequest(fmt.Sprintf("use %s", sidecardb.DefaultName), &sqltypes.Result{}, nil)
			vre := vreplication.NewSimpleTestEngine(ts, cell, mysqld, dbClientFactory, dbClientFactory, dbName, nil)
			vre.Open(context.Background())
			defer vre.Close()
			tm := &TabletManager{
				MysqlDaemon:         mysqld,
				DBConfigs:           dbconfigs.NewTestDBConfigs(cp, cp, dbName),
				QueryServiceControl: tabletservermock.NewController(),
				VREngine:            vre,
			}
			dbClient.ExpectRequest(selectQuery, selectRes, nil)

			dbClient.ExpectRequest(fmt.Sprintf("use %s", sidecardb.DefaultName), &sqltypes.Result{}, nil)
			dbClient.ExpectRequest(idQuery, idRes, nil)
			dbClient.ExpectRequest(tt.query, &sqltypes.Result{RowsAffected: 1}, shortCircuitErr)
			_, err = tm.UpdateVRWorkflow(ctx, tt.request)
			dbClient.Wait()
			require.ErrorIs(t, err, shortCircuitErr)
		})
	}
}
