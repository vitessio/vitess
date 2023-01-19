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

package vreplication

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"

	"github.com/buger/jsonparser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/schemadiff"
)

func TestRecalculatePKColsInfoByColumnNames(t *testing.T) {
	tt := []struct {
		name             string
		colNames         []string
		colInfos         []*ColumnInfo
		expectPKColInfos []*ColumnInfo
	}{
		{
			name:             "trivial, single column",
			colNames:         []string{"c1"},
			colInfos:         []*ColumnInfo{{Name: "c1", IsPK: true}},
			expectPKColInfos: []*ColumnInfo{{Name: "c1", IsPK: true}},
		},
		{
			name:             "trivial, multiple columns",
			colNames:         []string{"c1"},
			colInfos:         []*ColumnInfo{{Name: "c1", IsPK: true}, {Name: "c2", IsPK: false}, {Name: "c3", IsPK: false}},
			expectPKColInfos: []*ColumnInfo{{Name: "c1", IsPK: true}, {Name: "c2", IsPK: false}, {Name: "c3", IsPK: false}},
		},
		{
			name:             "last column, multiple columns",
			colNames:         []string{"c3"},
			colInfos:         []*ColumnInfo{{Name: "c1", IsPK: false}, {Name: "c2", IsPK: false}, {Name: "c3", IsPK: true}},
			expectPKColInfos: []*ColumnInfo{{Name: "c3", IsPK: true}, {Name: "c1", IsPK: false}, {Name: "c2", IsPK: false}},
		},
		{
			name:             "change of key, single column",
			colNames:         []string{"c2"},
			colInfos:         []*ColumnInfo{{Name: "c1", IsPK: false}, {Name: "c2", IsPK: false}, {Name: "c3", IsPK: true}},
			expectPKColInfos: []*ColumnInfo{{Name: "c2", IsPK: true}, {Name: "c1", IsPK: false}, {Name: "c3", IsPK: false}},
		},
		{
			name:             "change of key, multiple columns",
			colNames:         []string{"c2", "c3"},
			colInfos:         []*ColumnInfo{{Name: "c1", IsPK: false}, {Name: "c2", IsPK: false}, {Name: "c3", IsPK: true}},
			expectPKColInfos: []*ColumnInfo{{Name: "c2", IsPK: true}, {Name: "c3", IsPK: true}, {Name: "c1", IsPK: false}},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			pkColInfos := recalculatePKColsInfoByColumnNames(tc.colNames, tc.colInfos)
			assert.Equal(t, tc.expectPKColInfos, pkColInfos)
		})
	}
}

func TestPrimaryKeyEquivalentColumns(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name    string
		table   string
		ddl     string
		want    []string
		wantErr bool
	}{
		{
			name:  "WITHPK",
			table: "withpk_t",
			ddl: `CREATE TABLE withpk_t (pkid INT NOT NULL AUTO_INCREMENT, col1 VARCHAR(25),
				PRIMARY KEY (pkid))`,
			want: []string{"pkid"},
		},
		{
			name:  "0PKE",
			table: "zeropke_t",
			ddl:   `CREATE TABLE zeropke_t (id INT NULL, col1 VARCHAR(25), UNIQUE KEY (id))`,
			want:  []string{},
		},
		{
			name:  "1PKE",
			table: "onepke_t",
			ddl:   `CREATE TABLE onepke_t (id INT NOT NULL, col1 VARCHAR(25), UNIQUE KEY (id))`,
			want:  []string{"id"},
		},
		{
			name:  "3MULTICOL1PKE",
			table: "onemcpke_t",
			ddl: `CREATE TABLE onemcpke_t (col1 VARCHAR(25) NOT NULL, col2 VARCHAR(25) NOT NULL,
					col3 VARCHAR(25) NOT NULL, col4 VARCHAR(25), UNIQUE KEY c4_c2_c1 (col4, col2, col1),
					UNIQUE KEY c1_c2 (col1, col2), UNIQUE KEY c1_c2_c4 (col1, col2, col4),
					KEY nc1_nc2 (col1, col2))`,
			want: []string{"col1", "col2"},
		},
		{
			name:  "3MULTICOL2PKE",
			table: "twomcpke_t",
			ddl: `CREATE TABLE twomcpke_t (col1 VARCHAR(25) NOT NULL, col2 VARCHAR(25) NOT NULL,
					col3 VARCHAR(25) NOT NULL, col4 VARCHAR(25), UNIQUE KEY (col4), UNIQUE KEY c4_c2_c1 (col4, col2, col1),
					UNIQUE KEY c1_c2_c3 (col1, col2, col3), UNIQUE KEY c1_c2 (col1, col2))`,
			want: []string{"col1", "col2"},
		},
		{
			name:  "1INTPKE1CHARPKE",
			table: "oneintpke1charpke_t",
			ddl: `CREATE TABLE oneintpke1charpke_t (col1 VARCHAR(25) NOT NULL, col2 VARCHAR(25) NOT NULL,
					col3 VARCHAR(25) NOT NULL, id1 INT NOT NULL, id2 INT NOT NULL, 
					UNIQUE KEY c1_c2 (col1, col2), UNIQUE KEY id1_id2 (id1, id2))`,
			want: []string{"id1", "id2"},
		},
		{
			name:  "INTINTVSVCHAR",
			table: "twointvsvcharpke_t",
			ddl: `CREATE TABLE twointvsvcharpke_t (col1 VARCHAR(25) NOT NULL, id1 INT NOT NULL, id2 INT NOT NULL, 
					UNIQUE KEY c1 (col1), UNIQUE KEY id1_id2 (id1, id2))`,
			want: []string{"id1", "id2"},
		},
		{
			name:  "TINYINTVSBIGINT",
			table: "tinyintvsbigint_t",
			ddl: `CREATE TABLE tinyintvsbigint_t (tid1 TINYINT NOT NULL, id1 INT NOT NULL, 
					UNIQUE KEY tid1 (tid1), UNIQUE KEY id1 (id1))`,
			want: []string{"tid1"},
		},
		{
			name:  "VCHARINTVSINT2VARCHAR",
			table: "vcharintvsinttwovchar_t",
			ddl: `CREATE TABLE vcharintvsinttwovchar_t (id1 INT NOT NULL, col1 VARCHAR(25) NOT NULL, col2 VARCHAR(25) NOT NULL,
					UNIQUE KEY col1_id1 (col1, id1), UNIQUE KEY id1_col1_col2 (id1, col1, col2))`,
			want: []string{"col1", "id1"},
		},
		{
			name:  "VCHARVSINT3",
			table: "vcharvsintthree_t",
			ddl: `CREATE TABLE vcharvsintthree_t (id1 INT NOT NULL, id2 INT NOT NULL, id3 INT NOT NULL, col1 VARCHAR(50) NOT NULL,
					UNIQUE KEY col1 (col1), UNIQUE KEY id1_id2_id3 (id1, id2, id3))`,
			want: []string{"id1", "id2", "id3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, env.Mysqld.ExecuteSuperQuery(ctx, tt.ddl))
			got, err := env.Mysqld.GetPrimaryKeyEquivalentColumns(ctx, env.Dbcfgs.DBName, tt.table)
			if (err != nil) != tt.wantErr {
				t.Errorf("Mysqld.GetPrimaryKeyEquivalentColumns() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Mysqld.GetPrimaryKeyEquivalentColumns() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestDeferSecondaryKeys confirms the behavior of the
// --defer-secondary-keys MoveTables/Migrate, and Reshard
// workflow/command flag.
//  1. We drop the secondary keys
//  2. We store the secondary key definitions for step 3
//  3. We add the secondary keys back after the rows are copied
func TestDeferSecondaryKeys(t *testing.T) {
	ctx := context.Background()
	tablet := addTablet(100)
	defer deleteTablet(tablet)
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "t1",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
	}
	id := uint32(1)
	vsclient := newTabletConnector(tablet)
	stats := binlogplayer.NewStats()
	dbClient := playerEngine.dbClientFactoryFiltered()
	err := dbClient.Connect()
	require.NoError(t, err)
	defer dbClient.Close()
	dbName := dbClient.DBName()
	// Ensure there's a dummy vreplication workflow record
	_, err = dbClient.ExecuteFetch(fmt.Sprintf("insert into _vt.vreplication (id, workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state, db_name) values (%d, 'test', '', '', 99999, 99999, 0, 0, 'Running', '%s') on duplicate key update workflow='test', source='', pos='', max_tps=99999, max_replication_lag=99999, time_updated=0, transaction_timestamp=0, state='Running', db_name='%s'",
		id, dbName, dbName), 1)
	require.NoError(t, err)
	defer func() {
		_, err = dbClient.ExecuteFetch(fmt.Sprintf("delete from _vt.vreplication where id = %d", id), 1)
		require.NoError(t, err)
	}()
	vr := newVReplicator(id, bls, vsclient, stats, dbClient, env.Mysqld, playerEngine)
	getActionsSQLf := "select action from _vt.post_copy_action where table_name='%s'"
	getCurrentDDL := func(tableName string) string {
		req := &tabletmanagerdatapb.GetSchemaRequest{Tables: []string{tableName}}
		sd, err := env.Mysqld.GetSchema(ctx, dbName, req)
		require.NoError(t, err)
		require.Equal(t, 1, len(sd.TableDefinitions))
		return removeVersionDifferences(sd.TableDefinitions[0].Schema)
	}
	_, err = dbClient.ExecuteFetch("use "+dbName, 1)
	require.NoError(t, err)
	diffHints := &schemadiff.DiffHints{
		StrictIndexOrdering: false,
	}

	tests := []struct {
		name                  string
		tableName             string
		initialDDL            string
		strippedDDL           string
		intermediateDDL       string
		actionDDL             string
		WorkflowType          int32
		wantStashErr          string
		wantExecErr           string
		expectFinalSchemaDiff bool
		postStashHook         func() error
	}{
		{
			name:         "0SK",
			tableName:    "t1",
			initialDDL:   "create table t1 (id int not null, primary key (id))",
			strippedDDL:  "create table t1 (id int not null, primary key (id))",
			WorkflowType: int32(binlogdatapb.VReplicationWorkflowType_MoveTables),
		},
		{
			name:         "1SK:Materialize",
			tableName:    "t1",
			initialDDL:   "create table t1 (id int not null, c1 int default null, primary key (id), key c1 (c1))",
			strippedDDL:  "create table t1 (id int not null, c1 int default null, primary key (id), key c1 (c1))",
			WorkflowType: int32(binlogdatapb.VReplicationWorkflowType_Materialize),
			wantStashErr: "deferring secondary key creation is not supported for Materialize workflows",
		},
		{
			name:         "1SK:OnlineDDL",
			tableName:    "t1",
			initialDDL:   "create table t1 (id int not null, c1 int default null, primary key (id), key c1 (c1))",
			strippedDDL:  "create table t1 (id int not null, c1 int default null, primary key (id), key c1 (c1))",
			WorkflowType: int32(binlogdatapb.VReplicationWorkflowType_OnlineDDL),
			wantStashErr: "deferring secondary key creation is not supported for OnlineDDL workflows",
		},
		{
			name:         "1SK",
			tableName:    "t1",
			initialDDL:   "create table t1 (id int not null, c1 int default null, primary key (id), key c1 (c1))",
			strippedDDL:  "create table t1 (id int not null, c1 int default null, primary key (id))",
			actionDDL:    "alter table %s.t1 add key c1 (c1)",
			WorkflowType: int32(binlogdatapb.VReplicationWorkflowType_Reshard),
		},
		{
			name:         "2SK",
			tableName:    "t1",
			initialDDL:   "create table t1 (id int not null, c1 int default null, c2 int default null, primary key (id), key c1 (c1), key c2 (c2))",
			strippedDDL:  "create table t1 (id int not null, c1 int default null, c2 int default null, primary key (id))",
			actionDDL:    "alter table %s.t1 add key c1 (c1), add key c2 (c2)",
			WorkflowType: int32(binlogdatapb.VReplicationWorkflowType_MoveTables),
		},
		{
			name:         "2tSK",
			tableName:    "t1",
			initialDDL:   "create table t1 (id int not null, c1 varchar(10) default null, c2 varchar(10) default null, primary key (id), key c1_c2 (c1,c2), key c2 (c2))",
			strippedDDL:  "create table t1 (id int not null, c1 varchar(10) default null, c2 varchar(10) default null, primary key (id))",
			actionDDL:    "alter table %s.t1 add key c1_c2 (c1, c2), add key c2 (c2)",
			WorkflowType: int32(binlogdatapb.VReplicationWorkflowType_MoveTables),
		},
		{
			name:         "2FPK2SK",
			tableName:    "t1",
			initialDDL:   "create table t1 (id int not null, c1 varchar(10) not null, c2 varchar(10) default null, primary key (id,c1), key c1_c2 (c1,c2), key c2 (c2))",
			strippedDDL:  "create table t1 (id int not null, c1 varchar(10) not null, c2 varchar(10) default null, primary key (id,c1))",
			actionDDL:    "alter table %s.t1 add key c1_c2 (c1, c2), add key c2 (c2)",
			WorkflowType: int32(binlogdatapb.VReplicationWorkflowType_MoveTables),
		},
		{
			name:         "3FPK1SK",
			tableName:    "t1",
			initialDDL:   "create table t1 (id int not null, c1 varchar(10) not null, c2 varchar(10) not null, primary key (id,c1,c2), key c2 (c2))",
			strippedDDL:  "create table t1 (id int not null, c1 varchar(10) not null, c2 varchar(10) not null, primary key (id,c1,c2))",
			actionDDL:    "alter table %s.t1 add key c2 (c2)",
			WorkflowType: int32(binlogdatapb.VReplicationWorkflowType_Reshard),
		},
		{
			name:        "3FPK1SK_ShardMerge",
			tableName:   "t1",
			initialDDL:  "create table t1 (id int not null, c1 varchar(10) not null, c2 varchar(10) not null, primary key (id,c1,c2), key c2 (c2))",
			strippedDDL: "create table t1 (id int not null, c1 varchar(10) not null, c2 varchar(10) not null, primary key (id,c1,c2))",
			actionDDL:   "alter table %s.t1 add key c2 (c2)",
			postStashHook: func() error {
				myid := id + 1000
				// Insert second vreplication record to simulate a second controller/vreplicator
				_, err = dbClient.ExecuteFetch(fmt.Sprintf("insert into _vt.vreplication (id, workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state, db_name) values (%d, 'test', '', '', 99999, 99999, 0, 0, 'Running', '%s')",
					myid, dbName), 1)
				if err != nil {
					return err
				}
				myvr := newVReplicator(myid, bls, vsclient, stats, dbClient, env.Mysqld, playerEngine)
				myvr.WorkflowType = int32(binlogdatapb.VReplicationWorkflowType_Reshard)
				// Insert second post copy action record to simulate a shard merge where you
				// have N controllers/replicators running for the same table on the tablet.
				// This forces a second row, which would otherwise not get created beacause
				// when this is called there's no secondary keys to stash anymore.
				addlAction, err := json.Marshal(PostCopyAction{
					Type: PostCopyActionSQL,
					Task: fmt.Sprintf("alter table %s.t1 add key c2 (c2)", dbName),
				})
				if err != nil {
					return err
				}
				_, err = dbClient.ExecuteFetch(fmt.Sprintf("insert into _vt.post_copy_action (vrepl_id, table_name, action) values (%d, 't1', '%s')",
					myid, string(addlAction)), 1)
				if err != nil {
					return err
				}
				err = myvr.execPostCopyActions(ctx, "t1")
				if err != nil {
					return err
				}
				return nil
			},
			WorkflowType: int32(binlogdatapb.VReplicationWorkflowType_Reshard),
		},
		{
			name:         "0FPK2tSK",
			tableName:    "t1",
			initialDDL:   "create table t1 (id int not null, c1 varchar(10) default null, c2 varchar(10) default null, key c1_c2 (c1,c2), key c2 (c2))",
			strippedDDL:  "create table t1 (id int not null, c1 varchar(10) default null, c2 varchar(10) default null)",
			actionDDL:    "alter table %s.t1 add key c1_c2 (c1, c2), add key c2 (c2)",
			WorkflowType: int32(binlogdatapb.VReplicationWorkflowType_MoveTables),
		},
		{
			name:            "2SKRetryNoErr",
			tableName:       "t1",
			initialDDL:      "create table t1 (id int not null, c1 int default null, c2 int default null, primary key (id), key c1 (c1), key c2 (c2))",
			strippedDDL:     "create table t1 (id int not null, c1 int default null, c2 int default null, primary key (id))",
			intermediateDDL: "alter table %s.t1 add key c1 (c1), add key c2 (c2)",
			actionDDL:       "alter table %s.t1 add key c1 (c1), add key c2 (c2)",
			WorkflowType:    int32(binlogdatapb.VReplicationWorkflowType_MoveTables),
		},
		{
			name:            "2SKRetryNoErr2",
			tableName:       "t1",
			initialDDL:      "create table t1 (id int not null, c1 int default null, c2 int default null, primary key (id), key c1 (c1), key c2 (c2))",
			strippedDDL:     "create table t1 (id int not null, c1 int default null, c2 int default null, primary key (id))",
			intermediateDDL: "alter table %s.t1 add key c2 (c2), add key c1 (c1)",
			actionDDL:       "alter table %s.t1 add key c1 (c1), add key c2 (c2)",
			WorkflowType:    int32(binlogdatapb.VReplicationWorkflowType_MoveTables),
		},
		{
			name:                  "SKSuperSetNoErr", // a superset of the original keys is allowed
			tableName:             "t1",
			initialDDL:            "create table t1 (id int not null, c1 int default null, c2 int default null, primary key (id), key c1 (c1), key c2 (c2))",
			strippedDDL:           "create table t1 (id int not null, c1 int default null, c2 int default null, primary key (id))",
			intermediateDDL:       "alter table %s.t1 add unique key c1_c2 (c1,c2), add key c2 (c2), add key c1 (c1)",
			actionDDL:             "alter table %s.t1 add key c1 (c1), add key c2 (c2)",
			WorkflowType:          int32(binlogdatapb.VReplicationWorkflowType_MoveTables),
			expectFinalSchemaDiff: true,
		},
		{
			name:            "2SKRetryErr",
			tableName:       "t1",
			initialDDL:      "create table t1 (id int not null, c1 int default null, c2 int default null, primary key (id), key c1 (c1), key c2 (c2))",
			strippedDDL:     "create table t1 (id int not null, c1 int default null, c2 int default null, primary key (id))",
			intermediateDDL: "alter table %s.t1 add key c2 (c2)",
			actionDDL:       "alter table %s.t1 add key c1 (c1), add key c2 (c2)",
			WorkflowType:    int32(binlogdatapb.VReplicationWorkflowType_MoveTables),
			wantExecErr:     "Duplicate key name 'c2' (errno 1061) (sqlstate 42000)",
		},
		{
			name:            "2SKRetryErr2",
			tableName:       "t1",
			initialDDL:      "create table t1 (id int not null, c1 int default null, c2 int default null, primary key (id), key c1 (c1), key c2 (c2))",
			strippedDDL:     "create table t1 (id int not null, c1 int default null, c2 int default null, primary key (id))",
			intermediateDDL: "alter table %s.t1 add key c1 (c1)",
			actionDDL:       "alter table %s.t1 add key c1 (c1), add key c2 (c2)",
			WorkflowType:    int32(binlogdatapb.VReplicationWorkflowType_MoveTables),
			wantExecErr:     "Duplicate key name 'c1' (errno 1061) (sqlstate 42000)",
		},
	}

	for _, tcase := range tests {
		t.Run(tcase.name, func(t *testing.T) {
			// Deferred secondary indexes are only supported for
			// MoveTables and Reshard workflows.
			vr.WorkflowType = tcase.WorkflowType

			// Create the table.
			_, err := dbClient.ExecuteFetch(tcase.initialDDL, 1)
			require.NoError(t, err)
			defer func() {
				_, err = dbClient.ExecuteFetch(fmt.Sprintf("drop table %s.%s", dbName, tcase.tableName), 1)
				require.NoError(t, err)
				_, err = dbClient.ExecuteFetch("delete from _vt.post_copy_action", 1)
				require.NoError(t, err)
			}()

			confirmNoSecondaryKeys := func() {
				// Confirm that the table now has no secondary keys.
				tcase.strippedDDL = removeVersionDifferences(tcase.strippedDDL)
				currentDDL := getCurrentDDL(tcase.tableName)
				require.True(t, strings.EqualFold(stripCruft(tcase.strippedDDL), stripCruft(currentDDL)),
					"Expected: %s\n     Got: %s", forError(tcase.strippedDDL), forError(currentDDL))
			}

			// If the table has any secondary keys, drop them and
			// store an ALTER TABLE statement to re-add them after
			// the table is copied.
			err = vr.stashSecondaryKeys(ctx, tcase.tableName)
			if tcase.wantStashErr != "" {
				require.EqualError(t, err, tcase.wantStashErr)
			} else {
				require.NoError(t, err)
			}
			confirmNoSecondaryKeys()

			if tcase.postStashHook != nil {
				err = tcase.postStashHook()
				require.NoError(t, err)

				// We should still NOT have any secondary keys because there's still
				// a running controller/vreplicator in the copy phase.
				confirmNoSecondaryKeys()
			}

			// If we expect post-copy SQL actions, then ensure
			// that the stored DDL matches what we expect.
			if tcase.actionDDL != "" {
				res, err := dbClient.ExecuteFetch(fmt.Sprintf(getActionsSQLf, tcase.tableName), 1)
				require.Equal(t, 1, len(res.Rows))
				require.NoError(t, err)
				val, err := res.Rows[0][0].ToBytes()
				require.NoError(t, err)
				alter, err := jsonparser.GetString(val, "task")
				require.NoError(t, err)
				require.True(t, strings.EqualFold(stripCruft(fmt.Sprintf(tcase.actionDDL, dbName)), stripCruft(alter)),
					"Expected: %s\n     Got: %s", forError(fmt.Sprintf(tcase.actionDDL, dbName)), forError(alter))
			}

			if tcase.intermediateDDL != "" {
				_, err := dbClient.ExecuteFetch(fmt.Sprintf(tcase.intermediateDDL, dbName), 1)
				require.NoError(t, err)
			}

			err = vr.execPostCopyActions(ctx, tcase.tableName)
			expectedPostCopyActionRecs := 0
			if tcase.wantExecErr != "" {
				require.Contains(t, err.Error(), tcase.wantExecErr)
				expectedPostCopyActionRecs = 1
			} else {
				require.NoError(t, err)
				// Confirm that the final DDL logically matches the initial DDL.
				// We do not require that the index definitions are in the same
				// order in the table schema.
				if !tcase.expectFinalSchemaDiff {
					currentDDL := getCurrentDDL(tcase.tableName)
					sdiff, err := schemadiff.DiffCreateTablesQueries(currentDDL, tcase.initialDDL, diffHints)
					require.NoError(t, err)
					require.Nil(t, sdiff, "Expected no schema difference but got: %s", sdiff.CanonicalStatementString())
				}
			}

			// Confirm that the post copy action record(s) are deleted when there's
			// no exec error or conversely that it still exists when there was
			// one.
			res, err := dbClient.ExecuteFetch(fmt.Sprintf(getActionsSQLf, tcase.tableName), expectedPostCopyActionRecs)
			require.NoError(t, err)
			require.Equal(t, expectedPostCopyActionRecs, len(res.Rows),
				"Expected %d post copy action records, got %d", expectedPostCopyActionRecs, len(res.Rows))
		})
	}
}

// TestCancelledDeferSecondaryKeys tests that the ALTER
// TABLE statement used to re-add secondary keys (when
// the --defer-secondary-keys flag was used), after
// copying all rows, is properly killed when the context
// is cancelled -- e.g. due to the VReplication engine
// closing for a tablet transition during a PRS.
func TestCancelledDeferSecondaryKeys(t *testing.T) {
	// Skip the test for MariaDB as it does not have
	// performance_schema enabled by default.
	version, err := mysqlctl.GetVersionString()
	require.NoError(t, err)
	flavor, _, err := mysqlctl.ParseVersionString(version)
	require.NoError(t, err)
	if flavor == mysqlctl.FlavorMariaDB {
		t.Skipf("Skipping test as it's not supported with %s", flavor)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tablet := addTablet(100)
	defer deleteTablet(tablet)
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "t1",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
	}
	// The test env uses the same factory for both dba and
	// filtered connections.
	dbconfigs.GlobalDBConfigs.Filtered.User = "vt_dba"
	id := uint32(1)
	vsclient := newTabletConnector(tablet)
	stats := binlogplayer.NewStats()
	dbaconn := playerEngine.dbClientFactoryDba()
	err = dbaconn.Connect()
	require.NoError(t, err)
	defer dbaconn.Close()
	dbClient := playerEngine.dbClientFactoryFiltered()
	err = dbClient.Connect()
	require.NoError(t, err)
	defer dbClient.Close()
	dbName := dbClient.DBName()
	// Ensure there's a dummy vreplication workflow record
	_, err = dbClient.ExecuteFetch(fmt.Sprintf("insert into _vt.vreplication (id, workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state, db_name) values (%d, 'test', '', '', 99999, 99999, 0, 0, 'Running', '%s') on duplicate key update workflow='test', source='', pos='', max_tps=99999, max_replication_lag=99999, time_updated=0, transaction_timestamp=0, state='Running', db_name='%s'",
		id, dbName, dbName), 1)
	require.NoError(t, err)
	defer func() {
		_, err = dbClient.ExecuteFetch(fmt.Sprintf("delete from _vt.vreplication where id = %d", id), 1)
		require.NoError(t, err)
	}()
	vr := newVReplicator(id, bls, vsclient, stats, dbClient, env.Mysqld, playerEngine)
	vr.WorkflowType = int32(binlogdatapb.VReplicationWorkflowType_MoveTables)
	getCurrentDDL := func(tableName string) string {
		req := &tabletmanagerdatapb.GetSchemaRequest{Tables: []string{tableName}}
		sd, err := env.Mysqld.GetSchema(context.Background(), dbName, req)
		require.NoError(t, err)
		require.Equal(t, 1, len(sd.TableDefinitions))
		return removeVersionDifferences(sd.TableDefinitions[0].Schema)
	}
	getActionsSQLf := "select action from _vt.post_copy_action where vrepl_id=%d and table_name='%s'"

	tableName := "t1"
	ddl := fmt.Sprintf("create table %s.t1 (id int not null, c1 int default null, c2 int default null, primary key(id), key c1 (c1), key c2 (c2))", dbName)
	withoutPKs := "create table t1 (id int not null, c1 int default null, c2 int default null, primary key(id))"
	alter := fmt.Sprintf("alter table %s.t1 add key c1 (c1), add key c2 (c2)", dbName)

	// Create the table.
	_, err = dbClient.ExecuteFetch(ddl, 1)
	require.NoError(t, err)

	// Setup the ALTER work.
	err = vr.stashSecondaryKeys(ctx, tableName)
	require.NoError(t, err)

	// Lock the table to block execution of the ALTER so
	// that we can be sure that it runs and we can KILL it.
	_, err = dbaconn.ExecuteFetch(fmt.Sprintf("lock table %s.%s write", dbName, tableName), 1)
	require.NoError(t, err)

	// The ALTER should block on the table lock.
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := vr.execPostCopyActions(ctx, tableName)
		assert.True(t, strings.EqualFold(err.Error(), fmt.Sprintf("EOF (errno 2013) (sqlstate HY000) during query: %s", alter)))
	}()

	// Confirm that the expected ALTER query is being attempted.
	query := fmt.Sprintf("select count(*) from performance_schema.events_statements_current where sql_text = '%s'", alter)
	waitForQueryResult(t, dbaconn, query, "1")

	// Cancel the context while the ALTER is running/blocked
	// and wait for it to be KILLed off.
	playerEngine.cancel()
	wg.Wait()

	_, err = dbaconn.ExecuteFetch("unlock tables", 1)
	assert.NoError(t, err)

	// Confirm that the ALTER to re-add the secondary keys
	// did not succeed.
	currentDDL := getCurrentDDL(tableName)
	assert.True(t, strings.EqualFold(stripCruft(withoutPKs), stripCruft(currentDDL)),
		"Expected: %s\n     Got: %s", forError(withoutPKs), forError(currentDDL))

	// Confirm that we successfully attempted to kill it.
	query = "select count(*) from performance_schema.events_statements_history where digest_text = 'KILL ?' and errors = 0"
	res, err := dbaconn.ExecuteFetch(query, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(res.Rows))
	// TODO: figure out why the KILL never shows up...
	//require.Equal(t, "1", res.Rows[0][0].ToString())

	// Confirm that the post copy action record still exists
	// so it will later be retried.
	res, err = dbClient.ExecuteFetch(fmt.Sprintf(getActionsSQLf, id, tableName), 1)
	require.NoError(t, err)
	require.Equal(t, 1, len(res.Rows))
}

// stripCruft removes all whitespace unicode chars and backticks.
func stripCruft(in string) string {
	out := strings.Builder{}
	for _, r := range in {
		if unicode.IsSpace(r) || r == '`' {
			continue
		}
		out.WriteRune(r)
	}
	return out.String()
}

// forError returns a string for humans to easily compare in
// in error messages.
func forError(in string) string {
	mid := strings.ToLower(in)
	// condense multiple spaces into one.
	mid = regexp.MustCompile(`\s+`).ReplaceAllString(mid, " ")
	sr := strings.NewReplacer(
		"\t", "",
		"\n", "",
		"\r", "",
		"`", "",
		"( ", "(",
		" )", ")",
	)
	return sr.Replace(mid)
}

// removeVersionDifferences removes portions of a CREATE TABLE statement
// that differ between versions:
//   - 8.0 no longer includes display widths for integer or year types
//   - MySQL and MariaDB versions differ in what table options they display
func removeVersionDifferences(in string) string {
	out := in
	var re *regexp.Regexp
	for _, baseType := range []string{"int", "year"} {
		re = regexp.MustCompile(fmt.Sprintf(`(?i)%s\(([0-9]*)?\)`, baseType))
		out = re.ReplaceAllString(out, baseType)
	}
	re = regexp.MustCompile(`(?i)engine[\s]*=[\s]*innodb.*$`)
	out = re.ReplaceAllString(out, "")
	return out
}

func waitForQueryResult(t *testing.T, dbc binlogplayer.DBClient, query, val string) {
	tmr := time.NewTimer(1 * time.Second)
	defer tmr.Stop()
	for {
		res, err := dbc.ExecuteFetch(query, 1)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(res.Rows))
		if res.Rows[0][0].ToString() == val {
			return
		}
		select {
		case <-tmr.C:
			t.Fatalf("query %s did not return expected value of %s", query, val)
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
}
