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
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"unicode"

	"github.com/buger/jsonparser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
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
	vr := newVReplicator(id, bls, vsclient, stats, dbClient, env.Mysqld, playerEngine)
	getActionsSQLf := "select action from _vt.post_copy_action where vrepl_id=%d and table_name='%s'"
	getCurrentDDL := func(tableName string) string {
		req := &tabletmanagerdatapb.GetSchemaRequest{Tables: []string{tableName}}
		sd, err := env.Mysqld.GetSchema(ctx, dbName, req)
		require.NoError(t, err)
		require.Equal(t, 1, len(sd.TableDefinitions))
		return removeVersionDifferences(sd.TableDefinitions[0].Schema)
	}
	_, err = dbClient.ExecuteFetch("use "+dbName, 1)
	require.NoError(t, err)

	tests := []struct {
		name         string
		tableName    string
		initialDDL   string
		strippedDDL  string
		actionDDL    string
		WorkflowType int32
		wantErr      string
	}{
		{
			name:         "0SK",
			tableName:    "t1",
			initialDDL:   "create table t1 (id int not null, primary key(id))",
			strippedDDL:  "create table t1 (id int not null, primary key(id))",
			WorkflowType: int32(binlogdatapb.VReplicationWorkflowType_MoveTables),
		},
		{
			name:         "1SK:Materialize",
			tableName:    "t1",
			initialDDL:   "create table t1 (id int not null, c1 int default null, primary key(id), key c1 (c1))",
			strippedDDL:  "create table t1 (id int not null, c1 int default null, primary key(id), key c1 (c1))",
			WorkflowType: int32(binlogdatapb.VReplicationWorkflowType_Materialize),
			wantErr:      "temporarily removing secondary keys is only supported for MoveTables, Migrate, and Reshard workflows",
		},
		{
			name:         "1SK:OnlineDDL",
			tableName:    "t1",
			initialDDL:   "create table t1 (id int not null, c1 int default null, primary key(id), key c1 (c1))",
			strippedDDL:  "create table t1 (id int not null, c1 int default null, primary key(id), key c1 (c1))",
			WorkflowType: int32(binlogdatapb.VReplicationWorkflowType_OnlineDDL),
			wantErr:      "temporarily removing secondary keys is only supported for MoveTables, Migrate, and Reshard workflows",
		},
		{
			name:         "1SK",
			tableName:    "t1",
			initialDDL:   "create table t1 (id int not null, c1 int default null, primary key(id), key c1 (c1))",
			strippedDDL:  "create table t1 (id int not null, c1 int default null, primary key(id))",
			actionDDL:    "alter table %s.t1 add key c1 (c1)",
			WorkflowType: int32(binlogdatapb.VReplicationWorkflowType_Reshard),
		},
		{
			name:         "2SK",
			tableName:    "t1",
			initialDDL:   "create table t1 (id int not null, c1 int default null, c2 int default null, primary key(id), key c1 (c1), key c2 (c2))",
			strippedDDL:  "create table t1 (id int not null, c1 int default null, c2 int default null, primary key(id))",
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
			name:         "0FK2tSK",
			tableName:    "t1",
			initialDDL:   "create table t1 (id int not null, c1 varchar(10) default null, c2 varchar(10) default null, key c1_c2 (c1,c2), key c2 (c2))",
			strippedDDL:  "create table t1 (id int not null, c1 varchar(10) default null, c2 varchar(10) default null)",
			actionDDL:    "alter table %s.t1 add key c1_c2 (c1, c2), add key c2 (c2)",
			WorkflowType: int32(binlogdatapb.VReplicationWorkflowType_MoveTables),
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

			// If the table has any secondary keys, drop them and
			// store an ALTER TABLE statement to re-add them after
			// the table is copied.
			err = vr.stashSecondaryKeys(ctx, tcase.tableName)
			if tcase.wantErr != "" {
				require.EqualError(t, err, tcase.wantErr)
			} else {
				require.NoError(t, err)
			}

			// Confirm that the table now has no secondary keys.
			tcase.strippedDDL = removeVersionDifferences(tcase.strippedDDL)
			currentDDL := getCurrentDDL(tcase.tableName)
			require.True(t, strings.EqualFold(stripCruft(tcase.strippedDDL), stripCruft(currentDDL)),
				"Expected: %s\n     Got: %s", forError(tcase.strippedDDL), forError(currentDDL))

			// If we expect post-copy SQL actions, then ensure
			// that the stored DDL matches what we expect.
			if tcase.actionDDL != "" {
				res, err := dbClient.ExecuteFetch(fmt.Sprintf(getActionsSQLf, id, tcase.tableName), 1)
				require.Equal(t, 1, len(res.Rows))
				require.NoError(t, err)
				val, err := res.Rows[0][0].ToBytes()
				require.NoError(t, err)
				alter, err := jsonparser.GetString(val, "task")
				require.NoError(t, err)
				require.True(t, strings.EqualFold(stripCruft(fmt.Sprintf(tcase.actionDDL, dbName)), stripCruft(alter)),
					"Expected: %s\n     Got: %s", forError(fmt.Sprintf(tcase.actionDDL, dbName)), forError(alter))
			}

			// Confirm that the final DDL matches the initial DDL.
			err = vr.execPostCopyActions(ctx, tcase.tableName)
			require.NoError(t, err)
			currentDDL = getCurrentDDL(tcase.tableName)
			require.True(t, strings.EqualFold(stripCruft(tcase.initialDDL), stripCruft(currentDDL)),
				"Expected: %s\n     Got: %s", forError(tcase.initialDDL), forError(currentDDL))

		})
	}
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
