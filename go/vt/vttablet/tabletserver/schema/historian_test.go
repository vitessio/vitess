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

package schema

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

func getTable(name string, fieldNames []string, fieldTypes []querypb.Type, pks []int64) *binlogdatapb.MinimalTable {
	if name == "" || len(fieldNames) == 0 || len(fieldNames) != len(fieldTypes) || len(pks) == 0 {
		return nil
	}
	fields := []*querypb.Field{}
	for i := range fieldNames {
		typ := fieldTypes[i]
		cs := collations.CollationForType(typ, collations.MySQL8().DefaultConnectionCharset())
		fields = append(fields, &querypb.Field{
			Name:    fieldNames[i],
			Type:    typ,
			Charset: uint32(cs),
			Flags:   mysql.FlagsForColumn(typ, cs),
			Table:   name,
		})
	}
	table := &binlogdatapb.MinimalTable{
		Name:      name,
		Fields:    fields,
		PKColumns: pks,
	}
	return table
}

func getDbSchemaBlob(t *testing.T, tables map[string]*binlogdatapb.MinimalTable) string {
	dbSchema := &binlogdatapb.MinimalSchema{
		Tables: []*binlogdatapb.MinimalTable{},
	}
	for name, table := range tables {
		t := &binlogdatapb.MinimalTable{
			Name:   name,
			Fields: table.Fields,
		}
		pks := make([]int64, 0)
		for _, pk := range table.PKColumns {
			pks = append(pks, int64(pk))
		}
		t.PKColumns = pks
		dbSchema.Tables = append(dbSchema.Tables, t)
	}
	blob, err := dbSchema.MarshalVT()
	require.NoError(t, err)
	return string(blob)
}

func TestHistorian(t *testing.T) {
	se, db, cancel := getTestSchemaEngine(t, 0)
	defer cancel()

	se.EnableHistorian(false)
	require.Nil(t, se.RegisterVersionEvent())
	gtidPrefix := "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:"
	gtid1 := gtidPrefix + "1-10"
	ddl1 := "create table tracker_test (id int)"
	ts1 := int64(1427325876)
	_, _, _ = ddl1, ts1, db
	_, err := se.GetTableForPos(sqlparser.NewIdentifierCS("t1"), gtid1)
	require.Equal(t, "table t1 not found in vttablet schema", err.Error())
	tab, err := se.GetTableForPos(sqlparser.NewIdentifierCS("dual"), gtid1)
	require.NoError(t, err)
	require.Equal(t, `name:"dual"`, fmt.Sprintf("%v", tab))
	se.EnableHistorian(true)
	_, err = se.GetTableForPos(sqlparser.NewIdentifierCS("t1"), gtid1)
	require.Equal(t, "table t1 not found in vttablet schema", err.Error())
	var blob1 string

	fields := []*querypb.Field{{
		Name: "id",
		Type: sqltypes.Int32,
	}, {
		Name: "pos",
		Type: sqltypes.VarBinary,
	}, {
		Name: "ddl",
		Type: sqltypes.VarBinary,
	}, {
		Name: "time_updated",
		Type: sqltypes.Int32,
	}, {
		Name: "schemax",
		Type: sqltypes.Blob,
	}}

	table := getTable("t1", []string{"id1", "id2"}, []querypb.Type{querypb.Type_INT32, querypb.Type_INT32}, []int64{0})
	tables := make(map[string]*binlogdatapb.MinimalTable)
	tables["t1"] = table
	blob1 = getDbSchemaBlob(t, tables)
	db.AddQuery("select id, pos, ddl, time_updated, schemax from _vt.schema_version where id > 0 order by id asc", &sqltypes.Result{
		Fields: fields,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewInt32(1), sqltypes.NewVarBinary(gtid1), sqltypes.NewVarBinary(ddl1), sqltypes.NewInt32(int32(ts1)), sqltypes.NewVarBinary(blob1)},
		},
	})
	require.Nil(t, se.RegisterVersionEvent())
	exp1 := `name:"t1" fields:{name:"id1" type:INT32 table:"t1" charset:63 flags:32768} fields:{name:"id2" type:INT32 table:"t1" charset:63 flags:32768} p_k_columns:0`
	tab, err = se.GetTableForPos(sqlparser.NewIdentifierCS("t1"), gtid1)
	require.NoError(t, err)
	require.Equal(t, exp1, fmt.Sprintf("%v", tab))
	gtid2 := gtidPrefix + "1-20"
	_, err = se.GetTableForPos(sqlparser.NewIdentifierCS("t1"), gtid2)
	require.Equal(t, "table t1 not found in vttablet schema", err.Error())

	table = getTable("t1", []string{"id1", "id2"}, []querypb.Type{querypb.Type_INT32, querypb.Type_VARBINARY}, []int64{0})
	tables["t1"] = table
	blob2 := getDbSchemaBlob(t, tables)
	ddl2 := "alter table t1 modify column id2 varbinary"
	ts2 := ts1 + 100
	db.AddQuery("select id, pos, ddl, time_updated, schemax from _vt.schema_version where id > 1 order by id asc", &sqltypes.Result{
		Fields: fields,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewInt32(2), sqltypes.NewVarBinary(gtid2), sqltypes.NewVarBinary(ddl2), sqltypes.NewInt32(int32(ts2)), sqltypes.NewVarBinary(blob2)},
		},
	})
	require.Nil(t, se.RegisterVersionEvent())
	exp2 := `name:"t1" fields:{name:"id1" type:INT32 table:"t1" charset:63 flags:32768} fields:{name:"id2" type:VARBINARY table:"t1" charset:63 flags:128} p_k_columns:0`
	tab, err = se.GetTableForPos(sqlparser.NewIdentifierCS("t1"), gtid2)
	require.NoError(t, err)
	require.Equal(t, exp2, fmt.Sprintf("%v", tab))
	gtid3 := gtidPrefix + "1-30"
	_, err = se.GetTableForPos(sqlparser.NewIdentifierCS("t1"), gtid3)
	require.Equal(t, "table t1 not found in vttablet schema", err.Error())

	table = getTable("t1", []string{"id1", "id2", "id3"}, []querypb.Type{querypb.Type_INT32, querypb.Type_VARBINARY, querypb.Type_INT32}, []int64{0})
	tables["t1"] = table
	blob3 := getDbSchemaBlob(t, tables)
	ddl3 := "alter table t1 add column id3 int"
	ts3 := ts2 + 100
	db.AddQuery("select id, pos, ddl, time_updated, schemax from _vt.schema_version where id > 2 order by id asc", &sqltypes.Result{
		Fields: fields,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewInt32(3), sqltypes.NewVarBinary(gtid3), sqltypes.NewVarBinary(ddl3), sqltypes.NewInt32(int32(ts3)), sqltypes.NewVarBinary(blob3)},
		},
	})
	require.Nil(t, se.RegisterVersionEvent())
	exp3 := `name:"t1" fields:{name:"id1" type:INT32 table:"t1" charset:63 flags:32768} fields:{name:"id2" type:VARBINARY table:"t1" charset:63 flags:128} fields:{name:"id3" type:INT32 table:"t1" charset:63 flags:32768} p_k_columns:0`
	tab, err = se.GetTableForPos(sqlparser.NewIdentifierCS("t1"), gtid3)
	require.NoError(t, err)
	require.Equal(t, exp3, fmt.Sprintf("%v", tab))

	tab, err = se.GetTableForPos(sqlparser.NewIdentifierCS("t1"), gtid1)
	require.NoError(t, err)
	require.Equal(t, exp1, fmt.Sprintf("%v", tab))
	tab, err = se.GetTableForPos(sqlparser.NewIdentifierCS("t1"), gtid2)
	require.NoError(t, err)
	require.Equal(t, exp2, fmt.Sprintf("%v", tab))
	tab, err = se.GetTableForPos(sqlparser.NewIdentifierCS("t1"), gtid3)
	require.NoError(t, err)
	require.Equal(t, exp3, fmt.Sprintf("%v", tab))
}

func TestHistorianPurgeOldSchemas(t *testing.T) {
	schemaVersionMaxAgeSeconds := 3600 // 1 hour
	se, db, cancel := getTestSchemaEngine(t, int64(schemaVersionMaxAgeSeconds))
	defer cancel()

	gtidPrefix := "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:"
	gtid1 := gtidPrefix + "1-10"
	ddl1 := "create table tracker_test (id int)"
	// create the first record 1 day ago so it gets purged from memory
	ts1 := time.Now().Add(time.Duration(-24) * time.Hour)
	_, _, _ = ddl1, ts1, db
	se.EnableHistorian(true)
	_, err := se.GetTableForPos(sqlparser.NewIdentifierCS("t1"), gtid1)
	require.Equal(t, "table t1 not found in vttablet schema", err.Error())
	var blob1 string

	fields := []*querypb.Field{{
		Name: "id",
		Type: sqltypes.Int32,
	}, {
		Name: "pos",
		Type: sqltypes.VarBinary,
	}, {
		Name: "ddl",
		Type: sqltypes.VarBinary,
	}, {
		Name: "time_updated",
		Type: sqltypes.Int32,
	}, {
		Name: "schemax",
		Type: sqltypes.Blob,
	}}

	table := getTable("t1", []string{"id1", "id2"}, []querypb.Type{querypb.Type_INT32, querypb.Type_INT32}, []int64{0})
	tables := make(map[string]*binlogdatapb.MinimalTable)
	tables["t1"] = table
	blob1 = getDbSchemaBlob(t, tables)
	db.AddQueryPattern("select id, pos, ddl, time_updated, schemax from _vt\\.schema_version where time_updated \\>.*", &sqltypes.Result{
		Fields: fields,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewInt32(1), sqltypes.NewVarBinary(gtid1), sqltypes.NewVarBinary(ddl1), sqltypes.NewInt32(int32(ts1.Unix())), sqltypes.NewVarBinary(blob1)},
		},
	})
	require.Nil(t, se.RegisterVersionEvent())
	_, err = se.GetTableForPos(sqlparser.NewIdentifierCS("t1"), gtid1)
	// validate the old schema has been purged
	require.Equal(t, "table t1 not found in vttablet schema", err.Error())
	require.Equal(t, 0, len(se.historian.schemas))

	// add a second schema record row with a time_updated that won't be purged
	gtid2 := gtidPrefix + "1-20"
	_, err = se.GetTableForPos(sqlparser.NewIdentifierCS("t1"), gtid2)
	require.Equal(t, "table t1 not found in vttablet schema", err.Error())

	table = getTable("t1", []string{"id1", "id2"}, []querypb.Type{querypb.Type_INT32, querypb.Type_VARBINARY}, []int64{0})
	tables["t1"] = table
	blob2 := getDbSchemaBlob(t, tables)
	ddl2 := "alter table t1 modify column id2 varbinary"
	// set time_updated younger than the cutoff from historian.schemaMaxAgeSeconds
	ts2 := time.Now().Add(time.Duration(-60) * time.Second)
	db.AddQuery("select id, pos, ddl, time_updated, schemax from _vt.schema_version where id > 1 order by id asc", &sqltypes.Result{
		Fields: fields,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewInt32(2), sqltypes.NewVarBinary(gtid2), sqltypes.NewVarBinary(ddl2), sqltypes.NewInt32(int32(ts2.Unix())), sqltypes.NewVarBinary(blob2)},
		},
	})
	require.Nil(t, se.RegisterVersionEvent())
	exp2 := `name:"t1" fields:{name:"id1" type:INT32 table:"t1" charset:63 flags:32768} fields:{name:"id2" type:VARBINARY table:"t1" charset:63 flags:128} p_k_columns:0`
	tab, err := se.GetTableForPos(sqlparser.NewIdentifierCS("t1"), gtid2)
	require.NoError(t, err)
	require.Equal(t, exp2, fmt.Sprintf("%v", tab))
	require.Equal(t, 1, len(se.historian.schemas))
}
