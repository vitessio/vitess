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

package schema

import (
	"expvar"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema/schematest"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestOpenAndReload(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}

	// pre-advance to above the default 1427325875.
	db.AddQuery("select unix_timestamp()", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"t",
		"int64"),
		"1427325876",
	))
	se := newEngine(10, 10*time.Second, 10*time.Second, true, db)
	se.Open()
	defer se.Close()

	want := initialSchema()
	assert.Equal(t, want, se.GetSchema())

	// Advance time some more.
	db.AddQuery("select unix_timestamp()", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"t",
		"int64"),
		"1427325877",
	))
	// Modify test_table_03
	// Add test_table_04
	// Drop msg
	db.AddQuery(mysql.BaseShowTables, &sqltypes.Result{
		Fields: mysql.BaseShowTablesFields,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow("test_table_01", false, ""),
			mysql.BaseShowTablesRow("test_table_02", false, ""),
			{
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("test_table_03")),
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("BASE TABLE")),
				// Match the timestamp.
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("1427325877")),
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("")),
			},
			// test_table_04 will in spite of older timestamp because it doesn't exist yet.
			mysql.BaseShowTablesRow("test_table_04", false, ""),
			mysql.BaseShowTablesRow("seq", false, "vitess_sequence"),
		},
	})
	db.AddQuery("select * from test_table_03 where 1 != 1", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "pk1",
			Type: sqltypes.Int32,
		}, {
			Name: "pk2",
			Type: sqltypes.Int32,
		}, {
			Name: "val",
			Type: sqltypes.Int32,
		}},
	})
	db.AddQuery("select * from test_table_04 where 1 != 1", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "pk",
			Type: sqltypes.Int32,
		}},
	})
	db.AddQuery(mysql.BaseShowPrimary, &sqltypes.Result{
		Fields: mysql.ShowPrimaryFields,
		Rows: [][]sqltypes.Value{
			mysql.ShowPrimaryRow("test_table_01", "pk"),
			mysql.ShowPrimaryRow("test_table_02", "pk"),
			mysql.ShowPrimaryRow("test_table_03", "pk1"),
			mysql.ShowPrimaryRow("test_table_03", "pk2"),
			mysql.ShowPrimaryRow("test_table_04", "pk"),
			mysql.ShowPrimaryRow("seq", "id"),
		},
	})

	firstTime := true
	notifier := func(full map[string]*Table, created, altered, dropped []string) {
		if firstTime {
			firstTime = false
			sort.Strings(created)
			assert.Equal(t, []string{"dual", "msg", "seq", "test_table_01", "test_table_02", "test_table_03"}, created)
			assert.Equal(t, []string(nil), altered)
			assert.Equal(t, []string(nil), dropped)
		} else {
			assert.Equal(t, []string{"test_table_04"}, created)
			assert.Equal(t, []string{"test_table_03"}, altered)
			sort.Strings(dropped)
			assert.Equal(t, []string{"msg"}, dropped)
		}
	}
	se.RegisterNotifier("test", notifier)
	err := se.Reload(context.Background())
	require.NoError(t, err)

	want["test_table_03"] = &Table{
		Name: sqlparser.NewTableIdent("test_table_03"),
		Fields: []*querypb.Field{{
			Name: "pk1",
			Type: sqltypes.Int32,
		}, {
			Name: "pk2",
			Type: sqltypes.Int32,
		}, {
			Name: "val",
			Type: sqltypes.Int32,
		}},
		PKColumns: []int{0, 1},
	}
	want["test_table_04"] = &Table{
		Name: sqlparser.NewTableIdent("test_table_04"),
		Fields: []*querypb.Field{{
			Name: "pk",
			Type: sqltypes.Int32,
		}},
		PKColumns: []int{0},
	}
	delete(want, "msg")
	assert.Equal(t, want, se.GetSchema())

	//ReloadAt tests
	pos1, err := mysql.DecodePosition("MariaDB/0-41983-20")
	require.NoError(t, err)
	pos2, err := mysql.DecodePosition("MariaDB/0-41983-40")
	require.NoError(t, err)
	se.UnregisterNotifier("test")

	err = se.ReloadAt(context.Background(), mysql.Position{})
	require.NoError(t, err)
	assert.Equal(t, want, se.GetSchema())

	err = se.ReloadAt(context.Background(), pos1)
	require.NoError(t, err)
	assert.Equal(t, want, se.GetSchema())

	// delete table test_table_03
	db.AddQuery(mysql.BaseShowTables, &sqltypes.Result{
		Fields: mysql.BaseShowTablesFields,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow("test_table_01", false, ""),
			mysql.BaseShowTablesRow("test_table_02", false, ""),
			// test_table_04 will in spite of older timestamp because it doesn't exist yet.
			mysql.BaseShowTablesRow("test_table_04", false, ""),
			mysql.BaseShowTablesRow("seq", false, "vitess_sequence"),
		},
	})
	db.AddQuery(mysql.BaseShowPrimary, &sqltypes.Result{
		Fields: mysql.ShowPrimaryFields,
		Rows: [][]sqltypes.Value{
			mysql.ShowPrimaryRow("test_table_01", "pk"),
			mysql.ShowPrimaryRow("test_table_02", "pk"),
			mysql.ShowPrimaryRow("test_table_04", "pk"),
			mysql.ShowPrimaryRow("seq", "id"),
		},
	})
	err = se.ReloadAt(context.Background(), pos1)
	require.NoError(t, err)
	assert.Equal(t, want, se.GetSchema())

	delete(want, "test_table_03")
	err = se.ReloadAt(context.Background(), pos2)
	require.NoError(t, err)
	assert.Equal(t, want, se.GetSchema())
}

func TestOpenFailedDueToMissMySQLTime(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("select unix_timestamp()", &sqltypes.Result{
		// Make this query fail by returning 2 values.
		Fields: []*querypb.Field{
			{Type: sqltypes.Uint64},
		},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("1427325875")},
			{sqltypes.NewVarBinary("1427325875")},
		},
	})
	se := newEngine(10, 1*time.Second, 1*time.Second, false, db)
	err := se.Open()
	want := "could not get MySQL time"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("se.Open: %v, want %s", err, want)
	}
}

func TestOpenFailedDueToIncorrectMysqlRowNum(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("select unix_timestamp()", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Type: sqltypes.Uint64,
		}},
		Rows: [][]sqltypes.Value{
			// make this query fail by returning NULL
			{sqltypes.NULL},
		},
	})
	se := newEngine(10, 1*time.Second, 1*time.Second, false, db)
	err := se.Open()
	want := "unexpected result for MySQL time"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("se.Open: %v, want %s", err, want)
	}
}

func TestOpenFailedDueToInvalidTimeFormat(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("select unix_timestamp()", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Type: sqltypes.VarChar,
		}},
		Rows: [][]sqltypes.Value{
			// make safety check fail, invalid time format
			{sqltypes.NewVarBinary("invalid_time")},
		},
	})
	se := newEngine(10, 1*time.Second, 1*time.Second, false, db)
	err := se.Open()
	want := "could not parse time"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("se.Open: %v, want %s", err, want)
	}
}

func TestOpenFailedDueToExecErr(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}
	db.AddRejectedQuery(mysql.BaseShowTables, fmt.Errorf("injected error"))
	se := newEngine(10, 1*time.Second, 1*time.Second, false, db)
	err := se.Open()
	want := "injected error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("se.Open: %v, want %s", err, want)
	}
}

func TestOpenFailedDueToTableErr(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}
	db.AddQuery(mysql.BaseShowTables, &sqltypes.Result{
		Fields: mysql.BaseShowTablesFields,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow("test_table", false, ""),
		},
	})
	db.AddQuery("select * from test_table where 1 != 1", &sqltypes.Result{
		// this will cause NewTable error, as it expects zero rows.
		Fields: []*querypb.Field{
			{
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("")},
		},
	})
	se := newEngine(10, 1*time.Second, 1*time.Second, false, db)
	err := se.Open()
	want := "Row count exceeded"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("se.Open: %v, want %s", err, want)
	}
}

func TestExportVars(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}
	se := newEngine(10, 1*time.Second, 1*time.Second, true, db)
	se.Open()
	defer se.Close()
	expvar.Do(func(kv expvar.KeyValue) {
		_ = kv.Value.String()
	})
}

func TestStatsURL(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range schematest.Queries() {
		db.AddQuery(query, result)
	}
	se := newEngine(10, 1*time.Second, 1*time.Second, true, db)
	se.Open()
	defer se.Close()

	request, _ := http.NewRequest("GET", "/debug/schema", nil)
	response := httptest.NewRecorder()
	se.handleDebugSchema(response, request)
}

func newEngine(queryCacheSize int, reloadTime time.Duration, idleTimeout time.Duration, strict bool, db *fakesqldb.DB) *Engine {
	config := tabletenv.NewDefaultConfig()
	config.QueryCacheSize = queryCacheSize
	config.SchemaReloadIntervalSeconds.Set(reloadTime)
	config.OltpReadPool.IdleTimeoutSeconds.Set(idleTimeout)
	config.OlapReadPool.IdleTimeoutSeconds.Set(idleTimeout)
	config.TxPool.IdleTimeoutSeconds.Set(idleTimeout)
	se := NewEngine(tabletenv.NewEnv(config, "SchemaTest"))
	se.InitDBConfig(newDBConfigs(db).DbaWithDB())
	return se
}

func newDBConfigs(db *fakesqldb.DB) *dbconfigs.DBConfigs {
	params, _ := db.ConnParams().MysqlParams()
	cp := *params
	return dbconfigs.NewTestDBConfigs(cp, cp, "")
}

func initialSchema() map[string]*Table {
	return map[string]*Table{
		"dual": {
			Name: sqlparser.NewTableIdent("dual"),
		},
		"test_table_01": {
			Name: sqlparser.NewTableIdent("test_table_01"),
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}},
			PKColumns: []int{0},
		},
		"test_table_02": {
			Name: sqlparser.NewTableIdent("test_table_02"),
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}},
			PKColumns: []int{0},
		},
		"test_table_03": {
			Name: sqlparser.NewTableIdent("test_table_03"),
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}},
			PKColumns: []int{0},
		},
		"seq": {
			Name: sqlparser.NewTableIdent("seq"),
			Type: Sequence,
			Fields: []*querypb.Field{{
				Name: "id",
				Type: sqltypes.Int32,
			}, {
				Name: "next_id",
				Type: sqltypes.Int64,
			}, {
				Name: "cache",
				Type: sqltypes.Int64,
			}, {
				Name: "increment",
				Type: sqltypes.Int64,
			}},
			PKColumns:    []int{0},
			SequenceInfo: &SequenceInfo{},
		},
		"msg": {
			Name: sqlparser.NewTableIdent("msg"),
			Type: Message,
			Fields: []*querypb.Field{{
				Name: "id",
				Type: sqltypes.Int64,
			}, {
				Name: "priority",
				Type: sqltypes.Int64,
			}, {
				Name: "time_next",
				Type: sqltypes.Int64,
			}, {
				Name: "epoch",
				Type: sqltypes.Int64,
			}, {
				Name: "time_acked",
				Type: sqltypes.Int64,
			}, {
				Name: "message",
				Type: sqltypes.Int64,
			}},
			PKColumns: []int{0},
			MessageInfo: &MessageInfo{
				Fields: []*querypb.Field{{
					Name: "id",
					Type: sqltypes.Int64,
				}, {
					Name: "message",
					Type: sqltypes.Int64,
				}},
				AckWaitDuration:    30 * time.Second,
				PurgeAfterDuration: 120 * time.Second,
				MinBackoff:         30 * time.Second,
				BatchSize:          1,
				CacheSize:          10,
				PollInterval:       30 * time.Second,
			},
		},
	}
}
