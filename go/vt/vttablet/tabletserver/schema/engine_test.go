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
	"context"
	"errors"
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

	"vitess.io/vitess/go/event/syslogger"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/dbconfigs"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema/schematest"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

const baseShowTablesPattern = `SELECT t\.table_name.*`

var mustMatch = utils.MustMatchFn(".Mutex")

func TestOpenAndReload(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)
	db.AddQueryPattern(baseShowTablesPattern,
		&sqltypes.Result{
			Fields:       mysql.BaseShowTablesFields,
			RowsAffected: 0,
			InsertID:     0,
			Rows: [][]sqltypes.Value{
				mysql.BaseShowTablesRow("test_table_01", false, ""),
				mysql.BaseShowTablesRow("test_table_02", false, ""),
				mysql.BaseShowTablesRow("test_table_03", false, ""),
				mysql.BaseShowTablesRow("seq", false, "vitess_sequence"),
				mysql.BaseShowTablesRow("msg", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
			},
			SessionStateChanges: "",
			StatusFlags:         0,
		})

	// advance to one second after the default 1427325875.
	db.AddQuery("select unix_timestamp()", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"t",
		"int64"),
		"1427325876",
	))
	firstReadRowsValue := 12
	AddFakeInnoDBReadRowsResult(db, firstReadRowsValue)
	se := newEngine(10, 10*time.Second, 10*time.Second, db)
	se.Open()
	defer se.Close()

	want := initialSchema()
	mustMatch(t, want, se.GetSchema())
	assert.Equal(t, int64(100), se.tableFileSizeGauge.Counts()["msg"])
	assert.Equal(t, int64(150), se.tableAllocatedSizeGauge.Counts()["msg"])

	// Advance time some more.
	db.AddQuery("select unix_timestamp()", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"t",
		"int64"),
		"1427325877",
	))
	assert.EqualValues(t, firstReadRowsValue, se.innoDbReadRowsCounter.Get())

	// Modify test_table_03
	// Add test_table_04
	// Drop msg
	db.AddQueryPattern(baseShowTablesPattern, &sqltypes.Result{
		Fields: mysql.BaseShowTablesFields,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow("test_table_01", false, ""),
			mysql.BaseShowTablesRow("test_table_02", false, ""),
			{
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("test_table_03")), // table_name
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("BASE TABLE")),    // table_type
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("1427325877")),      // unix_timestamp(t.create_time)
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("")),              // table_comment
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("128")),             // file_size
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("256")),             // allocated_size
			},
			// test_table_04 will in spite of older timestamp because it doesn't exist yet.
			mysql.BaseShowTablesRow("test_table_04", false, ""),
			mysql.BaseShowTablesRow("seq", false, "vitess_sequence"),
		},
	})
	db.MockQueriesForTable("test_table_03", &sqltypes.Result{
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

	db.MockQueriesForTable("test_table_04", &sqltypes.Result{
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
	secondReadRowsValue := 123
	AddFakeInnoDBReadRowsResult(db, secondReadRowsValue)

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

	assert.EqualValues(t, secondReadRowsValue, se.innoDbReadRowsCounter.Get())

	want["test_table_03"] = &Table{
		Name: sqlparser.NewIdentifierCS("test_table_03"),
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
		PKColumns:     []int{0, 1},
		CreateTime:    1427325877,
		FileSize:      128,
		AllocatedSize: 256,
	}
	want["test_table_04"] = &Table{
		Name: sqlparser.NewIdentifierCS("test_table_04"),
		Fields: []*querypb.Field{{
			Name: "pk",
			Type: sqltypes.Int32,
		}},
		PKColumns:     []int{0},
		CreateTime:    1427325875,
		FileSize:      100,
		AllocatedSize: 150,
	}
	delete(want, "msg")
	assert.Equal(t, want, se.GetSchema())
	assert.Equal(t, int64(0), se.tableAllocatedSizeGauge.Counts()["msg"])
	assert.Equal(t, int64(0), se.tableFileSizeGauge.Counts()["msg"])

	// ReloadAt tests
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

	db.AddQueryPattern(baseShowTablesPattern, &sqltypes.Result{
		Fields: mysql.BaseShowTablesFields,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow("test_table_01", false, ""),
			mysql.BaseShowTablesRow("test_table_02", false, ""),
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

func TestReloadWithSwappedTables(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)
	db.AddQueryPattern(baseShowTablesPattern,
		&sqltypes.Result{
			Fields:       mysql.BaseShowTablesFields,
			RowsAffected: 0,
			InsertID:     0,
			Rows: [][]sqltypes.Value{
				mysql.BaseShowTablesRow("test_table_01", false, ""),
				mysql.BaseShowTablesRow("test_table_02", false, ""),
				mysql.BaseShowTablesRow("test_table_03", false, ""),
				mysql.BaseShowTablesRow("seq", false, "vitess_sequence"),
				mysql.BaseShowTablesRow("msg", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
			},
			SessionStateChanges: "",
			StatusFlags:         0,
		})
	firstReadRowsValue := 12
	AddFakeInnoDBReadRowsResult(db, firstReadRowsValue)

	se := newEngine(10, 10*time.Second, 10*time.Second, db)
	se.Open()
	defer se.Close()
	want := initialSchema()
	mustMatch(t, want, se.GetSchema())

	// Add test_table_04 with a newer timestamp
	// Advance time some more.
	db.AddQuery("select unix_timestamp()", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"t",
		"int64"),
		"1427325876",
	))
	db.AddQueryPattern(baseShowTablesPattern, &sqltypes.Result{
		Fields: mysql.BaseShowTablesFields,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow("test_table_01", false, ""),
			mysql.BaseShowTablesRow("test_table_02", false, ""),
			mysql.BaseShowTablesRow("test_table_03", false, ""),
			{
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("test_table_04")),
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("BASE TABLE")),
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("1427325877")), // unix_timestamp(create_time)
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("")),
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("128")), // file_size
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("256")), // allocated_size
			},
			mysql.BaseShowTablesRow("seq", false, "vitess_sequence"),
			mysql.BaseShowTablesRow("msg", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
		},
	})
	db.MockQueriesForTable("test_table_04", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "mypk",
			Type: sqltypes.Int32,
		}},
	})
	db.AddQuery(mysql.BaseShowPrimary, &sqltypes.Result{
		Fields: mysql.ShowPrimaryFields,
		Rows: [][]sqltypes.Value{
			mysql.ShowPrimaryRow("test_table_01", "pk"),
			mysql.ShowPrimaryRow("test_table_02", "pk"),
			mysql.ShowPrimaryRow("test_table_03", "pk"),
			mysql.ShowPrimaryRow("test_table_04", "mypk"),
			mysql.ShowPrimaryRow("seq", "id"),
			mysql.ShowPrimaryRow("msg", "id"),
		},
	})
	err := se.Reload(context.Background())
	require.NoError(t, err)
	want["test_table_04"] = &Table{
		Name: sqlparser.NewIdentifierCS("test_table_04"),
		Fields: []*querypb.Field{{
			Name: "mypk",
			Type: sqltypes.Int32,
		}},
		PKColumns:     []int{0},
		CreateTime:    1427325877,
		FileSize:      128,
		AllocatedSize: 256,
	}

	mustMatch(t, want, se.GetSchema())

	// swap test_table_03 and test_table_04
	// Advance time some more.
	db.AddQuery("select unix_timestamp()", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"t",
		"int64"),
		"1427325877",
	))
	db.AddQueryPattern(baseShowTablesPattern, &sqltypes.Result{
		Fields: mysql.BaseShowTablesFields,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow("test_table_01", false, ""),
			mysql.BaseShowTablesRow("test_table_02", false, ""),
			{
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("test_table_03")),
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("BASE TABLE")),
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("1427325877")), // unix_timestamp(create_time)
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("")),
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("128")), // file_size
				sqltypes.MakeTrusted(sqltypes.Int64, []byte("256")), // allocated_size
			},
			mysql.BaseShowTablesRow("test_table_04", false, ""),
			mysql.BaseShowTablesRow("seq", false, "vitess_sequence"),
			mysql.BaseShowTablesRow("msg", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
		},
	})
	db.MockQueriesForTable("test_table_03", &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "mypk",
			Type: sqltypes.Int32,
		}},
	})

	db.MockQueriesForTable("test_table_04", &sqltypes.Result{
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
			mysql.ShowPrimaryRow("test_table_03", "mypk"),
			mysql.ShowPrimaryRow("test_table_04", "pk"),
			mysql.ShowPrimaryRow("seq", "id"),
			mysql.ShowPrimaryRow("msg", "id"),
		},
	})
	err = se.Reload(context.Background())
	require.NoError(t, err)

	delete(want, "test_table_03")
	delete(want, "test_table_04")
	want["test_table_03"] = &Table{
		Name: sqlparser.NewIdentifierCS("test_table_03"),
		Fields: []*querypb.Field{{
			Name: "mypk",
			Type: sqltypes.Int32,
		}},
		PKColumns:     []int{0},
		CreateTime:    1427325877,
		FileSize:      128,
		AllocatedSize: 256,
	}
	want["test_table_04"] = &Table{
		Name: sqlparser.NewIdentifierCS("test_table_04"),
		Fields: []*querypb.Field{{
			Name: "pk",
			Type: sqltypes.Int32,
		}},
		PKColumns:     []int{0},
		CreateTime:    1427325875,
		FileSize:      100,
		AllocatedSize: 150,
	}
	mustMatch(t, want, se.GetSchema())
}

func TestOpenFailedDueToExecErr(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)
	want := "injected error"
	db.RejectQueryPattern(baseShowTablesPattern, want)
	se := newEngine(10, 1*time.Second, 1*time.Second, db)
	err := se.Open()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("se.Open: %v, want %s", err, want)
	}
}

// TestOpenFailedDueToLoadTableErr tests that schema engine load should not fail instead should log the failures.
func TestOpenFailedDueToLoadTableErr(t *testing.T) {
	tl := syslogger.NewTestLogger()
	defer tl.Close()
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)
	db.AddQueryPattern(baseShowTablesPattern, &sqltypes.Result{
		Fields: mysql.BaseShowTablesFields,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow("test_table", false, ""),
			mysql.BaseShowTablesRow("test_view", true, "VIEW"),
		},
	})
	// this will cause NewTable error, as it expects zero rows.
	db.MockQueriesForTable("test_table", sqltypes.MakeTestResult(sqltypes.MakeTestFields("foo", "varchar"), ""))

	// adding column query for table_view
	db.AddQueryPattern(fmt.Sprintf(mysql.GetColumnNamesQueryPatternForTable, "test_view"),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("column_name", "varchar"), ""))
	// rejecting the impossible query
	db.AddRejectedQuery("SELECT * FROM `fakesqldb`.`test_view` WHERE 1 != 1", mysql.NewSQLErrorFromError(errors.New("The user specified as a definer ('root'@'%') does not exist (errno 1449) (sqlstate HY000)")))

	AddFakeInnoDBReadRowsResult(db, 0)
	se := newEngine(10, 1*time.Second, 1*time.Second, db)
	err := se.Open()
	// failed load should return an error because of test_table
	assert.ErrorContains(t, err, "Row count exceeded")

	logs := tl.GetAllLogs()
	logOutput := strings.Join(logs, ":::")
	assert.Contains(t, logOutput, "WARNING:Failed reading schema for the table: test_view")
	assert.Contains(t, logOutput, "The user specified as a definer ('root'@'%') does not exist (errno 1449) (sqlstate HY000)")
}

// TestOpenFailedDueToEmptyColumnInView tests that schema engine load should not fail instead should log the failures for empty columns in view
func TestOpenFailedDueToEmptyColumnInView(t *testing.T) {
	tl := syslogger.NewTestLogger()
	defer tl.Close()
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)
	db.AddQueryPattern(baseShowTablesPattern, &sqltypes.Result{
		Fields: mysql.BaseShowTablesFields,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow("test_view", true, "VIEW"),
		},
	})

	// adding column query for table_view
	db.AddQueryPattern(fmt.Sprintf(mysql.GetColumnNamesQueryPatternForTable, "test_view"),
		&sqltypes.Result{})

	AddFakeInnoDBReadRowsResult(db, 0)
	se := newEngine(10, 1*time.Second, 1*time.Second, 0, db)
	err := se.Open()
	require.NoError(t, err)

	logs := tl.GetAllLogs()
	logOutput := strings.Join(logs, ":::")
	assert.Contains(t, logOutput, "WARNING:Failed reading schema for the table: test_view")
	assert.Contains(t, logOutput, "unable to get columns for table fakesqldb.test_view")
}

func TestExportVars(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)
	se := newEngine(10, 1*time.Second, 1*time.Second, db)
	se.Open()
	defer se.Close()
	expvar.Do(func(kv expvar.KeyValue) {
		_ = kv.Value.String()
	})
}

func TestStatsURL(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)
	se := newEngine(10, 1*time.Second, 1*time.Second, db)
	se.Open()
	defer se.Close()

	request, _ := http.NewRequest("GET", "/debug/schema", nil)
	response := httptest.NewRecorder()
	se.handleDebugSchema(response, request)
}

func TestSchemaEngineCloseTickRace(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	schematest.AddDefaultQueries(db)
	db.AddQueryPattern(baseShowTablesPattern,
		&sqltypes.Result{
			Fields:       mysql.BaseShowTablesFields,
			RowsAffected: 0,
			InsertID:     0,
			Rows: [][]sqltypes.Value{
				mysql.BaseShowTablesRow("test_table_01", false, ""),
				mysql.BaseShowTablesRow("test_table_02", false, ""),
				mysql.BaseShowTablesRow("test_table_03", false, ""),
				mysql.BaseShowTablesRow("seq", false, "vitess_sequence"),
				mysql.BaseShowTablesRow("msg", false, "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30"),
			},
			SessionStateChanges: "",
			StatusFlags:         0,
		})
	AddFakeInnoDBReadRowsResult(db, 12)
	// Start the engine with a small reload tick
	se := newEngine(10, 100*time.Millisecond, 1*time.Second, db)
	err := se.Open()
	require.NoError(t, err)

	finished := make(chan bool)
	go func() {
		{
			// Emulate the command of se.Close(), but with a wait in between
			// to ensure that a reload-tick happens after locking the mutex but before
			// stopping the ticks
			se.mu.Lock()
			// We wait for 200 milliseconds to be sure that the timer tick happens after acquiring the lock
			// before we call closeLocked function
			time.Sleep(200 * time.Millisecond)
			se.closeLocked()
		}
		finished <- true
	}()
	// Wait until the ticks are stopped or 2 seonds have expired.
	select {
	case <-finished:
		return
	case <-time.After(2 * time.Second):
		t.Fatal("Could not stop the ticks after 2 seconds")
	}
}

func newEngine(queryCacheSize int, reloadTime time.Duration, idleTimeout time.Duration, db *fakesqldb.DB) *Engine {
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
	return dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
}

func initialSchema() map[string]*Table {
	return map[string]*Table{
		"dual": {
			Name: sqlparser.NewIdentifierCS("dual"),
		},
		"test_table_01": {
			Name: sqlparser.NewIdentifierCS("test_table_01"),
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}},
			PKColumns:     []int{0},
			CreateTime:    1427325875,
			FileSize:      0x64,
			AllocatedSize: 0x96,
		},
		"test_table_02": {
			Name: sqlparser.NewIdentifierCS("test_table_02"),
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}},
			PKColumns:     []int{0},
			CreateTime:    1427325875,
			FileSize:      0x64,
			AllocatedSize: 0x96,
		},
		"test_table_03": {
			Name: sqlparser.NewIdentifierCS("test_table_03"),
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}},
			PKColumns:     []int{0},
			CreateTime:    1427325875,
			FileSize:      0x64,
			AllocatedSize: 0x96,
		},
		"seq": {
			Name: sqlparser.NewIdentifierCS("seq"),
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
			PKColumns:     []int{0},
			CreateTime:    1427325875,
			FileSize:      0x64,
			AllocatedSize: 0x96,
			SequenceInfo:  &SequenceInfo{},
		},
		"msg": {
			Name: sqlparser.NewIdentifierCS("msg"),
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
			PKColumns:     []int{0},
			CreateTime:    1427325875,
			FileSize:      0x64,
			AllocatedSize: 0x96,
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

func AddFakeInnoDBReadRowsResult(db *fakesqldb.DB, value int) *fakesqldb.ExpectedResult {
	return db.AddQuery("show status like 'Innodb_rows_read'", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"Variable_name|Value",
		"varchar|int64"),
		fmt.Sprintf("Innodb_rows_read|%d", value),
	))
}
