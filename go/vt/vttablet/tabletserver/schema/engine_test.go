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
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/dbconfigs"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
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
	se := newEngine(10, 10*time.Second, 10*time.Second, 0, db)
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
	notifier := func(full map[string]*Table, created, altered, dropped []*Table) {
		if firstTime {
			firstTime = false
			createTables := extractNamesFromTablesList(created)
			sort.Strings(createTables)
			assert.Equal(t, []string{"dual", "msg", "seq", "test_table_01", "test_table_02", "test_table_03"}, createTables)
			assert.Equal(t, []*Table(nil), altered)
			assert.Equal(t, []*Table(nil), dropped)
		} else {
			assert.Equal(t, []string{"test_table_04"}, extractNamesFromTablesList(created))
			assert.Equal(t, []string{"test_table_03"}, extractNamesFromTablesList(altered))
			assert.Equal(t, []string{"msg"}, extractNamesFromTablesList(dropped))
		}
	}
	se.RegisterNotifier("test", notifier, true)
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

	se := newEngine(10, 10*time.Second, 10*time.Second, 0, db)
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
	se := newEngine(10, 1*time.Second, 1*time.Second, 0, db)
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
	se := newEngine(10, 1*time.Second, 1*time.Second, 0, db)
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
	se := newEngine(10, 1*time.Second, 1*time.Second, 0, db)
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
	se := newEngine(10, 1*time.Second, 1*time.Second, 0, db)
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
	se := newEngine(10, 100*time.Millisecond, 1*time.Second, 0, db)
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

func newEngine(queryCacheSize int, reloadTime time.Duration, idleTimeout time.Duration, schemaMaxAgeSeconds int64, db *fakesqldb.DB) *Engine {
	config := tabletenv.NewDefaultConfig()
	config.QueryCacheSize = queryCacheSize
	_ = config.SchemaReloadIntervalSeconds.Set(reloadTime.String())
	_ = config.OltpReadPool.IdleTimeoutSeconds.Set(idleTimeout.String())
	_ = config.OlapReadPool.IdleTimeoutSeconds.Set(idleTimeout.String())
	_ = config.TxPool.IdleTimeoutSeconds.Set(idleTimeout.String())
	config.SchemaVersionMaxAgeSeconds = schemaMaxAgeSeconds
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

// TestEngineMysqlTime tests the functionality of Engine.mysqlTime function
func TestEngineMysqlTime(t *testing.T) {
	tests := []struct {
		name            string
		timeStampResult []string
		timeStampErr    error
		wantTime        int64
		wantErr         string
	}{
		{
			name:            "Success",
			timeStampResult: []string{"1685115631"},
			wantTime:        1685115631,
		}, {
			name:         "Error in result",
			timeStampErr: errors.New("some error in MySQL"),
			wantErr:      "some error in MySQL",
		}, {
			name:            "Error in parsing",
			timeStampResult: []string{"16851r15631"},
			wantErr:         "could not parse time",
		}, {
			name:            "More than 1 result",
			timeStampResult: []string{"1685115631", "3241241"},
			wantErr:         "could not get MySQL time",
		}, {
			name:            "Null result",
			timeStampResult: []string{"null"},
			wantErr:         "unexpected result for MySQL time",
		},
	}

	query := "SELECT UNIX_TIMESTAMP()"
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			se := &Engine{}
			db := fakesqldb.New(t)
			conn, err := connpool.NewDBConnNoPool(context.Background(), db.ConnParams(), nil, nil)
			require.NoError(t, err)

			if tt.timeStampErr != nil {
				db.AddRejectedQuery(query, tt.timeStampErr)
			} else {
				db.AddQuery(query, sqltypes.MakeTestResult(sqltypes.MakeTestFields("UNIX_TIMESTAMP", "int64"), tt.timeStampResult...))
			}

			gotTime, err := se.mysqlTime(context.Background(), conn)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.EqualValues(t, tt.wantTime, gotTime)
			require.NoError(t, db.LastError())
		})
	}
}

// TestEnginePopulatePrimaryKeys tests the functionality of Engine.populatePrimaryKeys function
func TestEnginePopulatePrimaryKeys(t *testing.T) {
	tests := []struct {
		name            string
		tables          map[string]*Table
		pkIndexes       map[string]int
		expectedQueries map[string]*sqltypes.Result
		queriesToReject map[string]error
		expectedError   string
	}{
		{
			name: "Success",
			tables: map[string]*Table{
				"t1": {
					Name: sqlparser.NewIdentifierCS("t1"),
					Fields: []*querypb.Field{
						{
							Name: "col1",
						}, {
							Name: "col2",
						},
					},
					Type: NoType,
				}, "t2": {
					Name: sqlparser.NewIdentifierCS("t2"),
					Fields: []*querypb.Field{
						{
							Name: "id",
						},
					},
					Type: NoType,
				},
			},
			expectedQueries: map[string]*sqltypes.Result{
				mysql.BaseShowPrimary: sqltypes.MakeTestResult(mysql.ShowPrimaryFields,
					"t1|col2",
					"t2|id"),
			},
			pkIndexes: map[string]int{
				"t1": 1,
				"t2": 0,
			},
		}, {
			name: "Error in finding column",
			tables: map[string]*Table{
				"t1": {
					Name: sqlparser.NewIdentifierCS("t1"),
					Fields: []*querypb.Field{
						{
							Name: "col1",
						}, {
							Name: "col2",
						},
					},
					Type: NoType,
				},
			},
			expectedQueries: map[string]*sqltypes.Result{
				mysql.BaseShowPrimary: sqltypes.MakeTestResult(mysql.ShowPrimaryFields,
					"t1|col5"),
			},
			expectedError: "column col5 is listed as primary key, but not present in table t1",
		}, {
			name: "Error in query",
			tables: map[string]*Table{
				"t1": {
					Name: sqlparser.NewIdentifierCS("t1"),
					Fields: []*querypb.Field{
						{
							Name: "col1",
						}, {
							Name: "col2",
						},
					},
					Type: NoType,
				},
			},
			queriesToReject: map[string]error{
				mysql.BaseShowPrimary: errors.New("some error in MySQL"),
			},
			expectedError: "could not get table primary key info",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := fakesqldb.New(t)
			conn, err := connpool.NewDBConnNoPool(context.Background(), db.ConnParams(), nil, nil)
			require.NoError(t, err)
			se := &Engine{}

			for query, result := range tt.expectedQueries {
				db.AddQuery(query, result)
			}
			for query, errToThrow := range tt.queriesToReject {
				db.AddRejectedQuery(query, errToThrow)
			}

			err = se.populatePrimaryKeys(context.Background(), conn, tt.tables)
			if tt.expectedError != "" {
				require.ErrorContains(t, err, tt.expectedError)
				return
			}
			require.NoError(t, err)
			require.NoError(t, db.LastError())
			for table, index := range tt.pkIndexes {
				require.Equal(t, index, tt.tables[table].PKColumns[0])
			}
		})
	}
}

// TestEngineUpdateInnoDBRowsRead tests the functionality of Engine.updateInnoDBRowsRead function
func TestEngineUpdateInnoDBRowsRead(t *testing.T) {
	showRowsReadFields := sqltypes.MakeTestFields("Variable_name|Value", "varchar|int64")
	tests := []struct {
		name                  string
		innoDbReadRowsCounter int
		expectedQueries       map[string]*sqltypes.Result
		queriesToReject       map[string]error
		expectedError         string
	}{
		{
			name: "Success",
			expectedQueries: map[string]*sqltypes.Result{
				mysql.ShowRowsRead: sqltypes.MakeTestResult(showRowsReadFields,
					"Innodb_rows_read|35"),
			},
			innoDbReadRowsCounter: 35,
		}, {
			name: "Unexpected result",
			expectedQueries: map[string]*sqltypes.Result{
				mysql.ShowRowsRead: sqltypes.MakeTestResult(showRowsReadFields,
					"Innodb_rows_read|35",
					"Innodb_rows_read|37"),
			},
			innoDbReadRowsCounter: 0,
		}, {
			name: "Error in query",
			queriesToReject: map[string]error{
				mysql.ShowRowsRead: errors.New("some error in MySQL"),
			},
			expectedError: "some error in MySQL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := fakesqldb.New(t)
			conn, err := connpool.NewDBConnNoPool(context.Background(), db.ConnParams(), nil, nil)
			require.NoError(t, err)
			se := &Engine{}
			se.innoDbReadRowsCounter = stats.NewCounter("TestEngineUpdateInnoDBRowsRead-"+tt.name, "")

			for query, result := range tt.expectedQueries {
				db.AddQuery(query, result)
			}
			for query, errToThrow := range tt.queriesToReject {
				db.AddRejectedQuery(query, errToThrow)
			}

			err = se.updateInnoDBRowsRead(context.Background(), conn)
			if tt.expectedError != "" {
				require.ErrorContains(t, err, tt.expectedError)
				return
			}
			require.NoError(t, err)
			require.NoError(t, db.LastError())
			require.EqualValues(t, tt.innoDbReadRowsCounter, se.innoDbReadRowsCounter.Get())
		})
	}
}

// TestEngineGetTableData tests the functionality of getTableData function
func TestEngineGetTableData(t *testing.T) {
	db := fakesqldb.New(t)
	conn, err := connpool.NewDBConnNoPool(context.Background(), db.ConnParams(), nil, nil)
	require.NoError(t, err)

	tests := []struct {
		name            string
		expectedQueries map[string]*sqltypes.Result
		queriesToReject map[string]error
		includeStats    bool
		expectedError   string
	}{
		{
			name: "Success",
			expectedQueries: map[string]*sqltypes.Result{
				conn.BaseShowTables(): {},
			},
			includeStats: false,
		}, {
			name: "Success with include stats",
			expectedQueries: map[string]*sqltypes.Result{
				conn.BaseShowTablesWithSizes(): {},
			},
			includeStats: true,
		}, {
			name: "Error in query",
			queriesToReject: map[string]error{
				conn.BaseShowTables(): errors.New("some error in MySQL"),
			},
			expectedError: "some error in MySQL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db.ClearQueryPattern()

			for query, result := range tt.expectedQueries {
				db.AddQuery(query, result)
				defer db.DeleteQuery(query)
			}
			for query, errToThrow := range tt.queriesToReject {
				db.AddRejectedQuery(query, errToThrow)
				defer db.DeleteRejectedQuery(query)
			}

			_, err = getTableData(context.Background(), conn, tt.includeStats)
			if tt.expectedError != "" {
				require.ErrorContains(t, err, tt.expectedError)
				return
			}
			require.NoError(t, err)
			require.NoError(t, db.LastError())
		})
	}
}

// TestEngineGetDroppedTables tests the functionality of Engine.getDroppedTables function
func TestEngineGetDroppedTables(t *testing.T) {
	tests := []struct {
		name              string
		tables            map[string]*Table
		curTables         map[string]bool
		changedViews      map[string]any
		mismatchTables    map[string]any
		wantDroppedTables []*Table
	}{
		{
			name: "No mismatched tables or changed views",
			tables: map[string]*Table{
				"t1": NewTable("t1", NoType),
				"t2": NewTable("t2", NoType),
				"t3": NewTable("t3", NoType),
			},
			curTables: map[string]bool{
				"t4": true,
				"t2": true,
			},
			wantDroppedTables: []*Table{
				NewTable("t1", NoType),
				NewTable("t3", NoType),
			},
		}, {
			name: "Mismatched tables having a dropped table",
			tables: map[string]*Table{
				"t1": NewTable("t1", NoType),
				"t2": NewTable("t2", NoType),
				"t3": NewTable("t3", NoType),
				"v2": NewTable("v2", View),
			},
			curTables: map[string]bool{
				"t4": true,
				"t2": true,
			},
			mismatchTables: map[string]any{
				"t5": true,
				"v2": true,
			},
			wantDroppedTables: []*Table{
				NewTable("t1", NoType),
				NewTable("t3", NoType),
				NewTable("t5", NoType),
				NewTable("v2", View),
			},
		}, {
			name: "Changed views having a dropped view",
			tables: map[string]*Table{
				"t1": NewTable("t1", NoType),
				"t2": NewTable("t2", NoType),
				"t3": NewTable("t3", NoType),
				"v2": NewTable("v2", NoType),
			},
			curTables: map[string]bool{
				"t4": true,
				"t2": true,
			},
			changedViews: map[string]any{
				"v1": true,
				"v2": true,
			},
			wantDroppedTables: []*Table{
				NewTable("t1", NoType),
				NewTable("t3", NoType),
				NewTable("v1", View),
				NewTable("v2", NoType),
			},
		}, {
			name: "Both have dropped tables",
			tables: map[string]*Table{
				"t1": NewTable("t1", NoType),
				"t2": NewTable("t2", NoType),
				"t3": NewTable("t3", NoType),
				"v2": NewTable("v2", NoType),
				"v3": NewTable("v3", View),
			},
			curTables: map[string]bool{
				"t4": true,
				"t2": true,
			},
			changedViews: map[string]any{
				"v1": true,
				"v2": true,
			},
			mismatchTables: map[string]any{
				"t5": true,
				"v3": true,
			},
			wantDroppedTables: []*Table{
				NewTable("t1", NoType),
				NewTable("t3", NoType),
				NewTable("t5", NoType),
				NewTable("v1", View),
				NewTable("v3", View),
				NewTable("v2", NoType),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			se := &Engine{
				tables: tt.tables,
			}
			se.tableFileSizeGauge = stats.NewGaugesWithSingleLabel("TestEngineGetDroppedTables-"+tt.name, "", "Table")
			se.tableAllocatedSizeGauge = stats.NewGaugesWithSingleLabel("TestEngineGetDroppedTables-allocated-"+tt.name, "", "Table")
			gotDroppedTables := se.getDroppedTables(tt.curTables, tt.changedViews, tt.mismatchTables)
			require.ElementsMatch(t, gotDroppedTables, tt.wantDroppedTables)
		})
	}
}

// TestEngineReload tests the entire functioning of engine.Reload testing all the queries that we end up running against MySQL
// while simulating the responses and verifies the final list of created, altered and dropped tables.
func TestEngineReload(t *testing.T) {
	db := fakesqldb.New(t)
	cfg := tabletenv.NewDefaultConfig()
	cfg.DB = newDBConfigs(db)
	cfg.SignalWhenSchemaChange = true
	conn, err := connpool.NewDBConnNoPool(context.Background(), db.ConnParams(), nil, nil)
	require.NoError(t, err)

	se := newEngine(10, 10*time.Second, 10*time.Second, 0, db)
	se.conns.Open(se.cp, se.cp, se.cp)
	se.isOpen = true
	se.notifiers = make(map[string]notifier)
	se.MakePrimary(true)

	// If we have to skip the meta check, then there is nothing to do
	se.SkipMetaCheck = true
	err = se.reload(context.Background(), false)
	require.NoError(t, err)

	se.SkipMetaCheck = false
	se.lastChange = 987654321

	// Initial tables in the schema engine
	se.tables = map[string]*Table{
		"t1": {
			Name:       sqlparser.NewIdentifierCS("t1"),
			Type:       NoType,
			CreateTime: 123456789,
		},
		"t2": {
			Name:       sqlparser.NewIdentifierCS("t2"),
			Type:       NoType,
			CreateTime: 123456789,
		},
		"t4": {
			Name:       sqlparser.NewIdentifierCS("t4"),
			Type:       NoType,
			CreateTime: 123456789,
		},
		"v1": {
			Name:       sqlparser.NewIdentifierCS("v1"),
			Type:       View,
			CreateTime: 123456789,
		},
		"v2": {
			Name:       sqlparser.NewIdentifierCS("v2"),
			Type:       View,
			CreateTime: 123456789,
		},
		"v4": {
			Name:       sqlparser.NewIdentifierCS("v4"),
			Type:       View,
			CreateTime: 123456789,
		},
	}
	// MySQL unix timestamp query.
	db.AddQuery("SELECT UNIX_TIMESTAMP()", sqltypes.MakeTestResult(sqltypes.MakeTestFields("UNIX_TIMESTAMP", "int64"), "987654326"))
	// Table t2 is updated, t3 is created and t4 is deleted.
	// View v2 is updated, v3 is created and v4 is deleted.
	db.AddQuery(conn.BaseShowTables(), sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name|table_type|unix_timestamp(create_time)|table_comment",
		"varchar|varchar|int64|varchar"),
		"t1|BASE_TABLE|123456789|",
		"t2|BASE_TABLE|123456790|",
		"t3|BASE_TABLE|123456789|",
		"v1|VIEW|123456789|",
		"v2|VIEW|123456789|",
		"v3|VIEW|123456789|",
	))

	// Detecting view changes.
	// According to the database, v2, v3, v4, and v5 require updating.
	db.AddQuery(fmt.Sprintf(detectViewChange, sidecardb.GetIdentifier()), sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name", "varchar"),
		"v2",
		"v3",
		"v4",
		"v5",
	))

	// Finding mismatches in the tables.
	// t5 exists in the database.
	db.AddQuery("SELECT TABLE_NAME, CREATE_TIME FROM _vt.`tables`", sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name|create_time", "varchar|int64"),
		"t1|123456789",
		"t2|123456789",
		"t4|123456789",
		"t5|123456789",
	))

	// Read Innodb_rows_read.
	db.AddQuery(mysql.ShowRowsRead, sqltypes.MakeTestResult(sqltypes.MakeTestFields("Variable_name|Value", "varchar|int64"),
		"Innodb_rows_read|35"))

	// Queries to load the tables' information.
	for _, tableName := range []string{"t2", "t3", "v2", "v3"} {
		db.AddQuery(fmt.Sprintf(`SELECT COLUMN_NAME as column_name
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = 'fakesqldb' AND TABLE_NAME = '%s'
		ORDER BY ORDINAL_POSITION`, tableName),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("column_name", "varchar"),
				"col1"))
		db.AddQuery(fmt.Sprintf("SELECT `col1` FROM `fakesqldb`.`%v` WHERE 1 != 1", tableName), sqltypes.MakeTestResult(sqltypes.MakeTestFields("col1", "varchar")))
	}

	// Primary key information.
	db.AddQuery(mysql.BaseShowPrimary, sqltypes.MakeTestResult(mysql.ShowPrimaryFields,
		"t1|col1",
		"t2|col1",
		"t3|col1",
	))

	// Queries for reloading the tables' information.
	{
		for _, tableName := range []string{"t2", "t3"} {
			db.AddQuery(fmt.Sprintf(`show create table %s`, tableName),
				sqltypes.MakeTestResult(sqltypes.MakeTestFields("Table | Create Table", "varchar|varchar"),
					fmt.Sprintf("%v|create_table_%v", tableName, tableName)))
		}
		db.AddQuery("begin", &sqltypes.Result{})
		db.AddQuery("commit", &sqltypes.Result{})
		db.AddQuery("rollback", &sqltypes.Result{})
		// We are adding both the variants of the delete statements that we can see in the test, since the deleted tables are initially stored as a map, the order is not defined.
		db.AddQuery("delete from _vt.`tables` where TABLE_SCHEMA = database() and TABLE_NAME in ('t5', 't4', 't3', 't2')", &sqltypes.Result{})
		db.AddQuery("delete from _vt.`tables` where TABLE_SCHEMA = database() and TABLE_NAME in ('t4', 't5', 't3', 't2')", &sqltypes.Result{})
		db.AddQuery("insert into _vt.`tables`(TABLE_SCHEMA, TABLE_NAME, CREATE_STATEMENT, CREATE_TIME) values (database(), 't2', 'create_table_t2', 123456790)", &sqltypes.Result{})
		db.AddQuery("insert into _vt.`tables`(TABLE_SCHEMA, TABLE_NAME, CREATE_STATEMENT, CREATE_TIME) values (database(), 't3', 'create_table_t3', 123456789)", &sqltypes.Result{})
	}

	// Queries for reloading the views' information.
	{
		for _, tableName := range []string{"v2", "v3"} {
			db.AddQuery(fmt.Sprintf(`show create table %s`, tableName),
				sqltypes.MakeTestResult(sqltypes.MakeTestFields(" View | Create View | character_set_client | collation_connection", "varchar|varchar|varchar|varchar"),
					fmt.Sprintf("%v|create_table_%v|utf8mb4|utf8mb4_0900_ai_ci", tableName, tableName)))
		}
		// We are adding both the variants of the select statements that we can see in the test, since the deleted views are initially stored as a map, the order is not defined.
		db.AddQuery("select table_name, view_definition from information_schema.views where table_schema = database() and table_name in ('v4', 'v5', 'v3', 'v2')",
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name|view_definition", "varchar|varchar"),
				"v2|select_v2",
				"v3|select_v3",
			))
		db.AddQuery("select table_name, view_definition from information_schema.views where table_schema = database() and table_name in ('v5', 'v4', 'v3', 'v2')",
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name|view_definition", "varchar|varchar"),
				"v2|select_v2",
				"v3|select_v3",
			))

		// We are adding both the variants of the delete statements that we can see in the test, since the deleted views are initially stored as a map, the order is not defined.
		db.AddQuery("delete from _vt.views where TABLE_SCHEMA = database() and TABLE_NAME in ('v4', 'v5', 'v3', 'v2')", &sqltypes.Result{})
		db.AddQuery("delete from _vt.views where TABLE_SCHEMA = database() and TABLE_NAME in ('v5', 'v4', 'v3', 'v2')", &sqltypes.Result{})
		db.AddQuery("insert into _vt.views(TABLE_SCHEMA, TABLE_NAME, CREATE_STATEMENT, VIEW_DEFINITION) values (database(), 'v2', 'create_table_v2', 'select_v2')", &sqltypes.Result{})
		db.AddQuery("insert into _vt.views(TABLE_SCHEMA, TABLE_NAME, CREATE_STATEMENT, VIEW_DEFINITION) values (database(), 'v3', 'create_table_v3', 'select_v3')", &sqltypes.Result{})
	}

	// Verify the list of created, altered and dropped tables seen.
	se.RegisterNotifier("test", func(full map[string]*Table, created, altered, dropped []*Table) {
		require.ElementsMatch(t, extractNamesFromTablesList(created), []string{"t3", "v3"})
		require.ElementsMatch(t, extractNamesFromTablesList(altered), []string{"t2", "v2"})
		require.ElementsMatch(t, extractNamesFromTablesList(dropped), []string{"t4", "v4", "t5", "v5"})
	}, false)

	// Run the reload.
	err = se.reload(context.Background(), false)
	require.NoError(t, err)
	require.NoError(t, db.LastError())
}
