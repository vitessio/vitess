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

package tabletserver

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func TestHealthStreamerClosed(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	config := newConfig(db)
	env := tabletenv.NewEnv(config, "ReplTrackerTest")
	alias := &topodatapb.TabletAlias{
		Cell: "cell",
		Uid:  1,
	}
	blpFunc = testBlpFunc
	hs := newHealthStreamer(env, alias, &schema.Engine{})
	err := hs.Stream(context.Background(), func(shr *querypb.StreamHealthResponse) error {
		return nil
	})
	assert.Contains(t, err.Error(), "tabletserver is shutdown")
}

func newConfig(db *fakesqldb.DB) *tabletenv.TabletConfig {
	cfg := tabletenv.NewDefaultConfig()
	cfg.DB = newDBConfigs(db)
	return cfg
}

// TestNotServingPrimaryNoWrite makes sure that the health-streamer doesn't write anything to the database when
// the state is not serving primary.
func TestNotServingPrimaryNoWrite(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	config := newConfig(db)
	config.SignalWhenSchemaChange = true

	env := tabletenv.NewEnv(config, "TestNotServingPrimary")
	alias := &topodatapb.TabletAlias{
		Cell: "cell",
		Uid:  1,
	}
	// Create a new health streamer and set it to a serving primary state
	hs := newHealthStreamer(env, alias, &schema.Engine{})
	hs.isServingPrimary = true
	hs.InitDBConfig(&querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}, config.DB.DbaWithDB())
	hs.Open()
	defer hs.Close()
	target := &querypb.Target{}
	hs.InitDBConfig(target, db.ConnParams())

	// Let's say the tablet goes to a non-serving primary state.
	hs.MakePrimary(false)

	// A reload now should not write anything to the database. If any write happens it will error out since we have not
	// added any query to the database to expect.
	t1 := schema.NewTable("t1", schema.NoType)
	err := hs.reload(map[string]*schema.Table{"t1": t1}, []*schema.Table{t1}, nil, nil)
	require.NoError(t, err)
	require.NoError(t, db.LastError())
}

func TestHealthStreamerBroadcast(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	config := newConfig(db)
	config.SignalWhenSchemaChange = false

	env := tabletenv.NewEnv(config, "ReplTrackerTest")
	alias := &topodatapb.TabletAlias{
		Cell: "cell",
		Uid:  1,
	}
	blpFunc = testBlpFunc
	hs := newHealthStreamer(env, alias, &schema.Engine{})
	hs.InitDBConfig(&querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}, config.DB.DbaWithDB())
	hs.Open()
	defer hs.Close()
	target := &querypb.Target{}
	hs.InitDBConfig(target, db.ConnParams())

	ch, cancel := testStream(hs)
	defer cancel()

	shr := <-ch
	want := &querypb.StreamHealthResponse{
		Target:      &querypb.Target{},
		TabletAlias: alias,
		RealtimeStats: &querypb.RealtimeStats{
			HealthError: "tabletserver uninitialized",
		},
	}
	assert.Truef(t, proto.Equal(want, shr), "want: %v, got: %v", want, shr)

	hs.ChangeState(topodatapb.TabletType_REPLICA, time.Time{}, 0, nil, false)
	shr = <-ch
	want = &querypb.StreamHealthResponse{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_REPLICA,
		},
		TabletAlias: alias,
		RealtimeStats: &querypb.RealtimeStats{
			FilteredReplicationLagSeconds: 1,
			BinlogPlayersCount:            2,
		},
	}
	assert.Truef(t, proto.Equal(want, shr), "want: %v, got: %v", want, shr)

	// Test primary and timestamp.
	now := time.Now()
	hs.ChangeState(topodatapb.TabletType_PRIMARY, now, 0, nil, true)
	shr = <-ch
	want = &querypb.StreamHealthResponse{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_PRIMARY,
		},
		TabletAlias:                         alias,
		Serving:                             true,
		TabletExternallyReparentedTimestamp: now.Unix(),
		RealtimeStats: &querypb.RealtimeStats{
			FilteredReplicationLagSeconds: 1,
			BinlogPlayersCount:            2,
		},
	}
	assert.Truef(t, proto.Equal(want, shr), "want: %v, got: %v", want, shr)

	// Test non-serving, and 0 timestamp for non-primary.
	hs.ChangeState(topodatapb.TabletType_REPLICA, now, 1*time.Second, nil, false)
	shr = <-ch
	want = &querypb.StreamHealthResponse{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_REPLICA,
		},
		TabletAlias: alias,
		RealtimeStats: &querypb.RealtimeStats{
			ReplicationLagSeconds:         1,
			FilteredReplicationLagSeconds: 1,
			BinlogPlayersCount:            2,
		},
	}
	assert.Truef(t, proto.Equal(want, shr), "want: %v, got: %v", want, shr)

	// Test Health error.
	hs.ChangeState(topodatapb.TabletType_REPLICA, now, 0, errors.New("repl err"), false)
	shr = <-ch
	want = &querypb.StreamHealthResponse{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_REPLICA,
		},
		TabletAlias: alias,
		RealtimeStats: &querypb.RealtimeStats{
			HealthError:                   "repl err",
			FilteredReplicationLagSeconds: 1,
			BinlogPlayersCount:            2,
		},
	}
	assert.Truef(t, proto.Equal(want, shr), "want: %v, got: %v", want, shr)
}

func TestReloadSchema(t *testing.T) {
	testcases := []struct {
		name               string
		enableSchemaChange bool
	}{
		{
			name:               "Schema Change Enabled",
			enableSchemaChange: true,
		}, {
			name:               "Schema Change Disabled",
			enableSchemaChange: false,
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			db := fakesqldb.New(t)
			defer db.Close()
			config := newConfig(db)
			config.SignalWhenSchemaChange = testcase.enableSchemaChange
			_ = config.SchemaReloadIntervalSeconds.Set("100ms")

			env := tabletenv.NewEnv(config, "ReplTrackerTest")
			alias := &topodatapb.TabletAlias{
				Cell: "cell",
				Uid:  1,
			}
			blpFunc = testBlpFunc
			se := schema.NewEngine(env)
			hs := newHealthStreamer(env, alias, se)

			target := &querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
			configs := config.DB

			db.AddQueryPattern(sqlparser.BuildParsedQuery(mysql.ClearSchemaCopy, sidecardb.GetIdentifier()).Query+".*", &sqltypes.Result{})
			db.AddQueryPattern(sqlparser.BuildParsedQuery(mysql.InsertIntoSchemaCopy, sidecardb.GetIdentifier()).Query+".*", &sqltypes.Result{})
			db.AddQueryPattern("SELECT UNIX_TIMESTAMP()"+".*", sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"UNIX_TIMESTAMP(now())",
					"varchar",
				),
				"1684759138",
			))
			db.AddQuery("begin", &sqltypes.Result{})
			db.AddQuery("commit", &sqltypes.Result{})
			db.AddQuery("rollback", &sqltypes.Result{})
			// Add the query pattern for the query that schema.Engine uses to get the tables.
			db.AddQueryPattern("SELECT .* information_schema.innodb_tablespaces .*",
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields(
						"TABLE_NAME | TABLE_TYPE | UNIX_TIMESTAMP(t.create_time) | TABLE_COMMENT | SUM(i.file_size) | SUM(i.allocated_size)",
						"varchar|varchar|int64|varchar|int64|int64",
					),
					"product|BASE TABLE|1684735966||114688|114688",
					"users|BASE TABLE|1684735966||114688|114688",
				))
			db.AddQueryPattern("SELECT COLUMN_NAME as column_name.*", sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"column_name",
					"varchar",
				),
				"id",
			))
			db.AddQueryPattern("SELECT `id` FROM `fakesqldb`.*", sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id",
					"int64",
				),
			))
			db.AddQuery(mysql.ShowRowsRead, sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("Variable_name|Value", "varchar|int32"),
				"Innodb_rows_read|50"))
			db.AddQuery(mysql.BaseShowPrimary, sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name | column_name", "varchar|varchar"),
				"product|id",
				"users|id",
			))

			hs.InitDBConfig(target, configs.DbaWithDB())
			se.InitDBConfig(configs.DbaWithDB())
			hs.Open()
			defer hs.Close()
			err := se.Open()
			require.NoError(t, err)
			defer se.Close()
			// Start schema notifications.
			hs.MakePrimary(true)

			// Update the query pattern for the query that schema.Engine uses to get the tables so that it runs a reload again.
			// If we don't change the t.create_time to a value greater than before, then the schema engine doesn't reload the database.
			db.AddQueryPattern("SELECT .* information_schema.innodb_tablespaces .*",
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields(
						"TABLE_NAME | TABLE_TYPE | UNIX_TIMESTAMP(t.create_time) | TABLE_COMMENT | SUM(i.file_size) | SUM(i.allocated_size)",
						"varchar|varchar|int64|varchar|int64|int64",
					),
					"product|BASE TABLE|1684735967||114688|114688",
					"users|BASE TABLE|1684735967||114688|114688",
				))

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				hs.Stream(ctx, func(response *querypb.StreamHealthResponse) error {
					if response.RealtimeStats.TableSchemaChanged != nil {
						assert.Equal(t, []string{"product", "users"}, response.RealtimeStats.TableSchemaChanged)
						wg.Done()
					}
					return nil
				})
			}()

			c := make(chan struct{})
			go func() {
				defer close(c)
				wg.Wait()
			}()
			timeout := false
			select {
			case <-c:
			case <-time.After(1 * time.Second):
				timeout = true
			}

			require.Equal(t, testcase.enableSchemaChange, !timeout, "If schema change tracking is enabled, then we shouldn't time out, otherwise we should")
		})
	}
}

// TestReloadView tests that the health streamer tracks view changes correctly
func TestReloadView(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	config := newConfig(db)
	config.SignalWhenSchemaChange = true
	_ = config.SchemaReloadIntervalSeconds.Set("100ms")
	config.EnableViews = true

	env := tabletenv.NewEnv(config, "TestReloadView")
	alias := &topodatapb.TabletAlias{Cell: "cell", Uid: 1}
	se := schema.NewEngine(env)
	hs := newHealthStreamer(env, alias, se)

	target := &querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	configs := config.DB

	db.AddQueryPattern(sqlparser.BuildParsedQuery(mysql.ClearSchemaCopy, sidecardb.GetIdentifier()).Query+".*", &sqltypes.Result{})
	db.AddQueryPattern(sqlparser.BuildParsedQuery(mysql.InsertIntoSchemaCopy, sidecardb.GetIdentifier()).Query+".*", &sqltypes.Result{})
	db.AddQueryPattern("SELECT UNIX_TIMESTAMP()"+".*", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"UNIX_TIMESTAMP(now())",
			"varchar",
		),
		"1684759138",
	))
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("commit", &sqltypes.Result{})
	db.AddQuery("rollback", &sqltypes.Result{})
	// Add the query pattern for the query that schema.Engine uses to get the tables.
	db.AddQueryPattern("SELECT .* information_schema.innodb_tablespaces .*",
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"TABLE_NAME | TABLE_TYPE | UNIX_TIMESTAMP(t.create_time) | TABLE_COMMENT | SUM(i.file_size) | SUM(i.allocated_size)",
				"varchar|varchar|int64|varchar|int64|int64",
			),
		))
	db.AddQueryPattern("SELECT COLUMN_NAME as column_name.*", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"column_name",
			"varchar",
		),
		"id",
	))
	db.AddQueryPattern("SELECT `id` FROM `fakesqldb`.*", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id",
			"int64",
		),
	))
	db.AddQuery(mysql.ShowRowsRead, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Variable_name|Value", "varchar|int32"),
		"Innodb_rows_read|50"))
	db.AddQuery(mysql.BaseShowPrimary, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("table_name | column_name", "varchar|varchar"),
	))
	db.AddQueryPattern(".*SELECT table_name, view_definition.*schema_engine_views.*", &sqltypes.Result{})
	db.AddQuery("SELECT TABLE_NAME, CREATE_TIME FROM _vt.schema_engine_tables", &sqltypes.Result{})

	hs.InitDBConfig(target, configs.DbaWithDB())
	se.InitDBConfig(configs.DbaWithDB())
	hs.Open()
	defer hs.Close()
	err := se.Open()
	require.NoError(t, err)
	se.MakePrimary(true)
	defer se.Close()
	// Start schema notifications.
	hs.MakePrimary(true)

	showCreateViewFields := sqltypes.MakeTestFields(
		"View|Create View|character_set_client|collation_connection",
		"varchar|text|varchar|varchar")
	showTableSizesFields := sqltypes.MakeTestFields(
		"TABLE_NAME | TABLE_TYPE | UNIX_TIMESTAMP(t.create_time) | TABLE_COMMENT | SUM(i.file_size) | SUM(i.allocated_size)",
		"varchar|varchar|int64|varchar|int64|int64",
	)

	tcases := []struct {
		detectViewChangeOutput    *sqltypes.Result
		showTablesWithSizesOutput *sqltypes.Result

		expCreateStmtQuery []string
		createStmtOutput   []*sqltypes.Result

		expGetViewDefinitionsQuery string
		viewDefinitionsOutput      *sqltypes.Result

		expClearQuery   string
		expHsClearQuery string
		expInsertQuery  []string
		expViewsChanged []string
	}{
		{
			// view_a and view_b added.
			detectViewChangeOutput: sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name", "varchar"),
				"view_a", "view_b"),
			showTablesWithSizesOutput: sqltypes.MakeTestResult(showTableSizesFields, "view_a|VIEW|12345678||123|123", "view_b|VIEW|12345678||123|123"),
			viewDefinitionsOutput: sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name|view_definition", "varchar|text"),
				"view_a|def_a", "view_b|def_b"),
			createStmtOutput: []*sqltypes.Result{sqltypes.MakeTestResult(showCreateViewFields, "view_a|create_view_a|utf8|utf8_general_ci"),
				sqltypes.MakeTestResult(showCreateViewFields, "view_b|create_view_b|utf8|utf8_general_ci")},
			expViewsChanged:            []string{"view_a", "view_b"},
			expGetViewDefinitionsQuery: "select table_name, view_definition from information_schema.views where table_schema = database() and table_name in ('view_a', 'view_b')",
			expCreateStmtQuery:         []string{"show create table view_a", "show create table view_b"},
			expClearQuery:              "delete from _vt.schema_engine_views where TABLE_SCHEMA = database() and TABLE_NAME in ('view_a', 'view_b')",
			expHsClearQuery:            "delete from _vt.views where table_schema = database() and table_name in ('view_a', 'view_b')",
			expInsertQuery: []string{
				"insert into _vt.schema_engine_views(TABLE_SCHEMA, TABLE_NAME, CREATE_STATEMENT, VIEW_DEFINITION) values (database(), 'view_a', 'create_view_a', 'def_a')",
				"insert into _vt.schema_engine_views(TABLE_SCHEMA, TABLE_NAME, CREATE_STATEMENT, VIEW_DEFINITION) values (database(), 'view_b', 'create_view_b', 'def_b')",
				"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'view_a', 'create_view_a', 'def_a')",
				"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'view_b', 'create_view_b', 'def_b')",
			},
		},
		{
			// view_b modified
			showTablesWithSizesOutput: sqltypes.MakeTestResult(showTableSizesFields, "view_a|VIEW|12345678||123|123", "view_b|VIEW|12345678||123|123"),
			detectViewChangeOutput: sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name", "varchar"),
				"view_b"),
			viewDefinitionsOutput: sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name|view_definition", "varchar|text"),
				"view_b|def_mod_b"),
			createStmtOutput:           []*sqltypes.Result{sqltypes.MakeTestResult(showCreateViewFields, "view_b|create_view_mod_b|utf8|utf8_general_ci")},
			expViewsChanged:            []string{"view_b"},
			expGetViewDefinitionsQuery: "select table_name, view_definition from information_schema.views where table_schema = database() and table_name in ('view_b')",
			expCreateStmtQuery:         []string{"show create table view_b"},
			expHsClearQuery:            "delete from _vt.views where table_schema = database() and table_name in ('view_b')",
			expClearQuery:              "delete from _vt.schema_engine_views where TABLE_SCHEMA = database() and TABLE_NAME in ('view_b')",
			expInsertQuery: []string{
				"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'view_b', 'create_view_mod_b', 'def_mod_b')",
				"insert into _vt.schema_engine_views(TABLE_SCHEMA, TABLE_NAME, CREATE_STATEMENT, VIEW_DEFINITION) values (database(), 'view_b', 'create_view_mod_b', 'def_mod_b')",
			},
		},
		{
			// view_a modified, view_b deleted and view_c added.
			showTablesWithSizesOutput: sqltypes.MakeTestResult(showTableSizesFields, "view_c|VIEW|98732432||123|123", "view_a|VIEW|12345678||123|123"),
			detectViewChangeOutput: sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name", "varchar"),
				"view_a", "view_b", "view_c"),
			viewDefinitionsOutput: sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name|view_definition", "varchar|text"),
				"view_a|def_mod_a", "view_c|def_c"),
			createStmtOutput: []*sqltypes.Result{sqltypes.MakeTestResult(showCreateViewFields, "view_a|create_view_mod_a|utf8|utf8_general_ci"),
				sqltypes.MakeTestResult(showCreateViewFields, "view_c|create_view_c|utf8|utf8_general_ci")},
			expViewsChanged:            []string{"view_a", "view_b", "view_c"},
			expGetViewDefinitionsQuery: "select table_name, view_definition from information_schema.views where table_schema = database() and table_name in ('view_b', 'view_c', 'view_a')",
			expCreateStmtQuery:         []string{"show create table view_a", "show create table view_c"},
			expClearQuery:              "delete from _vt.views where table_schema = database() and table_name in ('view_b', 'view_c', 'view_a')",
			expHsClearQuery:            "delete from _vt.schema_engine_views where TABLE_SCHEMA = database() and TABLE_NAME in ('view_b', 'view_c', 'view_a')",
			expInsertQuery: []string{
				"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'view_a', 'create_view_mod_a', 'def_mod_a')",
				"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'view_c', 'create_view_c', 'def_c')",
				"insert into _vt.schema_engine_views(TABLE_SCHEMA, TABLE_NAME, CREATE_STATEMENT, VIEW_DEFINITION) values (database(), 'view_a', 'create_view_mod_a', 'def_mod_a')",
				"insert into _vt.schema_engine_views(TABLE_SCHEMA, TABLE_NAME, CREATE_STATEMENT, VIEW_DEFINITION) values (database(), 'view_c', 'create_view_c', 'def_c')",
			},
		},
	}

	// setting first test case result.
	db.AddQueryPattern("SELECT .* information_schema.innodb_tablespaces .*", tcases[0].showTablesWithSizesOutput)
	db.AddQueryPattern(".*SELECT table_name, view_definition.*schema_engine_views.*", tcases[0].detectViewChangeOutput)

	db.AddQuery(tcases[0].expGetViewDefinitionsQuery, tcases[0].viewDefinitionsOutput)
	for idx := range tcases[0].expCreateStmtQuery {
		db.AddQuery(tcases[0].expCreateStmtQuery[idx], tcases[0].createStmtOutput[idx])
	}
	for idx := range tcases[0].expInsertQuery {
		db.AddQuery(tcases[0].expInsertQuery[idx], &sqltypes.Result{})
	}
	db.AddQuery(tcases[0].expClearQuery, &sqltypes.Result{})
	db.AddQuery(tcases[0].expHsClearQuery, &sqltypes.Result{})

	var tcCount atomic.Int32
	ch := make(chan struct{})

	go func() {
		hs.Stream(ctx, func(response *querypb.StreamHealthResponse) error {
			if response.RealtimeStats.ViewSchemaChanged != nil {
				sort.Strings(response.RealtimeStats.ViewSchemaChanged)
				assert.Equal(t, tcases[tcCount.Load()].expViewsChanged, response.RealtimeStats.ViewSchemaChanged)
				tcCount.Add(1)
				db.AddQueryPattern(".*SELECT table_name, view_definition.*schema_engine_views.*", &sqltypes.Result{})
				ch <- struct{}{}
				require.NoError(t, db.LastError())
			}
			return nil
		})
	}()

	for {
		select {
		case <-ch:
			if tcCount.Load() == int32(len(tcases)) {
				return
			}
			idx := tcCount.Load()
			db.AddQuery(tcases[idx].expGetViewDefinitionsQuery, tcases[idx].viewDefinitionsOutput)
			for i := range tcases[idx].expCreateStmtQuery {
				db.AddQuery(tcases[idx].expCreateStmtQuery[i], tcases[idx].createStmtOutput[i])
			}
			for i := range tcases[idx].expInsertQuery {
				db.AddQuery(tcases[idx].expInsertQuery[i], &sqltypes.Result{})
			}
			db.AddQuery(tcases[idx].expClearQuery, &sqltypes.Result{})
			db.AddQuery(tcases[idx].expHsClearQuery, &sqltypes.Result{})
			db.AddQueryPattern("SELECT .* information_schema.innodb_tablespaces .*", tcases[idx].showTablesWithSizesOutput)
			db.AddQueryPattern(".*SELECT table_name, view_definition.*schema_engine_views.*", tcases[idx].detectViewChangeOutput)
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out")
		}
	}
}

func testStream(hs *healthStreamer) (<-chan *querypb.StreamHealthResponse, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan *querypb.StreamHealthResponse)
	go func() {
		_ = hs.Stream(ctx, func(shr *querypb.StreamHealthResponse) error {
			ch <- shr
			return nil
		})
	}()
	return ch, cancel
}

func testBlpFunc() (int64, int32) {
	return 1, 2
}
