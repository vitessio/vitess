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
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
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
	hs := newHealthStreamer(env, alias)
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
	hs := newHealthStreamer(env, alias)
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

	hs.ChangeState(topodatapb.TabletType_REPLICA, time.Time{}, 0, 0, nil, nil, false)
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
	hs.ChangeState(topodatapb.TabletType_PRIMARY, now, 0, 0, nil, nil, true)
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
	hs.ChangeState(topodatapb.TabletType_REPLICA, now, 1*time.Second, 1.0, nil, nil, false)
	shr = <-ch
	want = &querypb.StreamHealthResponse{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_REPLICA,
		},
		TabletAlias: alias,
		RealtimeStats: &querypb.RealtimeStats{
			ReplicationLagSeconds:         1,
			FilteredReplicationLagSeconds: 1,
			ThrottlerMetric:               1.0,
			BinlogPlayersCount:            2,
		},
	}
	assert.Truef(t, proto.Equal(want, shr), "want: %v, got: %v", want, shr)

	// Test Health error.
	hs.ChangeState(topodatapb.TabletType_REPLICA, now, 0, 0, nil, errors.New("repl err"), false)
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
	db := fakesqldb.New(t)
	defer db.Close()
	config := newConfig(db)
	_ = config.SignalSchemaChangeReloadIntervalSeconds.Set("100ms")
	config.SignalWhenSchemaChange = true

	env := tabletenv.NewEnv(config, "ReplTrackerTest")
	alias := &topodatapb.TabletAlias{
		Cell: "cell",
		Uid:  1,
	}
	blpFunc = testBlpFunc
	hs := newHealthStreamer(env, alias)

	target := &querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	configs := config.DB

	db.AddQueryPattern(sqlparser.BuildParsedQuery(mysql.ClearSchemaCopy, sidecardb.GetIdentifier()).Query+".*", &sqltypes.Result{})
	db.AddQueryPattern(sqlparser.BuildParsedQuery(mysql.InsertIntoSchemaCopy, sidecardb.GetIdentifier()).Query+".*", &sqltypes.Result{})
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("commit", &sqltypes.Result{})
	db.AddQuery("rollback", &sqltypes.Result{})
	db.AddQuery(sqlparser.BuildParsedQuery(mysql.DetectSchemaChange, sidecardb.GetIdentifier()).Query,
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"table_name",
				"varchar",
			),
			"product",
			"users",
		))
	db.AddQuery(sqlparser.BuildParsedQuery(mysql.DetectViewChange, sidecardb.GetIdentifier()).Query, &sqltypes.Result{})

	hs.InitDBConfig(target, configs.DbaWithDB())
	hs.Open()
	defer hs.Close()
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
	select {
	case <-c:
	case <-time.After(1 * time.Second):
		t.Errorf("timed out")
	}
}

func TestDoesNotReloadSchema(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	config := newConfig(db)
	_ = config.SignalSchemaChangeReloadIntervalSeconds.Set("100ms")
	config.SignalWhenSchemaChange = false

	env := tabletenv.NewEnv(config, "ReplTrackerTest")
	alias := &topodatapb.TabletAlias{
		Cell: "cell",
		Uid:  1,
	}
	blpFunc = testBlpFunc
	hs := newHealthStreamer(env, alias)

	target := &querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	configs := config.DB

	hs.InitDBConfig(target, configs.DbaWithDB())
	hs.Open()
	defer hs.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		hs.Stream(ctx, func(response *querypb.StreamHealthResponse) error {
			if response.RealtimeStats.TableSchemaChanged != nil {
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

	// here we will wait for a second, to make sure that we are not signaling a changed schema.
	select {
	case <-c:
	case <-time.After(1 * time.Second):
		timeout = true
	}

	assert.True(t, timeout, "should have timed out")
}

func TestInitialReloadSchema(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	config := newConfig(db)
	// Setting the signal schema change reload interval to one minute
	// that way we can test the initial reload trigger.
	_ = config.SignalSchemaChangeReloadIntervalSeconds.Set("1m")
	config.SignalWhenSchemaChange = true

	env := tabletenv.NewEnv(config, "ReplTrackerTest")
	alias := &topodatapb.TabletAlias{
		Cell: "cell",
		Uid:  1,
	}
	blpFunc = testBlpFunc
	hs := newHealthStreamer(env, alias)

	target := &querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	configs := config.DB

	db.AddQueryPattern(sqlparser.BuildParsedQuery(mysql.ClearSchemaCopy, sidecardb.GetIdentifier()).Query+".*", &sqltypes.Result{})
	db.AddQueryPattern(sqlparser.BuildParsedQuery(mysql.InsertIntoSchemaCopy, sidecardb.GetIdentifier()).Query+".*", &sqltypes.Result{})
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("commit", &sqltypes.Result{})
	db.AddQuery("rollback", &sqltypes.Result{})
	db.AddQuery(sqlparser.BuildParsedQuery(mysql.DetectSchemaChange, sidecardb.GetIdentifier()).Query,
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"table_name",
				"varchar",
			),
			"product",
			"users",
		))
	db.AddQuery(sqlparser.BuildParsedQuery(mysql.DetectViewChange, sidecardb.GetIdentifier()).Query, &sqltypes.Result{})

	hs.InitDBConfig(target, configs.DbaWithDB())
	hs.Open()
	defer hs.Close()
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
	select {
	case <-c:
	case <-time.After(1 * time.Second):
		// should not timeout despite SignalSchemaChangeReloadIntervalSeconds being set to 1 minute
		t.Errorf("timed out")
	}
}

// TestReloadView tests that the health streamer tracks view changes correctly
func TestReloadView(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	config := newConfig(db)
	_ = config.SignalSchemaChangeReloadIntervalSeconds.Set("100ms")
	config.EnableViews = true

	env := tabletenv.NewEnv(config, "TestReloadView")
	alias := &topodatapb.TabletAlias{Cell: "cell", Uid: 1}
	hs := newHealthStreamer(env, alias)

	target := &querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	configs := config.DB

	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("commit", &sqltypes.Result{})
	db.AddQuery("rollback", &sqltypes.Result{})
	db.AddQuery(sqlparser.BuildParsedQuery(mysql.DetectSchemaChangeOnlyBaseTable, sidecardb.GetIdentifier()).Query, &sqltypes.Result{})
	db.AddQuery(sqlparser.BuildParsedQuery(mysql.DetectViewChange, sidecardb.GetIdentifier()).Query, &sqltypes.Result{})

	hs.InitDBConfig(target, configs.DbaWithDB())
	hs.Open()
	defer hs.Close()

	showCreateViewFields := sqltypes.MakeTestFields(
		"View|Create View|character_set_client|collation_connection",
		"varchar|text|varchar|varchar")
	tcases := []struct {
		tbl            *sqltypes.Result
		def            *sqltypes.Result
		stmt           []*sqltypes.Result
		expTbl         []string
		expDefQuery    string
		expStmtQuery   []string
		expClearQuery  string
		expInsertQuery []string
	}{{
		// view_a and view_b added.
		tbl: sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name", "varchar"),
			"view_a", "view_b"),
		def: sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name|view_definition", "varchar|text"),
			"view_a|def_a", "view_b|def_b"),
		stmt: []*sqltypes.Result{sqltypes.MakeTestResult(showCreateViewFields, "view_a|create_view_a|utf8|utf8_general_ci"),
			sqltypes.MakeTestResult(showCreateViewFields, "view_b|create_view_b|utf8|utf8_general_ci")},
		expTbl:        []string{"view_a", "view_b"},
		expDefQuery:   "select table_name, view_definition from information_schema.views where table_schema = database() and table_name in ('view_a', 'view_b')",
		expStmtQuery:  []string{"show create table view_a", "show create table view_b"},
		expClearQuery: "delete from _vt.views where table_schema = database() and table_name in ('view_a', 'view_b')",
		expInsertQuery: []string{
			"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'view_a', 'create_view_a', 'def_a')",
			"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'view_b', 'create_view_b', 'def_b')",
		},
	}, {
		// view_b modified
		tbl: sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name", "varchar"),
			"view_b"),
		def: sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name|view_definition", "varchar|text"),
			"view_b|def_mod_b"),
		stmt:          []*sqltypes.Result{sqltypes.MakeTestResult(showCreateViewFields, "view_b|create_view_mod_b|utf8|utf8_general_ci")},
		expTbl:        []string{"view_b"},
		expDefQuery:   "select table_name, view_definition from information_schema.views where table_schema = database() and table_name in ('view_b')",
		expStmtQuery:  []string{"show create table view_b"},
		expClearQuery: "delete from _vt.views where table_schema = database() and table_name in ('view_b')",
		expInsertQuery: []string{
			"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'view_b', 'create_view_mod_b', 'def_mod_b')",
		},
	}, {
		// view_a modified, view_b deleted and view_c added.
		tbl: sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name", "varchar"),
			"view_a", "view_b", "view_c"),
		def: sqltypes.MakeTestResult(sqltypes.MakeTestFields("table_name|view_definition", "varchar|text"),
			"view_a|def_mod_a", "view_c|def_c"),
		stmt: []*sqltypes.Result{sqltypes.MakeTestResult(showCreateViewFields, "view_a|create_view_mod_a|utf8|utf8_general_ci"),
			sqltypes.MakeTestResult(showCreateViewFields, "view_c|create_view_c|utf8|utf8_general_ci")},
		expTbl:        []string{"view_a", "view_b", "view_c"},
		expDefQuery:   "select table_name, view_definition from information_schema.views where table_schema = database() and table_name in ('view_a', 'view_b', 'view_c')",
		expStmtQuery:  []string{"show create table view_a", "show create table view_c"},
		expClearQuery: "delete from _vt.views where table_schema = database() and table_name in ('view_a', 'view_b', 'view_c')",
		expInsertQuery: []string{
			"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'view_a', 'create_view_mod_a', 'def_mod_a')",
			"insert into _vt.views(table_schema, table_name, create_statement, view_definition) values (database(), 'view_c', 'create_view_c', 'def_c')",
		},
	}}

	// setting first test case result.
	db.AddQuery(sqlparser.BuildParsedQuery(mysql.DetectViewChange, sidecardb.GetIdentifier()).Query, tcases[0].tbl)
	db.AddQuery(tcases[0].expDefQuery, tcases[0].def)
	for idx, stmt := range tcases[0].stmt {
		db.AddQuery(tcases[0].expStmtQuery[idx], stmt)
		db.AddQuery(tcases[0].expInsertQuery[idx], &sqltypes.Result{})
	}
	db.AddQuery(tcases[0].expClearQuery, &sqltypes.Result{})

	var tcCount atomic.Int32
	ch := make(chan struct{})

	go func() {
		hs.Stream(ctx, func(response *querypb.StreamHealthResponse) error {
			if response.RealtimeStats.ViewSchemaChanged != nil {
				sort.Strings(response.RealtimeStats.ViewSchemaChanged)
				assert.Equal(t, tcases[tcCount.Load()].expTbl, response.RealtimeStats.ViewSchemaChanged)
				tcCount.Add(1)
				db.AddQuery(sqlparser.BuildParsedQuery(mysql.DetectViewChange, sidecardb.GetIdentifier()).Query, &sqltypes.Result{})
				ch <- struct{}{}
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
			db.AddQuery(tcases[idx].expDefQuery, tcases[idx].def)
			for i, stmt := range tcases[idx].stmt {
				db.AddQuery(tcases[idx].expStmtQuery[i], stmt)
				db.AddQuery(tcases[idx].expInsertQuery[i], &sqltypes.Result{})
			}
			db.AddQuery(tcases[idx].expClearQuery, &sqltypes.Result{})
			db.AddQuery(sqlparser.BuildParsedQuery(mysql.DetectViewChange, sidecardb.GetIdentifier()).Query, tcases[idx].tbl)
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
