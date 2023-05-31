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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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
	hs := newHealthStreamer(env, alias)
	hs.isServingPrimary = true
	hs.InitDBConfig(&querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}, config.DB.DbaWithDB())
	hs.Open()
	defer hs.Close()

	// Let's say the tablet goes to a non-serving primary state.
	hs.MakePrimary(false)

	// A reload now should not write anything to the database. If any write happens it will error out since we have not
	// added any query to the database to expect.
	err := hs.reload()
	require.NoError(t, err)
	require.NoError(t, db.LastError())
}

func TestHealthStreamerBroadcast(t *testing.T) {
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
	assert.Equal(t, want, shr)

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
	assert.Equal(t, want, shr)

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
	assert.Equal(t, want, shr)

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
	assert.Equal(t, want, shr)

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
	assert.Equal(t, want, shr)
}

func TestReloadSchema(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	config := newConfig(db)
	config.SignalSchemaChangeReloadIntervalSeconds.Set(100 * time.Millisecond)
	config.SignalWhenSchemaChange = true

	env := tabletenv.NewEnv(config, "ReplTrackerTest")
	alias := &topodatapb.TabletAlias{
		Cell: "cell",
		Uid:  1,
	}
	blpFunc = testBlpFunc
	hs := newHealthStreamer(env, alias)
	hs.MakePrimary(true)

	target := &querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	configs := config.DB

	db.AddQuery(mysql.CreateVTDatabase, &sqltypes.Result{})
	db.AddQuery(mysql.CreateSchemaCopyTable, &sqltypes.Result{})
	db.AddQueryPattern(mysql.ClearSchemaCopy+".*", &sqltypes.Result{})
	db.AddQueryPattern(mysql.InsertIntoSchemaCopy+".*", &sqltypes.Result{})
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("commit", &sqltypes.Result{})
	db.AddQuery("rollback", &sqltypes.Result{})
	db.AddQuery(mysql.DetectSchemaChange, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"table_name",
			"varchar",
		),
		"product",
		"users",
	))

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
	config.SignalSchemaChangeReloadIntervalSeconds.Set(100 * time.Millisecond)
	config.SignalWhenSchemaChange = false

	env := tabletenv.NewEnv(config, "ReplTrackerTest")
	alias := &topodatapb.TabletAlias{
		Cell: "cell",
		Uid:  1,
	}
	blpFunc = testBlpFunc
	hs := newHealthStreamer(env, alias)
	hs.MakePrimary(true)

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
	config.SignalSchemaChangeReloadIntervalSeconds.Set(1 * time.Minute)
	config.SignalWhenSchemaChange = true

	env := tabletenv.NewEnv(config, "ReplTrackerTest")
	alias := &topodatapb.TabletAlias{
		Cell: "cell",
		Uid:  1,
	}
	blpFunc = testBlpFunc
	hs := newHealthStreamer(env, alias)
	hs.MakePrimary(true)

	target := &querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}
	configs := config.DB

	db.AddQuery(mysql.CreateVTDatabase, &sqltypes.Result{})
	db.AddQuery(mysql.CreateSchemaCopyTable, &sqltypes.Result{})
	db.AddQueryPattern(mysql.ClearSchemaCopy+".*", &sqltypes.Result{})
	db.AddQueryPattern(mysql.InsertIntoSchemaCopy+".*", &sqltypes.Result{})
	db.AddQuery("begin", &sqltypes.Result{})
	db.AddQuery("commit", &sqltypes.Result{})
	db.AddQuery("rollback", &sqltypes.Result{})
	db.AddQuery(mysql.DetectSchemaChange, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"table_name",
			"varchar",
		),
		"product",
		"users",
	))

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
