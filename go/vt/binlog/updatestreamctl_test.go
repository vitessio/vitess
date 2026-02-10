/*
Copyright 2026 The Vitess Authors.

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

package binlog

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestNewUpdateStream(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()

	parser := sqlparser.NewTestParser()
	us := NewUpdateStream(ts, "test_keyspace", "cell1", nil, parser)

	require.NotNil(t, us)
	assert.Equal(t, ts, us.ts)
	assert.Equal(t, "test_keyspace", us.keyspace)
	assert.Equal(t, "cell1", us.cell)
	assert.Equal(t, parser, us.parser)
	assert.Nil(t, us.se)
}

func TestUpdateStreamImpl_InitDBConfig(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()

	parser := sqlparser.NewTestParser()
	us := NewUpdateStream(ts, "test_keyspace", "cell1", nil, parser)

	genParams := mysql.ConnParams{
		DbName: "vt_test_keyspace",
	}
	dbcfgs := dbconfigs.NewTestDBConfigs(genParams, genParams, "vt_test_keyspace")

	us.InitDBConfig(dbcfgs)

	require.NotNil(t, us.cp)
	assert.Equal(t, "vt_test_keyspace", us.cp.DBName())
}

func TestUpdateStreamImpl_EnableDisable(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()

	parser := sqlparser.NewTestParser()
	us := NewUpdateStream(ts, "test_keyspace", "cell1", nil, parser)

	genParams := mysql.ConnParams{
		DbName: "vt_test_keyspace",
	}
	dbcfgs := dbconfigs.NewTestDBConfigs(genParams, genParams, "vt_test_keyspace")
	us.InitDBConfig(dbcfgs)

	// Initially disabled
	assert.False(t, us.IsEnabled())

	// Enable
	us.Enable()
	assert.True(t, us.IsEnabled())

	// Enable again should be no-op
	us.Enable()
	assert.True(t, us.IsEnabled())

	// Disable
	us.Disable()
	assert.False(t, us.IsEnabled())

	// Disable again should be no-op
	us.Disable()
	assert.False(t, us.IsEnabled())
}

func TestUpdateStreamImpl_EnableDisableConcurrent(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()

	parser := sqlparser.NewTestParser()
	us := NewUpdateStream(ts, "test_keyspace", "cell1", nil, parser)

	genParams := mysql.ConnParams{
		DbName: "vt_test_keyspace",
	}
	dbcfgs := dbconfigs.NewTestDBConfigs(genParams, genParams, "vt_test_keyspace")
	us.InitDBConfig(dbcfgs)

	var wg sync.WaitGroup

	// Test concurrent enable/disable
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			us.Enable()
		}()
		go func() {
			defer wg.Done()
			us.Disable()
		}()
	}
	wg.Wait()

	// Should be in a valid state (either enabled or disabled)
	_ = us.IsEnabled()
}

func TestStreamList_Init(t *testing.T) {
	sl := &StreamList{}
	sl.Init()

	sl.Lock()
	defer sl.Unlock()
	assert.NotNil(t, sl.streams)
	assert.Equal(t, 0, sl.currentIndex)
	assert.Empty(t, sl.streams)
}

func TestStreamList_AddDelete(t *testing.T) {
	sl := &StreamList{}
	sl.Init()

	called := false
	cancel := func() {
		called = true
	}

	// Add
	idx := sl.Add(cancel)
	assert.Equal(t, 1, idx)

	sl.Lock()
	assert.Len(t, sl.streams, 1)
	sl.Unlock()

	// Add another
	idx2 := sl.Add(cancel)
	assert.Equal(t, 2, idx2)

	sl.Lock()
	assert.Len(t, sl.streams, 2)
	sl.Unlock()

	// Delete first
	sl.Delete(idx)

	sl.Lock()
	assert.Len(t, sl.streams, 1)
	_, exists := sl.streams[idx]
	assert.False(t, exists)
	sl.Unlock()

	// Delete second
	sl.Delete(idx2)

	sl.Lock()
	assert.Empty(t, sl.streams)
	sl.Unlock()

	// Deleting non-existent index should be safe
	sl.Delete(999)

	assert.False(t, called, "cancel should not have been called during delete")
}

func TestStreamList_Stop(t *testing.T) {
	sl := &StreamList{}
	sl.Init()

	callCount := 0
	var mu sync.Mutex

	cancel1 := func() {
		mu.Lock()
		callCount++
		mu.Unlock()
	}
	cancel2 := func() {
		mu.Lock()
		callCount++
		mu.Unlock()
	}

	sl.Add(cancel1)
	sl.Add(cancel2)

	sl.Stop()

	mu.Lock()
	assert.Equal(t, 2, callCount, "both cancel functions should be called")
	mu.Unlock()
}

func TestStreamList_Concurrent(t *testing.T) {
	sl := &StreamList{}
	sl.Init()

	var wg sync.WaitGroup

	// Concurrent adds
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			idx := sl.Add(func() {})
			// Concurrent delete
			sl.Delete(idx)
		}()
	}
	wg.Wait()
}

func TestUsStateNames(t *testing.T) {
	assert.Equal(t, "Enabled", usStateNames[usEnabled])
	assert.Equal(t, "Disabled", usStateNames[usDisabled])
}

func TestUpdateStreamImpl_StreamKeyRange_NotEnabled(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()

	parser := sqlparser.NewTestParser()
	us := NewUpdateStream(ts, "test_keyspace", "cell1", nil, parser)

	genParams := mysql.ConnParams{
		DbName: "vt_test_keyspace",
	}
	dbcfgs := dbconfigs.NewTestDBConfigs(genParams, genParams, "vt_test_keyspace")
	us.InitDBConfig(dbcfgs)

	// Attempt to stream when not enabled
	err := us.StreamKeyRange(ctx, "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5", nil, nil, func(trans *binlogdatapb.BinlogTransaction) error {
		return nil
	})

	assert.ErrorContains(t, err, "update stream service is not enabled")
}

func TestUpdateStreamImpl_StreamTables_NotEnabled(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()

	parser := sqlparser.NewTestParser()
	us := NewUpdateStream(ts, "test_keyspace", "cell1", nil, parser)

	genParams := mysql.ConnParams{
		DbName: "vt_test_keyspace",
	}
	dbcfgs := dbconfigs.NewTestDBConfigs(genParams, genParams, "vt_test_keyspace")
	us.InitDBConfig(dbcfgs)

	// Attempt to stream when not enabled
	err := us.StreamTables(ctx, "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5", []string{"table1"}, nil, func(trans *binlogdatapb.BinlogTransaction) error {
		return nil
	})

	assert.ErrorContains(t, err, "update stream service is not enabled")
}

func TestUpdateStreamImpl_StreamKeyRange_InvalidPosition(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()

	parser := sqlparser.NewTestParser()
	us := NewUpdateStream(ts, "test_keyspace", "cell1", nil, parser)

	genParams := mysql.ConnParams{
		DbName: "vt_test_keyspace",
	}
	dbcfgs := dbconfigs.NewTestDBConfigs(genParams, genParams, "vt_test_keyspace")
	us.InitDBConfig(dbcfgs)

	us.Enable()
	defer us.Disable()

	// Invalid position
	err := us.StreamKeyRange(ctx, "invalid-position", nil, nil, func(trans *binlogdatapb.BinlogTransaction) error {
		return nil
	})

	require.Error(t, err)
}

func TestUpdateStreamImpl_StreamTables_InvalidPosition(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()

	parser := sqlparser.NewTestParser()
	us := NewUpdateStream(ts, "test_keyspace", "cell1", nil, parser)

	genParams := mysql.ConnParams{
		DbName: "vt_test_keyspace",
	}
	dbcfgs := dbconfigs.NewTestDBConfigs(genParams, genParams, "vt_test_keyspace")
	us.InitDBConfig(dbcfgs)

	us.Enable()
	defer us.Disable()

	// Invalid position
	err := us.StreamTables(ctx, "invalid-position", []string{"table1"}, nil, func(trans *binlogdatapb.BinlogTransaction) error {
		return nil
	})

	require.Error(t, err)
}

func TestUpdateStreamImpl_HandlePanic(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()

	parser := sqlparser.NewTestParser()
	us := NewUpdateStream(ts, "test_keyspace", "cell1", nil, parser)

	// Test HandlePanic with no panic
	var err error
	func() {
		defer us.HandlePanic(&err)
	}()
	assert.NoError(t, err)

	// Test HandlePanic with panic
	err = nil
	func() {
		defer us.HandlePanic(&err)
		panic("test panic")
	}()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "test panic")
}

func TestUpdateStreamImpl_HandlePanic_WithError(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()

	parser := sqlparser.NewTestParser()
	us := NewUpdateStream(ts, "test_keyspace", "cell1", nil, parser)

	// Test HandlePanic with error panic
	var err error
	func() {
		defer us.HandlePanic(&err)
		panic(errors.New("test error panic"))
	}()
	assert.ErrorContains(t, err, "test error panic")
}

func TestUpdateStreamImpl_RegisterService(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()

	parser := sqlparser.NewTestParser()
	us := NewUpdateStream(ts, "test_keyspace", "cell1", nil, parser)

	genParams := mysql.ConnParams{
		DbName: "vt_test_keyspace",
	}
	dbcfgs := dbconfigs.NewTestDBConfigs(genParams, genParams, "vt_test_keyspace")
	us.InitDBConfig(dbcfgs)

	// Clear any existing services
	oldServices := RegisterUpdateStreamServices
	RegisterUpdateStreamServices = nil
	defer func() {
		RegisterUpdateStreamServices = oldServices
	}()

	// Add a test registration function
	registered := false
	RegisterUpdateStreamServices = append(RegisterUpdateStreamServices, func(u UpdateStream) {
		registered = true
	})

	// This should call registration functions
	us.RegisterService()

	assert.True(t, registered, "registration function should have been called")
}

func TestUpdateStreamControl_Interface(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()

	parser := sqlparser.NewTestParser()
	us := NewUpdateStream(ts, "test_keyspace", "cell1", nil, parser)

	// Verify UpdateStreamImpl implements UpdateStreamControl
	var _ UpdateStreamControl = us
}

func TestUpdateStream_Interface(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()

	parser := sqlparser.NewTestParser()
	us := NewUpdateStream(ts, "test_keyspace", "cell1", nil, parser)

	// Verify UpdateStreamImpl implements UpdateStream
	var _ UpdateStream = us
}

func TestUpdateStreamImpl_StreamKeyRange_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()

	parser := sqlparser.NewTestParser()
	us := NewUpdateStream(ts, "test_keyspace", "cell1", nil, parser)

	genParams := mysql.ConnParams{
		DbName: "vt_test_keyspace",
	}
	dbcfgs := dbconfigs.NewTestDBConfigs(genParams, genParams, "vt_test_keyspace")
	us.InitDBConfig(dbcfgs)

	us.Enable()
	defer us.Disable()

	// Cancel context before streaming
	cancel()

	// Should fail due to context cancellation (either with context error or other expected error)
	keyRange := &topodatapb.KeyRange{
		Start: []byte{0x40},
		End:   []byte{0x80},
	}
	err := us.StreamKeyRange(ctx, "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5", keyRange, nil, func(trans *binlogdatapb.BinlogTransaction) error {
		return nil
	})

	// The error could be context canceled or resolver factory failure
	require.Error(t, err)
}

func TestUpdateStreamImpl_DisableStopsActiveStreams(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()

	parser := sqlparser.NewTestParser()
	us := NewUpdateStream(ts, "test_keyspace", "cell1", nil, parser)

	genParams := mysql.ConnParams{
		DbName: "vt_test_keyspace",
	}
	dbcfgs := dbconfigs.NewTestDBConfigs(genParams, genParams, "vt_test_keyspace")
	us.InitDBConfig(dbcfgs)

	// Test that streams are properly initialized
	us.Enable()

	// Add a fake stream to the list
	stopped := false
	us.streams.Add(func() {
		stopped = true
	})

	us.Disable()

	assert.True(t, stopped, "stream should have been stopped on disable")
}

func TestUpdateStreamImpl_MultipleEnableDisableCycles(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell1")
	defer ts.Close()

	parser := sqlparser.NewTestParser()
	us := NewUpdateStream(ts, "test_keyspace", "cell1", nil, parser)

	genParams := mysql.ConnParams{
		DbName: "vt_test_keyspace",
	}
	dbcfgs := dbconfigs.NewTestDBConfigs(genParams, genParams, "vt_test_keyspace")
	us.InitDBConfig(dbcfgs)

	// Multiple cycles
	for i := 0; i < 5; i++ {
		us.Enable()
		assert.True(t, us.IsEnabled())

		us.Disable()
		assert.False(t, us.IsEnabled())
	}
}
