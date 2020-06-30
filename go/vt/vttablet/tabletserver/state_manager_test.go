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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/history"
	"vitess.io/vitess/go/sync2"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func TestStateManagerStateByName(t *testing.T) {
	states := []int64{
		StateNotConnected,
		StateNotServing,
		StateServing,
	}
	// Don't reuse stateName.
	names := []string{
		"NOT_SERVING",
		"NOT_SERVING",
		"SERVING",
	}
	sm := &stateManager{}
	for i, state := range states {
		sm.state = state
		require.Equal(t, names[i], sm.StateByName(), "StateByName")
	}
	sm.EnterLameduck()
	require.Equal(t, "NOT_SERVING", sm.StateByName(), "StateByName")
}

func TestStateManagerServeMaster(t *testing.T) {
	sm := newTestStateManager(t)
	sm.EnterLameduck()
	stateChanged, err := sm.SetServingType(topodatapb.TabletType_MASTER, StateServing, nil)
	require.NoError(t, err)
	assert.True(t, stateChanged)

	assert.Equal(t, int32(0), sm.lameduck.Get())

	verifySubcomponent(t, sm.watcher, 1, testStateClosed)
	verifySubcomponent(t, sm.hr, 2, testStateClosed)

	verifySubcomponent(t, sm.se, 3, testStateOpen)
	verifySubcomponent(t, sm.vstreamer, 4, testStateOpen)
	verifySubcomponent(t, sm.qe, 5, testStateOpen)
	verifySubcomponent(t, sm.txThrottler, 6, testStateOpen)
	verifySubcomponent(t, sm.hw, 7, testStateOpen)
	verifySubcomponent(t, sm.tracker, 8, testStateOpen)
	verifySubcomponent(t, sm.te, 9, testStateAcceptReadWrite)
	verifySubcomponent(t, sm.messager, 10, testStateOpen)

	assert.False(t, sm.se.(*testSchemaEngine).nonMaster)
	assert.True(t, sm.qe.(*testQueryEngine).isReachable)
	assert.False(t, sm.qe.(*testQueryEngine).stopServing)

	assert.Equal(t, topodatapb.TabletType_MASTER, sm.target.TabletType)
	assert.Equal(t, int64(StateServing), sm.state)
}

func TestStateManagerServeNonMaster(t *testing.T) {
	sm := newTestStateManager(t)
	stateChanged, err := sm.SetServingType(topodatapb.TabletType_REPLICA, StateServing, nil)
	require.NoError(t, err)
	assert.True(t, stateChanged)

	verifySubcomponent(t, sm.messager, 1, testStateClosed)
	verifySubcomponent(t, sm.tracker, 2, testStateClosed)
	verifySubcomponent(t, sm.hw, 3, testStateClosed)
	assert.True(t, sm.se.(*testSchemaEngine).nonMaster)

	verifySubcomponent(t, sm.se, 4, testStateOpen)
	verifySubcomponent(t, sm.vstreamer, 5, testStateOpen)
	verifySubcomponent(t, sm.qe, 6, testStateOpen)
	verifySubcomponent(t, sm.txThrottler, 7, testStateOpen)
	verifySubcomponent(t, sm.te, 8, testStateAcceptReadOnly)
	verifySubcomponent(t, sm.hr, 9, testStateOpen)
	verifySubcomponent(t, sm.watcher, 10, testStateOpen)

	assert.Equal(t, topodatapb.TabletType_REPLICA, sm.target.TabletType)
	assert.Equal(t, int64(StateServing), sm.state)
}

func TestStateManagerUnserveMaster(t *testing.T) {
	sm := newTestStateManager(t)
	stateChanged, err := sm.SetServingType(topodatapb.TabletType_MASTER, StateNotServing, nil)
	require.NoError(t, err)
	assert.True(t, stateChanged)

	verifySubcomponent(t, sm.messager, 1, testStateClosed)
	verifySubcomponent(t, sm.te, 2, testStateClosed)
	assert.True(t, sm.qe.(*testQueryEngine).stopServing)

	verifySubcomponent(t, sm.watcher, 3, testStateClosed)
	verifySubcomponent(t, sm.hr, 4, testStateClosed)

	verifySubcomponent(t, sm.se, 5, testStateOpen)
	verifySubcomponent(t, sm.vstreamer, 6, testStateOpen)
	verifySubcomponent(t, sm.qe, 7, testStateOpen)
	verifySubcomponent(t, sm.txThrottler, 8, testStateOpen)

	verifySubcomponent(t, sm.hw, 9, testStateOpen)
	verifySubcomponent(t, sm.tracker, 10, testStateOpen)

	assert.Equal(t, topodatapb.TabletType_MASTER, sm.target.TabletType)
	assert.Equal(t, int64(StateNotServing), sm.state)
}

func TestStateManagerUnserveNonmaster(t *testing.T) {
	sm := newTestStateManager(t)
	stateChanged, err := sm.SetServingType(topodatapb.TabletType_RDONLY, StateNotServing, nil)
	require.NoError(t, err)
	assert.True(t, stateChanged)

	verifySubcomponent(t, sm.messager, 1, testStateClosed)
	verifySubcomponent(t, sm.te, 2, testStateClosed)
	assert.True(t, sm.qe.(*testQueryEngine).stopServing)

	verifySubcomponent(t, sm.tracker, 3, testStateClosed)
	verifySubcomponent(t, sm.hw, 4, testStateClosed)
	assert.True(t, sm.se.(*testSchemaEngine).nonMaster)

	verifySubcomponent(t, sm.se, 5, testStateOpen)
	verifySubcomponent(t, sm.vstreamer, 6, testStateOpen)
	verifySubcomponent(t, sm.qe, 7, testStateOpen)
	verifySubcomponent(t, sm.txThrottler, 8, testStateOpen)

	verifySubcomponent(t, sm.hr, 9, testStateOpen)
	verifySubcomponent(t, sm.watcher, 10, testStateOpen)

	assert.Equal(t, topodatapb.TabletType_RDONLY, sm.target.TabletType)
	assert.Equal(t, int64(StateNotServing), sm.state)
}

func TestStateManagerClose(t *testing.T) {
	sm := newTestStateManager(t)
	stateChanged, err := sm.SetServingType(topodatapb.TabletType_RDONLY, StateNotConnected, nil)
	require.NoError(t, err)
	assert.True(t, stateChanged)

	verifySubcomponent(t, sm.messager, 1, testStateClosed)
	verifySubcomponent(t, sm.te, 2, testStateClosed)
	assert.True(t, sm.qe.(*testQueryEngine).stopServing)

	verifySubcomponent(t, sm.txThrottler, 3, testStateClosed)
	verifySubcomponent(t, sm.qe, 4, testStateClosed)
	verifySubcomponent(t, sm.watcher, 5, testStateClosed)
	verifySubcomponent(t, sm.tracker, 6, testStateClosed)
	verifySubcomponent(t, sm.vstreamer, 7, testStateClosed)
	verifySubcomponent(t, sm.hr, 8, testStateClosed)
	verifySubcomponent(t, sm.hw, 9, testStateClosed)
	verifySubcomponent(t, sm.se, 10, testStateClosed)

	assert.Equal(t, topodatapb.TabletType_RDONLY, sm.target.TabletType)
	assert.Equal(t, int64(StateNotConnected), sm.state)
}

func TestStateManagerStopService(t *testing.T) {
	sm := newTestStateManager(t)
	stateChanged, err := sm.SetServingType(topodatapb.TabletType_REPLICA, StateServing, nil)
	require.NoError(t, err)
	assert.True(t, stateChanged)

	assert.Equal(t, topodatapb.TabletType_REPLICA, sm.target.TabletType)
	assert.Equal(t, int64(StateServing), sm.state)

	sm.StopService()
	assert.Equal(t, topodatapb.TabletType_REPLICA, sm.target.TabletType)
	assert.Equal(t, int64(StateNotConnected), sm.state)
}

// testWatcher is used as a hook to invoke another transition
type testWatcher struct {
	t  *testing.T
	sm *stateManager
	wg sync.WaitGroup
}

func (te *testWatcher) Open() {
}

func (te *testWatcher) Close() {
	te.wg.Add(1)
	go func() {
		defer te.wg.Done()

		stateChanged, err := te.sm.SetServingType(topodatapb.TabletType_RDONLY, StateNotServing, nil)
		assert.NoError(te.t, err)
		assert.True(te.t, stateChanged)
	}()
}

func TestStateManagerSetServingTypeRace(t *testing.T) {
	sm := newTestStateManager(t)
	te := &testWatcher{
		t:  t,
		sm: sm,
	}
	sm.watcher = te
	stateChanged, err := sm.SetServingType(topodatapb.TabletType_MASTER, StateServing, nil)
	require.NoError(t, err)
	assert.True(t, stateChanged)

	// Ensure the next call waits and then succeeds.
	te.wg.Wait()

	// End state should be the final desired state.
	assert.Equal(t, topodatapb.TabletType_RDONLY, sm.target.TabletType)
	assert.Equal(t, int64(StateNotServing), sm.state)
}

func TestStateManagerSetServingTypeNoChange(t *testing.T) {
	sm := newTestStateManager(t)
	stateChanged, err := sm.SetServingType(topodatapb.TabletType_REPLICA, StateServing, nil)
	require.NoError(t, err)
	assert.True(t, stateChanged)

	stateChanged, err = sm.SetServingType(topodatapb.TabletType_REPLICA, StateServing, nil)
	require.NoError(t, err)
	assert.False(t, stateChanged)

	verifySubcomponent(t, sm.messager, 1, testStateClosed)
	verifySubcomponent(t, sm.tracker, 2, testStateClosed)
	verifySubcomponent(t, sm.hw, 3, testStateClosed)
	assert.True(t, sm.se.(*testSchemaEngine).nonMaster)

	verifySubcomponent(t, sm.se, 4, testStateOpen)
	verifySubcomponent(t, sm.vstreamer, 5, testStateOpen)
	verifySubcomponent(t, sm.qe, 6, testStateOpen)
	verifySubcomponent(t, sm.txThrottler, 7, testStateOpen)
	verifySubcomponent(t, sm.te, 8, testStateAcceptReadOnly)
	verifySubcomponent(t, sm.hr, 9, testStateOpen)
	verifySubcomponent(t, sm.watcher, 10, testStateOpen)

	assert.Equal(t, topodatapb.TabletType_REPLICA, sm.target.TabletType)
	assert.Equal(t, int64(StateServing), sm.state)
}

func TestStateManagerTransitionFailRetry(t *testing.T) {
	defer func(saved time.Duration) { transitionRetryInterval = saved }(transitionRetryInterval)
	transitionRetryInterval = 10 * time.Millisecond

	sm := newTestStateManager(t)
	sm.qe.(*testQueryEngine).failMySQL = true

	stateChanged, err := sm.SetServingType(topodatapb.TabletType_MASTER, StateServing, nil)
	require.Error(t, err)
	assert.True(t, stateChanged)

	for {
		sm.mu.Lock()
		retrying := sm.retrying
		sm.mu.Unlock()
		if !retrying {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	assert.Equal(t, topodatapb.TabletType_MASTER, sm.Target().TabletType)
	assert.Equal(t, int64(StateServing), sm.State())
}

func TestStateManagerRestoreType(t *testing.T) {
	sm := newTestStateManager(t)
	sm.EnterLameduck()
	stateChanged, err := sm.SetServingType(topodatapb.TabletType_RESTORE, StateNotServing, nil)
	require.NoError(t, err)
	assert.True(t, stateChanged)

	assert.Equal(t, topodatapb.TabletType_RESTORE, sm.target.TabletType)
	// RESTORE can only be in StateNotConnected.
	assert.Equal(t, int64(StateNotConnected), sm.state)
}

func TestStateManagerCheckMySQL(t *testing.T) {
	defer func(saved time.Duration) { transitionRetryInterval = saved }(transitionRetryInterval)
	transitionRetryInterval = 10 * time.Millisecond

	sm := newTestStateManager(t)

	stateChanged, err := sm.SetServingType(topodatapb.TabletType_MASTER, StateServing, nil)
	require.NoError(t, err)
	assert.True(t, stateChanged)

	sm.qe.(*testQueryEngine).failMySQL = true
	order.Set(0)
	sm.CheckMySQL()

	// Wait for closeAll to get under way.
	for {
		if order.Get() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Wait to get out of transitioning state.
	for {
		if !sm.isTransitioning() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for retry to finish.
	for {
		sm.mu.Lock()
		retrying := sm.retrying
		sm.mu.Unlock()
		if !retrying {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	assert.Equal(t, topodatapb.TabletType_MASTER, sm.Target().TabletType)
	assert.Equal(t, int64(StateServing), sm.State())
}

func TestStateManagerValidations(t *testing.T) {
	sm := newTestStateManager(t)
	target := &querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	sm.target = *target

	err := sm.StartRequest(ctx, target, false)
	assert.Contains(t, err.Error(), "operation not allowed")

	sm.state = StateServing
	sm.wantState = StateNotServing
	err = sm.StartRequest(ctx, target, false)
	assert.Contains(t, err.Error(), "operation not allowed")

	err = sm.StartRequest(ctx, target, true)
	assert.NoError(t, err)

	sm.wantState = StateServing
	target.Keyspace = "a"
	err = sm.StartRequest(ctx, target, false)
	assert.Contains(t, err.Error(), "invalid keyspace")
	err = sm.VerifyTarget(ctx, target)
	assert.Contains(t, err.Error(), "invalid keyspace")

	target.Keyspace = ""
	target.Shard = "a"
	err = sm.StartRequest(ctx, target, false)
	assert.Contains(t, err.Error(), "invalid shard")
	err = sm.VerifyTarget(ctx, target)
	assert.Contains(t, err.Error(), "invalid shard")

	target.Shard = ""
	target.TabletType = topodatapb.TabletType_REPLICA
	err = sm.StartRequest(ctx, target, false)
	assert.Contains(t, err.Error(), "invalid tablet type")
	err = sm.VerifyTarget(ctx, target)
	assert.Contains(t, err.Error(), "invalid tablet type")

	sm.alsoAllow = []topodatapb.TabletType{topodatapb.TabletType_REPLICA}
	err = sm.StartRequest(ctx, target, false)
	assert.NoError(t, err)
	err = sm.VerifyTarget(ctx, target)
	assert.NoError(t, err)

	err = sm.StartRequest(ctx, nil, false)
	assert.Contains(t, err.Error(), "No target")
	err = sm.VerifyTarget(ctx, nil)
	assert.Contains(t, err.Error(), "No target")

	localctx := tabletenv.LocalContext()
	err = sm.StartRequest(localctx, nil, false)
	assert.NoError(t, err)
	err = sm.VerifyTarget(localctx, nil)
	assert.NoError(t, err)
}

func TestStateManagerWaitForRequests(t *testing.T) {
	sm := newTestStateManager(t)
	target := &querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	sm.target = *target
	sm.timebombDuration = 10 * time.Second

	_, err := sm.SetServingType(topodatapb.TabletType_MASTER, StateServing, nil)
	require.NoError(t, err)

	err = sm.StartRequest(ctx, target, false)
	require.NoError(t, err)

	// This will go into transition and wait.
	// Wait for that state.
	go sm.StopService()
	for {
		if !sm.isTransitioning() {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		break
	}

	// Verify that we're still transitioning.
	assert.True(t, sm.isTransitioning())

	sm.EndRequest()

	for {
		if sm.isTransitioning() {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		break
	}
	assert.Equal(t, int64(StateNotConnected), sm.State())
}

func verifySubcomponent(t *testing.T, component interface{}, order int64, state testState) {
	tos := component.(orderState)
	assert.Equal(t, order, tos.Order())
	assert.Equal(t, state, tos.State())
}

func newTestStateManager(t *testing.T) *stateManager {
	order.Set(0)
	return &stateManager{
		se:          &testSchemaEngine{},
		hw:          &testSubcomponent{},
		hr:          &testSubcomponent{},
		vstreamer:   &testSubcomponent{},
		tracker:     &testSubcomponent{},
		watcher:     &testSubcomponent{},
		qe:          &testQueryEngine{},
		txThrottler: &testTxThrottler{},
		te:          &testTxEngine{},
		messager:    &testSubcomponent{},

		transitioning:       sync2.NewSemaphore(1, 0),
		checkMySQLThrottler: sync2.NewSemaphore(1, 0),
		history:             history.New(10),
		timebombDuration:    time.Duration(10 * time.Millisecond),
	}
}

func (sm *stateManager) isTransitioning() bool {
	if sm.transitioning.TryAcquire() {
		sm.transitioning.Release()
		return false
	}
	return true
}

var order sync2.AtomicInt64

type testState int

const (
	testStateUnknown = testState(iota)
	testStateOpen
	testStateClosed
	testStateMakeNonMaster
	testStateAcceptReadOnly
	testStateAcceptReadWrite
)

type orderState interface {
	Order() int64
	State() testState
}

type testOrderState struct {
	order int64
	state testState
}

func (tos testOrderState) Order() int64 {
	return tos.order
}

func (tos testOrderState) State() testState {
	return tos.state
}

type testSchemaEngine struct {
	testOrderState
	nonMaster bool
}

func (te *testSchemaEngine) Open() error {
	te.order = order.Add(1)
	te.state = testStateOpen
	return nil
}

func (te *testSchemaEngine) MakeNonMaster() {
	te.nonMaster = true
}

func (te *testSchemaEngine) Close() {
	te.order = order.Add(1)
	te.state = testStateClosed
}

type testQueryEngine struct {
	testOrderState
	isReachable bool
	stopServing bool

	failMySQL bool
}

func (te *testQueryEngine) Open() error {
	te.order = order.Add(1)
	te.state = testStateOpen
	return nil
}

func (te *testQueryEngine) IsMySQLReachable() error {
	if te.failMySQL {
		te.failMySQL = false
		return errors.New("intentional error")
	}
	te.isReachable = true
	return nil
}

func (te *testQueryEngine) StopServing() {
	te.stopServing = true
}

func (te *testQueryEngine) Close() {
	te.order = order.Add(1)
	te.state = testStateClosed
}

type testTxEngine struct {
	testOrderState
}

func (te *testTxEngine) AcceptReadWrite() error {
	te.order = order.Add(1)
	te.state = testStateAcceptReadWrite
	return nil
}

func (te *testTxEngine) AcceptReadOnly() error {
	te.order = order.Add(1)
	te.state = testStateAcceptReadOnly
	return nil
}

func (te *testTxEngine) Close() {
	te.order = order.Add(1)
	te.state = testStateClosed
}

type testSubcomponent struct {
	testOrderState
}

func (te *testSubcomponent) Open() {
	te.order = order.Add(1)
	te.state = testStateOpen
}

func (te *testSubcomponent) Close() {
	te.order = order.Add(1)
	te.state = testStateClosed
}

type testTxThrottler struct {
	testOrderState
}

func (te *testTxThrottler) Open() error {
	te.order = order.Add(1)
	te.state = testStateOpen
	return nil
}

func (te *testTxThrottler) Close() {
	te.order = order.Add(1)
	te.state = testStateClosed
}
