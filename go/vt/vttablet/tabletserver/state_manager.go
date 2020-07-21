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
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/history"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

type servingState int64

const (
	// StateNotConnected is the state where tabletserver is not
	// connected to an underlying mysql instance.
	StateNotConnected = servingState(iota)
	// StateNotServing is the state where tabletserver is connected
	// to an underlying mysql instance, but is not serving queries.
	StateNotServing
	// StateServing is where queries are allowed.
	StateServing
)

// transitionRetryInterval is for tests.
var transitionRetryInterval = 1 * time.Second

// stateName names every state. The number of elements must
// match the number of states. Names can overlap.
var stateName = []string{
	"NOT_SERVING",
	"NOT_SERVING",
	"SERVING",
}

// stateDetail matches every state and optionally more information about the reason
// why the state is serving / not serving.
var stateDetail = []string{
	"Not Connected",
	"Not Serving",
	"",
}

// stateManager manages state transition for all the TabletServer
// subcomponents.
type stateManager struct {
	// transitioning is a semaphore that must to be obtained
	// before attempting a state transition. To prevent deadlocks,
	// this must be acquired before the mu lock. We use a semaphore
	// because we need TryAcquire, which is not supported by sync.Mutex.
	// If an acquire is successful, we must either Release explicitly
	// or invoke execTransition, which will release once it's done.
	transitioning *sync2.Semaphore

	// mu should be held to access the group of variables under it.
	// It is required in spite of the transitioning semaphore.
	// This is because other goroutines will still want
	// read the values while a transition is in progress.
	//
	// If a transition fails, we set retrying to true and launch
	// retryTransition which loops until the state converges.
	mu             sync.Mutex
	wantState      servingState
	wantTabletType topodatapb.TabletType
	state          servingState
	target         querypb.Target
	retrying       bool
	// TODO(sougou): deprecate alsoAllow
	alsoAllow []topodatapb.TabletType

	requests sync.WaitGroup
	lameduck sync2.AtomicInt32

	// Open must be done in forward order.
	// Close must be done in reverse order.
	// All Close functions must be called before Open.
	se          schemaEngine
	hw          subComponent
	hr          subComponent
	vstreamer   subComponent
	tracker     subComponent
	watcher     subComponent
	qe          queryEngine
	txThrottler txThrottler
	te          txEngine
	messager    subComponent

	// checkMySQLThrottler ensures that CheckMysql
	// doesn't get spammed.
	checkMySQLThrottler *sync2.Semaphore
	history             *history.History
	timebombDuration    time.Duration
}

type schemaEngine interface {
	Open() error
	MakeNonMaster()
	Close()
}

type queryEngine interface {
	Open() error
	IsMySQLReachable() error
	StopServing()
	Close()
}

type txEngine interface {
	AcceptReadWrite() error
	AcceptReadOnly() error
	Close()
}

type subComponent interface {
	Open()
	Close()
}

type txThrottler interface {
	Open() error
	Close()
}

// SetServingType changes the state to the specified settings.
// If a transition is in progress, it waits and then executes the
// new request. If the transition fails, it returns an error, and
// launches retryTransition to ensure that the request will eventually
// be honored.
// If sm is already in the requested state, it returns stateChanged as
// false.
func (sm *stateManager) SetServingType(tabletType topodatapb.TabletType, state servingState, alsoAllow []topodatapb.TabletType) (stateChanged bool, err error) {
	defer sm.ExitLameduck()

	if tabletType == topodatapb.TabletType_RESTORE {
		// TODO(sougou): remove this code once tm can give us more accurate state requests.
		state = StateNotConnected
	}

	log.Infof("Starting transition to %v %v", tabletType, stateName[state])
	if sm.mustTransition(tabletType, state, alsoAllow) {
		return true, sm.execTransition(tabletType, state)
	}
	return false, nil
}

// mustTransition returns true if the requested state does not match the current
// state. If so, it acquires the semaphore and returns true. If a transition is
// already in progress, it waits. If the desired state is already reached, it
// returns false without acquiring the semaphore.
func (sm *stateManager) mustTransition(tabletType topodatapb.TabletType, state servingState, alsoAllow []topodatapb.TabletType) bool {
	sm.transitioning.Acquire()
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.wantTabletType = tabletType
	sm.wantState = state
	sm.alsoAllow = alsoAllow
	if sm.target.TabletType == tabletType && sm.state == state {
		sm.transitioning.Release()
		return false
	}
	return true
}

func (sm *stateManager) execTransition(tabletType topodatapb.TabletType, state servingState) error {
	defer sm.transitioning.Release()

	var err error
	switch state {
	case StateServing:
		if tabletType == topodatapb.TabletType_MASTER {
			err = sm.serveMaster()
		} else {
			err = sm.serveNonMaster(tabletType)
		}
	case StateNotServing:
		if tabletType == topodatapb.TabletType_MASTER {
			err = sm.unserveMaster()
		} else {
			err = sm.unserveNonMaster(tabletType)
		}
	case StateNotConnected:
		sm.closeAll()
	}
	if err != nil {
		sm.retryTransition(fmt.Sprintf("Error transitioning to the desired state: %v, %v, will keep retrying: %v", tabletType, stateName[state], err))
	}
	return err
}

func (sm *stateManager) retryTransition(message string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.retrying {
		return
	}
	sm.retrying = true

	log.Error(message)
	go func() {
		for {
			time.Sleep(transitionRetryInterval)
			if sm.recheckState() {
				return
			}
		}
	}()
}

func (sm *stateManager) recheckState() bool {
	if !sm.transitioning.TryAcquire() {
		return false
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.wantState == sm.state && sm.wantTabletType == sm.target.TabletType {
		sm.retrying = false
		sm.transitioning.Release()
		return true
	}
	go sm.execTransition(sm.wantTabletType, sm.wantState)
	return false
}

// CheckMySQL verifies that we can connect to mysql.
// If it fails, then we shutdown the service and initiate
// the retry loop.
func (sm *stateManager) CheckMySQL() {
	if !sm.checkMySQLThrottler.TryAcquire() {
		return
	}
	go func() {
		defer func() {
			time.Sleep(1 * time.Second)
			sm.checkMySQLThrottler.Release()
		}()

		err := sm.qe.IsMySQLReachable()
		if err == nil {
			return
		}

		if !sm.transitioning.TryAcquire() {
			// If we're already transitioning, don't interfere.
			return
		}
		defer sm.transitioning.Release()

		sm.closeAll()
		sm.retryTransition(fmt.Sprintf("Cannot connect to MySQL, shutting down query service: %v", err))
	}()
}

// StopService shuts down sm. If the shutdown doesn't complete
// within timeBombDuration, it crashes the process.
func (sm *stateManager) StopService() {
	defer close(sm.setTimeBomb())
	sm.SetServingType(sm.Target().TabletType, StateNotConnected, nil)
}

// StartRequest validates the current state and target and registers
// the request (a waitgroup) as started. Every StartRequest must be
// ended with an EndRequest.
func (sm *stateManager) StartRequest(ctx context.Context, target *querypb.Target, allowOnShutdown bool) (err error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.state != StateServing {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "operation not allowed in state %s", stateName[sm.state])
	}

	shuttingDown := sm.wantState != StateServing
	if shuttingDown && !allowOnShutdown {
		// This specific error string needs to be returned for vtgate buffering to work.
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "operation not allowed in state SHUTTING_DOWN")
	}

	if target != nil {
		switch {
		case target.Keyspace != sm.target.Keyspace:
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid keyspace %v does not match expected %v", target.Keyspace, sm.target.Keyspace)
		case target.Shard != sm.target.Shard:
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid shard %v does not match expected %v", target.Shard, sm.target.Shard)
		case target.TabletType != sm.target.TabletType:
			for _, otherType := range sm.alsoAllow {
				if target.TabletType == otherType {
					goto ok
				}
			}
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "invalid tablet type: %v, want: %v or %v", target.TabletType, sm.target.TabletType, sm.alsoAllow)
		}
	} else {
		if !tabletenv.IsLocalContext(ctx) {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No target")
		}
	}

ok:
	sm.requests.Add(1)
	return nil
}

// EndRequest unregisters the current request (a waitgroup) as done.
func (sm *stateManager) EndRequest() {
	sm.requests.Done()
}

// VerifyTarget allows requests to be executed even in non-serving state.
// Such requests will get terminated without wait on shutdown.
func (sm *stateManager) VerifyTarget(ctx context.Context, target *querypb.Target) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if target != nil {
		switch {
		case target.Keyspace != sm.target.Keyspace:
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid keyspace %v does not match expected %v", target.Keyspace, sm.target.Keyspace)
		case target.Shard != sm.target.Shard:
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid shard %v does not match expected %v", target.Shard, sm.target.Shard)
		case target.TabletType != sm.target.TabletType:
			for _, otherType := range sm.alsoAllow {
				if target.TabletType == otherType {
					return nil
				}
			}
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "invalid tablet type: %v, want: %v or %v", target.TabletType, sm.target.TabletType, sm.alsoAllow)
		}
	} else {
		if !tabletenv.IsLocalContext(ctx) {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No target")
		}
	}
	return nil
}

func (sm *stateManager) serveMaster() error {
	sm.watcher.Close()
	sm.hr.Close()

	if err := sm.connect(); err != nil {
		return err
	}

	sm.hw.Open()
	sm.tracker.Open()
	if err := sm.te.AcceptReadWrite(); err != nil {
		return err
	}
	sm.messager.Open()
	sm.setState(topodatapb.TabletType_MASTER, StateServing)
	return nil
}

func (sm *stateManager) unserveMaster() error {
	sm.unserveCommon()

	sm.watcher.Close()
	sm.hr.Close()

	if err := sm.connect(); err != nil {
		return err
	}

	sm.hw.Open()
	sm.tracker.Open()
	sm.setState(topodatapb.TabletType_MASTER, StateNotServing)
	return nil
}

func (sm *stateManager) serveNonMaster(wantTabletType topodatapb.TabletType) error {
	sm.messager.Close()
	sm.tracker.Close()
	sm.hw.Close()
	sm.se.MakeNonMaster()

	if err := sm.connect(); err != nil {
		return err
	}

	if err := sm.te.AcceptReadOnly(); err != nil {
		return err
	}
	sm.hr.Open()
	sm.watcher.Open()
	sm.setState(wantTabletType, StateServing)
	return nil
}

func (sm *stateManager) unserveNonMaster(wantTabletType topodatapb.TabletType) error {
	sm.unserveCommon()

	sm.tracker.Close()
	sm.hw.Close()
	sm.se.MakeNonMaster()

	if err := sm.connect(); err != nil {
		return err
	}

	sm.hr.Open()
	sm.watcher.Open()
	sm.setState(wantTabletType, StateNotServing)
	return nil
}

func (sm *stateManager) connect() error {
	if err := sm.qe.IsMySQLReachable(); err != nil {
		return err
	}
	if err := sm.se.Open(); err != nil {
		return err
	}
	sm.vstreamer.Open()
	if err := sm.qe.Open(); err != nil {
		return err
	}
	return sm.txThrottler.Open()
}

func (sm *stateManager) unserveCommon() {
	sm.messager.Close()
	sm.te.Close()
	sm.qe.StopServing()
	sm.requests.Wait()
}

func (sm *stateManager) closeAll() {
	sm.unserveCommon()
	sm.txThrottler.Close()
	sm.qe.Close()
	sm.watcher.Close()
	sm.tracker.Close()
	sm.vstreamer.Close()
	sm.hr.Close()
	sm.hw.Close()
	sm.se.Close()
	sm.setState(topodatapb.TabletType_UNKNOWN, StateNotConnected)
}

func (sm *stateManager) setTimeBomb() chan struct{} {
	done := make(chan struct{})
	go func() {
		if sm.timebombDuration == 0 {
			return
		}
		tmr := time.NewTimer(sm.timebombDuration)
		defer tmr.Stop()
		select {
		case <-tmr.C:
			log.Fatal("Shutdown took too long. Crashing")
		case <-done:
		}
	}()
	return done
}

// setState changes the state and logs the event.
func (sm *stateManager) setState(tabletType topodatapb.TabletType, state servingState) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if tabletType == topodatapb.TabletType_UNKNOWN {
		tabletType = sm.wantTabletType
	}
	log.Infof("TabletServer transition: %v -> %v, %s -> %s", sm.target.TabletType, tabletType, stateInfo(sm.state), stateInfo(state))
	sm.target.TabletType = tabletType
	sm.state = state
	sm.history.Add(&historyRecord{
		Time:         time.Now(),
		ServingState: stateInfo(state),
		TabletType:   sm.target.TabletType.String(),
	})
}

// EnterLameduck causes tabletserver to enter the lameduck state. This
// state causes health checks to fail, but the behavior of tabletserver
// otherwise remains the same. Any subsequent calls to SetServingType will
// cause the tabletserver to exit this mode.
func (sm *stateManager) EnterLameduck() {
	sm.lameduck.Set(1)
}

// ExitLameduck causes the tabletserver to exit the lameduck mode.
func (sm *stateManager) ExitLameduck() {
	sm.lameduck.Set(0)
}

// IsServing returns true if TabletServer is in SERVING state.
func (sm *stateManager) IsServing() bool {
	return sm.StateByName() == "SERVING"
}

func (sm *stateManager) State() servingState {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.state
}

func (sm *stateManager) Target() querypb.Target {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	target := sm.target
	return target
}

// StateByName returns the name of the current TabletServer state.
func (sm *stateManager) StateByName() string {
	if sm.lameduck.Get() != 0 {
		return "NOT_SERVING"
	}
	return stateName[sm.State()]
}

// stateInfo returns a string representation of the state and optional detail
// about the reason for the state transition
func stateInfo(state servingState) string {
	if state == StateServing {
		return "SERVING"
	}
	return fmt.Sprintf("%s (%s)", stateName[state], stateDetail[state])
}
