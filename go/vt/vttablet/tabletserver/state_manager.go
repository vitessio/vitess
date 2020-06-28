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
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/history"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

const (
	// StateNotConnected is the state where tabletserver is not
	// connected to an underlying mysql instance.
	StateNotConnected = iota
	// StateNotServing is the state where tabletserver is connected
	// to an underlying mysql instance, but is not serving queries.
	StateNotServing
	// StateServing is where queries are allowed.
	StateServing
)

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
	mu             sync.Mutex
	wantState      int64
	wantTabletType topodatapb.TabletType
	state          int64
	target         querypb.Target
	transitioning  bool
	connecting     bool
	// TODO(sougou): deprecate alsoAllow
	alsoAllow []topodatapb.TabletType

	requests sync.WaitGroup
	lameduck sync2.AtomicInt32

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

const (
	actionNone = iota
	actionFullStart
	actionServeNewType
	actionGracefulStop
)

func (sm *stateManager) SetServingType(tabletType topodatapb.TabletType, state int64, alsoAllow []topodatapb.TabletType) (stateChanged bool, err error) {
	// TODO(sougou): deprecate the waits after tabletmanager has been refactored.
	startTime := time.Now()
	stateChanged = sm.setDesiredState(tabletType, state, alsoAllow)
	for {
		curState, curTabletType := sm.State(), sm.Target().TabletType
		if curState == StateNotConnected {
			return stateChanged, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "MySQL is unavailable")
		}
		if curState == state && curTabletType == tabletType {
			return stateChanged, nil
		}
		time.Sleep(10 * time.Millisecond)
		if time.Since(startTime) > 1*time.Second {
			return stateChanged, vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "State transition deadline exceeded")
		}
	}
}

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

		log.Errorf("Cannot connect to MySQL, shutting down query service: %v", err)
		sm.mu.Lock()
		// If we're already transitioning, don't interfere.
		if sm.transitioning {
			sm.mu.Unlock()
			return
		}
		// Setting this flag will ensure that no one else will
		// invoke sm.executeTransition while we sleep.
		sm.transitioning = true
		sm.mu.Unlock()

		// This code path emulates the error case at the end of the loop
		// of executeTransition where it waits for 1s and retries.
		sm.closeAll()
		time.Sleep(1 * time.Second)
		go sm.executeTransition()
	}()
}

func (sm *stateManager) StopService() {
	defer close(sm.setTimeBomb())

	sm.SetServingType(sm.Target().TabletType, StateNotConnected, nil)
	for {
		if sm.State() == StateNotConnected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// StartRequest validates the current state and target and registers
// the request (a waitgroup) as started. Every StartRequest must be
// ended with an EndRequest.
func (sm *stateManager) StartRequest(ctx context.Context, target *querypb.Target, allowOnTransition bool) (err error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// All the checks below must pass.
	switch {
	case sm.state != StateServing:
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "operation not allowed in state %s", stateName[sm.state])
	case sm.transitioning && !allowOnTransition:
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "operation not allowed in state %s", stateName[sm.state])
	case target == nil && !tabletenv.IsLocalContext(ctx):
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No target")
	case target.Keyspace != sm.target.Keyspace:
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid keyspace %v", target.Keyspace)
	case target.Shard != sm.target.Shard:
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid shard %v", target.Shard)
	case target.TabletType != sm.target.TabletType:
		for _, otherType := range sm.alsoAllow {
			if target.TabletType == otherType {
				goto ok
			}
		}
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "invalid tablet type: %v, want: %v or %v", target.TabletType, sm.target.TabletType, sm.alsoAllow)
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

	switch {
	case target == nil && !tabletenv.IsLocalContext(ctx):
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No target")
	case target.Keyspace != sm.target.Keyspace:
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid keyspace %v", target.Keyspace)
	case target.Shard != sm.target.Shard:
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid shard %v", target.Shard)
	case target.TabletType != sm.target.TabletType:
		for _, otherType := range sm.alsoAllow {
			if target.TabletType == otherType {
				return nil
			}
		}
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "invalid tablet type: %v, want: %v or %v", target.TabletType, sm.target.TabletType, sm.alsoAllow)
	}
	return nil
}

func (sm *stateManager) setDesiredState(tabletType topodatapb.TabletType, state int64, alsoAllow []topodatapb.TabletType) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	stateChanged := false
	if sm.wantTabletType != tabletType {
		stateChanged = true
		sm.wantTabletType = tabletType
	}
	if sm.wantState != state {
		stateChanged = true
		sm.wantState = state
	}
	sm.alsoAllow = alsoAllow
	if sm.transitioning {
		return stateChanged
	}
	if sm.wantState == sm.state && sm.wantTabletType == sm.target.TabletType {
		return stateChanged
	}
	sm.transitioning = true
	go sm.executeTransition()
	return stateChanged
}

// executeTransition must be invoked after setting sm.transitioning to true.
// If the flag is already set, it must not be called. The function will
// reset the flag to false when it returns.
func (sm *stateManager) executeTransition() {
	// Repeat until desired state is reached.
	errorReported := false
	for {
		ok, wantTabletType, wantState := sm.transitionDone()
		if ok {
			return
		}

		var err error
		switch wantTabletType {
		case topodatapb.TabletType_MASTER:
			if wantState == StateServing {
				err = sm.serveMaster()
			} else {
				err = sm.unserveMaster()
			}
		default:
			if wantState == StateServing {
				err = sm.serveNonMaster(wantTabletType)
			} else {
				err = sm.unserveNonMaster(wantTabletType)
			}
		}
		// If there was an error, shut everything down
		// and retry after a delay.
		// If there was no error, we restart the loop
		// which verifies that the desired state was
		// not changed before returning. If it was changed,
		// it executes a new transition.
		if err != nil {
			if !errorReported {
				errorReported = true
				log.Errorf("Error transitioning to the desired state: %v, %v, will keep retrying: %v", wantTabletType, stateName[wantState], err)
			}
			sm.closeAll()
			time.Sleep(1 * time.Second)
		}
	}
}

// transitionDone returns true if the desired state matches the current state.
// Otherwise, it returns false, the desired tablet type and state.
func (sm *stateManager) transitionDone() (bool, topodatapb.TabletType, int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	wantTabletType := sm.wantTabletType
	wantState := sm.wantState
	if wantState == sm.state && wantTabletType == sm.target.TabletType {
		sm.transitioning = false
		return true, wantTabletType, wantState
	}
	return false, wantTabletType, wantState
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
	sm.setState(wantTabletType, StateServing)
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
func (sm *stateManager) setState(tabletType topodatapb.TabletType, state int64) {
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

func (sm *stateManager) State() int64 {
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
	sm.mu.Lock()
	defer sm.mu.Unlock()
	name := stateName[sm.state]
	return name
}

// stateInfo returns a string representation of the state and optional detail
// about the reason for the state transition
func stateInfo(state int64) string {
	if state == StateServing {
		return "SERVING"
	}
	return fmt.Sprintf("%s (%s)", stateName[state], stateDetail[state])
}
