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
	// wantState and wantTabletType represent the desired state.
	// If these values are changed and don't match the current
	// state and target, transitioning is set to true, and executeTransition
	// is invoked. This function returns after it transitions state
	// and target to match the desired state, at which point it sets
	// transitioning to false.
	// wantState and wantTabletType can be changed if transitioning is true.
	// executeTransition will check the latest values and continue transitioning
	// until the state reaches the latest values.
	// If a transition fails, execute transition will retry every second (dictated
	// by transitionRetryInterval) until it reaches the desired state.
	// If connection to MySQL is lost, the CheckMySQL function will launch
	// executeTransition to make it retry until connection to MySQL is restored
	// and the desired state is reached.
	mu             sync.Mutex
	wantState      int64
	wantTabletType topodatapb.TabletType
	state          int64
	target         querypb.Target
	transitioning  bool
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
// If sm is in the middle of a transition, it accepts the values, but returns
// an error saying that it's in the middle of a transition.
// If the desired state is already reached, it returns no error.
// If the first attempt at transitioning fails, it returns the error
// from that transition, but sm continues to retry until the desired
// state is reached.
func (sm *stateManager) SetServingType(tabletType topodatapb.TabletType, state int64, alsoAllow []topodatapb.TabletType) (stateChanged bool, err error) {
	defer sm.ExitLameduck()

	log.Infof("Starting transition to %v %v", tabletType, stateName[state])
	stateChanged, errch := sm.setDesiredState(tabletType, state, alsoAllow)
	err = <-errch
	return stateChanged, err
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
		time.Sleep(transitionRetryInterval)
		go sm.executeTransition(make(chan error, 1))
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
func (sm *stateManager) StartRequest(ctx context.Context, target *querypb.Target, allowOnTransition bool) (err error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.state != StateServing {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "operation not allowed in state %s", stateName[sm.state])
	}

	shuttingDown := sm.transitioning && sm.wantState != StateServing
	if shuttingDown && !allowOnTransition {
		// This specific error string needs to be returned for vtgate buffering to work.
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "operation not allowed in state SHUTTING_DOWN")
	}

	if target != nil {
		switch {
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
	} else {
		if !tabletenv.IsLocalContext(ctx) {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No target")
		}
	}
	return nil
}

func (sm *stateManager) setDesiredState(tabletType topodatapb.TabletType, state int64, alsoAllow []topodatapb.TabletType) (bool, <-chan error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	ch := make(chan error, 1)

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
		ch <- vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "a transition is already in progress")
		return stateChanged, ch
	}
	if sm.wantState == sm.state && sm.wantTabletType == sm.target.TabletType {
		ch <- nil
		return stateChanged, ch
	}
	sm.transitioning = true
	go sm.executeTransition(ch)
	return stateChanged, ch
}

// executeTransition must be invoked after setting sm.transitioning to true.
// If the flag is already set, it must not be called. The function will
// reset the flag to false when it returns.
func (sm *stateManager) executeTransition(ch chan<- error) {
	// Repeat until desired state is reached.
	errorReported := false
	for {
		ok, wantTabletType, wantState := sm.transitionDone()
		if ok {
			if !errorReported {
				ch <- nil
			}
			return
		}

		var err error
		switch wantState {
		case StateServing:
			if wantTabletType == topodatapb.TabletType_MASTER {
				err = sm.serveMaster()
			} else {
				err = sm.serveNonMaster(wantTabletType)
			}
		case StateNotServing:
			if wantTabletType == topodatapb.TabletType_MASTER {
				err = sm.unserveMaster()
			} else {
				err = sm.unserveNonMaster(wantTabletType)
			}
		case StateNotConnected:
			sm.closeAll()
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
				ch <- err
				log.Errorf("Error transitioning to the desired state: %v, %v, will keep retrying: %v", wantTabletType, stateName[wantState], err)
			}
			sm.closeAll()
			time.Sleep(transitionRetryInterval)
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
	return stateName[sm.State()]
}

// stateInfo returns a string representation of the state and optional detail
// about the reason for the state transition
func stateInfo(state int64) string {
	if state == StateServing {
		return "SERVING"
	}
	return fmt.Sprintf("%s (%s)", stateName[state], stateDetail[state])
}
