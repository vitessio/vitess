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
	// StateTransitioning is a transient state indicating that
	// the tabletserver is tranisitioning to a new state.
	// In order to achieve clean transitions, no requests are
	// allowed during this state.
	StateTransitioning
	// StateShuttingDown indicates that the tabletserver
	// is shutting down. In this state, we wait for outstanding
	// requests and transactions to conclude.
	StateShuttingDown
)

// stateName names every state. The number of elements must
// match the number of states. Names can overlap.
var stateName = []string{
	"NOT_SERVING",
	"NOT_SERVING",
	"SERVING",
	"NOT_SERVING",
	"SHUTTING_DOWN",
}

// stateDetail matches every state and optionally more information about the reason
// why the state is serving / not serving.
var stateDetail = []string{
	"Not Connected",
	"Not Serving",
	"",
	"Transitioning",
	"Shutting Down",
}

// stateManager manages state transition for all the TabletServer
// subcomponents.
type stateManager struct {
	mu        sync.Mutex
	state     int64
	lameduck  sync2.AtomicInt32
	target    querypb.Target
	alsoAllow []topodatapb.TabletType
	requests  sync.WaitGroup

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
	name := stateName[sm.state]
	sm.mu.Unlock()
	return name
}

// setState changes the state and logs the event.
// It requires the caller to hold a lock on mu.
func (sm *stateManager) setState(state int64) {
	log.Infof("TabletServer state: %s -> %s", stateInfo(sm.state), stateInfo(state))
	sm.state = state
	sm.history.Add(&historyRecord{
		Time:         time.Now(),
		ServingState: stateInfo(state),
		TabletType:   sm.target.TabletType.String(),
	})
}

// transition obtains a lock and changes the state.
func (sm *stateManager) transition(newState int64) {
	sm.mu.Lock()
	sm.setState(newState)
	sm.mu.Unlock()
}

// IsServing returns true if TabletServer is in SERVING state.
func (sm *stateManager) IsServing() bool {
	return sm.StateByName() == "SERVING"
}

const (
	actionNone = iota
	actionFullStart
	actionServeNewType
	actionGracefulStop
)

func (sm *stateManager) SetServingType(tabletType topodatapb.TabletType, serving bool, alsoAllow []topodatapb.TabletType) (stateChanged bool, err error) {
	defer sm.ExitLameduck()

	action, err := sm.decideAction(tabletType, serving, alsoAllow)
	if err != nil {
		return false, err
	}
	switch action {
	case actionNone:
		return false, nil
	case actionFullStart:
		if err := sm.fullStart(); err != nil {
			sm.closeAll()
			return true, err
		}
		return true, nil
	case actionServeNewType:
		if err := sm.serveNewType(); err != nil {
			sm.closeAll()
			return true, err
		}
		return true, nil
	case actionGracefulStop:
		sm.gracefulStop()
		return true, nil
	}
	panic("unreachable")
}

func (sm *stateManager) decideAction(tabletType topodatapb.TabletType, serving bool, alsoAllow []topodatapb.TabletType) (action int, err error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.alsoAllow = alsoAllow

	// Handle the case where the requested TabletType and serving state
	// match our current state. This avoids an unnecessary transition.
	// There's no similar shortcut if serving is false, because there
	// are different 'not serving' states that require different actions.
	if sm.target.TabletType == tabletType {
		if serving && sm.state == StateServing {
			// We're already in the desired state.
			return actionNone, nil
		}
	}
	sm.target.TabletType = tabletType
	switch sm.state {
	case StateNotConnected:
		if serving {
			sm.setState(StateTransitioning)
			return actionFullStart, nil
		}
	case StateNotServing:
		if serving {
			sm.setState(StateTransitioning)
			return actionServeNewType, nil
		}
	case StateServing:
		if !serving {
			sm.setState(StateShuttingDown)
			return actionGracefulStop, nil
		}
		sm.setState(StateTransitioning)
		return actionServeNewType, nil
	case StateTransitioning, StateShuttingDown:
		return actionNone, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot SetServingType, current state: %s", stateName[sm.state])
	default:
		panic("unreachable")
	}
	return actionNone, nil
}

func (sm *stateManager) fullStart() error {
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
	if err := sm.txThrottler.Open(); err != nil {
		return err
	}
	return sm.serveNewType()
}

func (sm *stateManager) serveNewType() (err error) {
	if sm.target.TabletType == topodatapb.TabletType_MASTER {
		sm.watcher.Close()
		sm.hr.Close()

		sm.hw.Open()
		sm.tracker.Open()
		if err := sm.te.AcceptReadWrite(); err != nil {
			return err
		}
		sm.messager.Open()
	} else {
		sm.messager.Close()
		if err := sm.te.AcceptReadOnly(); err != nil {
			return err
		}
		sm.tracker.Close()
		sm.hw.Close()
		sm.se.MakeNonMaster()

		sm.hr.Open()
		sm.watcher.Open()
	}
	sm.transition(StateServing)
	return nil
}

func (sm *stateManager) gracefulStop() {
	defer close(sm.setTimeBomb())
	sm.waitForShutdown()
	sm.transition(StateNotServing)
}

func (sm *stateManager) StopService() {
	defer close(sm.setTimeBomb())

	sm.mu.Lock()
	if sm.state != StateServing && sm.state != StateNotServing {
		sm.mu.Unlock()
		return
	}
	sm.setState(StateShuttingDown)
	sm.mu.Unlock()

	log.Info("Executing complete shutdown.")
	sm.waitForShutdown()
	sm.qe.Close()
	sm.watcher.Close()
	sm.vstreamer.Close()
	sm.hr.Close()
	sm.hw.Close()
	sm.se.Close()
	log.Info("Shutdown complete.")
	sm.transition(StateNotConnected)
}

func (sm *stateManager) waitForShutdown() {
	sm.messager.Close()
	sm.te.Close()
	sm.txThrottler.Close()
	sm.tracker.Close()
	sm.qe.StopServing()
	sm.requests.Wait()
}

// closeAll is called if TabletServer fails to start.
// It forcibly shuts down everything.
func (sm *stateManager) closeAll() {
	sm.messager.Close()
	sm.te.Close()
	sm.txThrottler.Close()
	sm.qe.StopServing()
	sm.qe.Close()
	sm.watcher.Close()
	sm.tracker.Close()
	sm.vstreamer.Close()
	sm.hr.Close()
	sm.hw.Close()
	sm.se.Close()
	sm.transition(StateNotConnected)
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

func (sm *stateManager) CheckMySQL() {
	if !sm.checkMySQLThrottler.TryAcquire() {
		return
	}
	go func() {
		defer func() {
			time.Sleep(1 * time.Second)
			sm.checkMySQLThrottler.Release()
		}()
		if sm.isMySQLReachable() {
			return
		}
		log.Info("Check MySQL failed. Shutting down query service")
		sm.StopService()
	}()
}

// isMySQLReachable returns true if we can connect to MySQL.
// The function returns false only if the query service is
// in StateServing or StateNotServing.
func (sm *stateManager) isMySQLReachable() bool {
	sm.mu.Lock()
	switch sm.state {
	case StateServing:
		// Prevent transition out of this state by
		// reserving a request.
		sm.requests.Add(1)
		defer sm.requests.Done()
	case StateNotServing:
		// Prevent transition out of this state by
		// temporarily switching to StateTransitioning.
		sm.setState(StateTransitioning)
		defer func() {
			sm.transition(StateNotServing)
		}()
	default:
		sm.mu.Unlock()
		return true
	}
	sm.mu.Unlock()
	if err := sm.qe.IsMySQLReachable(); err != nil {
		log.Errorf("Cannot connect to MySQL: %v", err)
		return false
	}
	return true
}

// startRequest validates the current state and target and registers
// the request (a waitgroup) as started. Every startRequest requires
// one and only one corresponding endRequest. When the service shuts
// down, StopService will wait on this waitgroup to ensure that there
// are no requests in flight.
func (sm *stateManager) startRequest(ctx context.Context, target *querypb.Target, allowOnShutdown bool) (err error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.state == StateServing {
		goto verifyTarget
	}
	if allowOnShutdown && sm.state == StateShuttingDown {
		goto verifyTarget
	}
	return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "operation not allowed in state %s", stateName[sm.state])

verifyTarget:
	if target != nil {
		// a valid target needs to be used
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
	} else if !tabletenv.IsLocalContext(ctx) {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No target")
	}

ok:
	sm.requests.Add(1)
	return nil
}

// endRequest unregisters the current request (a waitgroup) as done.
func (sm *stateManager) endRequest() {
	sm.requests.Done()
}

// verifyTarget allows requests to be executed even in non-serving state.
func (sm *stateManager) verifyTarget(ctx context.Context, target *querypb.Target) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if target != nil {
		// a valid target needs to be used
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
	} else if !tabletenv.IsLocalContext(ctx) {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No target")
	}
	return nil
}

// stateInfo returns a string representation of the state and optional detail
// about the reason for the state transition
func stateInfo(state int64) string {
	if state == StateServing {
		return "SERVING"
	}
	return fmt.Sprintf("%s (%s)", stateName[state], stateDetail[state])
}
