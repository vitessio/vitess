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
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/dbconfigs"
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

// stateInfo returns a string representation of the state and optional detail
// about the reason for the state transition
func stateInfo(state int64) string {
	if state == StateServing {
		return "SERVING"
	}
	return fmt.Sprintf("%s (%s)", stateName[state], stateDetail[state])
}

// EnterLameduck causes tabletserver to enter the lameduck state. This
// state causes health checks to fail, but the behavior of tabletserver
// otherwise remains the same. Any subsequent calls to SetServingType will
// cause the tabletserver to exit this mode.
func (tsv *TabletServer) EnterLameduck() {
	tsv.lameduck.Set(1)
}

// ExitLameduck causes the tabletserver to exit the lameduck mode.
func (tsv *TabletServer) ExitLameduck() {
	tsv.lameduck.Set(0)
}

// GetState returns the name of the current TabletServer state.
func (tsv *TabletServer) GetState() string {
	if tsv.lameduck.Get() != 0 {
		return "NOT_SERVING"
	}
	tsv.mu.Lock()
	name := stateName[tsv.state]
	tsv.mu.Unlock()
	return name
}

// setState changes the state and logs the event.
// It requires the caller to hold a lock on mu.
func (tsv *TabletServer) setState(state int64) {
	log.Infof("TabletServer state: %s -> %s", stateInfo(tsv.state), stateInfo(state))
	tsv.state = state
	tsv.history.Add(&historyRecord{
		Time:         time.Now(),
		ServingState: stateInfo(state),
		TabletType:   tsv.target.TabletType.String(),
	})
}

// transition obtains a lock and changes the state.
func (tsv *TabletServer) transition(newState int64) {
	tsv.mu.Lock()
	tsv.setState(newState)
	tsv.mu.Unlock()
}

// IsServing returns true if TabletServer is in SERVING state.
func (tsv *TabletServer) IsServing() bool {
	return tsv.GetState() == "SERVING"
}

// StartService is a convenience function for InitDBConfig->SetServingType
// with serving=true.
func (tsv *TabletServer) StartService(target querypb.Target, dbcfgs *dbconfigs.DBConfigs) (err error) {
	err = tsv.InitDBConfig(target, dbcfgs)
	if err != nil {
		return err
	}
	_ /* state changed */, err = tsv.SetServingType(target.TabletType, true, nil)
	return err
}

const (
	actionNone = iota
	actionFullStart
	actionServeNewType
	actionGracefulStop
)

// SetServingType changes the serving type of the tabletserver. It starts or
// stops internal services as deemed necessary. The tabletType determines the
// primary serving type, while alsoAllow specifies other tablet types that
// should also be honored for serving.
// Returns true if the state of QueryService or the tablet type changed.
func (tsv *TabletServer) SetServingType(tabletType topodatapb.TabletType, serving bool, alsoAllow []topodatapb.TabletType) (stateChanged bool, err error) {
	defer tsv.ExitLameduck()

	action, err := tsv.decideAction(tabletType, serving, alsoAllow)
	if err != nil {
		return false, err
	}
	switch action {
	case actionNone:
		return false, nil
	case actionFullStart:
		if err := tsv.fullStart(); err != nil {
			tsv.closeAll()
			return true, err
		}
		return true, nil
	case actionServeNewType:
		if err := tsv.serveNewType(); err != nil {
			tsv.closeAll()
			return true, err
		}
		return true, nil
	case actionGracefulStop:
		tsv.gracefulStop()
		return true, nil
	}
	panic("unreachable")
}

func (tsv *TabletServer) decideAction(tabletType topodatapb.TabletType, serving bool, alsoAllow []topodatapb.TabletType) (action int, err error) {
	tsv.mu.Lock()
	defer tsv.mu.Unlock()

	tsv.alsoAllow = alsoAllow

	// Handle the case where the requested TabletType and serving state
	// match our current state. This avoids an unnecessary transition.
	// There's no similar shortcut if serving is false, because there
	// are different 'not serving' states that require different actions.
	if tsv.target.TabletType == tabletType {
		if serving && tsv.state == StateServing {
			// We're already in the desired state.
			return actionNone, nil
		}
	}
	tsv.target.TabletType = tabletType
	switch tsv.state {
	case StateNotConnected:
		if serving {
			tsv.setState(StateTransitioning)
			return actionFullStart, nil
		}
	case StateNotServing:
		if serving {
			tsv.setState(StateTransitioning)
			return actionServeNewType, nil
		}
	case StateServing:
		if !serving {
			tsv.setState(StateShuttingDown)
			return actionGracefulStop, nil
		}
		tsv.setState(StateTransitioning)
		return actionServeNewType, nil
	case StateTransitioning, StateShuttingDown:
		return actionNone, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot SetServingType, current state: %s", stateName[tsv.state])
	default:
		panic("unreachable")
	}
	return actionNone, nil
}

func (tsv *TabletServer) fullStart() error {
	if err := tsv.qe.IsMySQLReachable(); err != nil {
		return err
	}
	if err := tsv.se.Open(); err != nil {
		return err
	}
	tsv.vstreamer.Open()
	if err := tsv.qe.Open(); err != nil {
		return err
	}
	if err := tsv.txThrottler.Open(); err != nil {
		return err
	}
	return tsv.serveNewType()
}

func (tsv *TabletServer) serveNewType() (err error) {
	if tsv.target.TabletType == topodatapb.TabletType_MASTER {
		tsv.watcher.Close()
		tsv.hr.Close()

		tsv.hw.Open()
		tsv.tracker.Open()
		tsv.te.AcceptReadWrite()
		tsv.messager.Open()
	} else {
		tsv.messager.Close()
		tsv.te.AcceptReadOnly()
		tsv.tracker.Close()
		tsv.hw.Close()
		tsv.se.MakeNonMaster()

		tsv.hr.Open()
		tsv.watcher.Open()
	}
	tsv.transition(StateServing)
	return nil
}

func (tsv *TabletServer) gracefulStop() {
	defer close(tsv.setTimeBomb())
	tsv.waitForShutdown()
	tsv.transition(StateNotServing)
}

// StopService shuts down the tabletserver to the uninitialized state.
// It first transitions to StateShuttingDown, then waits for active
// services to shut down. Then it shuts down the rest. This function
// should be called before process termination, or if MySQL is unreachable.
// Under normal circumstances, SetServingType should be called.
func (tsv *TabletServer) StopService() {
	defer close(tsv.setTimeBomb())
	defer tsv.LogError()

	tsv.mu.Lock()
	if tsv.state != StateServing && tsv.state != StateNotServing {
		tsv.mu.Unlock()
		return
	}
	tsv.setState(StateShuttingDown)
	tsv.mu.Unlock()

	log.Info("Executing complete shutdown.")
	tsv.waitForShutdown()
	tsv.qe.Close()
	tsv.watcher.Close()
	tsv.vstreamer.Close()
	tsv.hr.Close()
	tsv.hw.Close()
	tsv.se.Close()
	log.Info("Shutdown complete.")
	tsv.transition(StateNotConnected)
}

func (tsv *TabletServer) waitForShutdown() {
	tsv.messager.Close()
	tsv.te.Close()
	tsv.txThrottler.Close()
	tsv.tracker.Close()
	tsv.qe.StopServing()
	tsv.requests.Wait()
}

// closeAll is called if TabletServer fails to start.
// It forcibly shuts down everything.
func (tsv *TabletServer) closeAll() {
	tsv.messager.Close()
	tsv.te.Close()
	tsv.txThrottler.Close()
	tsv.qe.StopServing()
	tsv.qe.Close()
	tsv.watcher.Close()
	tsv.tracker.Close()
	tsv.vstreamer.Close()
	tsv.hr.Close()
	tsv.hw.Close()
	tsv.se.Close()
	tsv.transition(StateNotConnected)
}

func (tsv *TabletServer) setTimeBomb() chan struct{} {
	done := make(chan struct{})
	go func() {
		qt := tsv.QueryTimeout.Get()
		if qt == 0 {
			return
		}
		tmr := time.NewTimer(10 * qt)
		defer tmr.Stop()
		select {
		case <-tmr.C:
			log.Fatal("Shutdown took too long. Crashing")
		case <-done:
		}
	}()
	return done
}

// CheckMySQL initiates a check to see if MySQL is reachable.
// If not, it shuts down the query service. The check is rate-limited
// to no more than once per second.
// The function satisfies tabletenv.Env.
func (tsv *TabletServer) CheckMySQL() {
	if !tsv.checkMySQLThrottler.TryAcquire() {
		return
	}
	go func() {
		defer func() {
			tsv.LogError()
			time.Sleep(1 * time.Second)
			tsv.checkMySQLThrottler.Release()
		}()
		if tsv.isMySQLReachable() {
			return
		}
		log.Info("Check MySQL failed. Shutting down query service")
		tsv.StopService()
	}()
}

// isMySQLReachable returns true if we can connect to MySQL.
// The function returns false only if the query service is
// in StateServing or StateNotServing.
func (tsv *TabletServer) isMySQLReachable() bool {
	tsv.mu.Lock()
	switch tsv.state {
	case StateServing:
		// Prevent transition out of this state by
		// reserving a request.
		tsv.requests.Add(1)
		defer tsv.requests.Done()
	case StateNotServing:
		// Prevent transition out of this state by
		// temporarily switching to StateTransitioning.
		tsv.setState(StateTransitioning)
		defer func() {
			tsv.transition(StateNotServing)
		}()
	default:
		tsv.mu.Unlock()
		return true
	}
	tsv.mu.Unlock()
	if err := tsv.qe.IsMySQLReachable(); err != nil {
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
func (tsv *TabletServer) startRequest(ctx context.Context, target *querypb.Target, allowOnShutdown bool) (err error) {
	tsv.mu.Lock()
	defer tsv.mu.Unlock()
	if tsv.state == StateServing {
		goto verifyTarget
	}
	if allowOnShutdown && tsv.state == StateShuttingDown {
		goto verifyTarget
	}
	return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "operation not allowed in state %s", stateName[tsv.state])

verifyTarget:
	if target != nil {
		// a valid target needs to be used
		switch {
		case target.Keyspace != tsv.target.Keyspace:
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid keyspace %v", target.Keyspace)
		case target.Shard != tsv.target.Shard:
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid shard %v", target.Shard)
		case target.TabletType != tsv.target.TabletType:
			for _, otherType := range tsv.alsoAllow {
				if target.TabletType == otherType {
					goto ok
				}
			}
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "invalid tablet type: %v, want: %v or %v", target.TabletType, tsv.target.TabletType, tsv.alsoAllow)
		}
	} else if !tabletenv.IsLocalContext(ctx) {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No target")
	}

ok:
	tsv.requests.Add(1)
	return nil
}

// endRequest unregisters the current request (a waitgroup) as done.
func (tsv *TabletServer) endRequest() {
	tsv.requests.Done()
}

// verifyTarget allows requests to be executed even in non-serving state.
func (tsv *TabletServer) verifyTarget(ctx context.Context, target *querypb.Target) error {
	tsv.mu.Lock()
	defer tsv.mu.Unlock()

	if target != nil {
		// a valid target needs to be used
		switch {
		case target.Keyspace != tsv.target.Keyspace:
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid keyspace %v", target.Keyspace)
		case target.Shard != tsv.target.Shard:
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid shard %v", target.Shard)
		case target.TabletType != tsv.target.TabletType:
			for _, otherType := range tsv.alsoAllow {
				if target.TabletType == otherType {
					return nil
				}
			}
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "invalid tablet type: %v, want: %v or %v", target.TabletType, tsv.target.TabletType, tsv.alsoAllow)
		}
	} else if !tabletenv.IsLocalContext(ctx) {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No target")
	}
	return nil
}
