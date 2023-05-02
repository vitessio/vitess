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
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

type servingState int64

const (
	// StateNotConnected is the state where tabletserver is not
	// connected to an underlying mysql instance. In this state we close
	// query engine since MySQL is probably unavailable
	StateNotConnected = servingState(iota)
	// StateNotServing is the state where tabletserver is connected
	// to an underlying mysql instance, but is not serving queries.
	// We do not close the query engine to not close the pool. We keep
	// the query engine open but prevent queries from running by blocking them
	// in StartRequest.
	StateNotServing
	// StateServing is where queries are allowed.
	StateServing
)

func (state servingState) String() string {
	switch state {
	case StateServing:
		return "Serving"
	case StateNotServing:
		return "Not Serving"
	}
	return "Not connected to mysql"
}

// transitionRetryInterval is for tests.
var transitionRetryInterval = 1 * time.Second

// stateManager manages state transition for all the TabletServer
// subcomponents.
type stateManager struct {
	// transitioning is a semaphore that must to be obtained
	// before attempting a state transition. To prevent deadlocks,
	// this must be acquired before the mu lock. We use a semaphore
	// because we need TryAcquire, which is not supported by sync.Mutex.
	// If an acquire is successful, we must either Release explicitly
	// or invoke execTransition, which will release once it's done.
	// There are no ordering restrictions on using TryAcquire.
	transitioning *semaphore.Weighted

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
	target         *querypb.Target
	terTimestamp   time.Time
	retrying       bool
	replHealthy    bool
	lameduck       bool
	alsoAllow      []topodatapb.TabletType
	reason         string
	transitionErr  error

	requests sync.WaitGroup

	// QueryList does not have an Open or Close.
	statelessql *QueryList
	statefulql  *QueryList
	olapql      *QueryList

	// Open must be done in forward order.
	// Close must be done in reverse order.
	// All Close functions must be called before Open.
	hs          *healthStreamer
	se          schemaEngine
	rt          replTracker
	vstreamer   subComponent
	tracker     subComponent
	watcher     subComponent
	qe          queryEngine
	txThrottler txThrottler
	te          txEngine
	messager    subComponent
	ddle        onlineDDLExecutor
	throttler   lagThrottler
	tableGC     tableGarbageCollector

	// hcticks starts on initialiazation and runs forever.
	hcticks *timer.Timer

	// checkMySQLThrottler ensures that CheckMysql
	// doesn't get spammed.
	checkMySQLThrottler *semaphore.Weighted
	checkMySQLRunning   atomic.Bool

	timebombDuration      time.Duration
	unhealthyThreshold    atomic.Int64
	shutdownGracePeriod   time.Duration
	transitionGracePeriod time.Duration
}

type (
	schemaEngine interface {
		EnsureConnectionAndDB(topodatapb.TabletType) error
		Open() error
		MakeNonPrimary()
		Close()
	}

	replTracker interface {
		MakePrimary()
		MakeNonPrimary()
		Close()
		Status() (time.Duration, error)
	}

	queryEngine interface {
		Open() error
		IsMySQLReachable() error
		Close()
	}

	txEngine interface {
		AcceptReadWrite()
		AcceptReadOnly()
		Close()
	}

	subComponent interface {
		Open()
		Close()
	}

	txThrottler interface {
		Open() error
		Close()
	}

	onlineDDLExecutor interface {
		Open() error
		Close()
	}

	lagThrottler interface {
		Open() error
		Close()
	}

	tableGarbageCollector interface {
		Open() error
		Close()
	}
)

// Init performs the second phase of initialization.
func (sm *stateManager) Init(env tabletenv.Env, target *querypb.Target) {
	sm.target = proto.Clone(target).(*querypb.Target)
	sm.transitioning = semaphore.NewWeighted(1)
	sm.checkMySQLThrottler = semaphore.NewWeighted(1)
	sm.timebombDuration = env.Config().OltpReadPool.TimeoutSeconds.Get() * 10
	sm.hcticks = timer.NewTimer(env.Config().Healthcheck.IntervalSeconds.Get())
	sm.unhealthyThreshold.Store(env.Config().Healthcheck.UnhealthyThresholdSeconds.Get().Nanoseconds())
	sm.shutdownGracePeriod = env.Config().GracePeriods.ShutdownSeconds.Get()
	sm.transitionGracePeriod = env.Config().GracePeriods.TransitionSeconds.Get()
}

// SetServingType changes the state to the specified settings.
// If a transition is in progress, it waits and then executes the
// new request. If the transition fails, it returns an error, and
// launches retryTransition to ensure that the request will eventually
// be honored.
// If sm is already in the requested state, it returns stateChanged as
// false.
func (sm *stateManager) SetServingType(tabletType topodatapb.TabletType, terTimestamp time.Time, state servingState, reason string) error {
	defer sm.ExitLameduck()

	sm.hs.Open()
	sm.hcticks.Start(sm.Broadcast)

	if tabletType == topodatapb.TabletType_RESTORE || tabletType == topodatapb.TabletType_BACKUP {
		state = StateNotConnected
	}

	log.Infof("Starting transition to %v %v, timestamp: %v", tabletType, state, terTimestamp)
	if sm.mustTransition(tabletType, terTimestamp, state, reason) {
		return sm.execTransition(tabletType, state)
	}
	return nil
}

// mustTransition returns true if the requested state does not match the current
// state. If so, it acquires the semaphore and returns true. If a transition is
// already in progress, it waits. If the desired state is already reached, it
// returns false without acquiring the semaphore.
func (sm *stateManager) mustTransition(tabletType topodatapb.TabletType, terTimestamp time.Time, state servingState, reason string) bool {
	if sm.transitioning.Acquire(context.Background(), 1) != nil {
		return false
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.wantTabletType = tabletType
	sm.wantState = state
	sm.terTimestamp = terTimestamp
	sm.reason = reason
	if sm.target.TabletType == tabletType && sm.state == state {
		sm.transitioning.Release(1)
		return false
	}
	return true
}

func (sm *stateManager) execTransition(tabletType topodatapb.TabletType, state servingState) error {
	defer sm.transitioning.Release(1)

	var err error
	switch state {
	case StateServing:
		if tabletType == topodatapb.TabletType_PRIMARY {
			err = sm.servePrimary()
		} else {
			err = sm.serveNonPrimary(tabletType)
		}
	case StateNotServing:
		if tabletType == topodatapb.TabletType_PRIMARY {
			err = sm.unservePrimary()
		} else {
			err = sm.unserveNonPrimary(tabletType)
		}
	case StateNotConnected:
		sm.closeAll()
	}
	sm.mu.Lock()
	sm.transitionErr = err
	sm.mu.Unlock()
	if err != nil {
		sm.retryTransition(fmt.Sprintf("Error transitioning to the desired state: %v, %v, will keep retrying: %v", tabletType, state, err))
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
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.wantState == sm.state && sm.wantTabletType == sm.target.TabletType {
		sm.retrying = false
		return true
	}
	if !sm.transitioning.TryAcquire(1) {
		return false
	}
	go sm.execTransition(sm.wantTabletType, sm.wantState)
	return false
}

// checkMySQL verifies that we can connect to mysql.
// If it fails, then we shutdown the service and initiate
// the retry loop.
func (sm *stateManager) checkMySQL() {
	if !sm.checkMySQLThrottler.TryAcquire(1) {
		return
	}
	log.Infof("CheckMySQL started")
	sm.checkMySQLRunning.Store(true)
	go func() {
		defer func() {
			time.Sleep(1 * time.Second)
			sm.checkMySQLRunning.Store(false)
			sm.checkMySQLThrottler.Release(1)
			log.Infof("CheckMySQL finished")
		}()

		err := sm.qe.IsMySQLReachable()
		if err == nil {
			return
		}

		if !sm.transitioning.TryAcquire(1) {
			// If we're already transitioning, don't interfere.
			return
		}
		defer sm.transitioning.Release(1)

		// This is required to prevent new queries from running in StartRequest
		// unless they are part of a running transaction.
		sm.setWantState(StateNotConnected)
		sm.closeAll()

		// Now that we reached the NotConnected state, we want to go back to the
		// Serving state. The retry will only succeed once MySQL is reachable again
		// Until then EnsureConnectionAndDB will error out.
		sm.setWantState(StateServing)
		sm.retryTransition(fmt.Sprintf("Cannot connect to MySQL, shutting down query service: %v", err))
	}()
}

func (sm *stateManager) setWantState(stateWanted servingState) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.wantState = stateWanted
}

// isCheckMySQLRunning returns 1 if CheckMySQL function is in progress
func (sm *stateManager) isCheckMySQLRunning() int64 {
	if sm.checkMySQLRunning.Load() {
		return 1
	}
	return 0
}

// StopService shuts down sm. If the shutdown doesn't complete
// within timeBombDuration, it crashes the process.
func (sm *stateManager) StopService() {
	defer close(sm.setTimeBomb())

	log.Info("Stopping TabletServer")
	sm.SetServingType(sm.Target().TabletType, time.Time{}, StateNotConnected, "service stopped")
	sm.hcticks.Stop()
	sm.hs.Close()
}

// StartRequest validates the current state and target and registers
// the request (a waitgroup) as started. Every StartRequest must be
// ended with an EndRequest.
func (sm *stateManager) StartRequest(ctx context.Context, target *querypb.Target, allowOnShutdown bool) (err error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.state != StateServing || !sm.replHealthy {
		// This specific error string needs to be returned for vtgate buffering to work.
		return vterrors.New(vtrpcpb.Code_CLUSTER_EVENT, vterrors.NotServing)
	}

	shuttingDown := sm.wantState != StateServing
	if shuttingDown && !allowOnShutdown {
		// This specific error string needs to be returned for vtgate buffering to work.
		return vterrors.New(vtrpcpb.Code_CLUSTER_EVENT, vterrors.ShuttingDown)
	}

	err = sm.verifyTargetLocked(ctx, target)
	if err != nil {
		return err
	}
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
	return sm.verifyTargetLocked(ctx, target)
}

func (sm *stateManager) verifyTargetLocked(ctx context.Context, target *querypb.Target) error {
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
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "%s: %v, want: %v or %v", vterrors.WrongTablet, target.TabletType, sm.target.TabletType, sm.alsoAllow)
		}
	} else {
		if !tabletenv.IsLocalContext(ctx) {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No target")
		}
	}
	return nil
}

func (sm *stateManager) servePrimary() error {
	sm.watcher.Close()

	if err := sm.connect(topodatapb.TabletType_PRIMARY); err != nil {
		return err
	}

	sm.rt.MakePrimary()
	sm.tracker.Open()
	// We instantly kill all stateful queries to allow for
	// te to quickly transition into RW, but olap and stateless
	// queries can continue serving.
	sm.statefulql.TerminateAll()
	sm.te.AcceptReadWrite()
	sm.messager.Open()
	sm.throttler.Open()
	sm.tableGC.Open()
	sm.ddle.Open()
	sm.setState(topodatapb.TabletType_PRIMARY, StateServing)
	return nil
}

func (sm *stateManager) unservePrimary() error {
	sm.unserveCommon()

	sm.watcher.Close()

	if err := sm.connect(topodatapb.TabletType_PRIMARY); err != nil {
		return err
	}

	sm.rt.MakePrimary()
	sm.setState(topodatapb.TabletType_PRIMARY, StateNotServing)
	return nil
}

func (sm *stateManager) serveNonPrimary(wantTabletType topodatapb.TabletType) error {
	// We are likely transitioning from primary. We have to honor
	// the shutdown grace period.
	cancel := sm.handleShutdownGracePeriod()
	defer cancel()

	sm.ddle.Close()
	sm.tableGC.Close()
	sm.messager.Close()
	sm.tracker.Close()
	sm.se.MakeNonPrimary()

	if err := sm.connect(wantTabletType); err != nil {
		return err
	}

	sm.te.AcceptReadOnly()
	sm.rt.MakeNonPrimary()
	sm.watcher.Open()
	sm.throttler.Open()
	sm.setState(wantTabletType, StateServing)
	return nil
}

func (sm *stateManager) unserveNonPrimary(wantTabletType topodatapb.TabletType) error {
	sm.unserveCommon()

	sm.se.MakeNonPrimary()

	if err := sm.connect(wantTabletType); err != nil {
		return err
	}

	sm.rt.MakeNonPrimary()
	sm.watcher.Open()
	sm.setState(wantTabletType, StateNotServing)
	return nil
}

func (sm *stateManager) connect(tabletType topodatapb.TabletType) error {
	if err := sm.se.EnsureConnectionAndDB(tabletType); err != nil {
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
	log.Infof("Started execution of unserveCommon")
	cancel := sm.handleShutdownGracePeriod()
	log.Infof("Finished execution of handleShutdownGracePeriod")
	defer cancel()

	log.Infof("Started online ddl executor close")
	sm.ddle.Close()
	log.Infof("Finished online ddl executor close. Started table garbage collector close")
	sm.tableGC.Close()
	log.Infof("Finished table garbage collector close. Started lag throttler close")
	sm.throttler.Close()
	log.Infof("Finished lag throttler close. Started messager close")
	sm.messager.Close()
	log.Infof("Finished messager close. Started txEngine close")
	sm.te.Close()
	log.Infof("Finished txEngine close. Killing all OLAP queries")
	sm.olapql.TerminateAll()
	log.Info("Finished Killing all OLAP queries. Started tracker close")
	sm.tracker.Close()
	log.Infof("Finished tracker close. Started wait for requests")
	sm.requests.Wait()
	log.Infof("Finished wait for requests. Finished execution of unserveCommon")
}

func (sm *stateManager) handleShutdownGracePeriod() (cancel func()) {
	if sm.shutdownGracePeriod == 0 {
		return func() {}
	}
	ctx, cancel := context.WithCancel(context.TODO())
	go func() {
		if err := timer.SleepContext(ctx, sm.shutdownGracePeriod); err != nil {
			return
		}
		log.Infof("Grace Period %v exceeded. Killing all OLTP queries.", sm.shutdownGracePeriod)
		sm.statelessql.TerminateAll()
		log.Infof("Killed all stateful OLTP queries.")
		sm.statefulql.TerminateAll()
		log.Infof("Killed all OLTP queries.")
	}()
	return cancel
}

func (sm *stateManager) closeAll() {
	defer close(sm.setTimeBomb())

	sm.unserveCommon()
	sm.txThrottler.Close()
	sm.qe.Close()
	sm.watcher.Close()
	sm.vstreamer.Close()
	sm.rt.Close()
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
	defer func() {
		log.Infof("Tablet Init took %d ms", time.Since(servenv.GetInitStartTime()).Milliseconds())
	}()
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if tabletType == topodatapb.TabletType_UNKNOWN {
		tabletType = sm.wantTabletType
	}
	log.Infof("TabletServer transition: %v -> %v for tablet %s:%s/%s",
		sm.stateStringLocked(sm.target.TabletType, sm.state), sm.stateStringLocked(tabletType, state),
		sm.target.Cell, sm.target.Keyspace, sm.target.Shard)
	sm.handleGracePeriod(tabletType)
	sm.target.TabletType = tabletType
	if sm.state == StateNotConnected {
		// If we're transitioning out of StateNotConnected, we have
		// to also ensure replication status is healthy.
		_, _, _ = sm.refreshReplHealthLocked()
	}
	sm.state = state
	// Broadcast also obtains a lock. Trigger in a goroutine to avoid a deadlock.
	go sm.hcticks.Trigger()
}

func (sm *stateManager) stateStringLocked(tabletType topodatapb.TabletType, state servingState) string {
	if tabletType != topodatapb.TabletType_PRIMARY {
		return fmt.Sprintf("%v: %v", tabletType, state)
	}
	return fmt.Sprintf("%v: %v, %v", tabletType, state, sm.terTimestamp.Local().Format("Jan 2, 2006 at 15:04:05 (MST)"))
}

func (sm *stateManager) handleGracePeriod(tabletType topodatapb.TabletType) {
	if tabletType != topodatapb.TabletType_PRIMARY {
		// We allow serving of previous type only for a primary transition.
		sm.alsoAllow = nil
		return
	}

	if tabletType == topodatapb.TabletType_PRIMARY &&
		sm.target.TabletType != topodatapb.TabletType_PRIMARY &&
		sm.transitionGracePeriod != 0 {

		sm.alsoAllow = []topodatapb.TabletType{sm.target.TabletType}
		// This is not a perfect solution because multiple back and forth
		// transitions will launch multiple of these goroutines. But the
		// system will eventually converge.
		go func() {
			time.Sleep(sm.transitionGracePeriod)

			sm.mu.Lock()
			defer sm.mu.Unlock()
			sm.alsoAllow = nil
		}()
	}
}

// Broadcast fetches the replication status and broadcasts
// the state to all subscribed.
func (sm *stateManager) Broadcast() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	lag, throttlerReplicationLag, err := sm.refreshReplHealthLocked()
	sm.hs.ChangeState(sm.target.TabletType, sm.terTimestamp, lag, throttlerReplicationLag, err, sm.isServingLocked())
}

func (sm *stateManager) refreshReplHealthLocked() (time.Duration, time.Duration, error) {
	lag, err := sm.rt.Status()
	if sm.target.TabletType == topodatapb.TabletType_PRIMARY {
		sm.replHealthy = true
		return 0, lag, nil
	}
	if err != nil {
		if sm.replHealthy {
			log.Infof("Going unhealthy due to replication error: %v", err)
		}
		sm.replHealthy = false
	} else {
		if lag > time.Duration(sm.unhealthyThreshold.Load()) {
			if sm.replHealthy {
				log.Infof("Going unhealthy due to high replication lag: %v", lag)
			}
			sm.replHealthy = false
		} else {
			if !sm.replHealthy {
				log.Infof("Replication is healthy")
			}
			sm.replHealthy = true
		}
	}
	return lag, lag, err
}

// EnterLameduck causes tabletserver to enter the lameduck state. This
// state causes health checks to fail, but the behavior of tabletserver
// otherwise remains the same. Any subsequent calls to SetServingType will
// cause the tabletserver to exit this mode.
func (sm *stateManager) EnterLameduck() {
	log.Info("State: entering lameduck")
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.lameduck = true
}

// ExitLameduck causes the tabletserver to exit the lameduck mode.
func (sm *stateManager) ExitLameduck() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.lameduck = false
	log.Info("State: exiting lameduck")
}

// IsServing returns true if TabletServer is in SERVING state.
func (sm *stateManager) IsServing() bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.isServingLocked()
}

func (sm *stateManager) isServingLocked() bool {
	return sm.state == StateServing && sm.wantState == StateServing && sm.replHealthy && !sm.lameduck
}

func (sm *stateManager) AppendDetails(details []*kv) []*kv {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	stateClass := func(state servingState) string {
		switch state {
		case StateServing:
			return healthyClass
		case StateNotServing:
			return unhappyClass
		}
		return unhealthyClass
	}

	details = append(details, &kv{
		Key:   "Current State",
		Class: stateClass(sm.state),
		Value: sm.stateStringLocked(sm.target.TabletType, sm.state),
	})
	if sm.target.TabletType != sm.wantTabletType && sm.state != sm.wantState {
		details = append(details, &kv{
			Key:   "Desired State",
			Class: stateClass(sm.wantState),
			Value: sm.stateStringLocked(sm.wantTabletType, sm.wantState),
		})
	}
	if sm.reason != "" {
		details = append(details, &kv{
			Key:   "Reason",
			Class: unhappyClass,
			Value: sm.reason,
		})
	}
	if sm.transitionErr != nil {
		details = append(details, &kv{
			Key:   "Transition Error",
			Class: unhealthyClass,
			Value: sm.transitionErr.Error(),
		})
	}
	if sm.lameduck {
		details = append(details, &kv{
			Key:   "Lameduck",
			Class: unhealthyClass,
			Value: "ON",
		})
	}
	if len(sm.alsoAllow) != 0 {
		details = append(details, &kv{
			Key:   "Also Serving",
			Class: healthyClass,
			Value: sm.alsoAllow[0].String(),
		})
	}
	return details
}

func (sm *stateManager) State() servingState {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	// We should not change these state numbers without
	// an announcement. Even though this is not perfect,
	// this behavior keeps things backward compatible.
	if !sm.replHealthy {
		return StateNotConnected
	}
	return sm.state
}

func (sm *stateManager) Target() *querypb.Target {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return proto.Clone(sm.target).(*querypb.Target)
}

// IsServingString returns the name of the current TabletServer state.
func (sm *stateManager) IsServingString() string {
	if sm.IsServing() {
		return "SERVING"
	}
	return "NOT_SERVING"
}

func (sm *stateManager) SetUnhealthyThreshold(v time.Duration) {
	sm.unhealthyThreshold.Store(v.Nanoseconds())
}
