// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package throttler provides a client-side, local throttler which is used to
// throttle (and actively pace) writes during the resharding process.
//
// The throttler has two main goals:
// a) allow resharding data into an existing keyspace by throttling at a fixed
// rate
// b) ensure that the MySQL replicas do not become overloaded
//
// To support b), the throttler constantly monitors the health of all replicas
// and reduces the allowed rate if the replication lag is above a certain
// threshold.
package throttler

import (
	"fmt"
	"math"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/proto/throttlerdata"
)

const (
	// NotThrottled will be returned by Throttle() if the application is currently
	// not throttled.
	NotThrottled time.Duration = 0

	// ZeroRateNoProgess can be used to set maxRate to 0. In this case, the
	// throttler won't let any requests through until the rate is increased again.
	ZeroRateNoProgess = 0

	// MaxRateModuleDisabled can be set in NewThrottler() to disable throttling
	// by a fixed rate.
	MaxRateModuleDisabled = math.MaxInt64

	// InvalidMaxRate is a constant which will fail in a NewThrottler() call.
	// It should be used when returning maxRate in an error case.
	InvalidMaxRate = -1

	// ReplicationLagModuleDisabled can be set in NewThrottler() to disable
	// throttling based on the MySQL replication lag.
	ReplicationLagModuleDisabled = math.MaxInt64

	// InvalidMaxReplicationLag is a constant which will fail in a NewThrottler()
	// call. It should be used when returning maxReplicationlag in an error case.
	InvalidMaxReplicationLag = -1
)

// Throttler provides a client-side, thread-aware throttler.
// See the package doc for more information.
//
// Calls of Throttle() and ThreadFinished() take threadID as parameter which is
// in the range [0, threadCount). (threadCount is set in NewThrottler().)
// NOTE: Trottle() and ThreadFinished() assume that *per thread* calls to them
//       are serialized and must not happen concurrently.
type Throttler struct {
	// name describes the Throttler instance and is used e.g. in the webinterface.
	name string
	// unit describes the entity the throttler is limiting e.g. "queries" or
	// "transactions". It is used for example in the webinterface.
	unit string
	// manager is where this throttler registers and unregisters itself
	// at creation and deletion (Close) respectively.
	manager *managerImpl

	closed bool
	// modules is the list of modules which can limit the current rate. The order
	// of the list defines the priority of the module. (Lower rates trump.)
	modules []Module
	// maxRateModule is stored in its own field because we need it when we want
	// to change its maxRate. It's also included in the "modules" list.
	maxRateModule *MaxRateModule
	// rateUpdateChan is used to notify the throttler that the current max rate
	// must be recalculated and updated.
	rateUpdateChan chan struct{}
	// maxReplicationLagModule is stored in its own field because we need it to
	// record the replication lag. It's also included in the "modules" list.
	maxReplicationLagModule *MaxReplicationLagModule

	// The group below is unguarded because the fields are immutable and each
	// thread accesses an element at a different index.
	threadThrottlers []*threadThrottler
	threadFinished   []bool

	mu sync.Mutex
	// runningThreads tracks which threads have not finished yet.
	runningThreads map[int]bool
	// threadRunningsLastUpdate caches for updateMaxRate() how many threads were
	// running at the previous run.
	threadRunningsLastUpdate int

	nowFunc func() time.Time

	// actualRateHistory tracks for past seconds the total actual rate (based on
	// the number of unthrottled Throttle() calls).
	actualRateHistory *aggregatedIntervalHistory
}

// NewThrottler creates a new Throttler instance.
// Use the constants MaxRateModuleDisabled or ReplicationLagModuleDisabled
// if you want to disable parts of its functionality.
// maxRate will be distributed across all threadCount threads and must be >=
// threadCount. If it's lower, it will be automatically set to threadCount.
// maxRate can also be set to 0 which will effectively pause the user and
// constantly block until the rate has been increased again.
// unit refers to the type of entity you want to throttle e.g. "queries" or
// "transactions".
// name describes the Throttler instance and will be used by the webinterface.
func NewThrottler(name, unit string, threadCount int, maxRate, maxReplicationLag int64) (*Throttler, error) {
	return newThrottler(GlobalManager, name, unit, threadCount, maxRate, maxReplicationLag, time.Now)
}

func newThrottler(manager *managerImpl, name, unit string, threadCount int, maxRate, maxReplicationLag int64, nowFunc func() time.Time) (*Throttler, error) {
	// Verify input parameters.
	if maxRate < 0 {
		return nil, fmt.Errorf("maxRate must be >= 0: %v", maxRate)
	}
	if maxReplicationLag < 0 {
		return nil, fmt.Errorf("maxReplicationLag must be >= 0: %v", maxReplicationLag)
	}

	// Enable the configured modules.
	maxRateModule := NewMaxRateModule(maxRate)
	actualRateHistory := newAggregatedIntervalHistory(1024, 1*time.Second, threadCount)
	maxReplicationLagModule, err := NewMaxReplicationLagModule(NewMaxReplicationLagModuleConfig(maxReplicationLag), actualRateHistory, nowFunc)
	if err != nil {
		return nil, err
	}

	var modules []Module
	modules = append(modules, maxRateModule)
	modules = append(modules, maxReplicationLagModule)

	// Start each module (which might start own Go routines).
	rateUpdateChan := make(chan struct{}, 10)
	for _, m := range modules {
		m.Start(rateUpdateChan)
	}

	runningThreads := make(map[int]bool, threadCount)
	threadThrottlers := make([]*threadThrottler, threadCount, threadCount)
	for i := 0; i < threadCount; i++ {
		threadThrottlers[i] = newThreadThrottler(i, actualRateHistory)
		runningThreads[i] = true
	}
	t := &Throttler{
		name:                    name,
		unit:                    unit,
		manager:                 manager,
		modules:                 modules,
		maxRateModule:           maxRateModule,
		maxReplicationLagModule: maxReplicationLagModule,
		rateUpdateChan:          rateUpdateChan,
		threadThrottlers:        threadThrottlers,
		threadFinished:          make([]bool, threadCount, threadCount),
		runningThreads:          runningThreads,
		nowFunc:                 nowFunc,
		actualRateHistory:       actualRateHistory,
	}

	// Initialize maxRate.
	t.updateMaxRate()

	if err := t.manager.registerThrottler(name, t); err != nil {
		return nil, err
	}

	// Watch for rate updates from the modules and update the internal maxRate.
	go func() {
		for range rateUpdateChan {
			t.updateMaxRate()
		}
	}()

	return t, nil
}

// Throttle returns a backoff duration which specifies for how long "threadId"
// should wait before it issues the next request.
// If the duration is zero, the thread is not throttled.
// If the duration is not zero, the thread must call Throttle() again after
// the backoff duration elapsed.
// The maximum value for the returned backoff is 1 second since the throttler
// internally operates on a per-second basis.
func (t *Throttler) Throttle(threadID int) time.Duration {
	if t.closed {
		panic(fmt.Sprintf("BUG: thread with ID: %v must not access closed Throttler", threadID))
	}
	if t.threadFinished[threadID] {
		panic(fmt.Sprintf("BUG: thread with ID: %v already finished", threadID))
	}
	return t.threadThrottlers[threadID].throttle(t.nowFunc())
}

// ThreadFinished marks threadID as finished and redistributes the thread's
// rate allotment across the other threads.
// After ThreadFinished() is called, Throttle() must not be called anymore.
func (t *Throttler) ThreadFinished(threadID int) {
	if t.threadFinished[threadID] {
		panic(fmt.Sprintf("BUG: thread with ID: %v already finished", threadID))
	}

	t.mu.Lock()
	delete(t.runningThreads, threadID)
	t.mu.Unlock()

	t.threadFinished[threadID] = true
	t.rateUpdateChan <- struct{}{}
}

// Close stops all modules and frees all resources.
// When Close() returned, the Throttler object must not be used anymore.
func (t *Throttler) Close() {
	for _, m := range t.modules {
		m.Stop()
	}
	close(t.rateUpdateChan)
	t.closed = true
	t.manager.unregisterThrottler(t.name)
}

// updateMaxRate recalculates the current max rate and updates all
// threadThrottlers accordingly.
// The rate changes when the number of thread changes or a module updated its
// max rate.
func (t *Throttler) updateMaxRate() {
	// Set it to infinite initially.
	maxRate := int64(math.MaxInt64)

	// Find out the new max rate (minimum among all modules).
	for _, m := range t.modules {
		if moduleMaxRate := m.MaxRate(); moduleMaxRate < maxRate {
			maxRate = moduleMaxRate
		}
	}

	// Set the new max rate on each thread.
	t.mu.Lock()
	defer t.mu.Unlock()
	threadsRunning := len(t.runningThreads)
	if threadsRunning == 0 {
		// Throttler is done. Set rates don't matter anymore.
		return
	}

	if maxRate != ZeroRateNoProgess && maxRate < int64(threadsRunning) {
		log.Warningf("Set maxRate is less than the number of threads (%v). To prevent threads from starving, maxRate was increased from: %v to: %v.", threadsRunning, maxRate, threadsRunning)
		maxRate = int64(threadsRunning)
	}
	maxRatePerThread := maxRate / int64(threadsRunning)
	// Distribute the remainder of the division across all threads.
	remainder := maxRate % int64(threadsRunning)
	remainderPerThread := make(map[int]int64, threadsRunning)
	for id := 0; remainder > 0; {
		remainderPerThread[id]++
		remainder--
		id++
	}

	for threadID := range t.runningThreads {
		t.threadThrottlers[threadID].setMaxRate(maxRatePerThread + remainderPerThread[threadID])
	}
	t.threadRunningsLastUpdate = threadsRunning
}

// MaxRate returns the current rate of the MaxRateModule.
func (t *Throttler) MaxRate() int64 {
	return t.maxRateModule.MaxRate()
}

// SetMaxRate updates the rate of the MaxRateModule.
func (t *Throttler) SetMaxRate(rate int64) {
	t.maxRateModule.SetMaxRate(rate)
}

// RecordReplicationLag must be called by users to report the "ts" tablet health
// data observed at "time".
// Note: After Close() is called, this method must not be called anymore.
func (t *Throttler) RecordReplicationLag(time time.Time, ts *discovery.TabletStats) {
	t.maxReplicationLagModule.RecordReplicationLag(time, ts)
}

// GetConfiguration returns the configuration of the MaxReplicationLag module.
func (t *Throttler) GetConfiguration() *throttlerdata.Configuration {
	return t.maxReplicationLagModule.getConfiguration()
}

// UpdateConfiguration updates the configuration of the MaxReplicationLag module.
func (t *Throttler) UpdateConfiguration(configuration *throttlerdata.Configuration, copyZeroValues bool) error {
	return t.maxReplicationLagModule.updateConfiguration(configuration, copyZeroValues)
}

// ResetConfiguration resets the configuration of the MaxReplicationLag module
// to its initial settings.
func (t *Throttler) ResetConfiguration() {
	t.maxReplicationLagModule.resetConfiguration()
}

// Log returns the most recent changes of the MaxReplicationLag module.
func (t *Throttler) Log() []result {
	return t.maxReplicationLagModule.log()
}
