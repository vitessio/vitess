// Package throttler provides a client-side, local throttler which is used to
// throttle (and actively pace) writes during the resharding process.
//
// The throttler has two main goals:
// a) allow resharding data into an existing keyspace by throttling at a fixed
//    rate
// b) ensure that the MySQL replicas do not become overloaded
//
// To support b), the throttler constantly monitors the health of all replicas
// and reduces the allowed rate if the replication lag is above a certain
// threshold.
// TODO(mberlin): Implement b).
package throttler

import (
	"fmt"
	"math"
	"sync"
	"time"

	log "github.com/golang/glog"
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
	MaxRateModuleDisabled = int64(math.MaxInt64)

	// InvalidMaxRate is a constant which will fail in a NewThrottler() call.
	// It should be used when returning maxRate in an error case.
	InvalidMaxRate = -1

	// ReplicationLagModuleDisabled can be set in NewThrottler() to disable
	// throttling based on the MySQL replication lag.
	ReplicationLagModuleDisabled = int64(math.MaxInt64)

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

	closed bool
	// modules is the list of modules which can limit the current rate. The order
	// of the list defines the priority of the module. (Lower rates trump.)
	modules []Module
	// rateUpdateChan is used to notify the throttler that the current max rate
	// must be recalculated and updated.
	rateUpdateChan chan struct{}

	// The group below is unguarded because the fields are immutable and each
	// thread accesses an element at a different index.
	threadThrottlers []*threadThrottler
	threadFinished   []bool

	nowFunc func() time.Time

	// The group below is unguarded because it's only accessed by the update Go
	// routine which reads the rateUpdateChan channel.
	maxRate          int64
	maxRatePerThread int64

	mu sync.Mutex
	// runningThreads tracks which threads have not finished yet.
	runningThreads map[int]bool
	// threadRunningsLastUpdate caches for updateMaxRate() how many threads were
	// running at the previous run.
	threadRunningsLastUpdate int
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
func NewThrottler(name, unit string, threadCount int, maxRate int64, maxReplicationLag int64) (*Throttler, error) {
	return newThrottler(name, unit, threadCount, maxRate, maxReplicationLag, time.Now)
}

// newThrottlerWithClock should only be used for testing.
func newThrottlerWithClock(name, unit string, threadCount int, maxRate int64, maxReplicationLag int64, nowFunc func() time.Time) (*Throttler, error) {
	return newThrottler(name, unit, threadCount, maxRate, maxReplicationLag, nowFunc)
}

func newThrottler(name, unit string, threadCount int, maxRate int64, maxReplicationLag int64, nowFunc func() time.Time) (*Throttler, error) {
	// Verify input parameters.
	if maxRate < 0 {
		return nil, fmt.Errorf("maxRate must be >= 0: %v", maxRate)
	}
	if maxReplicationLag < 0 {
		return nil, fmt.Errorf("maxReplicationLag must be >= 0: %v", maxReplicationLag)
	}

	// Enable the configured modules.
	var modules []Module
	modules = append(modules, NewMaxRateModule(maxRate))
	// TODO(mberlin): Append ReplicationLagModule once it's implemented.

	// Start each module (which might start own Go routines).
	rateUpdateChan := make(chan struct{}, 10)
	for _, m := range modules {
		m.Start(rateUpdateChan)
	}

	runningThreads := make(map[int]bool, threadCount)
	threadThrottlers := make([]*threadThrottler, threadCount, threadCount)
	for i := 0; i < threadCount; i++ {
		threadThrottlers[i] = newThreadThrottler(i)
		runningThreads[i] = true
	}
	t := &Throttler{
		name:             name,
		unit:             unit,
		modules:          modules,
		rateUpdateChan:   rateUpdateChan,
		threadThrottlers: threadThrottlers,
		threadFinished:   make([]bool, threadCount, threadCount),
		runningThreads:   runningThreads,
		nowFunc:          nowFunc,
	}

	// Initialize maxRate.
	t.updateMaxRate()

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
		return
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
}

// updateMaxRate recalculates the current max rate and updates all
// threadThrottlers accordingly.
// The rate changes when the number of thread changes or a module updated its
// max rate.
func (t *Throttler) updateMaxRate() {
	// Set it to infinite initially.
	maxRate := int64(math.MaxInt64)

	// Find out the new max rate.
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
	if maxRate == t.maxRate && threadsRunning == t.threadRunningsLastUpdate {
		// Rate for each thread is unchanged.
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
		t.threadThrottlers[threadID].setMaxRate(
			maxRatePerThread + remainderPerThread[threadID])
	}
	t.threadRunningsLastUpdate = threadsRunning
}
