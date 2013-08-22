// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hourglass

import (
	"container/heap"
	"sync"
	"testing"
	"time"
)

// sandboxEvent abstracts sandboxTimer and sandboxTicker,
// which supports generic start/stop/trigger actions.
type sandboxEvent interface {
	trigger()
	targetTime() time.Time
}

// timerEventQueue holds scheduled sandboxTimer and sandboxTicker.
type timerEventQueue []sandboxEvent

var mu sync.Mutex
var currentTime time.Time
var eventQueue *timerEventQueue

// initSandboxMode enables sandbox mode.
// It resets sandbox time function when called with a start time.
func initSandboxMode(start time.Time) {
	mu.Lock()
	defer mu.Unlock()
	currentTime = start
	eventQueue = &timerEventQueue{}
	heap.Init(eventQueue)
}

// InitSandboxTest initializes the sandbox mode in tests with a given start time.
func InitSandboxTest(_ *testing.T, start time.Time) {
	initSandboxMode(start)
}

// InitSandboxBench initializes the sandbox mode in benchmarks with a given start time.
func InitSandboxBench(_ *testing.B, start time.Time) {
	initSandboxMode(start)
}

// isSandboxMode returns true if currently it is in sandbox mode.
func isSandboxMode() bool {
	return eventQueue != nil
}

// It implements heap interface for timerEventQueue
func (queue timerEventQueue) Len() int {
	return len(queue)
}

func (queue timerEventQueue) Less(i, j int) bool {
	return queue[i].targetTime().Before(queue[j].targetTime())
}

func (queue timerEventQueue) Swap(i, j int) {
	queue[i], queue[j] = queue[j], queue[i]
}

func (queue *timerEventQueue) Push(x interface{}) {
	*queue = append(*queue, x.(sandboxEvent))
}

func (queue *timerEventQueue) Pop() interface{} {
	n := len(*queue)
	popped := (*queue)[n-1]
	*queue = (*queue)[:n-1]
	return popped
}

// Advance the sandbox time by the duration d.
// It will panic if it is not called in sandbox mode.
func Advance(t *testing.T, d time.Duration) {
	if !isSandboxMode() {
		panic("func Hourglass.Advance can only be called in sandbox mode")
	}

	targetTime := currentTime.Add(d)
	// notify Tick and Timer
	for {
		mu.Lock()
		if eventQueue.Len() == 0 {
			mu.Unlock()
			break
		}
		event := heap.Pop(eventQueue).(sandboxEvent)
		if event.targetTime().After(targetTime) {
			// It encounters an event with scheduled time after advanced time,
			// so it puts it back to the eventQueue and breaks.
			heap.Push(eventQueue, event)
			mu.Unlock()
			break
		}
		// advance currentTime to the scheduled time of the processing event.
		// So inside the trigger we have Now() == event.targetTime()
		currentTime = event.targetTime()
		mu.Unlock()
		event.trigger()
	}
	// advance currentTime to the required time
	mu.Lock()
	currentTime = targetTime
	mu.Unlock()
}

// Since returns the time elapsed since t, and is a replacement for time.Since.
// It is shorthand for hourglass.Now().Sub(t).
func Since(t time.Time) time.Duration {
	return Now().Sub(t)
}

// Now returns the current local time, and is a replacement for time.Now.
// Sandbox time when in sandbox mode; system time otherwise.
func Now() time.Time {
	if isSandboxMode() {
		mu.Lock()
		defer mu.Unlock()
		return currentTime
	}
	return time.Now()
}

// Sleep pauses the current goroutine for the duration d, and is a replacement for time.Sleep.
func Sleep(d time.Duration) {
	if isSandboxMode() {
		// register for interrupt
		<-newSandTimer(d).C
	} else {
		time.Sleep(d)
	}
}

// RealSleep always pauses the current goroutine for duration d at system level.
// It will panic if it is not called in sandbox mode.
func RealSleep(t *testing.T, d time.Duration) {
	if !isSandboxMode() {
		panic("func Hourglass.RealSleep can only be called in sandbox mode")
	}
	time.Sleep(d)
}

// when is a helper function for setting the 'when' field of a sandboxTimer and sandboxTicker.
// It returns the future time, which is currentTime + d.
// If d is negative or zero, it is ignored by returning currentTime.
func when(d time.Duration) time.Time {
	mu.Lock()
	defer mu.Unlock()
	if d > 0 {
		return currentTime.Add(d)
	}
	return currentTime
}

// startEvent starts the sandboxEvent.
// If the scheduled time of the event is current or earlier, it calls function f immediately,
// otherwise it pushes the event into the event queue.
func startEvent(t sandboxEvent) {
	mu.Lock()
	currTime := currentTime
	mu.Unlock()
	if t.targetTime().After(currTime) {
		mu.Lock()
		heap.Push(eventQueue, t)
		mu.Unlock()
	} else {
		go t.trigger()
	}
}

// stopEvent stops the sandboxEvent.
// It removes the event from the event queue.
func stopEvent(t sandboxEvent) {
	mu.Lock()
	for i, item := range *eventQueue {
		if item == t {
			heap.Remove(eventQueue, i)
			break
		}
	}
	mu.Unlock()
}

// sandboxTimer implements the function of time.Timer in sandbox mode.
// They can be scheduled to trigger at a later time (when), with the scheduled Time or a callback function.
// Timer only triggers once.
type sandboxTimer struct {
	C           chan time.Time
	started     bool
	when        time.Time
	callbackArg interface{}
}

// newSandTimer creates a new SandTimer that will send
// the sandbox time on its channel after at least duration d.
// If duration p is greater than 0, it is treated as a periodic ticker.
func newSandTimer(d time.Duration) *sandboxTimer {
	c := make(chan time.Time)
	t := &sandboxTimer{C: c, when: when(d), callbackArg: c, started: true}
	startEvent(t)
	return t
}

func (t *sandboxTimer) targetTime() time.Time {
	return t.when
}

// trigger handles time-up action depending on the type of callbackArg.
// It sends the target time when if callbackArg is a channel,
// and calls the func in another goroutine if callbackArg is a func.
func (t *sandboxTimer) trigger() {
	t.started = false
	switch t.callbackArg.(type) {
	case chan time.Time:
		// Blocking send of time on sandboxTimer.callbackArg,
		// which is also sandboxTimer.C.
		// Used in NewTimer, it may block as the chan is synchronized.
		// Used in NewTicker, it does not drop messages,
		// as we assume callback will not be slower than the interval.
		t.callbackArg.(chan time.Time) <- t.when
	case func():
		t.callbackArg.(func())()
	}
}

// Reset changes the sandbox timer to expire after duration d, simulating time.Timer.Reset behavior.
// It returns true if the sandbox timer had been active,
// false if the sandbox timer expired or been stopped.
func (t *sandboxTimer) Reset(d time.Duration) bool {
	active := t.started
	if active {
		stopEvent(t)
	}
	t.when = when(d)
	t.started = true
	startEvent(t)
	return active
}

// Stop prevents the Timer from firing, simulating time.Timer.Stop behavior.
// It returns true if the call stops the timer, false if the timer has already expired or been stopped.
// Stop does not close the channel, to prevent a read from the channel succeeding incorrectly.
func (t *sandboxTimer) Stop() bool {
	active := t.started
	if active {
		stopEvent(t)
		t.started = false
	}
	return active
}

// Timer represents a system time.Timer, or a sandboxTimer.
// When the Timer expires, the current time will be sent on C,
// unless the Timer was created by AfterFunc.
type Timer struct {
	C         <-chan time.Time
	sysTimer  *time.Timer
	sandTimer *sandboxTimer
}

// NewTimer creats a new Timer that will send the current time on its channel after at least duration d,
// and is a replacement of time.NewTimer.
func NewTimer(d time.Duration) *Timer {
	if isSandboxMode() {
		sandT := newSandTimer(d)
		return &Timer{C: sandT.C, sandTimer: sandT}
	}
	sysT := time.NewTimer(d)
	return &Timer{C: sysT.C, sysTimer: sysT}
}

// Reset changes the timer to expire after duration d, which is a replacement of time.Timer.Reset.
// It returns true if the timer had been active, false if the timer had expired or been fired.
func (t *Timer) Reset(d time.Duration) bool {
	if isSandboxMode() {
		return t.sandTimer.Reset(d)
	}
	return t.sysTimer.Reset(d)
}

// Stop prevents the Timer from firing, and is a replacement of time.Timer.Stop.
// It return true if the call stops the timer, false if the timer has already expired or been stopped.
// Stop does not close the channel, to prevent a read from the channel succeeding incorrectly.
func (t *Timer) Stop() bool {
	if isSandboxMode() {
		return t.sandTimer.Stop()
	}
	return t.sysTimer.Stop()
}

// After waits for the duration to elapse and then sends the current time on the returned channel,
// and is a replacement of time.After.
// It is equivalent to NewTimer(d).C.
func After(d time.Duration) <-chan time.Time {
	return NewTimer(d).C
}

// AfterFunc waits for the duration to elapse and then calls f in its own goroutine,
// and is a replacement of time.AfterFunc.
// It returns a Timer that can be used to cancel the call using its Stop method.
func AfterFunc(d time.Duration, f func()) *Timer {
	if isSandboxMode() {
		sandT := &sandboxTimer{when: when(d), callbackArg: f, started: true}
		startEvent(sandT)
		return &Timer{sandTimer: sandT}
	}
	return &Timer{sysTimer: time.AfterFunc(d, f)}
}

// sandboxTicker implements the function of time.Ticker in sandbox mode.
// It can be scheduled to trigger periodically, with the scheduled Time sent on channel.
type sandboxTicker struct {
	C      chan time.Time
	when   time.Time
	period time.Duration
}

func (t *sandboxTicker) targetTime() time.Time {
	return t.when
}

// trigger handles time-up action for sandboxTicker.
// It queues itself with the next target time, and sends the target time.
func (t *sandboxTicker) trigger() {
	// It must queue itself first before send the time.
	// Otherwise if application code stops ticker right after it receives the time,
	// a race condition may occur that the stop may fail because ticker has not been queued.
	currTime := t.when
	t.when = t.when.Add(t.period)
	mu.Lock()
	heap.Push(eventQueue, t)
	mu.Unlock()
	t.C <- currTime
}

// Stop prevents the Timer from firing, simulating time.Timer.Stop behavior.
// It returns true if the call stops the timer, false if the timer has already expired or been stopped.
// Stop does not close the channel, to prevent a read from the channel succeeding incorrectly.
func (t *sandboxTicker) Stop() {
	stopEvent(t)
}

// newSandTimer creates a new SandTimer that will send
// the sandbox time on its channel after at least duration d.
// If duration p is greater than 0, it is treated as a periodic ticker.
func newSandTicker(d time.Duration) *sandboxTicker {
	c := make(chan time.Time)
	t := &sandboxTicker{C: c, when: when(d), period: d}
	startEvent(t)
	return t
}

// Ticker holds a channel that delivers ticks of a clock at intervals, and is a replacement of time.Ticker.
type Ticker struct {
	C          <-chan time.Time // The channel on which the ticks are delivered
	sysTicker  *time.Ticker
	sandTicker *sandboxTicker
}

// NewTicker returns a new Ticker containing a channel that will send the
// time with a period specified by the duration argument,
// and is a replacement of time.NewTicker.
// It adjusts the intervals or drops ticks to make up for slow receivers.
// The duration d must be greater than zero; if not, NewTicker will panic.
func NewTicker(d time.Duration) *Ticker {
	if d <= 0 {
		panic("non-positive interval for NewTicker")
	}
	if isSandboxMode() {
		sandT := newSandTicker(d)
		return &Ticker{C: sandT.C, sandTicker: sandT}
	}
	sysT := time.NewTicker(d)
	return &Ticker{C: sysT.C, sysTicker: sysT}
}

// Stop turns off a ticker. After being stopped, it will not send more ticks.
// Stop does not close the channel, to prevent a read from the channel succeeding.
// It is a replacement of time.Ticker.Stop.
func (t *Ticker) Stop() {
	if isSandboxMode() {
		t.sandTicker.Stop()
	} else {
		t.sysTicker.Stop()
	}
}

// Tick is a convenience wrapper for NewTicker providing access to the ticking channel only,
// and is a replacement of time.Tick.
// Useful for clients that have no need to shut down the ticker.
func Tick(d time.Duration) <-chan time.Time {
	if d <= 0 {
		return nil
	}
	return NewTicker(d).C
}
