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
	setStart(bool)
	isStarted() bool
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
		panic("Hourglass: func Advance can only be called in sandbox mode")
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
		mu.Unlock()
		if event.targetTime().After(targetTime) {
			// It encounters an event with scheduled time after advanced time,
			// so it puts it back to the eventQueue and breaks.
			mu.Lock()
			heap.Push(eventQueue, event)
			mu.Unlock()
			break
		}
		// advance currentTime to the scheduled time of the processing event.
		// So inside the trigger we have Now() == event.targetTime()
		mu.Lock()
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

// SleepSys always pauses the current goroutine for duration d at system level.
// It will panic if it is not called in sandbox mode.
func SleepSys(t *testing.T, d time.Duration) {
	if !isSandboxMode() {
		panic("Hourglass: func SleepSys can only be called in sandbox mode")
	}

	time.Sleep(d)
}

// when is a helper function for setting the 'when' field of a sandboxTimer and sandboxTicker.
// It returns that the time will be, Duration d in the future.
// If d is negative or zero, it is ignored.
func when(d time.Duration) time.Time {
	mu.Lock()
	defer mu.Unlock()
	if d > 0 {
		return currentTime.Add(d)
	}
	return currentTime
}

// startTimer starts the sandboxEvent
// If the target time is current or earlier, call the function f immediately,
// otherwise push into the event queue
func startTimer(t sandboxEvent) {
	mu.Lock()
	currTime := currentTime
	mu.Unlock()
	if t.targetTime().After(currTime) {
		t.setStart(true)
		mu.Lock()
		heap.Push(eventQueue, t)
		mu.Unlock()
	} else {
		go t.trigger()
	}
}

// stopTimer stops the sandboxEvent
// If the target is active, remove it from the event queue
// It returns if the timer is active
func stopTimer(t sandboxEvent) bool {
	active := t.isStarted()
	if active {
		// delete timer from eventQueue
		mu.Lock()
		for i, item := range *eventQueue {
			if item == t {
				heap.Remove(eventQueue, i)
				break
			}
		}
		mu.Unlock()
		t.setStart(false)
	}
	return active
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
	t := &sandboxTimer{C: c, when: when(d), callbackArg: c}
	startTimer(t)
	return t
}

func (t *sandboxTimer) targetTime() time.Time {
	return t.when
}
func (t *sandboxTimer) setStart(b bool) {
	t.started = b
}
func (t *sandboxTimer) isStarted() bool {
	return t.started
}

// Ch returns the channel used to communicate time.Time when sandboxTimer fires.
func (t *sandboxTimer) Ch() <-chan time.Time {
	return t.C
}

// trigger handles time-up action depending on the type of callbackArg.
// It sends the target time when if callbackArg is a channel,
// and call the func in another goroutine if callbackArg is a func.
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
	active := stopTimer(t)
	t.when = when(d)
	startTimer(t)
	return active
}

// Stop prevents the Timer from firing, simulating time.Timer.Stop behavior.
// It returns true if the call stops the timer, false if the timer has already expired or been stopped.
// Stop does not close the channel, to prevent a read from the channel succeeding incorrectly.
func (t *sandboxTimer) Stop() bool {
	return stopTimer(t)
}

// Timer represents a system time.Timer, or a sandboxTimer.
// When the Timer expires, the current time will be sent on C,
// unless the Timer was created by AfterFunc.
type Timer interface {
	Reset(d time.Duration) bool
	Stop() bool
	Ch() <-chan time.Time
}

// sysTimer wraps the time.Timer to implement interface Timer.
type sysTimer struct {
	*time.Timer
}

// Ch returns the channel of underlying time.Timer.
func (t sysTimer) Ch() <-chan time.Time {
	return t.C
}

// NewTimer creats a new Timer that will send the current time on its channel after at least duration d,
// and is a replacement of time.NewTimer.
func NewTimer(d time.Duration) Timer {
	if isSandboxMode() {
		return newSandTimer(d)
	}
	return &sysTimer{time.NewTimer(d)}
}

// After waits for the duration to elapse and then sends the current time on the returned channel,
// and is a replacement of time.After.
// It is equivalent to NewTimer(d).C.
func After(d time.Duration) <-chan time.Time {
	return NewTimer(d).Ch()
}

// AfterFunc waits for the duration to elapse and then calls f in its own goroutine,
// and is a replacement of time.AfterFunc.
// It returns a Timer that can be used to cancel the call using its Stop method.
func AfterFunc(d time.Duration, f func()) Timer {
	if isSandboxMode() {
		t := &sandboxTimer{when: when(d), callbackArg: f}
		startTimer(t)
		return t
	}
	return &sysTimer{time.AfterFunc(d, f)}
}

// sandboxTicker implements the function of time.Ticker in sandbox mode.
// It can be scheduled to trigger periodically, with the scheduled Time sent on channel.
type sandboxTicker struct {
	C       chan time.Time
	started bool
	when    time.Time
	period  time.Duration
}

func (t *sandboxTicker) targetTime() time.Time {
	return t.when
}
func (t *sandboxTicker) setStart(b bool) {
	t.started = b
}
func (t *sandboxTicker) isStarted() bool {
	return t.started
}

// Ch returns the channel used to communicate time.Time when sandboxTimer fires.
func (t *sandboxTicker) Ch() <-chan time.Time {
	return t.C
}

// trigger handles time-up action for sandboxTicker.
// It sends the target time, and queues itself with the next target time.
func (t *sandboxTicker) trigger() {
	t.C <- t.when
	if t.started {
		t.when = t.when.Add(t.period)
		mu.Lock()
		heap.Push(eventQueue, t)
		mu.Unlock()
	}
}

// Stop prevents the Timer from firing, simulating time.Timer.Stop behavior.
// It returns true if the call stops the timer, false if the timer has already expired or been stopped.
// Stop does not close the channel, to prevent a read from the channel succeeding incorrectly.
func (t *sandboxTicker) Stop() {
	stopTimer(t)
}

// newSandTimer creates a new SandTimer that will send
// the sandbox time on its channel after at least duration d.
// If duration p is greater than 0, it is treated as a periodic ticker.
func newSandTicker(d time.Duration) *sandboxTicker {
	c := make(chan time.Time)
	t := &sandboxTicker{C: c, when: when(d), period: d}
	startTimer(t)
	return t
}

// Ticker holds a channel that delivers ticks of a clock at intervals, and is a replacement of time.Ticker.
type Ticker interface {
	Stop()
	Ch() <-chan time.Time // The channel on which the ticks are delivered
}

// sysTicker wraps time.Ticker to implement interface Ticker.
type sysTicker struct {
	*time.Ticker
}

// Stop stops the sysTicker, which calls underlying time.Ticker.Stop.
// It always returns true as a dummy value.
func (t *sysTicker) Stop() {
	t.Ticker.Stop()
}

// Ch returns the channel of underlying time.Ticker.
func (t *sysTicker) Ch() <-chan time.Time {
	return t.C
}

// NewTicker returns a new Ticker containing a channel that will send the
// time with a period specified by the duration argument,
// and is a replacement of time.NewTicker.
// It adjusts the intervals or drops ticks to make up for slow receivers.
// The duration d must be greater than zero; if not, NewTicker will panic.
func NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("non-positive interval for NewTicker")
	}
	if isSandboxMode() {
		return newSandTicker(d)
	}
	return &sysTicker{time.NewTicker(d)}
}

// Tick is a convenience wrapper for NewTicker providing access to the ticking channel only,
// and is a replacement of time.Tick.
// Useful for clients that have no need to shut down the ticker.
func Tick(d time.Duration) <-chan time.Time {
	if d <= 0 {
		return nil
	}
	return NewTicker(d).Ch()
}
