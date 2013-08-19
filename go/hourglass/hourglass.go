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

type timerEventQueue []*sandboxTimer

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

func isSandboxMode() bool {
	return eventQueue != nil
}

// It implements heap interface
func (queue timerEventQueue) Len() int {
	return len(queue)
}

func (queue timerEventQueue) Less(i, j int) bool {
	return queue[i].when.Before(queue[j].when)
}

func (queue timerEventQueue) Swap(i, j int) {
	queue[i], queue[j] = queue[j], queue[i]
}

func (queue *timerEventQueue) Push(x interface{}) {
	*queue = append(*queue, x.(*sandboxTimer))
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

	mu.Lock()
	defer mu.Unlock()
	currentTime = currentTime.Add(d)
	// notify Tick and Timer
	for eventQueue.Len() > 0 {
		sandtimer := heap.Pop(eventQueue).(*sandboxTimer)
		// timer is scheduled for a later time, stop.
		if sandtimer.when.After(currentTime) {
			heap.Push(eventQueue, sandtimer)
			break
		}
		if sandtimer.period > 0 {
			// ticker (repeat)
			var i time.Time
			for i = sandtimer.when; !i.After(currentTime); i = i.Add(sandtimer.period) {
				sandtimer.trigger()
			}
			sandtimer.when = i
			heap.Push(eventQueue, sandtimer)
		} else {
			// timer
			sandtimer.started = false
			sandtimer.trigger()
		}
	}
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
		<-newSandTimer(d, 0).C
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

// sandboxTimer implements the function of time.Timer and time.Ticker in sandbox mode.
// They can be scheduled to trigger at a later time (when), with the scheduled Time or a callback function.
// Timer only triggers once, and Ticker triggers in an interval with period.
type sandboxTimer struct {
	C           <-chan time.Time
	started     bool
	when        time.Time
	period      time.Duration
	callbackArg interface{}
}

// newSandTimer creates a new SandTimer that will send
// the sandbox time on its channel after at least duration d.
// If duration p is greater than 0, it is treated as a periodic ticker.
func newSandTimer(d time.Duration, p time.Duration) *sandboxTimer {
	c := make(chan time.Time, 1)
	t := &sandboxTimer{C: c, when: when(d), period: p, callbackArg: c}
	t.startTimer()
	return t
}

// when is a helper function for setting the 'when' field of a SandTimer.
// It returns that the time will be, Duration d in the future.
// If d is negative or zero, it is ignored.
func when(d time.Duration) time.Time {
	if d > 0 {
		return currentTime.Add(d)
	}
	return currentTime
}

// startTimer starts the Sandbox Timer
// If the target time is current or earlier, call the function f immediately,
// otherwise push into the event queue
func (t *sandboxTimer) startTimer() {
	mu.Lock()
	defer mu.Unlock()
	if t.when.After(currentTime) {
		t.started = true
		heap.Push(eventQueue, t)
	} else {
		t.trigger()
	}
}

// stopTimer stops the Sandbox Timer
// If the target is active, remove it from the event queue
// It returns if the timer is active
func (t *sandboxTimer) stopTimer() bool {
	mu.Lock()
	defer mu.Unlock()
	active := t.started
	if active {
		// delete timer from eventQueue
		for i, item := range *eventQueue {
			if item == t {
				heap.Remove(eventQueue, i)
				break
			}
		}
		t.started = false
	}
	return active
}

// trigger handles time-up action depending on the type of callbackArg.
// It sends the target time when if callbackArg is a channel,
// and call the func in another goroutine if callbackArg is a func.
func (t *sandboxTimer) trigger() {
	switch t.callbackArg.(type) {
	case chan time.Time:
		// Non-blocking send of time on sandboxTimer.callbackArg,
		// which is also sandboxTimer.C.
		// Used in NewTimer, it cannot block anyway (buffer).
		// Used in NewTicker, dropping sends on the floor is
		// the desired behavior when the reader gets behind,
		// because the sends are periodic.
		select {
		case t.callbackArg.(chan time.Time) <- t.when:
		default:
		}
	case func():
		go t.callbackArg.(func())()
	}
}

// Reset changes the sandbox timer to expire after duration d, simulating time.Timer.Reset behavior.
// It returns true if the sandbox timer had been active,
// false if the sandbox timer expired or been stopped.
func (t *sandboxTimer) Reset(d time.Duration) bool {
	active := t.stopTimer()
	t.when = when(d)
	t.startTimer()
	return active
}

// Stop prevents the Timer from firing, simulating time.Timer.Stop behavior.
// It returns true if the call stops the timer, false if the timer has already expired or been stopped.
// Stop does not close the channel, to prevent a read from the channel succeeding incorrectly.
func (t *sandboxTimer) Stop() bool {
	return t.stopTimer()
}

// Ch returns the channel used to communicate time.Time when sandboxTimer fires.
func (t *sandboxTimer) Ch() <-chan time.Time {
	return t.C
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
		return newSandTimer(d, 0)
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
		sandT := &sandboxTimer{when: when(d), callbackArg: f}
		sandT.startTimer()
		return sandT
	}
	return &sysTimer{time.AfterFunc(d, f)}
}

// Ticker holds a channel that delivers ticks of a clock at intervals, and is a replacement of time.Ticker.
type Ticker interface {
	Stop() bool
	Ch() <-chan time.Time // The channel on which the ticks are delivered
}

// sysTicker wraps time.Ticker to implement interface Ticker.
type sysTicker struct {
	*time.Ticker
}

// Stop stops the sysTicker, which calls underlying time.Ticker.Stop.
// It always returns true as a dummy value.
func (t *sysTicker) Stop() bool {
	t.Ticker.Stop()
	return true
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
		return newSandTimer(d, d)
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
