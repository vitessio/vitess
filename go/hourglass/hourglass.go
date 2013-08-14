// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hourglass

import (
	"container/heap"
	"errors"
	"sync"
	"testing"
	"time"
)

type TimerEventQueue []*sandboxTimer

var _use_sys_time bool

var _lock sync.Mutex
var _current_time time.Time
var _event_queue *TimerEventQueue

// package func
func init() {
	_use_sys_time = true
}

// Set if using system time; if not, it uses sandbox time.
// It resets sandbox time function when called with sys_time as false.
func SetRealTime(sys_time bool) {
	_use_sys_time = sys_time
	if !sys_time {
		_lock.Lock()
		defer _lock.Unlock()

		_current_time = time.Now()
		_event_queue = &TimerEventQueue{}
		heap.Init(_event_queue)
	}
}

// helper to make sure sandbox specific func not called when using system time.
func _verify_sandbox_mode(_ *testing.T) {
	if _use_sys_time {
		panic("Hourglass: func can only be called in sandbox mode")
	}
}

// It implements heap interface
func (queue TimerEventQueue) Len() int {
	return len(queue)
}

func (queue TimerEventQueue) Less(i, j int) bool {
	return queue[i].when.Before(queue[j].when)
}

func (queue TimerEventQueue) Swap(i, j int) {
	queue[i], queue[j] = queue[j], queue[i]
}

func (queue *TimerEventQueue) Push(x interface{}) {
	*queue = append(*queue, x.(*sandboxTimer))
}

func (queue *TimerEventQueue) Pop() interface{} {
	old := *queue
	n := len(old)
	x := old[n-1]
	*queue = old[0 : n-1]
	return x
}

// time.go

// **Sandbox Only**
// Advance the sandbox time by the duration d
func Advance(t *testing.T, d time.Duration) {
	_verify_sandbox_mode(t)

	_lock.Lock()
	defer _lock.Unlock()
	_current_time = _current_time.Add(d)
	// notify Tick and Timer
	for _event_queue.Len() > 0 {
		sandtimer := heap.Pop(_event_queue).(*sandboxTimer)
		// timer is scheduled for a later time, stop.
		if sandtimer.when.After(_current_time) {
			heap.Push(_event_queue, sandtimer)
			break
		}
		if sandtimer.period > 0 {
			// ticker (repeat)
			i := sandtimer.when
			for ; !i.After(_current_time); i = i.Add(sandtimer.period) {
				sandtimer.f(sandtimer.when, sandtimer.arg)
			}
			sandtimer.when = i
			heap.Push(_event_queue, sandtimer)
		} else {
			// timer
			sandtimer.started = false
			sandtimer.f(sandtimer.when, sandtimer.arg)
		}
	}
}

// **Replace time.Since(t Time)**
// Since returns the time elapsed since t.
// It is shorthand for hourglass.Now().Sub(t).
func Since(t time.Time) time.Duration {
	return Now().Sub(t)
}

// **Replace time.Now()**
// Now returns the current local time.
// Sandbox time when in sandbox mode; system time otherwise.
func Now() time.Time {
	if _use_sys_time {
		return time.Now()
	} else {
		_lock.Lock()
		defer _lock.Unlock()
		return _current_time
	}
}

// sleep.go

// **Replace time.Sleep()**
// Sleep pauses the current goroutine for the duration d.
func Sleep(d time.Duration) {
	if _use_sys_time {
		time.Sleep(d)
	} else {
		// register for interrupt
		t := newSandTimer(d, 0)
		<-t.C
	}
}

// **Sandbox Only**
// Always pauses the current goroutine for duration d at system level.
func SleepSys(t *testing.T, d time.Duration) {
	_verify_sandbox_mode(t)

	time.Sleep(d)
}

// The Sandbox Timer simulating time.Timer and time.Ticker in sandbox mode.
type sandboxTimer struct {
	C       <-chan time.Time
	started bool
	when    time.Time
	period  time.Duration
	f       func(time.Time, interface{})
	arg     interface{}
}

// NewSandTimer creates a new SandTimer that will send
// the sandbox time on its channel after at least duration d.
// If duration p is greater than 0, it is treated as a periodic ticker.
func newSandTimer(d time.Duration, p time.Duration) *sandboxTimer {
	c := make(chan time.Time, 1)
	t := &sandboxTimer{C: c, when: when(d), period: p, f: sendTimeSand, arg: c}
	startSandTimer(t)
	return t
}

func sendTimeSand(now time.Time, c interface{}) {
	// Non-blocking send of time on c.
	// Used in NewTimer, it cannot block anyway (buffer).
	// Used in NewTicker, dropping sends on the floor is
	// the desired behavior when the reader gets behind,
	// because the sends are periodic.
	select {
	case c.(chan time.Time) <- now:
	default:
	}
}

// when is a helper function for setting the 'when' field of a SandTimer.
// It returns that the time will be, Duration d in the future.
// If d is negative or zero, it is ignored.
func when(d time.Duration) time.Time {
	new_time := _current_time
	if d > 0 {
		new_time = new_time.Add(d)
	}
	return new_time
}

// Start the Sandbox Timer
// If the target time is current or earlier, call the function f immediately,
// otherwise push into the event queue
func startSandTimer(timer *sandboxTimer) {
	_lock.Lock()
	defer _lock.Unlock()
	if timer.when.After(_current_time) {
		timer.started = true
		heap.Push(_event_queue, timer)
	} else {
		timer.f(timer.when, timer.arg)
	}
}

// Stop the Sandbox Timer
// If the target is active, remove it from the event queue
// It returns if the timer is active
func stopSandTimer(timer *sandboxTimer) bool {
	_lock.Lock()
	defer _lock.Unlock()
	active := timer.started
	if active {
		// delete timer from _event_queue
		for i, n := 0, _event_queue.Len(); i < n; i++ {
			item := (*_event_queue)[i]
			if item == timer {
				_event_queue.Swap(i, n-1)
				(*_event_queue)[n-1] = nil
				*_event_queue = (*_event_queue)[0 : n-1]
				heap.Init(_event_queue)
				break
			}
		}
		timer.started = false
	}
	return active
}

// Simulate time.Timer.Reset(d) behavior.
// Reset changes the sandbox timer to expire after duration d.
// It returns true if the sandbox timer had been active,
// false if the sandbox timer expired or been stopped.
func (t *sandboxTimer) Reset(d time.Duration) bool {
	active := stopSandTimer(t)
	t.when = when(d)
	startSandTimer(t)
	return active
}

// Simulate time.Timer.Stop() behavior.
// Stop prevents the Timer from firing.
// It returns true if the call stops the timer, false if the timer has already
// expired or been stopped.
// Stop does not close the channel, to prevent a read from the channel succeeding
// incorrectly.
func (t *sandboxTimer) Stop() bool {
	return stopSandTimer(t)
}

// **Replace time.Timer**
// The Timer type represents a single event.
// When the Timer expires, the current time will be sent on C,
// unless the Timer was created by AfterFunc
type Timer struct {
	C           <-chan time.Time
	_sys_timer  *time.Timer
	_sand_timer *sandboxTimer
}

// **Replace time.NewTimer(d)**
// NewTimer creats a new Timer that will send
// the current time on its channel after at least duration d.
func NewTimer(d time.Duration) *Timer {
	var t *Timer
	if _use_sys_time {
		sys_t := time.NewTimer(d)
		t = &Timer{C: sys_t.C, _sys_timer: sys_t, _sand_timer: nil}
	} else {
		sand_t := newSandTimer(d, 0)
		t = &Timer{C: sand_t.C, _sys_timer: nil, _sand_timer: sand_t}
	}
	return t
}

// **Replace time.Timer.Reset(d)**
// Reset changes the timer to expire after duration d.
// It returns true if the timer had been active, false if the timer had
// expired or been stopped.
func (t *Timer) Reset(d time.Duration) bool {
	var res bool
	if _use_sys_time {
		res = t._sys_timer.Reset(d)
	} else {
		res = t._sand_timer.Reset(d)
	}
	return res
}

// **Replace time.Timer.Stop()**
// Stop prevents the Timer from firing.
// It returns true if the call stops the timer, false if the timer has already
// expired or been stopped.
// Stop does not close the channel, to prevent a read from the channel succeeding
// incorrectly.
func (t *Timer) Stop() bool {
	var res bool
	if _use_sys_time {
		res = t._sys_timer.Stop()
	} else {
		res = t._sand_timer.Stop()
	}
	return res
}

// **Replace time.After(d)**
// After waits for the duration to elapse and then sends the current time
// on the returned channel.
// It is equivalent to NewTimer(d).C.
func After(d time.Duration) <-chan time.Time {
	return NewTimer(d).C
}

// **Replace time.AfterFunc(d, f)**
// AfterFunc waits for the duration to elapse and then calls f
// in its own goroutine. It returns a Timer that can
// be used to cancel the call using its Stop method.
func AfterFunc(d time.Duration, f func()) *Timer {
	var t *Timer
	if _use_sys_time {
		sys_t := time.AfterFunc(d, f)
		t = &Timer{_sys_timer: sys_t}
	} else {
		sand_t := &sandboxTimer{when: when(d), f: goFuncSand, arg: f}
		startSandTimer(sand_t)
		t = &Timer{_sand_timer: sand_t}
	}
	return t
}

// helper to call the attached func in sandboxTimer
func goFuncSand(now time.Time, arg interface{}) {
	go arg.(func())()
}

// tick.go

// **Replace time.Ticker**
// A Ticker holds a channel that delivers `ticks` of a clock at intervals.
type Ticker struct {
	C            <-chan time.Time // The channel on which the ticks are delivered
	_sys_ticker  *time.Ticker
	_sand_ticker *sandboxTimer
}

// **Replace time.NewTicker(d)**
// NewTicker returns a new Ticker containing a channel that will send the
// time with a period specified by the duratino argument.
// It adjusts the intervals or drops ticks to make up for slow receivers.
// The duration d must be greater than zero; if not, NewTicker will panic.
func NewTicker(d time.Duration) *Ticker {
	if d <= 0 {
		panic(errors.New("non-positive interval for NewTicker"))
	}
	var t *Ticker
	if _use_sys_time {
		sys_t := time.NewTicker(d)
		t = &Ticker{C: sys_t.C, _sys_ticker: sys_t}
	} else {
		sand_t := newSandTimer(d, d)
		t = &Ticker{C: sand_t.C, _sand_ticker: sand_t}
	}
	return t
}

// **Replace time.Ticker.Stop()**
// Stop turns off a ticker. After stop, no more ticks will be sent.
// Stop does not close the channel, to prevent a read from the channel succeeding
// incorrectly.
func (t *Ticker) Stop() {
	if _use_sys_time {
		t._sys_ticker.Stop()
	} else {
		t._sand_ticker.Stop()
	}
}

// **Replace time.Tick(d)**
// Tick is a convenience wrapper for NewTicker providing access to the ticking
// channel only. Useful for clients that ahve no need to shut down the ticker.
func Tick(d time.Duration) <-chan time.Time {
	if d <= 0 {
		return nil
	}
	return NewTicker(d).C
}
