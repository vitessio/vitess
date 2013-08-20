package hourglass

import (
	"sort"
	"testing"
	"time"
)

// setSandboxMode is a helper function to change mode for unit testing
func setSandboxMode(sandboxMode bool) {
	if sandboxMode {
		initSandboxMode(time.Now())
	} else {
		eventQueue = nil
	}
}

func TestAdvance(t *testing.T) {
	setSandboxMode(true)
	t1 := Now()
	d := time.Second * 11
	Advance(t, d)
	t2 := Now()
	dt := t2.Sub(t1)
	if dt != d {
		t.Fatalf("TestAdvance: want %d, got %d", d, dt)
	}
}

func TestSleep(t *testing.T) {
	setSandboxMode(true)
	const delay = 100 * time.Millisecond
	go func() {
		// pause for a while to make sure main thread is waiting
		SleepSys(t, delay/2)
		Advance(t, delay)
	}()
	start := Now()
	Sleep(delay)
	duration := Now().Sub(start)
	if duration != delay {
		t.Fatalf("TestSleep slept: want %s, got %s", delay, duration)
	}
}

func TestAfterFunc(t *testing.T) {
	for _, ttype := range []bool{false, true} {
		setSandboxMode(ttype)
		i := 10
		c := make(chan bool)
		var f func()
		f = func() {
			i--
			if i >= 0 {
				AfterFunc(0, f)
				tempC := After(time.Second)
				if ttype {
					go Advance(t, time.Second)
				}
				<-tempC
			} else {
				c <- true
			}
		}
		AfterFunc(0, f)
		<-c
	}
}

func TestAfter(t *testing.T) {
	for _, ttype := range []bool{false, true} {
		setSandboxMode(ttype)
		const delay = 100 * time.Millisecond
		start := Now()
		c := After(delay)
		if ttype {
			go Advance(t, delay)
		}
		end := <-c
		if duration := Now().Sub(start); duration < delay {
			t.Fatalf("After slept: want %s, got %s", delay, duration)
		}
		if min := start.Add(delay); end.Before(min) {
			t.Fatalf("After(%s): want >= %s, got %s", delay, min, end)
		}
	}
}

func TestAfterFuncTime(t *testing.T) {
	setSandboxMode(true)
	const delay = 100 * time.Millisecond
	c := make(chan time.Time)
	t0 := Now()
	f := func() {
		c <- Now()
	}
	AfterFunc(delay, f)
	go Advance(t, 2*delay)
	t1 := <-c
	if delta := t1.Sub(t0); delta != delay {
		t.Fatalf("AfterFuncTime: triggered at incorrect time, expect %s, got %s", delay, delta)
	}
	if delta := Now().Sub(t0); delta != (2 * delay) {
		t.Fatalf("AfterFuncTime: time is incorrect after Advance, expect %s, got %s", (2 * delay), delta)
	}
}

func TestAfterTick(t *testing.T) {
	const Count = 10
	Delta := 100 * time.Millisecond
	if testing.Short() {
		Delta = 10 * time.Millisecond
	}
	for _, ttype := range []bool{false, true} {
		setSandboxMode(ttype)
		t0 := Now()
		for i := 0; i < Count; i++ {
			c := After(Delta)
			if ttype {
				go Advance(t, Delta)
			}
			<-c
		}
		t1 := Now()
		d := t1.Sub(t0)
		target := Delta * Count
		if d < target*9/10 {
			t.Fatalf("%d ticks of %s too fast: want %s, got %s", Count, Delta, target, d)
		}
		if !testing.Short() && d > target*30/10 {
			t.Fatalf("%d ticks of %s too slow: want %s, got %s", Count, Delta, target, d)
		}
	}
}

func TestAfterStop(t *testing.T) {
	for _, ttype := range []bool{false, true} {
		setSandboxMode(ttype)
		AfterFunc(100*time.Millisecond, func() {})
		t0 := NewTimer(50 * time.Millisecond)
		c1 := make(chan bool, 1)
		t1 := AfterFunc(150*time.Millisecond, func() { c1 <- true })
		c2 := After(200 * time.Millisecond)
		if !t0.Stop() {
			t.Fatalf("sandbox mode %t: failed to stop event 0", ttype)
		}
		if !t1.Stop() {
			t.Fatalf("sandbox mode %t: failed to stop event 1", ttype)
		}
		if ttype {
			go Advance(t, 250*time.Millisecond)
		}
		<-c2
		select {
		case <-t0.Ch():
			t.Fatalf("sandbox mode %t: event 0 was not stopped", ttype)
		case <-c1:
			t.Fatalf("sandbox mode %t: event 1 was not stopped", ttype)
		default:
		}
		if t1.Stop() {
			t.Fatalf("sandbox mode %t: Stop returned true twice", ttype)
		}
	}
}

type afterResult struct {
	slot int
	t    time.Time
}

func await(slot int, result chan afterResult, ac <-chan time.Time) {
	result <- afterResult{slot, <-ac}
}
func TestAfterQueuing(t *testing.T) {
	Delta := 100 * time.Millisecond
	if testing.Short() {
		Delta = 20 * time.Millisecond
	}
	for _, ttype := range []bool{false, true} {
		setSandboxMode(ttype)

		var slots = []int{5, 3, 6, 6, 6, 1, 1, 2, 7, 9, 4, 8, 0}
		result := make(chan afterResult, len(slots))

		t0 := Now()
		for _, slot := range slots {
			go await(slot, result, After(time.Duration(slot)*Delta))
		}

		if ttype {
			go Advance(t, time.Minute)
		}

		sort.Ints(slots)
		for _, slot := range slots {
			r := <-result
			if r.slot != slot {
				t.Fatalf("after slot: want %d, got %d", slot, r.slot)
			}
			dt := r.t.Sub(t0)
			target := time.Duration(slot) * Delta
			if dt < target-Delta/2 || dt > target+Delta*10 {
				t.Fatalf("After(%s) arrived at: want [%s,%s], got %s", target, target-Delta/2, target+Delta*10, dt)
			}
		}
	}
}

func TestReset(t *testing.T) {
	const delay = 100 * time.Millisecond
	for _, ttype := range []bool{false, true} {
		setSandboxMode(ttype)
		t0 := NewTimer(2 * delay)
		c := After(delay)
		if ttype {
			go Advance(t, delay)
		}
		<-c
		if t0.Reset(3*delay) != true {
			t.Fatalf("resetting unfired timer returned false")
		}
		c = After(2 * delay)
		if ttype {
			go Advance(t, 2*delay)
		}
		select {
		case <-c:
			// OK
		case <-t0.Ch():
			t.Fatalf("time fired early")
		}
		c = After(2 * delay)
		if ttype {
			go Advance(t, 2*delay)
		}
		select {
		case <-c:
			t.Fatalf("time did not fire")
		case <-t0.Ch():
			// OK
		}
		if t0.Reset(50*time.Millisecond) != false {
			t.Fatalf("resetting expired timer returned true")
		}
	}
}

func TestOverflowSleep(t *testing.T) {
	const timeout = 25 * time.Millisecond
	const big = time.Duration(int64(1<<63 - 1))
	for _, ttype := range []bool{false, true} {
		setSandboxMode(ttype)
		c1 := After(big)
		c2 := After(timeout)
		if ttype {
			go Advance(t, timeout)
		}
		select {
		case <-c1:
			t.Fatalf("big timeout fired")
		case <-c2:
			// OK
		}
		const neg = time.Duration(-1 << 63)
		select {
		case <-After(neg):
			// OK
		case <-After(timeout):
			t.Fatalf("negative timeout didn't fire")
		}
	}
}

func TestTicker(t *testing.T) {
	for _, ttype := range []bool{false, true} {
		setSandboxMode(ttype)
		const Count = 10
		Delta := 100 * time.Millisecond
		ticker := NewTicker(Delta)
		if ttype {
			go Advance(t, Count*Delta)
		}
		t0 := Now()
		for i := 0; i < Count; i++ {
			<-ticker.Ch()
		}
		ticker.Stop()
		t1 := Now()
		dt := t1.Sub(t0)
		target := Delta * Count
		slop := target * 2 / 10
		if dt < target-slop || (!testing.Short() && dt > target+slop) {
			t.Fatalf("%d %s ticks: want [%s,%s], got %s", Count, Delta, target-slop, target+slop, dt)
		}
		// Now test that the ticker stopped
		c := After(2 * Delta)
		if ttype {
			go Advance(t, 2*Delta)
		}
		select {
		case <-c:
			// ok
		case <-ticker.Ch():
			t.Fatal("Ticker did not shut down")
		}
	}
}

func TestTeardown(t *testing.T) {
	for _, ttype := range []bool{false, true} {
		setSandboxMode(ttype)
		Delta := 100 * time.Millisecond
		if testing.Short() {
			Delta = 20 * time.Millisecond
		}
		for i := 0; i < 3; i++ {
			ticker := NewTicker(Delta)
			if ttype {
				go Advance(t, Delta)
			}
			<-ticker.Ch()
			ticker.Stop()
		}
	}
}

var t time.Time
var u int64

func BenchmarkNowTime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		t = time.Now()
	}
}

func BenchmarkNowUnixNanoTime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		u = time.Now().UnixNano()
	}
}

func BenchmarkNowHourglassSys(b *testing.B) {
	setSandboxMode(false)
	for i := 0; i < b.N; i++ {
		t = Now()
	}
}

func BenchmarkNowUnixNanoHourglassSys(b *testing.B) {
	setSandboxMode(false)
	for i := 0; i < b.N; i++ {
		u = Now().UnixNano()
	}
}
