package timer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sync2"
)

const (
	half    = 50 * time.Millisecond
	quarter = 25 * time.Millisecond
	tenth   = 10 * time.Millisecond
)

var numcalls sync2.AtomicInt64

func f() {
	numcalls.Add(1)
}

func TestWait(t *testing.T) {
	numcalls.Set(0)
	timer := NewTimer(quarter)
	assert.False(t, timer.Running())
	timer.Start(f)
	defer timer.Stop()
	assert.True(t, timer.Running())
	time.Sleep(tenth)
	assert.Equal(t, int64(0), numcalls.Get())
	time.Sleep(quarter)
	assert.Equal(t, int64(1), numcalls.Get())
	time.Sleep(quarter)
	assert.Equal(t, int64(2), numcalls.Get())
}

func TestReset(t *testing.T) {
	numcalls.Set(0)
	timer := NewTimer(half)
	timer.Start(f)
	defer timer.Stop()
	timer.SetInterval(quarter)
	time.Sleep(tenth)
	assert.Equal(t, int64(0), numcalls.Get())
	time.Sleep(quarter)
	assert.Equal(t, int64(1), numcalls.Get())
}

func TestIndefinite(t *testing.T) {
	numcalls.Set(0)
	timer := NewTimer(0)
	timer.Start(f)
	defer timer.Stop()
	timer.TriggerAfter(quarter)
	time.Sleep(tenth)
	assert.Equal(t, int64(0), numcalls.Get())
	time.Sleep(quarter)
	assert.Equal(t, int64(1), numcalls.Get())
}
