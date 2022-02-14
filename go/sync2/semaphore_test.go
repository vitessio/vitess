package sync2

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSemaNoTimeout(t *testing.T) {
	s := NewSemaphore(1, 0)
	s.Acquire()
	released := false
	go func() {
		released = true
		s.Release()
	}()
	s.Acquire()
	assert.True(t, released)
}

func TestSemaTimeout(t *testing.T) {
	s := NewSemaphore(1, 1*time.Millisecond)
	s.Acquire()
	release := make(chan struct{})
	released := make(chan struct{})
	go func() {
		<-release
		s.Release()
		released <- struct{}{}
	}()
	assert.False(t, s.Acquire())
	release <- struct{}{}
	<-released
	assert.True(t, s.Acquire())
}

func TestSemaAcquireContext(t *testing.T) {
	s := NewSemaphore(1, 0)
	s.Acquire()
	release := make(chan struct{})
	released := make(chan struct{})
	go func() {
		<-release
		s.Release()
		released <- struct{}{}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, s.AcquireContext(ctx))
	release <- struct{}{}
	<-released
	assert.True(t, s.AcquireContext(context.Background()))
}

func TestSemaTryAcquire(t *testing.T) {
	s := NewSemaphore(1, 0)
	assert.True(t, s.TryAcquire())
	assert.False(t, s.TryAcquire())
	s.Release()
	assert.True(t, s.TryAcquire())
}
