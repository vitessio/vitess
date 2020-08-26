/*
Copyright 2019 The Vitess Authors.

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
