// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package concurrency

import (
	"fmt"
	"sync"

	"code.google.com/p/vitess/go/sync2"
)

// ResourceConstraint combines 3 different features:
// - a WaitGroup to wait for all tasks to be done
// - a Semaphore to control concurrency
// - an ErrorRecorder
type ResourceConstraint struct {
	semaphore sync2.Semaphore
	wg        sync.WaitGroup
	FirstErrorRecorder
}

func NewResourceConstraint(max int) *ResourceConstraint {
	return &ResourceConstraint{semaphore: sync2.NewSemaphore(max)}
}

func (rc *ResourceConstraint) Add(n int) {
	rc.wg.Add(n)
}

func (rc *ResourceConstraint) Done() {
	rc.wg.Done()
}

// Wait waits for the WG and returns the firstError we encountered, or nil
func (rc *ResourceConstraint) Wait() error {
	rc.wg.Wait()
	return rc.Error()
}

// Acquire will wait until we have a resource to use
func (rc *ResourceConstraint) Acquire() {
	rc.semaphore.Acquire()
}

func (rc *ResourceConstraint) Release() {
	rc.semaphore.Release()
}

func (rc *ResourceConstraint) ReleaseAndDone() {
	rc.Release()
	rc.Done()
}

// MultiResourceConstraint combines 3 different features:
// - a WaitGroup to wait for all tasks to be done
// - a Semaphore map to control multiple concurrencies
// - an ErrorRecorder
type MultiResourceConstraint struct {
	semaphoreMap map[string]sync2.Semaphore
	wg           sync.WaitGroup
	FirstErrorRecorder
}

func NewMultiResourceConstraint(semaphoreMap map[string]sync2.Semaphore) *MultiResourceConstraint {
	return &MultiResourceConstraint{semaphoreMap: semaphoreMap}
}

func (mrc *MultiResourceConstraint) Add(n int) {
	mrc.wg.Add(n)
}

func (mrc *MultiResourceConstraint) Done() {
	mrc.wg.Done()
}

// Returns the firstError we encountered, or nil
func (mrc *MultiResourceConstraint) Wait() error {
	mrc.wg.Wait()
	return mrc.Error()
}

// Acquire will wait until we have a resource to use
func (mrc *MultiResourceConstraint) Acquire(name string) {
	s, ok := mrc.semaphoreMap[name]
	if !ok {
		panic(fmt.Errorf("MultiResourceConstraint: No resource named %v in semaphore map", name))
	}
	s.Acquire()
}

func (mrc *MultiResourceConstraint) Release(name string) {
	s, ok := mrc.semaphoreMap[name]
	if !ok {
		panic(fmt.Errorf("MultiResourceConstraint: No resource named %v in semaphore map", name))
	}
	s.Release()
}

func (mrc *MultiResourceConstraint) ReleaseAndDone(name string) {
	mrc.Release(name)
	mrc.Done()
}
