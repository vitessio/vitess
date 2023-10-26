//go:build race

/*
Copyright 2023 The Vitess Authors.

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

package smartconnpool

import (
	"sync/atomic"
	"time"
)

// semaphore is a slow implementation of a single-use synchronization primitive.
// We use this inefficient implementation when running under the race detector
// because the detector doesn't understand the synchronization performed by the
// runtime's semaphore.
type semaphore struct {
	b atomic.Bool
}

func (s *semaphore) wait() {
	for !s.b.CompareAndSwap(true, false) {
		time.Sleep(time.Millisecond)
	}
}

func (s *semaphore) notify(_ bool) {
	s.b.Store(true)
}
