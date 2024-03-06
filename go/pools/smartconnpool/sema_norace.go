//go:build !race

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

import _ "unsafe"

//go:linkname sync_runtime_Semacquire sync.runtime_Semacquire
func sync_runtime_Semacquire(addr *uint32)

//go:linkname sync_runtime_Semrelease sync.runtime_Semrelease
func sync_runtime_Semrelease(addr *uint32, handoff bool, skipframes int)

// semaphore is a single-use synchronization primitive that allows a Goroutine
// to wait until signaled. We use the Go runtime's internal implementation.
type semaphore struct {
	f uint32
}

func (s *semaphore) wait() {
	sync_runtime_Semacquire(&s.f)
}
func (s *semaphore) notify(handoff bool) {
	sync_runtime_Semrelease(&s.f, handoff, 0)
}
