//go:build !race

package smartconnpool

import _ "unsafe"

//go:linkname sync_runtime_Semacquire sync.runtime_Semacquire
func sync_runtime_Semacquire(addr *uint32)

//go:linkname sync_runtime_Semrelease sync.runtime_Semrelease
func sync_runtime_Semrelease(addr *uint32, handoff bool, skipframes int)

type semaphore struct {
	f uint32
}

func (s *semaphore) init() {
	s.f = 0
}

func (s *semaphore) wait() {
	sync_runtime_Semacquire(&s.f)
}
func (s *semaphore) notify(handoff bool) {
	sync_runtime_Semrelease(&s.f, handoff, 0)
}
