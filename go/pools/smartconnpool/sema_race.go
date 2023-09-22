//go:build race

package smartconnpool

import (
	"sync/atomic"
	"time"
)

type semaphore struct {
	b atomic.Bool
}

func (s *semaphore) init() {
	s.b.Store(false)
}

func (s *semaphore) wait() {
	for !s.b.CompareAndSwap(true, false) {
		time.Sleep(time.Millisecond)
	}
}

func (s *semaphore) notify(_ bool) {
	s.b.Store(true)
}
