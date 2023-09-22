package smartconnpool

import (
	"runtime"
	"sync/atomic"
)

type Stack[C Connection] struct {
	top atomic.Pointer[Pooled[C]]
}

func (s *Stack[C]) Push(item *Pooled[C]) {
	for {
		oldHead := s.top.Load()
		item.next.Store(oldHead)
		if s.top.CompareAndSwap(oldHead, item) {
			return
		}
		runtime.Gosched()
	}
}

func (s *Stack[C]) Pop() (*Pooled[C], bool) {
	var oldHead *Pooled[C]
	var newHead *Pooled[C]

	for {
		oldHead = s.top.Load()
		if oldHead == nil {
			return nil, false
		}

		newHead = oldHead.next.Load()
		if s.top.CompareAndSwap(oldHead, newHead) {
			return oldHead, true
		}
		runtime.Gosched()
	}
}

func (s *Stack[C]) PopAll(out []*Pooled[C]) []*Pooled[C] {
	var oldHead *Pooled[C]

	for {
		oldHead = s.top.Load()
		if oldHead == nil {
			return out
		}
		if s.top.CompareAndSwap(oldHead, nil) {
			break
		}
		runtime.Gosched()
	}

	for oldHead != nil {
		out = append(out, oldHead)
		oldHead = oldHead.next.Load()
	}
	return out
}
