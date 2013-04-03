package sync2

// What's in a name? Channels have all you need to emulate a counting
// semaphore with a boatload of extra functionality. However, in some
// cases, you just want a familiar API.
//
// Additionally there is great debate on how to do this correctly:
//
// https://groups.google.com/forum/#!msg/golang-dev/ShqsqvCzkWg/Kg30VPN4QmUJ
// https://groups.google.com/forum/?fromgroups=#!topic/golang-nuts/Ug1DhZGGqTk
//
// http://golang.org/doc/effective_go.html
//
// It is looking like there might be a bug in the Effective Go
// example.  So it is implemented as the discussions suggest to
// protect against scheduler optimizations.

import ()

type Semaphore interface {
	Acquire()
	Release()
}

type semaphore struct {
	slots chan struct{}
}

func NewSemaphore(max int) Semaphore {
	sem := &semaphore{slots: make(chan struct{}, max)}
	for i := 0; i < max; i++ {
		sem.slots <- struct{}{}
	}
	return sem
}

func (sem *semaphore) Acquire() {
	<-sem.slots
}

func (sem *semaphore) Release() {
	sem.slots <- struct{}{}
}
