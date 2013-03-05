package sync2

// What's in a name? Channels have all you need to emulate a counting
// semaphore with a boatload of extra functionality. However, in some
// cases, you just want a familiar API.

import ()

type Semaphore interface {
	Acquire()
	Release()
}

type semaphore struct {
	slots chan bool
}

func NewSemaphore(max int) Semaphore {
	return &semaphore{slots: make(chan bool, max)}
}

func (sem *semaphore) Acquire() {
	sem.slots <- true
}

func (sem *semaphore) Release() {
	<-sem.slots
}
