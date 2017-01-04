// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import "sync"

// MessageManager manages messages for a message table.
type MessageManager struct {
	isOpen bool
	wg     sync.WaitGroup
	name   string
	cache  *MessagerCache

	mu          sync.Mutex
	cond        sync.Cond
	receivers   []MessageReceiver
	curReceiver int
}

// NewMessageManager creates a new message manager.
func NewMessageManager(name string) *MessageManager {
	mm := &MessageManager{
		name:  name,
		cache: NewMessagerCache(10000),
	}
	mm.cond.L = &mm.mu
	return mm
}

// Open starts the MessageManager service.
func (mm *MessageManager) Open() {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	if mm.isOpen {
		return
	}
	mm.isOpen = true
	mm.wg.Add(1)
	go mm.runSend()
}

// Close stops the MessageManager service.
func (mm *MessageManager) Close() {
	func() {
		mm.mu.Lock()
		defer mm.mu.Unlock()
		if !mm.isOpen {
			return
		}
		for _, rcvr := range mm.receivers {
			rcvr.Cancel()
		}
		mm.receivers = nil
		mm.cache.Close()
		mm.isOpen = false
		mm.cond.Broadcast()
	}()
	mm.wg.Wait()
}

// Subscribe adds the receiver to the list of subsribers.
func (mm *MessageManager) Subscribe(receiver MessageReceiver) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	for _, rcv := range mm.receivers {
		if rcv == receiver {
			return
		}
	}
	mm.receivers = append(mm.receivers, receiver)
	mm.cache.Open()
	mm.cond.Broadcast()
}

// Unsubscribe removes the receiver from the list of subscribers.
func (mm *MessageManager) Unsubscribe(receiver MessageReceiver) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	for i, rcv := range mm.receivers {
		if rcv != receiver {
			continue
		}
		// Delete the item at current position.
		n := len(mm.receivers)
		copy(mm.receivers[i:n-1], mm.receivers[i+1:n])
		mm.receivers = mm.receivers[0 : n-1]
		break
	}
	if len(mm.receivers) == 0 {
		mm.cache.Close()
	}
}

func (mm *MessageManager) runSend() {
	defer mm.wg.Done()
	var receiver MessageReceiver
	for {
		mr := mm.cache.Pop()
		mm.mu.Lock()
		for len(mm.receivers) == 0 {
			if !mm.isOpen {
				mm.mu.Unlock()
				return
			}
			// If we're here, cache is closed.
			// Discard mr.
			mr = nil
			mm.cond.Wait()
		}
		mm.curReceiver = (mm.curReceiver + 1) % len(mm.receivers)
		receiver = mm.receivers[mm.curReceiver]
		mm.mu.Unlock()
		if mr != nil {
			_ = receiver.Send(mm.name, mr)
			mm.cache.Discard(mr.id)
		}
	}
}
