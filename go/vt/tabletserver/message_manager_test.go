// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"testing"
	"time"
)

type testReceiver struct {
	name     string
	canceled bool
	ch       chan *MessageRow
}

func newTestReceiver() *testReceiver {
	return &testReceiver{ch: make(chan *MessageRow, 1)}
}

func (tr *testReceiver) Send(name string, mr *MessageRow) error {
	tr.name = name
	tr.ch <- mr
	return nil
}

func (tr *testReceiver) Cancel() {
	tr.canceled = true
}

func TestMessageManagerState(t *testing.T) {
	mm := NewMessageManager("foo")
	// Do it twice
	for i := 0; i < 2; i++ {
		mm.Open()
		// Idempotence.
		mm.Open()
		time.Sleep(10 * time.Millisecond)
		mm.Close()
		// Idempotence.
		mm.Close()
	}

	for i := 0; i < 2; i++ {
		mm.Open()
		r1 := newTestReceiver()
		mm.Subscribe(r1)
		time.Sleep(10 * time.Millisecond)
		mm.Close()
		if !r1.canceled {
			t.Errorf("tr.canceled: %v, want true", r1.canceled)
		}
	}
}

func TestMessageManagerSend(t *testing.T) {
	mm := NewMessageManager("foo")
	mm.Open()
	defer mm.Close()
	r1 := newTestReceiver()
	mm.Subscribe(r1)
	row1 := &MessageRow{
		id: "1",
	}
	mm.cache.Add(row1)
	got := <-r1.ch
	if got != row1 {
		t.Errorf("Received: %v, want %v", got, row1)
	}
	r2 := newTestReceiver()
	mm.Subscribe(r2)
	// Subscribing twice is a no-op.
	mm.Subscribe(r1)
	// Unsubscribe of non-registered receiver should be no-op.
	mm.Unsubscribe(newTestReceiver())
	row2 := &MessageRow{
		id: "2",
	}
	mm.cache.Add(row1)
	mm.cache.Add(row2)
	// Send should be round-robin.
	<-r1.ch
	<-r2.ch
	mm.Unsubscribe(r1)
	mm.cache.Add(row1)
	mm.cache.Add(row2)
	// Only r2 should be receiving.
	<-r2.ch
	<-r2.ch
	mm.Unsubscribe(r2)
	if err := mm.cache.Add(row1); err != ErrClosed {
		t.Errorf("mm.cache.Add: %v, want %v", err, ErrClosed)
	}
}
