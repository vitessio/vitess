// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"sync"

	"github.com/youtube/vitess/go/vt/schema"
)

// MessageReceiver defines the interface for the receivers.
type MessageReceiver interface {
	Send(name string, mr *MessageRow) error
	Cancel()
}

// MessagerEngine is the engine for handling messages.
type MessagerEngine struct {
	isOpen bool
	wg     sync.WaitGroup

	qe *QueryEngine

	mu       sync.Mutex
	managers map[string]*MessageManager
}

// NewMessagerEngine creates a new MessagerEngine.
func NewMessagerEngine(qe *QueryEngine) *MessagerEngine {
	return &MessagerEngine{
		qe:       qe,
		managers: make(map[string]*MessageManager),
	}
}

// Open starts the MessagerEngine service.
func (me *MessagerEngine) Open() error {
	if me.isOpen {
		return nil
	}
	me.qe.schemaInfo.RegisterNotifier("messages", me.schemaChanged)
	me.schemaChanged(me.qe.schemaInfo.GetSchema())
	me.isOpen = true
	return nil
}

// Close closes the MessagerEngine service.
func (me *MessagerEngine) Close() {
	if !me.isOpen {
		return
	}
	me.isOpen = false
	me.wg.Wait()
	me.qe.schemaInfo.UnregisterNotifier("messages")
}

// Subscribe subscribes the receiver against the message table.
func (me *MessagerEngine) Subscribe(name string, receiver MessageReceiver) error {
	me.mu.Lock()
	defer me.mu.Unlock()
	mm := me.managers[name]
	if mm == nil {
		return fmt.Errorf("message table %s not found", name)
	}
	mm.Subscribe(receiver)
	return nil
}

// Unsubscribe unsubscribes from all message tables.
func (me *MessagerEngine) Unsubscribe(receiver MessageReceiver) {
	me.mu.Lock()
	defer me.mu.Unlock()
	for _, mm := range me.managers {
		mm.Unsubscribe(receiver)
	}
}

func (me *MessagerEngine) schemaChanged(tables map[string]*schema.Table) {
	me.mu.Lock()
	defer me.mu.Unlock()
	for name, t := range tables {
		if t.Type != schema.Message {
			continue
		}
		if me.managers[name] != nil {
			continue
		}
		mm := NewMessageManager(name)
		me.managers[name] = mm
		mm.Open()
	}
}
