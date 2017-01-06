// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/dbconfigs"
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

	qe       *QueryEngine
	connpool *ConnPool

	mu       sync.Mutex
	managers map[string]*MessageManager
}

// NewMessagerEngine creates a new MessagerEngine.
func NewMessagerEngine(qe *QueryEngine, connpool *ConnPool) *MessagerEngine {
	return &MessagerEngine{
		qe:       qe,
		connpool: connpool,
		managers: make(map[string]*MessageManager),
	}
}

// Open starts the MessagerEngine service.
func (me *MessagerEngine) Open(dbconfigs dbconfigs.DBConfigs) error {
	if me.isOpen {
		return nil
	}
	me.connpool.Open(&dbconfigs.App, &dbconfigs.Dba)
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
	me.qe.schemaInfo.UnregisterNotifier("messages")
	for _, mm := range me.managers {
		mm.Close()
	}
	me.connpool.Close()
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
		// TODO(sougou): hardcoded values.
		mm := NewMessageManager(name, 10000, 30*time.Second, me.connpool)
		me.managers[name] = mm
		mm.Open()
	}
}
