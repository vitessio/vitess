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

	// TODO(sougou): This depndency should be cleaned up.
	tsv      *TabletServer
	connpool *ConnPool

	mu       sync.Mutex
	managers map[string]*MessageManager
}

// NewMessagerEngine creates a new MessagerEngine.
func NewMessagerEngine(tsv *TabletServer, config Config, queryServiceStats *QueryServiceStats) *MessagerEngine {
	return &MessagerEngine{
		tsv: tsv,
		connpool: NewConnPool(
			config.PoolNamePrefix+"MessagerPool",
			// TODO(sougou): hardcoded value.
			10,
			time.Duration(config.IdleTimeout*1e9),
			config.EnablePublishStats,
			queryServiceStats,
			tsv,
		),
		managers: make(map[string]*MessageManager),
	}
}

// Open starts the MessagerEngine service.
func (me *MessagerEngine) Open(dbconfigs dbconfigs.DBConfigs) error {
	if me.isOpen {
		return nil
	}
	me.connpool.Open(&dbconfigs.App, &dbconfigs.Dba)
	me.tsv.qe.schemaInfo.RegisterNotifier("messages", me.schemaChanged)
	me.schemaChanged(me.tsv.qe.schemaInfo.GetSchema())
	me.isOpen = true
	return nil
}

// Close closes the MessagerEngine service.
func (me *MessagerEngine) Close() {
	if !me.isOpen {
		return
	}
	me.isOpen = false
	me.tsv.qe.schemaInfo.UnregisterNotifier("messages")
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

// LockDB obtains db locks for all messages that need to
// be updated and returns the counterpart unlock function.
func (me *MessagerEngine) LockDB(newMessages map[string][]*MessageRow, changedMessages map[string][]string) func() {
	me.mu.Lock()
	defer me.mu.Unlock()
	combined := make(map[string]struct{})
	for name := range newMessages {
		combined[name] = struct{}{}
	}
	for name := range changedMessages {
		combined[name] = struct{}{}
	}
	for name := range combined {
		if mm := me.managers[name]; mm != nil {
			mm.DBLock.Lock()
		}
	}
	return func() {
		me.mu.Lock()
		defer me.mu.Unlock()
		for name := range combined {
			if mm := me.managers[name]; mm != nil {
				mm.DBLock.Unlock()
			}
		}
	}
}

// UpdateCaches updates the caches for the committed changes.
func (me *MessagerEngine) UpdateCaches(newMessages map[string][]*MessageRow, changedMessages map[string][]string) {
	me.mu.Lock()
	defer me.mu.Unlock()
	now := time.Now().UnixNano()
	for name, mrs := range newMessages {
		mm := me.managers[name]
		if mm == nil {
			continue
		}
		for _, mr := range mrs {
			if mr.TimeNext > now {
				// We don't handle future messages yet.
				continue
			}
			mm.Add(mr)
		}
	}
	for name, ids := range changedMessages {
		mm := me.managers[name]
		if mm == nil {
			continue
		}
		for _, id := range ids {
			mm.cache.Discard(id)
		}
	}
}

// GenerateAckQuery returns the query and bind vars for acking a message.
func (me *MessagerEngine) GenerateAckQuery(name string, ids []string) (string, map[string]interface{}, error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	mm := me.managers[name]
	if mm == nil {
		return "", nil, fmt.Errorf("message table %s not found in schema", name)
	}
	query, bv := mm.GenerateAckQuery(ids)
	return query, bv, nil
}

// GenerateRescheduleQuery returns the query and bind vars for rescheduling a message.
func (me *MessagerEngine) GenerateRescheduleQuery(name string, ids []string, timeNew int64) (string, map[string]interface{}, error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	mm := me.managers[name]
	if mm == nil {
		return "", nil, fmt.Errorf("message table %s not found in schema", name)
	}
	query, bv := mm.GenerateRescheduleQuery(ids, timeNew)
	return query, bv, nil
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
		mm := NewMessageManager(me.tsv, name, 30*time.Second, 10000, 30*time.Second, me.connpool)
		me.managers[name] = mm
		mm.Open()
	}
}
