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

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// MessagerEngine is the engine for handling messages.
type MessagerEngine struct {
	mu       sync.Mutex
	isOpen   bool
	managers map[string]*MessageManager

	// TODO(sougou): This depndency should be cleaned up.
	tsv      *TabletServer
	connpool *ConnPool
}

// NewMessagerEngine creates a new MessagerEngine.
func NewMessagerEngine(tsv *TabletServer, config Config) *MessagerEngine {
	return &MessagerEngine{
		tsv: tsv,
		connpool: NewConnPool(
			config.PoolNamePrefix+"MessagerPool",
			config.MessagePoolSize,
			time.Duration(config.IdleTimeout*1e9),
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
	me.isOpen = true
	return nil
}

// Close closes the MessagerEngine service.
func (me *MessagerEngine) Close() {
	me.mu.Lock()
	defer me.mu.Unlock()
	if !me.isOpen {
		return
	}
	me.isOpen = false
	me.tsv.qe.schemaInfo.UnregisterNotifier("messages")
	for _, mm := range me.managers {
		mm.Close()
	}
	me.managers = make(map[string]*MessageManager)
	me.connpool.Close()
}

// Subscribe subscribes to messages from the requested table.
func (me *MessagerEngine) Subscribe(name string, rcv *messageReceiver) error {
	me.mu.Lock()
	defer me.mu.Unlock()
	mm := me.managers[name]
	if mm == nil {
		return NewTabletError(vtrpcpb.ErrorCode_BAD_INPUT, "message table %s not found", name)
	}
	mm.Subscribe(rcv)
	return nil
}

// LockDB obtains db locks for all messages that need to
// be updated and returns the counterpart unlock function.
func (me *MessagerEngine) LockDB(newMessages map[string][]*MessageRow, changedMessages map[string][]string) func() {
	combined := make(map[string]struct{})
	for name := range newMessages {
		combined[name] = struct{}{}
	}
	for name := range changedMessages {
		combined[name] = struct{}{}
	}
	var mms []*MessageManager
	// Don't do DBLock while holding lock on mu.
	// It causes deadlocks.
	func() {
		me.mu.Lock()
		defer me.mu.Unlock()
		for name := range combined {
			if mm := me.managers[name]; mm != nil {
				mms = append(mms, mm)
			}
		}
	}()
	for _, mm := range mms {
		mm.DBLock.Lock()
	}
	return func() {
		for _, mm := range mms {
			mm.DBLock.Unlock()
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
		mm.cache.Discard(ids)
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

// GeneratePostponeQuery returns the query and bind vars for postponing a message.
func (me *MessagerEngine) GeneratePostponeQuery(name string, ids []string) (string, map[string]interface{}, error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	mm := me.managers[name]
	if mm == nil {
		return "", nil, fmt.Errorf("message table %s not found in schema", name)
	}
	query, bv := mm.GeneratePostponeQuery(ids)
	return query, bv, nil
}

// GeneratePurgeQuery returns the query and bind vars for purging messages.
func (me *MessagerEngine) GeneratePurgeQuery(name string, timeCutoff int64) (string, map[string]interface{}, error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	mm := me.managers[name]
	if mm == nil {
		return "", nil, fmt.Errorf("message table %s not found in schema", name)
	}
	query, bv := mm.GeneratePurgeQuery(timeCutoff)
	return query, bv, nil
}

func (me *MessagerEngine) schemaChanged(tables map[string]*TableInfo) {
	me.mu.Lock()
	defer me.mu.Unlock()
	for name, t := range tables {
		if t.Type != schema.Message {
			continue
		}
		if me.managers[name] != nil {
			// TODO(sougou): Need to update values instead.
			continue
		}
		mm := NewMessageManager(me.tsv, t, me.connpool)
		me.managers[name] = mm
		mm.Open()
	}
}
