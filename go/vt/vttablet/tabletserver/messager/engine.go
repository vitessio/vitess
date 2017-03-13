// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package messager

import (
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/connpool"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"github.com/youtube/vitess/go/vt/vterrors"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// TabletService defines the functions of TabletServer
// that the messager needs for callback.
type TabletService interface {
	CheckMySQL()
	PostponeMessages(ctx context.Context, target *querypb.Target, name string, ids []string) (count int64, err error)
	PurgeMessages(ctx context.Context, target *querypb.Target, name string, timeCutoff int64) (count int64, err error)
}

// Engine is the engine for handling messages.
type Engine struct {
	mu       sync.Mutex
	isOpen   bool
	managers map[string]*messageManager

	tsv   TabletService
	se    *schema.Engine
	conns *connpool.Pool
}

// NewEngine creates a new Engine.
func NewEngine(tsv TabletService, se *schema.Engine, config tabletenv.TabletConfig) *Engine {
	return &Engine{
		tsv: tsv,
		se:  se,
		conns: connpool.New(
			config.PoolNamePrefix+"MessagerPool",
			config.MessagePoolSize,
			time.Duration(config.IdleTimeout*1e9),
			tsv,
		),
		managers: make(map[string]*messageManager),
	}
}

// Open starts the Engine service.
func (me *Engine) Open(dbconfigs dbconfigs.DBConfigs) error {
	if me.isOpen {
		return nil
	}
	me.conns.Open(&dbconfigs.App, &dbconfigs.Dba)
	me.se.RegisterNotifier("messages", me.schemaChanged)
	me.isOpen = true
	return nil
}

// Close closes the Engine service.
func (me *Engine) Close() {
	me.mu.Lock()
	defer me.mu.Unlock()
	if !me.isOpen {
		return
	}
	me.isOpen = false
	me.se.UnregisterNotifier("messages")
	for _, mm := range me.managers {
		mm.Close()
	}
	me.managers = make(map[string]*messageManager)
	me.conns.Close()
}

// Subscribe subscribes to messages from the requested table.
// The function returns a done channel that will be closed when
// the subscription ends, which can be initiated by the send function
// returning io.EOF. The engine can also end a subscription which is
// usually triggered by Close. It's the responsibility of the send
// function to promptly return if the done channel is closed. Otherwise,
// the engine's Close function will hang indefinitely.
func (me *Engine) Subscribe(name string, send func(*sqltypes.Result) error) (done chan struct{}, err error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	mm := me.managers[name]
	if mm == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "message table %s not found", name)
	}
	rcv, done := newMessageReceiver(send)
	mm.Subscribe(rcv)
	return done, nil
}

// LockDB obtains db locks for all messages that need to
// be updated and returns the counterpart unlock function.
func (me *Engine) LockDB(newMessages map[string][]*MessageRow, changedMessages map[string][]string) func() {
	combined := make(map[string]struct{})
	for name := range newMessages {
		combined[name] = struct{}{}
	}
	for name := range changedMessages {
		combined[name] = struct{}{}
	}
	var mms []*messageManager
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
func (me *Engine) UpdateCaches(newMessages map[string][]*MessageRow, changedMessages map[string][]string) {
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
func (me *Engine) GenerateAckQuery(name string, ids []string) (string, map[string]interface{}, error) {
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
func (me *Engine) GeneratePostponeQuery(name string, ids []string) (string, map[string]interface{}, error) {
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
func (me *Engine) GeneratePurgeQuery(name string, timeCutoff int64) (string, map[string]interface{}, error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	mm := me.managers[name]
	if mm == nil {
		return "", nil, fmt.Errorf("message table %s not found in schema", name)
	}
	query, bv := mm.GeneratePurgeQuery(timeCutoff)
	return query, bv, nil
}

func (me *Engine) schemaChanged(tables map[string]*schema.Table, created, altered, dropped []string) {
	me.mu.Lock()
	defer me.mu.Unlock()
	for _, name := range created {
		t := tables[name]
		if t.Type != schema.Message {
			continue
		}
		if me.managers[name] != nil {
			// TODO(sougou): Increment internal error counter.
			log.Errorf("Newly created table alread exists in messages: %s", name)
			continue
		}
		mm := newMessageManager(me.tsv, t, me.conns)
		me.managers[name] = mm
		mm.Open()
	}

	// TODO(sougou): Update altered tables.

	for _, name := range dropped {
		mm := me.managers[name]
		if mm == nil {
			continue
		}
		mm.Close()
		delete(me.managers, name)
	}
}
