/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package messager

import (
	"sort"
	"sync"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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
	dbconfigs *dbconfigs.DBConfigs

	mu       sync.Mutex
	isOpen   bool
	managers map[string]*messageManager

	tsv          TabletService
	se           *schema.Engine
	conns        *connpool.Pool
	postponeSema *sync2.Semaphore
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
			0,
			tsv,
		),
		postponeSema: sync2.NewSemaphore(config.MessagePostponeCap, 0),
		managers:     make(map[string]*messageManager),
	}
}

// InitDBConfig must be called before Open.
func (me *Engine) InitDBConfig(dbcfgs *dbconfigs.DBConfigs) {
	me.dbconfigs = dbcfgs
}

// Open starts the Engine service.
func (me *Engine) Open() error {
	if me.isOpen {
		return nil
	}
	me.conns.Open(me.dbconfigs.AppWithDB(), me.dbconfigs.DbaWithDB(), me.dbconfigs.AppDebugWithDB())
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
func (me *Engine) Subscribe(ctx context.Context, name string, send func(*sqltypes.Result) error) (done <-chan struct{}, err error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	if !me.isOpen {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "messager engine is closed, probably because this is not a master any more")
	}
	mm := me.managers[name]
	if mm == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "message table %s not found", name)
	}
	return mm.Subscribe(ctx, send), nil
}

// LockDB obtains db locks for all messages that need to
// be updated and returns the counterpart unlock function.
func (me *Engine) LockDB(newMessages map[string][]*MessageRow, changedMessages map[string][]string) func() {
	// Build the set of affected messages tables.
	combined := make(map[string]struct{})
	for name := range newMessages {
		combined[name] = struct{}{}
	}
	for name := range changedMessages {
		combined[name] = struct{}{}
	}

	// Build the list of manager objects (one per table).
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
	if len(mms) > 1 {
		// Always use the same order in which manager objects are locked to avoid deadlocks.
		// The previous order in "mms" is not guaranteed for multiple reasons:
		// - We use a Go map above which does not guarantee an iteration order.
		// - Transactions may not always use the same order when writing to multiple
		//   messages tables.
		sort.Slice(mms, func(i, j int) bool { return mms[i].name.String() < mms[j].name.String() })
	}

	// Lock each manager/messages table.
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
		MessageStats.Add([]string{name, "Queued"}, int64(len(mrs)))
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

// GenerateLoadMessagesQuery returns the ParsedQuery for loading messages by pk.
// The results of the query can be used in a BuildMessageRow call.
func (me *Engine) GenerateLoadMessagesQuery(name string) (*sqlparser.ParsedQuery, error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	mm := me.managers[name]
	if mm == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "message table %s not found in schema", name)
	}
	return mm.loadMessagesQuery, nil
}

// GenerateAckQuery returns the query and bind vars for acking a message.
func (me *Engine) GenerateAckQuery(name string, ids []string) (string, map[string]*querypb.BindVariable, error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	mm := me.managers[name]
	if mm == nil {
		return "", nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "message table %s not found in schema", name)
	}
	query, bv := mm.GenerateAckQuery(ids)
	return query, bv, nil
}

// GeneratePostponeQuery returns the query and bind vars for postponing a message.
func (me *Engine) GeneratePostponeQuery(name string, ids []string) (string, map[string]*querypb.BindVariable, error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	mm := me.managers[name]
	if mm == nil {
		return "", nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "message table %s not found in schema", name)
	}
	query, bv := mm.GeneratePostponeQuery(ids)
	return query, bv, nil
}

// GeneratePurgeQuery returns the query and bind vars for purging messages.
func (me *Engine) GeneratePurgeQuery(name string, timeCutoff int64) (string, map[string]*querypb.BindVariable, error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	mm := me.managers[name]
	if mm == nil {
		return "", nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "message table %s not found in schema", name)
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
			tabletenv.InternalErrors.Add("Messages", 1)
			log.Errorf("Newly created table alread exists in messages: %s", name)
			continue
		}
		mm := newMessageManager(me.tsv, t, me.conns, me.postponeSema)
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
