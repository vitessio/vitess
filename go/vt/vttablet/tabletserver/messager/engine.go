/*
Copyright 2019 The Vitess Authors.

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
	"sync"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TabletService defines the functions of TabletServer
// that the messager needs for callback.
type TabletService interface {
	tabletenv.Env
	PostponeMessages(ctx context.Context, target *querypb.Target, name string, ids []string) (count int64, err error)
	PurgeMessages(ctx context.Context, target *querypb.Target, name string, timeCutoff int64) (count int64, err error)
}

// VStreamer defines  the functions of VStreamer
// that the messager needs.
type VStreamer interface {
	Stream(ctx context.Context, startPos string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error
	StreamResults(ctx context.Context, query string, send func(*binlogdatapb.VStreamResultsResponse) error) error
}

// Engine is the engine for handling messages.
type Engine struct {
	mu       sync.Mutex
	isOpen   bool
	managers map[string]*messageManager

	tsv          TabletService
	se           *schema.Engine
	vs           VStreamer
	postponeSema *sync2.Semaphore
}

// NewEngine creates a new Engine.
func NewEngine(tsv TabletService, se *schema.Engine, vs VStreamer) *Engine {
	return &Engine{
		tsv:          tsv,
		se:           se,
		vs:           vs,
		postponeSema: sync2.NewSemaphore(tsv.Config().MessagePostponeParallelism, 0),
		managers:     make(map[string]*messageManager),
	}
}

// Open starts the Engine service.
func (me *Engine) Open() {
	me.mu.Lock()
	if me.isOpen {
		me.mu.Unlock()
		return
	}
	me.mu.Unlock()
	log.Info("Messager: opening")
	// Unlock before invoking RegisterNotifier because it
	// obtains the same lock.
	me.se.RegisterNotifier("messages", me.schemaChanged)
	me.isOpen = true
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
	log.Info("Messager: closed")
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
	for _, name := range append(dropped, altered...) {
		mm := me.managers[name]
		if mm == nil {
			continue
		}
		log.Infof("Stopping messager for dropped/updated table: %v", name)
		mm.Close()
		delete(me.managers, name)
	}

	for _, name := range append(created, altered...) {
		t := tables[name]
		if t.Type != schema.Message {
			continue
		}
		if me.managers[name] != nil {
			me.tsv.Stats().InternalErrors.Add("Messages", 1)
			log.Errorf("Newly created table already exists in messages: %s", name)
			continue
		}
		mm := newMessageManager(me.tsv, me.vs, t, me.postponeSema)
		me.managers[name] = mm
		log.Infof("Starting messager for table: %v", name)
		mm.Open()
	}
}
