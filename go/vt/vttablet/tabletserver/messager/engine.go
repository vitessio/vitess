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
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/semaphore"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TabletService defines the functions of TabletServer
// that the messager needs for callback.
type TabletService interface {
	tabletenv.Env
	PostponeMessages(ctx context.Context, target *querypb.Target, querygen QueryGenerator, ids []string) (count int64, err error)
	PurgeMessages(ctx context.Context, target *querypb.Target, querygen QueryGenerator, timeCutoff int64) (count int64, err error)
}

// VStreamer defines  the functions of VStreamer
// that the messager needs.
type VStreamer interface {
	Stream(ctx context.Context, startPos string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter,
		throttlerApp throttlerapp.Name, send func([]*binlogdatapb.VEvent) error, options *binlogdatapb.VStreamOptions) error
	StreamResults(ctx context.Context, query string, send func(*binlogdatapb.VStreamResultsResponse) error) error
}

// Engine is the engine for handling messages.
type Engine struct {
	// mu is a mutex used to protect the isOpen variable
	// and for ensuring we don't call setup functions in parallel.
	mu     sync.Mutex
	isOpen bool

	// managersMu is a mutex used to protect the managers field.
	// We require two separate mutexes, so that we don't have to acquire the same mutex
	// in Close and schemaChanged which can lead to a deadlock described in https://github.com/vitessio/vitess/issues/17229.
	managersMu sync.Mutex
	managers   map[string]*messageManager

	tsv          TabletService
	se           *schema.Engine
	vs           VStreamer
	postponeSema *semaphore.Weighted
}

// NewEngine creates a new Engine.
func NewEngine(tsv TabletService, se *schema.Engine, vs VStreamer) *Engine {
	return &Engine{
		tsv:          tsv,
		se:           se,
		vs:           vs,
		postponeSema: semaphore.NewWeighted(int64(tsv.Config().MessagePostponeParallelism)),
		managers:     make(map[string]*messageManager),
	}
}

// Open starts the Engine service.
func (me *Engine) Open() {
	me.mu.Lock()
	defer me.mu.Unlock()
	if me.isOpen {
		return
	}
	me.isOpen = true
	log.InfoS("Messager: opening")
	me.se.RegisterNotifier("messages", me.schemaChanged, true)
}

// Close closes the Engine service.
func (me *Engine) Close() {
	log.InfoS("messager Engine - started execution of Close. Acquiring mu lock")
	me.mu.Lock()
	log.InfoS("messager Engine - acquired mu lock")
	defer me.mu.Unlock()
	if !me.isOpen {
		log.InfoS("messager Engine is not open")
		return
	}
	me.isOpen = false
	log.InfoS("messager Engine - unregistering notifiers")
	me.se.UnregisterNotifier("messages")
	log.InfoS("messager Engine - closing all managers")
	me.managersMu.Lock()
	defer me.managersMu.Unlock()
	for _, mm := range me.managers {
		mm.Close()
	}
	me.managers = make(map[string]*messageManager)
	log.InfoS("Messager: closed")
}

func (me *Engine) GetGenerator(name string) (QueryGenerator, error) {
	me.managersMu.Lock()
	defer me.managersMu.Unlock()
	mm := me.managers[name]
	if mm == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "message table %s not found in schema", name)
	}
	return mm, nil
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
		return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "messager engine is closed, probably because this is not a primary any more")
	}
	me.managersMu.Lock()
	defer me.managersMu.Unlock()
	mm := me.managers[name]
	if mm == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "message table %s not found", name)
	}
	return mm.Subscribe(ctx, send), nil
}

func (me *Engine) schemaChanged(tables map[string]*schema.Table, created, altered, dropped []*schema.Table, _ bool) {
	me.managersMu.Lock()
	defer me.managersMu.Unlock()
	for _, table := range append(dropped, altered...) {
		name := table.Name.String()
		mm := me.managers[name]
		if mm == nil {
			continue
		}
		log.InfoS(fmt.Sprintf("Stopping messager for dropped/updated table: %v", name))
		mm.Close()
		delete(me.managers, name)
	}

	for _, t := range append(created, altered...) {
		name := t.Name.String()
		if t.Type != schema.Message {
			continue
		}
		if me.managers[name] != nil {
			me.tsv.Stats().InternalErrors.Add("Messages", 1)
			log.ErrorS("Newly created table already exists in messages: " + name)
			continue
		}
		mm := newMessageManager(me.tsv, me.vs, t, me.postponeSema)
		me.managers[name] = mm
		log.InfoS(fmt.Sprintf("Starting messager for table: %v", name))
		mm.Open()
	}
}
