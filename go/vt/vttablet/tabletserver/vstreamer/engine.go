/*
Copyright 2018 The Vitess Authors.

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

package vstreamer

import (
	"context"
	"errors"
	"sync"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

var vschemaErrors *stats.Counter

// Engine is the engine for handling vseplication streaming requests.
type Engine struct {
	// mu protects isOpen, streamers, streamIdx and kschema.
	mu sync.Mutex

	isOpen bool
	// wg is incremented for every Stream, and decremented on end.
	wg        sync.WaitGroup
	streamers map[int]*vstreamer
	streamIdx int

	// watcherOnce is used for initializing kschema
	// and setting up the vschema watch. It's guaranteed that
	// no stream will start until kschema is initialized by
	// the first call through watcherOnce.
	watcherOnce sync.Once
	kschema     *vindexes.KeyspaceSchema

	ts       srvtopo.Server
	keyspace string
	cell     string
}

// NewEngine creates a new Engine.
func NewEngine(ts srvtopo.Server) *Engine {
	vschemaErrors = stats.NewCounter("VSchemaErrors", "Count of VSchema errors")
	return &Engine{
		streamers: make(map[int]*vstreamer),
		kschema:   &vindexes.KeyspaceSchema{},
		ts:        ts,
	}
}

// Open starts the Engine service.
func (vse *Engine) Open(keyspace, cell string) error {
	vse.mu.Lock()
	defer vse.mu.Unlock()
	if vse.isOpen {
		return nil
	}
	vse.isOpen = true
	vse.keyspace = keyspace
	vse.cell = cell
	return nil
}

// Close closes the Engine service.
func (vse *Engine) Close() {
	func() {
		vse.mu.Lock()
		defer vse.mu.Unlock()
		if !vse.isOpen {
			return
		}
		for _, s := range vse.streamers {
			// cancel is non-blocking.
			s.Cancel()
		}
		vse.isOpen = false
	}()

	// Wait only after releasing the lock because the end of every
	// stream will use the lock to remove the entry from streamers.
	vse.wg.Wait()
}

// Stream starts a new stream.
func (vse *Engine) Stream(ctx context.Context, cp *mysql.ConnParams, startPos mysql.Position, filter *binlogdatapb.Filter, f func()) error {
	// Ensure kschema is initialized and the watcher is started.
	vse.watcherOnce.Do(vse.setWatch)

	// Create stream and add it to the map.
	streamer, idx, err := func() (*vstreamer, int, error) {
		vse.mu.Lock()
		defer vse.mu.Unlock()
		if !vse.isOpen {
			return nil, 0, errors.New("VStreamer is not open")
		}
		streamer, err := newVStreamer(cp, startPos, filter, f)
		if err != nil {
			return nil, 0, err
		}
		idx := vse.streamIdx
		vse.streamIdx++
		vse.streamers[idx] = streamer
		// Now that we've added the stream, increment wg.
		// This must be done before releasing the lock.
		vse.wg.Add(1)
		return streamer, idx, nil
	}()
	if err != nil {
		return err
	}

	// Remove stream from map and decrement wg when it ends.
	defer func() {
		vse.mu.Lock()
		defer vse.mu.Unlock()
		delete(vse.streamers, idx)
		vse.wg.Done()
	}()

	// No lock is held while streaming, but wg is incremented.
	return streamer.Stream(ctx)
}

func (vse *Engine) setWatch() {
	// WatchSrvVSchema does not return until the inner func has been called at least once.
	vse.ts.WatchSrvVSchema(context.TODO(), vse.cell, func(v *vschemapb.SrvVSchema, err error) {
		var kschema *vindexes.KeyspaceSchema
		switch {
		case err == nil:
			kschema, err = vindexes.BuildKeyspaceSchema(v.Keyspaces[vse.keyspace], vse.keyspace)
			if err != nil {
				log.Errorf("Error building vschema %s: %v", vse.keyspace, err)
				vschemaErrors.Add(1)
				return
			}
		case topo.IsErrType(err, topo.NoNode):
			kschema = &vindexes.KeyspaceSchema{}
		default:
			log.Errorf("Error fetching vschema %s: %v", vse.keyspace, err)
			vschemaErrors.Add(1)
			return
		}

		// Broadcast the change to all streamers.
		vse.mu.Lock()
		defer vse.mu.Unlock()
		vse.kschema = kschema
		for _, s := range vse.streamers {
			s.SetKSchema(kschema)
		}
	})
}
