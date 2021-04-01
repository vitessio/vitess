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

package vstreamer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"

	"vitess.io/vitess/go/vt/servenv"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

const (
	throttlerAppName = "vstreamer"
)

// Engine is the engine for handling vreplication streaming requests.
type Engine struct {
	env  tabletenv.Env
	ts   srvtopo.Server
	se   *schema.Engine
	cell string

	// keyspace is initialized by InitDBConfig
	keyspace string

	// wg is incremented for every Stream, and decremented on end.
	// Close waits for all current streams to end by waiting on wg.
	wg sync.WaitGroup

	mu              sync.Mutex
	isOpen          bool
	streamIdx       int
	streamers       map[int]*uvstreamer
	rowStreamers    map[int]*rowStreamer
	resultStreamers map[int]*resultStreamer

	// watcherOnce is used for initializing vschema
	// and setting up the vschema watch. It's guaranteed that
	// no stream will start until vschema is initialized by
	// the first call through watcherOnce.
	watcherOnce sync.Once
	lvschema    *localVSchema

	// stats variables
	vschemaErrors  *stats.Counter
	vschemaUpdates *stats.Counter

	// vstreamer metrics
	vstreamerPhaseTimings     *servenv.TimingsWrapper
	vstreamerEventsStreamed   *stats.Counter
	vstreamerPacketSize       *stats.GaugeFunc
	vstreamerNumPackets       *stats.Counter
	resultStreamerNumRows     *stats.Counter
	resultStreamerNumPackets  *stats.Counter
	rowStreamerNumRows        *stats.Counter
	rowStreamerNumPackets     *stats.Counter
	errorCounts               *stats.CountersWithSingleLabel
	vstreamersCreated         *stats.Counter
	vstreamersEndedWithErrors *stats.Counter

	throttlerClient *throttle.Client
}

// NewEngine creates a new Engine.
// Initialization sequence is: NewEngine->InitDBConfig->Open.
// Open and Close can be called multiple times and are idempotent.
func NewEngine(env tabletenv.Env, ts srvtopo.Server, se *schema.Engine, lagThrottler *throttle.Throttler, cell string) *Engine {
	vse := &Engine{
		env:             env,
		ts:              ts,
		se:              se,
		cell:            cell,
		throttlerClient: throttle.NewBackgroundClient(lagThrottler, throttlerAppName, throttle.ThrottleCheckSelf),

		streamers:       make(map[int]*uvstreamer),
		rowStreamers:    make(map[int]*rowStreamer),
		resultStreamers: make(map[int]*resultStreamer),

		lvschema: &localVSchema{vschema: &vindexes.VSchema{}},

		vschemaErrors:  env.Exporter().NewCounter("VSchemaErrors", "Count of VSchema errors"),
		vschemaUpdates: env.Exporter().NewCounter("VSchemaUpdates", "Count of VSchema updates. Does not include errors"),

		vstreamerPhaseTimings:     env.Exporter().NewTimings("VStreamerPhaseTiming", "Time taken for different phases during vstream copy", "phase-timing"),
		vstreamerEventsStreamed:   env.Exporter().NewCounter("VStreamerEventsStreamed", "Count of events streamed in VStream API"),
		vstreamerPacketSize:       env.Exporter().NewGaugeFunc("VStreamPacketSize", "Max packet size for sending vstreamer events", getPacketSize),
		vstreamerNumPackets:       env.Exporter().NewCounter("VStreamerNumPackets", "Number of packets in vstreamer"),
		resultStreamerNumPackets:  env.Exporter().NewCounter("ResultStreamerNumPackets", "Number of packets in result streamer"),
		resultStreamerNumRows:     env.Exporter().NewCounter("ResultStreamerNumRows", "Number of rows sent in result streamer"),
		rowStreamerNumPackets:     env.Exporter().NewCounter("RowStreamerNumPackets", "Number of packets in row streamer"),
		rowStreamerNumRows:        env.Exporter().NewCounter("RowStreamerNumRows", "Number of rows sent in row streamer"),
		vstreamersCreated:         env.Exporter().NewCounter("VStreamersCreated", "Count of vstreamers created"),
		vstreamersEndedWithErrors: env.Exporter().NewCounter("VStreamersEndedWithErrors", "Count of vstreamers that ended with errors"),
		errorCounts:               env.Exporter().NewCountersWithSingleLabel("VStreamerErrors", "Tracks errors in vstreamer", "type", "Catchup", "Copy", "Send", "TablePlan"),
	}
	env.Exporter().HandleFunc("/debug/tablet_vschema", vse.ServeHTTP)
	return vse
}

// InitDBConfig initializes the target parameters for the Engine.
func (vse *Engine) InitDBConfig(keyspace string) {
	vse.keyspace = keyspace
}

// Open starts the Engine service.
func (vse *Engine) Open() {
	vse.mu.Lock()
	defer vse.mu.Unlock()
	if vse.isOpen {
		return
	}
	log.Info("VStreamer: opening")
	vse.isOpen = true
}

// IsOpen checks if the engine is opened
func (vse *Engine) IsOpen() bool {
	vse.mu.Lock()
	defer vse.mu.Unlock()
	return vse.isOpen
}

// Close closes the Engine service.
func (vse *Engine) Close() {
	func() {
		vse.mu.Lock()
		defer vse.mu.Unlock()
		if !vse.isOpen {
			return
		}
		// cancels are non-blocking.
		for _, s := range vse.streamers {
			s.Cancel()
		}
		for _, s := range vse.rowStreamers {
			s.Cancel()
		}
		for _, s := range vse.resultStreamers {
			s.Cancel()
		}
		vse.isOpen = false
	}()

	// Wait only after releasing the lock because the end of every
	// stream will use the lock to remove the entry from streamers.
	vse.wg.Wait()
	log.Info("VStreamer: closed")
}

func (vse *Engine) vschema() *vindexes.VSchema {
	vse.mu.Lock()
	defer vse.mu.Unlock()
	return vse.lvschema.vschema
}

// Stream starts a new stream.
// This streams events from the binary logs
func (vse *Engine) Stream(ctx context.Context, startPos string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	// Ensure vschema is initialized and the watcher is started.
	// Starting of the watcher has to be delayed till the first call to Stream
	// because this overhead should be incurred only if someone uses this feature.
	vse.watcherOnce.Do(vse.setWatch)

	// Create stream and add it to the map.
	streamer, idx, err := func() (*uvstreamer, int, error) {
		vse.mu.Lock()
		defer vse.mu.Unlock()
		if !vse.isOpen {
			return nil, 0, errors.New("VStreamer is not open")
		}
		streamer := newUVStreamer(ctx, vse, vse.env.Config().DB.AppWithDB(), vse.se, startPos, tablePKs, filter, vse.lvschema, send)
		idx := vse.streamIdx
		vse.streamers[idx] = streamer
		vse.streamIdx++
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
	return streamer.Stream()
}

// StreamRows streams rows.
// This streams the table data rows (so we can copy the table data snapshot)
func (vse *Engine) StreamRows(ctx context.Context, query string, lastpk []sqltypes.Value, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	// Ensure vschema is initialized and the watcher is started.
	// Starting of the watcher has to be delayed till the first call to Stream
	// because this overhead should be incurred only if someone uses this feature.
	vse.watcherOnce.Do(vse.setWatch)
	log.Infof("Streaming rows for query %s, lastpk: %s", query, lastpk)

	// Create stream and add it to the map.
	rowStreamer, idx, err := func() (*rowStreamer, int, error) {
		vse.mu.Lock()
		defer vse.mu.Unlock()
		if !vse.isOpen {
			return nil, 0, errors.New("VStreamer is not open")
		}

		rowStreamer := newRowStreamer(ctx, vse.env.Config().DB.AppWithDB(), vse.se, query, lastpk, vse.lvschema, send, vse)
		idx := vse.streamIdx
		vse.rowStreamers[idx] = rowStreamer
		vse.streamIdx++
		// Now that we've added the stream, increment wg.
		// This must be done before releasing the lock.
		vse.wg.Add(1)
		return rowStreamer, idx, nil
	}()
	if err != nil {
		return err
	}

	// Remove stream from map and decrement wg when it ends.
	defer func() {
		vse.mu.Lock()
		defer vse.mu.Unlock()
		delete(vse.rowStreamers, idx)
		vse.wg.Done()
	}()

	// No lock is held while streaming, but wg is incremented.
	return rowStreamer.Stream()
}

// StreamResults streams results of the query with the gtid.
func (vse *Engine) StreamResults(ctx context.Context, query string, send func(*binlogdatapb.VStreamResultsResponse) error) error {
	// Create stream and add it to the map.
	resultStreamer, idx, err := func() (*resultStreamer, int, error) {
		vse.mu.Lock()
		defer vse.mu.Unlock()
		if !vse.isOpen {
			return nil, 0, errors.New("VStreamer is not open")
		}
		resultStreamer := newResultStreamer(ctx, vse.env.Config().DB.AppWithDB(), query, send, vse)
		idx := vse.streamIdx
		vse.resultStreamers[idx] = resultStreamer
		vse.streamIdx++
		// Now that we've added the stream, increment wg.
		// This must be done before releasing the lock.
		vse.wg.Add(1)
		return resultStreamer, idx, nil
	}()
	if err != nil {
		return err
	}

	// Remove stream from map and decrement wg when it ends.
	defer func() {
		vse.mu.Lock()
		defer vse.mu.Unlock()
		delete(vse.resultStreamers, idx)
		vse.wg.Done()
	}()

	// No lock is held while streaming, but wg is incremented.
	return resultStreamer.Stream()
}

// ServeHTTP shows the current VSchema.
func (vse *Engine) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	vs := vse.vschema()
	if vs == nil {
		response.Write([]byte("{}"))
	}
	b, err := json.MarshalIndent(vs, "", "  ")
	if err != nil {
		response.Write([]byte(err.Error()))
		return
	}
	buf := bytes.NewBuffer(nil)
	json.HTMLEscape(buf, b)
	response.Write(buf.Bytes())
}

func (vse *Engine) setWatch() {
	// If there's no toposerver, create an empty vschema.
	if vse.ts == nil {
		vse.lvschema = &localVSchema{
			keyspace: vse.keyspace,
			vschema:  &vindexes.VSchema{},
		}
		return
	}

	// WatchSrvVSchema does not return until the inner func has been called at least once.
	vse.ts.WatchSrvVSchema(context.TODO(), vse.cell, func(v *vschemapb.SrvVSchema, err error) {
		switch {
		case err == nil:
			// Build vschema down below.
		case topo.IsErrType(err, topo.NoNode):
			v = nil
		default:
			log.Errorf("Error fetching vschema: %v", err)
			vse.vschemaErrors.Add(1)
			return
		}
		var vschema *vindexes.VSchema
		if v != nil {
			vschema, err = vindexes.BuildVSchema(v)
			if err != nil {
				log.Errorf("Error building vschema: %v", err)
				vse.vschemaErrors.Add(1)
				return
			}
		} else {
			vschema = &vindexes.VSchema{}
		}

		// Broadcast the change to all streamers.
		vse.mu.Lock()
		defer vse.mu.Unlock()
		vse.lvschema = &localVSchema{
			keyspace: vse.keyspace,
			vschema:  vschema,
		}
		b, _ := json.MarshalIndent(vschema, "", "  ")
		log.V(2).Infof("Updated vschema: %s", b)
		for _, s := range vse.streamers {
			s.SetVSchema(vse.lvschema)
		}
		vse.vschemaUpdates.Add(1)
	})
}

func getPacketSize() int64 {
	return int64(*PacketSize)
}
