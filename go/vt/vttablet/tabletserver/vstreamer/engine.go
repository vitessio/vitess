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
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"

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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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
	shard    string

	// wg is incremented for every Stream, and decremented on end.
	// Close waits for all current streams to end by waiting on wg.
	wg sync.WaitGroup

	mu              sync.Mutex
	isOpen          int32 // 0 or 1 in place of atomic.Bool added in go 1.19
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
	rowStreamerWaits          *servenv.TimingsWrapper
	errorCounts               *stats.CountersWithSingleLabel
	vstreamersCreated         *stats.Counter
	vstreamersEndedWithErrors *stats.Counter
	vstreamerFlushedBinlogs   *stats.Counter

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
		rowStreamerWaits:          env.Exporter().NewTimings("RowStreamerWaits", "Total counts and time we've waited when streaming rows in the vstream copy phase", "copy-phase-waits"),
		vstreamersCreated:         env.Exporter().NewCounter("VStreamersCreated", "Count of vstreamers created"),
		vstreamersEndedWithErrors: env.Exporter().NewCounter("VStreamersEndedWithErrors", "Count of vstreamers that ended with errors"),
		errorCounts:               env.Exporter().NewCountersWithSingleLabel("VStreamerErrors", "Tracks errors in vstreamer", "type", "Catchup", "Copy", "Send", "TablePlan"),
		vstreamerFlushedBinlogs:   env.Exporter().NewCounter("VStreamerFlushedBinlogs", "Number of times we've successfully executed a FLUSH BINARY LOGS statement when starting a vstream"),
	}
	env.Exporter().NewGaugeFunc("RowStreamerMaxInnoDBTrxHistLen", "", func() int64 { return env.Config().RowStreamer.MaxInnoDBTrxHistLen })
	env.Exporter().NewGaugeFunc("RowStreamerMaxMySQLReplLagSecs", "", func() int64 { return env.Config().RowStreamer.MaxMySQLReplLagSecs })
	env.Exporter().HandleFunc("/debug/tablet_vschema", vse.ServeHTTP)
	return vse
}

// InitDBConfig initializes the target parameters for the Engine.
func (vse *Engine) InitDBConfig(keyspace, shard string) {
	vse.keyspace = keyspace
	vse.shard = shard
}

// Open starts the Engine service.
func (vse *Engine) Open() {
	log.Info("VStreamer: opening")
	// If it's not already open, then open it now.
	atomic.CompareAndSwapInt32(&vse.isOpen, 0, 1)
}

// IsOpen checks if the engine is opened
func (vse *Engine) IsOpen() bool {
	return atomic.LoadInt32(&vse.isOpen) == 1
}

// Close closes the Engine service.
func (vse *Engine) Close() {
	func() {
		if atomic.LoadInt32(&vse.isOpen) == 0 {
			return
		}
		vse.mu.Lock()
		defer vse.mu.Unlock()
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
		atomic.StoreInt32(&vse.isOpen, 0)
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
		if atomic.LoadInt32(&vse.isOpen) == 0 {
			return nil, 0, errors.New("VStreamer is not open")
		}
		vse.mu.Lock()
		defer vse.mu.Unlock()
		streamer := newUVStreamer(ctx, vse, vse.env.Config().DB.FilteredWithDB(), vse.se, startPos, tablePKs, filter, vse.lvschema, send)
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
		if atomic.LoadInt32(&vse.isOpen) == 0 {
			return nil, 0, errors.New("VStreamer is not open")
		}
		vse.mu.Lock()
		defer vse.mu.Unlock()

		rowStreamer := newRowStreamer(ctx, vse.env.Config().DB.FilteredWithDB(), vse.se, query, lastpk, vse.lvschema, send, vse)
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
		if atomic.LoadInt32(&vse.isOpen) == 0 {
			return nil, 0, errors.New("VStreamer is not open")
		}
		vse.mu.Lock()
		defer vse.mu.Unlock()
		resultStreamer := newResultStreamer(ctx, vse.env.Config().DB.FilteredWithDB(), query, send, vse)
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
	vse.ts.WatchSrvVSchema(context.TODO(), vse.cell, func(v *vschemapb.SrvVSchema, err error) bool {
		switch {
		case err == nil:
			// Build vschema down below.
		case topo.IsErrType(err, topo.NoNode):
			v = nil
		default:
			log.Errorf("Error fetching vschema: %v", err)
			vse.vschemaErrors.Add(1)
			return true
		}
		var vschema *vindexes.VSchema
		if v != nil {
			vschema = vindexes.BuildVSchema(v)
			if err != nil {
				log.Errorf("Error building vschema: %v", err)
				vse.vschemaErrors.Add(1)
				return true
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
		return true
	})
}

func getPacketSize() int64 {
	return int64(defaultPacketSize)
}

// waitForMySQL ensures that the source is able to stay within defined bounds for
// its MVCC history list (trx rollback segment linked list for old versions of rows
// that should be purged ASAP) and its replica lag (which will be -1 for non-replicas)
// to help ensure that the vstream does not have an outsized harmful impact on the
// source's ability to function normally.
func (vse *Engine) waitForMySQL(ctx context.Context, db dbconfigs.Connector, tableName string) error {
	sourceEndpoint, _ := vse.getMySQLEndpoint(ctx, db)
	backoff := 1 * time.Second
	backoffLimit := backoff * 30
	ready := false
	recording := false

	loopFunc := func() error {
		// Exit if the context has been cancelled
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Check the config values each time as they can be updated in the running process via the /debug/env endpoint.
		// This allows the user to break out of a wait w/o incurring any downtime or restarting the workflow if they
		// need to.
		mhll := vse.env.Config().RowStreamer.MaxInnoDBTrxHistLen
		mrls := vse.env.Config().RowStreamer.MaxMySQLReplLagSecs
		hll := vse.getInnoDBTrxHistoryLen(ctx, db)
		rpl := vse.getMySQLReplicationLag(ctx, db)
		if hll <= mhll && rpl <= mrls {
			ready = true
		} else {
			log.Infof("VStream source (%s) is not ready to stream more rows. Max InnoDB history length is %d and it was %d, max replication lag is %d (seconds) and it was %d. Will pause and retry.",
				sourceEndpoint, mhll, hll, mrls, rpl)
		}
		return nil
	}

	for {
		if err := loopFunc(); err != nil {
			return err
		}
		if ready {
			break
		} else {
			if !recording {
				defer func() {
					ct := time.Now()
					// Global row streamer waits on the source tablet
					vse.rowStreamerWaits.Record("waitForMySQL", ct)
					// Waits by the table we're copying from the source tablet
					vse.vstreamerPhaseTimings.Record(fmt.Sprintf("%s:waitForMySQL", tableName), ct)
				}()
				recording = true
			}
			select {
			case <-ctx.Done():
				return vterrors.Errorf(vtrpcpb.Code_CANCELED, "context has expired")
			case <-time.After(backoff):
				// Exponential backoff with 1.5 as a factor
				if backoff != backoffLimit {
					nb := time.Duration(float64(backoff) * 1.5)
					if nb > backoffLimit {
						backoff = backoffLimit
					} else {
						backoff = nb
					}
				}
			}
		}
	}

	return nil
}

// getInnoDBTrxHistoryLen attempts to query InnoDB's current transaction rollback segment's history
// list length. If the value cannot be determined for any reason then -1 is returned, which means
// "unknown".
func (vse *Engine) getInnoDBTrxHistoryLen(ctx context.Context, db dbconfigs.Connector) int64 {
	histLen := int64(-1)
	conn, err := db.Connect(ctx)
	if err != nil {
		return histLen
	}
	defer conn.Close()

	res, err := conn.ExecuteFetch(trxHistoryLenQuery, 1, false)
	if err != nil || len(res.Rows) != 1 || res.Rows[0] == nil {
		return histLen
	}
	histLen, _ = res.Rows[0][0].ToInt64()
	return histLen
}

// getMySQLReplicationLag attempts to get the seconds_behind_master value.
// If the value cannot be determined for any reason then -1 is returned, which
// means "unknown" or "irrelevant" (meaning it's not actively replicating).
func (vse *Engine) getMySQLReplicationLag(ctx context.Context, db dbconfigs.Connector) int64 {
	lagSecs := int64(-1)
	conn, err := db.Connect(ctx)
	if err != nil {
		return lagSecs
	}
	defer conn.Close()

	res, err := conn.ExecuteFetch(replicaLagQuery, 1, true)
	if err != nil || len(res.Rows) != 1 || res.Rows[0] == nil {
		return lagSecs
	}
	row := res.Named().Row()
	return row.AsInt64("Seconds_Behind_Master", -1)
}

// getMySQLEndpoint returns the host:port value for the vstreamer (MySQL) instance
func (vse *Engine) getMySQLEndpoint(ctx context.Context, db dbconfigs.Connector) (string, error) {
	conn, err := db.Connect(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	res, err := conn.ExecuteFetch(hostQuery, 1, false)
	if err != nil || len(res.Rows) != 1 || res.Rows[0] == nil {
		return "", vterrors.Wrap(err, "could not get vstreamer MySQL endpoint")
	}
	host := res.Rows[0][0].ToString()
	port, _ := res.Rows[0][1].ToInt64()
	return fmt.Sprintf("%s:%d", host, port), nil
}

// mapPKEquivalentCols gets a PK equivalent from mysqld for the table
// and maps the column names to field indexes in the MinimalTable struct.
func (vse *Engine) mapPKEquivalentCols(ctx context.Context, table *binlogdatapb.MinimalTable) ([]int, error) {
	mysqld := mysqlctl.NewMysqld(vse.env.Config().DB)
	pkeColNames, err := mysqld.GetPrimaryKeyEquivalentColumns(ctx, vse.env.Config().DB.DBName, table.Name)
	if err != nil {
		return nil, err
	}
	pkeCols := make([]int, len(pkeColNames))
	matches := 0
	for n, field := range table.Fields {
		for i, pkeColName := range pkeColNames {
			if strings.EqualFold(field.Name, pkeColName) {
				pkeCols[i] = n
				matches++
				break
			}
		}
		if matches == len(pkeColNames) {
			break
		}
	}
	return pkeCols, nil
}
