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

package repltracker

import (
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/vterrors"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

const (
	sqlFetchMostRecentHeartbeat = "SELECT ts FROM %s.heartbeat WHERE keyspaceShard=%a"
)

// heartbeatReader reads the heartbeat table at a configured interval in order
// to calculate replication lag. It is meant to be run on a replica, and paired
// with a heartbeatWriter on a master.
// Lag is calculated by comparing the most recent timestamp in the heartbeat
// table against the current time at read time.
type heartbeatReader struct {
	env tabletenv.Env

	enabled       bool
	interval      time.Duration
	keyspaceShard string
	now           func() time.Time
	errorLog      *logutil.ThrottledLogger

	runMu  sync.Mutex
	isOpen bool
	pool   *connpool.Pool
	ticks  *timer.Timer

	lagMu          sync.Mutex
	lastKnownLag   time.Duration
	lastKnownError error
}

// newHeartbeatReader returns a new heartbeatReader.
func newHeartbeatReader(env tabletenv.Env) *heartbeatReader {
	config := env.Config()
	if config.ReplicationTracker.Mode != tabletenv.Heartbeat {
		return &heartbeatReader{}
	}

	heartbeatInterval := config.ReplicationTracker.HeartbeatIntervalSeconds.Get()
	return &heartbeatReader{
		env:      env,
		enabled:  true,
		now:      time.Now,
		interval: heartbeatInterval,
		ticks:    timer.NewTimer(heartbeatInterval),
		errorLog: logutil.NewThrottledLogger("HeartbeatReporter", 60*time.Second),
		pool: connpool.NewPool(env, "HeartbeatReadPool", tabletenv.ConnPoolConfig{
			Size:               1,
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		}),
	}
}

// InitDBConfig initializes the target name for the heartbeatReader.
func (r *heartbeatReader) InitDBConfig(target querypb.Target) {
	r.keyspaceShard = fmt.Sprintf("%s:%s", target.Keyspace, target.Shard)
}

// Open starts the heartbeat ticker and opens the db pool.
func (r *heartbeatReader) Open() {
	if !r.enabled {
		return
	}
	r.runMu.Lock()
	defer r.runMu.Unlock()
	if r.isOpen {
		return
	}
	log.Info("Heartbeat Reader: opening")

	r.pool.Open(r.env.Config().DB.AppWithDB(), r.env.Config().DB.DbaWithDB(), r.env.Config().DB.AppDebugWithDB())
	r.ticks.Start(func() { r.readHeartbeat() })
	r.isOpen = true
}

// Close cancels the watchHeartbeat periodic ticker and closes the db pool.
func (r *heartbeatReader) Close() {
	if !r.enabled {
		return
	}
	r.runMu.Lock()
	defer r.runMu.Unlock()
	if !r.isOpen {
		return
	}
	r.ticks.Stop()
	r.pool.Close()
	r.isOpen = false
	log.Info("Heartbeat Reader: closed")
}

// Status returns the most recently recorded lag measurement or error encountered.
func (r *heartbeatReader) Status() (time.Duration, error) {
	r.lagMu.Lock()
	defer r.lagMu.Unlock()
	if r.lastKnownError != nil {
		return 0, r.lastKnownError
	}
	return r.lastKnownLag, nil
}

// readHeartbeat reads from the heartbeat table exactly once, updating
// the last known lag and/or error, and incrementing counters.
func (r *heartbeatReader) readHeartbeat() {
	defer r.env.LogError()

	ctx, cancel := context.WithDeadline(context.Background(), r.now().Add(r.interval))
	defer cancel()

	res, err := r.fetchMostRecentHeartbeat(ctx)
	if err != nil {
		r.recordError(vterrors.Wrap(err, "Failed to read most recent heartbeat"))
		return
	}
	ts, err := parseHeartbeatResult(res)
	if err != nil {
		r.recordError(vterrors.Wrap(err, "Failed to parse heartbeat result"))
		return
	}

	lag := r.now().Sub(time.Unix(0, ts))
	cumulativeLagNs.Add(lag.Nanoseconds())
	currentLagNs.Set(lag.Nanoseconds())
	heartbeatLagNsHistogram.Add(lag.Nanoseconds())
	reads.Add(1)

	r.lagMu.Lock()
	r.lastKnownLag = lag
	r.lastKnownError = nil
	r.lagMu.Unlock()
}

// fetchMostRecentHeartbeat fetches the most recently recorded heartbeat from the heartbeat table,
// returning a result with the timestamp of the heartbeat.
func (r *heartbeatReader) fetchMostRecentHeartbeat(ctx context.Context) (*sqltypes.Result, error) {
	conn, err := r.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	sel, err := r.bindHeartbeatFetch()
	if err != nil {
		return nil, err
	}
	return conn.Exec(ctx, sel, 1, false)
}

// bindHeartbeatFetch takes a heartbeat read and adds the necessary
// fields to the query as bind vars. This is done to protect ourselves
// against a badly formed keyspace or shard name.
func (r *heartbeatReader) bindHeartbeatFetch() (string, error) {
	bindVars := map[string]*querypb.BindVariable{
		"ks": sqltypes.StringBindVariable(r.keyspaceShard),
	}
	parsed := sqlparser.BuildParsedQuery(sqlFetchMostRecentHeartbeat, "_vt", ":ks")
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return "", err
	}
	return bound, nil
}

// parseHeartbeatResult turns a raw result into the timestamp for processing.
func parseHeartbeatResult(res *sqltypes.Result) (int64, error) {
	if len(res.Rows) != 1 {
		return 0, fmt.Errorf("failed to read heartbeat: writer query did not result in 1 row. Got %v", len(res.Rows))
	}
	ts, err := evalengine.ToInt64(res.Rows[0][0])
	if err != nil {
		return 0, err
	}
	return ts, nil
}

// recordError keeps track of the lastKnown error for reporting to the healthcheck.
// Errors tracked here are logged with throttling to cut down on log spam since
// operations can happen very frequently in this package.
func (r *heartbeatReader) recordError(err error) {
	r.lagMu.Lock()
	r.lastKnownError = err
	r.lagMu.Unlock()
	r.errorLog.Errorf("%v", err)
	readErrors.Add(1)
}
