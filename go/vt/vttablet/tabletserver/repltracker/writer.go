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

	"vitess.io/vitess/go/vt/withddl"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	sqlCreateSidecarDB      = "create database if not exists %s"
	sqlCreateHeartbeatTable = `CREATE TABLE IF NOT EXISTS %s.heartbeat (
  keyspaceShard VARBINARY(256) NOT NULL PRIMARY KEY,
  tabletUid INT UNSIGNED NOT NULL,
  ts BIGINT UNSIGNED NOT NULL
        ) engine=InnoDB`
	sqlUpsertHeartbeat = "INSERT INTO %s.heartbeat (ts, tabletUid, keyspaceShard) VALUES (%a, %a, %a) ON DUPLICATE KEY UPDATE ts=VALUES(ts), tabletUid=VALUES(tabletUid)"
)

var withDDL = withddl.New([]string{
	fmt.Sprintf(sqlCreateSidecarDB, "_vt"),
	fmt.Sprintf(sqlCreateHeartbeatTable, "_vt"),
})

// heartbeatWriter runs on master tablets and writes heartbeats to the _vt.heartbeat
// table at a regular interval, defined by heartbeat_interval.
type heartbeatWriter struct {
	env tabletenv.Env

	enabled       bool
	interval      time.Duration
	tabletAlias   topodatapb.TabletAlias
	keyspaceShard string
	now           func() time.Time
	errorLog      *logutil.ThrottledLogger

	mu     sync.Mutex
	isOpen bool
	pool   *connpool.Pool
	ticks  *timer.Timer
}

// newHeartbeatWriter creates a new heartbeatWriter.
func newHeartbeatWriter(env tabletenv.Env, alias topodatapb.TabletAlias) *heartbeatWriter {
	config := env.Config()

	// config.EnableLagThrottler is a feature flag for the throttler; if throttler runs, then heartbeat must also run
	if config.ReplicationTracker.Mode != tabletenv.Heartbeat && !config.EnableLagThrottler {
		return &heartbeatWriter{}
	}
	heartbeatInterval := config.ReplicationTracker.HeartbeatIntervalSeconds.Get()
	return &heartbeatWriter{
		env:         env,
		enabled:     true,
		tabletAlias: alias,
		now:         time.Now,
		interval:    heartbeatInterval,
		ticks:       timer.NewTimer(heartbeatInterval),
		errorLog:    logutil.NewThrottledLogger("HeartbeatWriter", 60*time.Second),
		pool: connpool.NewPool(env, "HeartbeatWritePool", tabletenv.ConnPoolConfig{
			Size:               1,
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		}),
	}
}

// InitDBConfig initializes the target name for the heartbeatWriter.
func (w *heartbeatWriter) InitDBConfig(target querypb.Target) {
	w.keyspaceShard = fmt.Sprintf("%s:%s", target.Keyspace, target.Shard)
}

// Open sets up the heartbeatWriter's db connection and launches the ticker
// responsible for periodically writing to the heartbeat table.
func (w *heartbeatWriter) Open() {
	if !w.enabled {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.isOpen {
		return
	}
	log.Info("Hearbeat Writer: opening")

	w.pool.Open(w.env.Config().DB.AppWithDB(), w.env.Config().DB.DbaWithDB(), w.env.Config().DB.AppDebugWithDB())
	w.enableWrites(true)
	w.isOpen = true
}

// Close closes the heartbeatWriter's db connection and stops the periodic ticker.
func (w *heartbeatWriter) Close() {
	if !w.enabled {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.isOpen {
		return
	}

	w.enableWrites(false)
	w.pool.Close()
	w.isOpen = false
	log.Info("Hearbeat Writer: closed")
}

// bindHeartbeatVars takes a heartbeat write (insert or update) and
// adds the necessary fields to the query as bind vars. This is done
// to protect ourselves against a badly formed keyspace or shard name.
func (w *heartbeatWriter) bindHeartbeatVars(query string) (string, error) {
	bindVars := map[string]*querypb.BindVariable{
		"ks":  sqltypes.StringBindVariable(w.keyspaceShard),
		"ts":  sqltypes.Int64BindVariable(w.now().UnixNano()),
		"uid": sqltypes.Int64BindVariable(int64(w.tabletAlias.Uid)),
	}
	parsed := sqlparser.BuildParsedQuery(query, "_vt", ":ts", ":uid", ":ks")
	bound, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return "", err
	}
	return bound, nil
}

// writeHeartbeat updates the heartbeat row for this tablet with the current time in nanoseconds.
func (w *heartbeatWriter) writeHeartbeat() {
	if err := w.write(); err != nil {
		w.recordError(err)
		return
	}
	writes.Add(1)
}

func (w *heartbeatWriter) write() error {
	defer w.env.LogError()
	ctx, cancel := context.WithDeadline(context.Background(), w.now().Add(w.interval))
	defer cancel()
	upsert, err := w.bindHeartbeatVars(sqlUpsertHeartbeat)
	if err != nil {
		return err
	}
	conn, err := w.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()
	_, err = withDDL.Exec(ctx, upsert, conn.Exec)
	if err != nil {
		return err
	}
	return nil
}

func (w *heartbeatWriter) recordError(err error) {
	w.errorLog.Errorf("%v", err)
	writeErrors.Add(1)
}

// enableWrites actives or deactives heartbeat writes
func (w *heartbeatWriter) enableWrites(enable bool) {
	if w.ticks == nil {
		return
	}
	if enable {
		w.ticks.Start(w.writeHeartbeat)
	} else {
		w.ticks.Stop()
	}
}
