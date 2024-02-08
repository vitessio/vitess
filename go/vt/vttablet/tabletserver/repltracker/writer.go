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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/constants/sidecar"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	sqlUpsertHeartbeat = "INSERT INTO %s.heartbeat (ts, tabletUid, keyspaceShard) VALUES (%a, %a, %a) ON DUPLICATE KEY UPDATE ts=VALUES(ts), tabletUid=VALUES(tabletUid)"
)

// heartbeatWriter runs on primary tablets and writes heartbeats to the heartbeat
// table at a regular interval, defined by heartbeat_interval.
type heartbeatWriter struct {
	env tabletenv.Env

	enabled       bool
	interval      time.Duration
	tabletAlias   *topodatapb.TabletAlias
	keyspaceShard string
	now           func() time.Time
	errorLog      *logutil.ThrottledLogger

	mu           sync.Mutex
	isOpen       bool
	appPool      *dbconnpool.ConnectionPool
	allPrivsPool *dbconnpool.ConnectionPool
	ticks        *timer.Timer
	writeConnID  atomic.Int64

	onDemandDuration            time.Duration
	onDemandMu                  sync.Mutex
	concurrentHeartbeatRequests int64
	onDemandRequestTicks        int64
	onDemandLastRequestTick     int64
}

// newHeartbeatWriter creates a new heartbeatWriter.
func newHeartbeatWriter(env tabletenv.Env, alias *topodatapb.TabletAlias) *heartbeatWriter {
	config := env.Config()

	// config.EnableLagThrottler is a feature flag for the throttler; if throttler runs, then heartbeat must also run
	if config.ReplicationTracker.Mode != tabletenv.Heartbeat && config.ReplicationTracker.HeartbeatOnDemand == 0 {
		return &heartbeatWriter{}
	}
	heartbeatInterval := config.ReplicationTracker.HeartbeatInterval
	w := &heartbeatWriter{
		env:              env,
		enabled:          true,
		tabletAlias:      alias.CloneVT(),
		now:              time.Now,
		interval:         heartbeatInterval,
		onDemandDuration: config.ReplicationTracker.HeartbeatOnDemand,
		ticks:            timer.NewTimer(heartbeatInterval),
		errorLog:         logutil.NewThrottledLogger("HeartbeatWriter", 60*time.Second),
		// We make this pool size 2; to prevent pool exhausted
		// stats from incrementing continually, and causing concern
		appPool:      dbconnpool.NewConnectionPool("HeartbeatWriteAppPool", env.Exporter(), 2, mysqlctl.DbaIdleTimeout, 0, mysqlctl.PoolDynamicHostnameResolution),
		allPrivsPool: dbconnpool.NewConnectionPool("HeartbeatWriteAllPrivsPool", env.Exporter(), 2, mysqlctl.DbaIdleTimeout, 0, mysqlctl.PoolDynamicHostnameResolution),
	}
	w.writeConnID.Store(-1)
	if w.onDemandDuration > 0 {
		// see RequestHeartbeats() for use of onDemandRequestTicks
		// it's basically a mechanism to rate limit operation RequestHeartbeats().
		// and selectively drop excessive requests.
		w.allowNextHeartbeatRequest()
		go func() {
			// this will allow up to 1 request per (w.onDemandDuration / 4) to pass through
			tick := time.NewTicker(w.onDemandDuration / 4)
			defer tick.Stop()
			for range tick.C {
				w.allowNextHeartbeatRequest()
			}
		}()
	}
	return w
}

// InitDBConfig initializes the target name for the heartbeatWriter.
func (w *heartbeatWriter) InitDBConfig(target *querypb.Target) {
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
	log.Info("Heartbeat Writer: opening")

	// We cannot create the database and tables in this Open function
	// since, this is run when a tablet changes to Primary type. The other replicas
	// might not have started replication. So if we run the create commands, it will
	// block this thread, and we could end up in a deadlock.
	// Instead, we try creating the database and table in each tick which runs in a go routine
	// keeping us safe from hanging the main thread.
	w.appPool.Open(w.env.Config().DB.AppWithDB())
	w.allPrivsPool.Open(w.env.Config().DB.AllPrivsWithDB())
	if w.onDemandDuration == 0 {
		w.enableWrites(true)
		// when onDemandDuration > 0 we only enable writes per request
	} else {
		// A one-time kick off of heartbeats upon Open()
		go w.RequestHeartbeats()
	}

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
	w.appPool.Close()
	w.allPrivsPool.Close()
	w.isOpen = false
	log.Info("Heartbeat Writer: closed")
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
	parsed := sqlparser.BuildParsedQuery(query, sidecar.GetIdentifier(), ":ts", ":uid", ":ks")
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
	appConn, err := w.appPool.Get(ctx)
	if err != nil {
		return err
	}
	defer appConn.Recycle()
	w.writeConnID.Store(appConn.Conn.ID())
	defer w.writeConnID.Store(-1)
	_, err = appConn.Conn.ExecuteFetch(upsert, 1, false)
	if err != nil {
		return err
	}
	return nil
}

func (w *heartbeatWriter) recordError(err error) {
	if err == nil {
		return
	}
	w.errorLog.Errorf("%v", err)
	writeErrors.Add(1)
}

// enableWrites activates or deactivates heartbeat writes
func (w *heartbeatWriter) enableWrites(enable bool) {
	if w.ticks == nil {
		return
	}
	switch enable {
	case true:
		// We must combat a potential race condition: the writer is Open, and a request comes
		// to enableWrites(true), but simultaneously the writes gets Close()d.
		// We must not send any more ticks while the writer is closed.
		go func() {
			w.mu.Lock()
			defer w.mu.Unlock()
			if !w.isOpen {
				return
			}
			w.ticks.Start(w.writeHeartbeat)
		}()
	case false:
		// We stop the ticks in a separate go routine because it can block if the write is stuck on semi-sync ACKs.
		// At the same time we try and kill the write that is in progress. We use the context and its cancellation
		// for coordination between the two go-routines. In the end we will have guaranteed that the ticks have stopped
		// and no write is in progress.
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			w.ticks.Stop()
			cancel()
		}()
		w.killWritesUntilStopped(ctx)

		if w.onDemandDuration > 0 {
			// Let the next RequestHeartbeats() go through
			w.allowNextHeartbeatRequest()
		}
	}
}

// killWritesUntilStopped tries to kill the write in progress until the ticks have stopped.
func (w *heartbeatWriter) killWritesUntilStopped(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		// Actually try to kill the query.
		err := w.killWrite()
		w.recordError(err)
		select {
		case <-ctx.Done():
			// If the context has been cancelled, then we know that the ticks have stopped.
			// This guarantees that there are no writes in progress, so there is nothing to kill.
			return
		case <-ticker.C:
		}
	}
}

// killWrite kills the write in progress (if any).
func (w *heartbeatWriter) killWrite() error {
	defer w.env.LogError()
	writeId := w.writeConnID.Load()
	if writeId == -1 {
		return nil
	}

	ctx, cancel := context.WithDeadline(context.Background(), w.now().Add(w.interval))
	defer cancel()
	killConn, err := w.allPrivsPool.Get(ctx)
	if err != nil {
		log.Errorf("Kill conn didn't get connection :(")
		return err
	}
	defer killConn.Recycle()

	_, err = killConn.Conn.ExecuteFetch(fmt.Sprintf("kill %d", writeId), 1, false)
	return err
}

// allowNextHeartbeatRequest ensures that the next call to RequestHeartbeats() passes through and
// is not dropped.
func (w *heartbeatWriter) allowNextHeartbeatRequest() {
	atomic.AddInt64(&w.onDemandRequestTicks, 1)
}

// RequestHeartbeats implements heartbeat.HeartbeatWriter.RequestHeartbeats()
// A client (such as the throttler) may call this function as frequently as it wishes, to request
// for a heartbeat "lease".
// This function will selectively and silently drop some such requests, depending on arrival rate.
// This function is safe to call concurrently from goroutines
func (w *heartbeatWriter) RequestHeartbeats() {
	if w.onDemandDuration == 0 {
		// heartbeats are not by demand. Therefore they are just coming in on their own (if enabled)
		return
	}
	// In this function we're going to create a timer to activate heartbeats by-demand. Creating a timer has a cost.
	// Now, this function can be spammed by clients (the lag throttler). We therefore only allow this function to
	// actually operate once per X seconds (1/4 of onDemandDuration as a reasonable oversampling value):
	if atomic.LoadInt64(&w.onDemandLastRequestTick) >= atomic.LoadInt64(&w.onDemandRequestTicks) {
		// Too many requests. We're dropping this one.
		return
	}
	atomic.StoreInt64(&w.onDemandLastRequestTick, atomic.LoadInt64(&w.onDemandRequestTicks))

	// OK, the request passed through.

	w.onDemandMu.Lock()
	defer w.onDemandMu.Unlock()

	// Now for the actual logic. A client requests heartbeats. If it were only this client, we would
	// activate heartbeats for the duration of onDemandDuration, and then turn heartbeats off.
	// However, there may be multiple clients interested in heartbeats, or maybe the same single client
	// requesting heartbeats again and again. So we keep track of how many _concurrent_ requests we have.
	// We enable heartbeats as soon as we have a request; we turn heartbeats off once
	// we have zero concurrent requests
	w.enableWrites(true)
	w.concurrentHeartbeatRequests++

	time.AfterFunc(w.onDemandDuration, func() {
		w.onDemandMu.Lock()
		defer w.onDemandMu.Unlock()
		w.concurrentHeartbeatRequests--
		if w.concurrentHeartbeatRequests == 0 {
			// means there are currently no more clients interested in heartbeats
			w.enableWrites(false)
		}
		w.allowNextHeartbeatRequest()
	})
}
