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
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// We identify these heartbeat configuration types:
// - No configuration: the heartbeat writer is generally disabled and does not produce heartbeats (but see below).
// - On-demand: on-demand heartbeat interval was specified.
// - Always: the heartbeat writer is always on and produces heartbeats at a regular interval.
type HeartbeatConfigType int

const (
	HeartbeatConfigTypeNone HeartbeatConfigType = iota
	HeartbeatConfigTypeOnDemand
	HeartbeatConfigTypeAlways
)

const (
	sqlUpsertHeartbeat      = "INSERT INTO %s.heartbeat (ts, tabletUid, keyspaceShard) VALUES (%a, %a, %a) ON DUPLICATE KEY UPDATE ts=VALUES(ts), tabletUid=VALUES(tabletUid)"
	minimalOnDemandDuration = 4 * time.Second
)

var (
	// Can be overridden by unit tests
	defaultOnDemandDuration  = 10 * time.Second
	defaultHeartbeatInterval = 1 * time.Second
)

// heartbeatWriter runs on primary tablets and writes heartbeats to the heartbeat
// table, depending on the configuration:
//   - HeartbeatConfigTypeAlways: while open, the writer produces heartbeats at a regular interval.
//     RequetHeartbeats() is meaningless in this mode.
//   - HeartbeatConfigTypeOnDemand: when opened, the writer produces heartbeats for the configured lease.
//     The heartbeats then expire. Lease can be renewed (after expired) or extended (while running) via
//     RequetHeartbeats().
//   - HeartbeatConfigTypeNone: the writer does not initiate any heartbeats. However, RequetHeartbeats()
//     can be called to request a heartbeat lease. The lease duration is predetermined as `defaultOnDemandDuration`.
type heartbeatWriter struct {
	env tabletenv.Env

	configType    HeartbeatConfigType
	interval      time.Duration
	tabletAlias   *topodatapb.TabletAlias
	keyspaceShard string
	now           func() time.Time
	errorLog      *logutil.ThrottledLogger

	mu                          sync.Mutex
	isOpen                      bool
	appPool                     *dbconnpool.ConnectionPool
	allPrivsPool                *dbconnpool.ConnectionPool
	ticks                       *timer.Timer
	onDemandRequestsRateLimiter atomic.Pointer[timer.RateLimiter]
	writeConnID                 atomic.Int64

	onDemandDuration time.Duration
}

// newHeartbeatWriter creates a new heartbeatWriter.
func newHeartbeatWriter(env tabletenv.Env, alias *topodatapb.TabletAlias) *heartbeatWriter {
	config := env.Config()

	configType := HeartbeatConfigTypeNone
	onDemandDuration := defaultOnDemandDuration
	switch {
	case config.ReplicationTracker.HeartbeatOnDemand > 0:
		configType = HeartbeatConfigTypeOnDemand
		onDemandDuration = config.ReplicationTracker.HeartbeatOnDemand
		if onDemandDuration < minimalOnDemandDuration {
			onDemandDuration = minimalOnDemandDuration
		}
	case config.ReplicationTracker.Mode == tabletenv.Heartbeat:
		configType = HeartbeatConfigTypeAlways
		onDemandDuration = 0
	}
	heartbeatInterval := config.ReplicationTracker.HeartbeatInterval
	if heartbeatInterval == 0 {
		heartbeatInterval = defaultHeartbeatInterval
	}
	w := &heartbeatWriter{
		env:              env,
		configType:       configType,
		tabletAlias:      alias.CloneVT(),
		now:              time.Now,
		interval:         heartbeatInterval,
		onDemandDuration: onDemandDuration,
		ticks:            timer.NewTimer(heartbeatInterval),
		errorLog:         logutil.NewThrottledLogger("HeartbeatWriter", 60*time.Second),
		// We make this pool size 2; to prevent pool exhausted
		// stats from incrementing continually, and causing concern
		appPool:      dbconnpool.NewConnectionPool("HeartbeatWriteAppPool", env.Exporter(), 2, mysqlctl.DbaIdleTimeout, 0, mysqlctl.PoolDynamicHostnameResolution),
		allPrivsPool: dbconnpool.NewConnectionPool("HeartbeatWriteAllPrivsPool", env.Exporter(), 2, mysqlctl.DbaIdleTimeout, 0, mysqlctl.PoolDynamicHostnameResolution),
	}

	w.writeConnID.Store(-1)
	return w
}

// InitDBConfig initializes the target name for the heartbeatWriter.
func (w *heartbeatWriter) InitDBConfig(target *querypb.Target) {
	w.keyspaceShard = fmt.Sprintf("%s:%s", target.Keyspace, target.Shard)
}

// Open sets up the heartbeatWriter's db connection and launches the ticker
// responsible for periodically writing to the heartbeat table.
func (w *heartbeatWriter) Open() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.isOpen {
		return
	}
	defer func() {
		w.isOpen = true
	}()
	log.Info("Heartbeat Writer: opening")

	// We cannot create the database and tables in this Open function
	// since, this is run when a tablet changes to Primary type. The other replicas
	// might not have started replication. So if we run the create commands, it will
	// block this thread, and we could end up in a deadlock.
	// Instead, we try creating the database and table in each tick which runs in a go routine
	// keeping us safe from hanging the main thread.

	// This function could be running from within a unit test scope, in which case we use
	// mock pools that are already open. This is why we test for the pool being open.
	if !w.appPool.IsOpen() {
		w.appPool.Open(w.env.Config().DB.AppWithDB())
	}
	if !w.allPrivsPool.IsOpen() {
		w.allPrivsPool.Open(w.env.Config().DB.AllPrivsWithDB())
	}

	if w.onDemandDuration > 0 {
		// Clients are given access to RequestHeartbeats(). But such clients can also abuse
		// the system by initiating heartbeats too frequently. We rate-limit these requests.
		rateLimiter := timer.NewRateLimiter(time.Second)
		w.onDemandRequestsRateLimiter.Store(rateLimiter)
	}

	switch w.configType {
	case HeartbeatConfigTypeNone:
		// Do not kick any heartbeats
		return
	case HeartbeatConfigTypeAlways:
		// Heartbeats are always on
		w.enableWrites()
	case HeartbeatConfigTypeOnDemand:
		// A one-time kick off of heartbeats upon Open()
		go w.RequestHeartbeats()
	}
}

// Close closes the heartbeatWriter's db connection and stops the periodic ticker.
func (w *heartbeatWriter) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.isOpen {
		return
	}
	defer func() {
		w.isOpen = false
	}()

	w.disableWrites()
	w.appPool.Close()
	w.allPrivsPool.Close()

	if rateLimiter := w.onDemandRequestsRateLimiter.Load(); rateLimiter != nil {
		rateLimiter.Stop()
	}
	w.onDemandRequestsRateLimiter.Store(nil)

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
	} else {
		writes.Add(1)
	}

	if w.onDemandDuration > 0 {
		// See if we need to expire the heartbeats
		go func() {
			if rateLimiter := w.onDemandRequestsRateLimiter.Load(); rateLimiter != nil {
				if rateLimiter.Diff() > int64(w.onDemandDuration.Seconds()) {
					w.disableWrites()
				}
			}
		}()
	}
}

// write writes a single heartbeat update.
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
		return vterrors.Wrapf(err, "heartbeatWriter failed to get app connection from pool %v", w.appPool.Name)
	}
	defer appConn.Recycle()
	w.writeConnID.Store(appConn.Conn.ID())
	defer w.writeConnID.Store(-1)
	_, err = appConn.Conn.ExecuteFetch(upsert, 1, false)
	if err != nil {
		return vterrors.Wrapf(err, "heartbeatWriter failed to execute fetch on app connection %v", appConn.Conn.ID())
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

// enableWrites activates heartbeat writes
func (w *heartbeatWriter) enableWrites() {
	// We must combat a potential race condition: the writer is Open, and a request comes
	// to enableWrites(), but simultaneously the writes gets Close()d.
	// We must not send any more ticks while the writer is closed.
	go func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		if !w.isOpen {
			return
		}
		w.ticks.Start(w.writeHeartbeat)
	}()
}

// disableWrites deactivates heartbeat writes
func (w *heartbeatWriter) disableWrites() {
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

	// Let the next RequestHeartbeats() go through
	w.allowNextHeartbeatRequest()
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
// is not rate-limited.
// Use case: just as the on-demand lease expires, a new request for heartbeat comes, and it's in the same timeslot
// as the one before the expiration. We want to allow the new request to re-lease on demand heartbeats.
func (w *heartbeatWriter) allowNextHeartbeatRequest() {
	// The writer could be close()d while this function is running and thus the value of the rate limiter could be nil.
	// We thus use golang atomic here to avoid locking mutexes.
	if rateLimiter := w.onDemandRequestsRateLimiter.Load(); rateLimiter != nil {
		rateLimiter.AllowOne()
	}
}

// RequestHeartbeats implements heartbeat.HeartbeatWriter.RequestHeartbeats()
// A client (such as the throttler) may call this function as frequently as it wishes, to request
// for a heartbeat "lease".
// This function will selectively and silently drop some such requests, depending on arrival rate.
// This function is safe to call concurrently from goroutines
func (w *heartbeatWriter) RequestHeartbeats() {
	// The writer could be close()d while this function is running and thus the value of the rate limiter could be nil.
	// We thus use golang atomic here to avoid locking mutexes.
	if rateLimiter := w.onDemandRequestsRateLimiter.Load(); rateLimiter != nil {
		rateLimiter.Do(func() error {
			w.enableWrites()
			return nil
		})
	}
}
