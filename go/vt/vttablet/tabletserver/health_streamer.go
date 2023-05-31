/*
Copyright 2020 The Vitess Authors.

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

package tabletserver

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/servenv"

	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/vt/dbconfigs"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/history"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

var (
	// blpFunc is a legaacy feature.
	// TODO(sougou): remove after legacy resharding worflows are removed.
	blpFunc = vreplication.StatusSummary

	errUnintialized = "tabletserver uninitialized"

	streamHealthBufferSize = uint(20)
)

func init() {
	servenv.OnParseFor("vtcombo", registerHealthStreamerFlags)
	servenv.OnParseFor("vttablet", registerHealthStreamerFlags)
}

func registerHealthStreamerFlags(fs *pflag.FlagSet) {
	fs.UintVar(&streamHealthBufferSize, "stream_health_buffer_size", streamHealthBufferSize, "max streaming health entries to buffer per streaming health client")
}

// healthStreamer streams health information to callers.
type healthStreamer struct {
	stats              *tabletenv.Stats
	degradedThreshold  time.Duration
	unhealthyThreshold sync2.AtomicDuration

	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	clients map[chan *querypb.StreamHealthResponse]struct{}
	state   *querypb.StreamHealthResponse
	// isServingPrimary stores if this tablet is currently the serving primary or not.
	isServingPrimary bool

	history *history.History

	ticks                  *timer.Timer
	dbConfig               dbconfigs.Connector
	conns                  *connpool.Pool
	signalWhenSchemaChange bool

	viewsEnabled bool
	views        map[string]string
}

func newHealthStreamer(env tabletenv.Env, alias *topodatapb.TabletAlias) *healthStreamer {
	var newTimer *timer.Timer
	var pool *connpool.Pool
	if env.Config().SignalWhenSchemaChange {
		reloadTime := env.Config().SignalSchemaChangeReloadIntervalSeconds.Get()
		newTimer = timer.NewTimer(reloadTime)
		// We need one connection for the reloader.
		pool = connpool.NewPool(env, "", tabletenv.ConnPoolConfig{
			Size:               1,
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		})
	}
	return &healthStreamer{
		stats:              env.Stats(),
		degradedThreshold:  env.Config().Healthcheck.DegradedThresholdSeconds.Get(),
		unhealthyThreshold: sync2.NewAtomicDuration(env.Config().Healthcheck.UnhealthyThresholdSeconds.Get()),
		clients:            make(map[chan *querypb.StreamHealthResponse]struct{}),

		state: &querypb.StreamHealthResponse{
			Target:      &querypb.Target{},
			TabletAlias: alias,
			RealtimeStats: &querypb.RealtimeStats{
				HealthError: errUnintialized,
			},
		},

		history:                history.New(5),
		ticks:                  newTimer,
		conns:                  pool,
		signalWhenSchemaChange: env.Config().SignalWhenSchemaChange,
		viewsEnabled:           env.Config().EnableViews,
		views:                  map[string]string{},
	}
}

func (hs *healthStreamer) InitDBConfig(target *querypb.Target, cp dbconfigs.Connector) {
	hs.state.Target = proto.Clone(target).(*querypb.Target)
	hs.dbConfig = cp
}

func (hs *healthStreamer) Open() {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	if hs.cancel != nil {
		return
	}
	hs.ctx, hs.cancel = context.WithCancel(context.Background())
	if hs.conns != nil {
		// if we don't have a live conns object, it means we are not configured to signal when the schema changes
		hs.conns.Open(hs.dbConfig, hs.dbConfig, hs.dbConfig)
		hs.ticks.Start(func() {
			if err := hs.reload(); err != nil {
				log.Errorf("periodic schema reload failed in health stream: %v", err)
			}
		})

	}

}

func (hs *healthStreamer) Close() {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	if hs.cancel != nil {
		if hs.ticks != nil {
			hs.ticks.Stop()
			hs.conns.Close()
		}
		hs.cancel()
		hs.cancel = nil
	}
}

func (hs *healthStreamer) Stream(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	ch, hsCtx := hs.register()
	if hsCtx == nil {
		return vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "tabletserver is shutdown")
	}
	defer hs.unregister(ch)

	// trigger the initial schema reload
	if hs.signalWhenSchemaChange {
		hs.ticks.Trigger()
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-hsCtx.Done():
			return vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "tabletserver is shutdown")
		case shr, ok := <-ch:
			if !ok {
				return vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "stream health buffer overflowed. client should reconnect for up-to-date status")
			}
			if err := callback(shr); err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
		}
	}
}

func (hs *healthStreamer) register() (chan *querypb.StreamHealthResponse, context.Context) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	if hs.cancel == nil {
		return nil, nil
	}

	ch := make(chan *querypb.StreamHealthResponse, streamHealthBufferSize)
	hs.clients[ch] = struct{}{}

	// Send the current state immediately.
	ch <- proto.Clone(hs.state).(*querypb.StreamHealthResponse)
	return ch, hs.ctx
}

func (hs *healthStreamer) unregister(ch chan *querypb.StreamHealthResponse) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	delete(hs.clients, ch)
}

func (hs *healthStreamer) ChangeState(tabletType topodatapb.TabletType, terTimestamp time.Time, lag time.Duration, err error, serving bool) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.state.Target.TabletType = tabletType
	if tabletType == topodatapb.TabletType_PRIMARY {
		hs.state.TabletExternallyReparentedTimestamp = terTimestamp.Unix()
	} else {
		hs.state.TabletExternallyReparentedTimestamp = 0
	}
	if err != nil {
		hs.state.RealtimeStats.HealthError = err.Error()
	} else {
		hs.state.RealtimeStats.HealthError = ""
	}
	hs.state.RealtimeStats.ReplicationLagSeconds = uint32(lag.Seconds())
	hs.state.Serving = serving

	hs.state.RealtimeStats.FilteredReplicationLagSeconds, hs.state.RealtimeStats.BinlogPlayersCount = blpFunc()
	hs.state.RealtimeStats.Qps = hs.stats.QPSRates.TotalRate()

	shr := proto.Clone(hs.state).(*querypb.StreamHealthResponse)

	hs.broadCastToClients(shr)
	hs.history.Add(&historyRecord{
		Time:       time.Now(),
		serving:    shr.Serving,
		tabletType: shr.Target.TabletType,
		lag:        lag,
		err:        err,
	})
}

func (hs *healthStreamer) broadCastToClients(shr *querypb.StreamHealthResponse) {
	for ch := range hs.clients {
		select {
		case ch <- shr:
		default:
			// We can't block this state change on broadcasting to a streaming health client, but we
			// also don't want to silently fail to inform a streaming health client of a state change
			// because it can allow a vtgate to get wedged in a state where it's wrong about whether
			// a tablet is healthy and can't automatically recover (see
			//  https://github.com/vitessio/vitess/issues/5445). If we can't send a health update
			// to this client we'll close() the channel which will ultimate fail the streaming health
			// RPC and cause vtgates to reconnect.
			//
			// An alternative approach for streaming health would be to force a periodic broadcast even
			// when there hasn't been an update and/or move away from using channels toward a model where
			// old updates can be purged from the buffer in favor of more recent updates (since only the
			// most recent health state really matters to gates).
			log.Warning("A streaming health buffer is full. Closing the channel")
			close(ch)
			delete(hs.clients, ch)
		}
	}
}

func (hs *healthStreamer) AppendDetails(details []*kv) []*kv {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if hs.state.Target.TabletType == topodatapb.TabletType_PRIMARY {
		return details
	}
	sbm := time.Duration(hs.state.RealtimeStats.ReplicationLagSeconds) * time.Second
	class := healthyClass
	switch {
	case sbm > hs.unhealthyThreshold.Get():
		class = unhealthyClass
	case sbm > hs.degradedThreshold:
		class = unhappyClass
	}
	details = append(details, &kv{
		Key:   "Replication Lag",
		Class: class,
		Value: fmt.Sprintf("%ds", hs.state.RealtimeStats.ReplicationLagSeconds),
	})
	if hs.state.RealtimeStats.HealthError != "" {
		details = append(details, &kv{
			Key:   "Replication Error",
			Class: unhappyClass,
			Value: hs.state.RealtimeStats.HealthError,
		})
	}

	return details
}

// MakePrimary tells the healthstreamer that the current tablet is now the primary,
// so it can read and write to the MySQL instance for schema-tracking.
func (hs *healthStreamer) MakePrimary(serving bool) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.isServingPrimary = serving
}

// MakeNonPrimary tells the healthstreamer that the current tablet is now not a primary.
func (hs *healthStreamer) MakeNonPrimary() {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.isServingPrimary = false
}

func (hs *healthStreamer) SetUnhealthyThreshold(v time.Duration) {
	hs.unhealthyThreshold.Set(v)
	shr := proto.Clone(hs.state).(*querypb.StreamHealthResponse)
	for ch := range hs.clients {
		select {
		case ch <- shr:
		default:
			log.Info("Resetting health streamer clients due to unhealthy threshold change")
			close(ch)
			delete(hs.clients, ch)
		}
	}
}

// reload reloads the schema from the underlying mysql
func (hs *healthStreamer) reload() error {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	// Schema Reload to happen only on primary when it is serving.
	// We can be in a state when the primary is not serving after we have run DemotePrimary. In that case,
	// we don't want to run any queries in MySQL, so we shouldn't reload anything in the healthStreamer.
	if !hs.isServingPrimary {
		return nil
	}

	ctx := hs.ctx
	conn, err := hs.conns.Get(ctx, nil)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	tables, err := hs.getChangedTableNames(ctx, conn)
	if err != nil {
		return err
	}

	views, err := hs.getChangedViewNames(ctx, conn)
	if err != nil {
		return err
	}

	// no change detected
	if len(tables) == 0 && len(views) == 0 {
		return nil
	}

	hs.state.RealtimeStats.TableSchemaChanged = tables
	hs.state.RealtimeStats.ViewSchemaChanged = views
	shr := proto.Clone(hs.state).(*querypb.StreamHealthResponse)
	hs.broadCastToClients(shr)
	hs.state.RealtimeStats.TableSchemaChanged = nil
	hs.state.RealtimeStats.ViewSchemaChanged = nil

	return nil
}

func (hs *healthStreamer) getChangedTableNames(ctx context.Context, conn *connpool.DBConn) ([]string, error) {
	var tables []string
	var tableNames []string

	callback := func(qr *sqltypes.Result) error {
		for _, row := range qr.Rows {
			table := row[0].ToString()
			tables = append(tables, table)

			escapedTblName := sqlparser.String(sqlparser.NewStrLiteral(table))
			tableNames = append(tableNames, escapedTblName)
		}

		return nil
	}
	alloc := func() *sqltypes.Result { return &sqltypes.Result{} }
	bufferSize := 1000

	schemaChangeQuery := mysql.DetectSchemaChange
	// If views are enabled, then views are tracked/handled separately and schema change does not need to track them.
	if hs.viewsEnabled {
		schemaChangeQuery = mysql.DetectSchemaChangeOnlyBaseTable
	}
	err := conn.Stream(ctx, schemaChangeQuery, callback, alloc, bufferSize, 0)
	if err != nil {
		return nil, err
	}

	// If no change detected, then return
	if len(tables) == 0 {
		return nil, nil
	}

	tableNamePredicate := fmt.Sprintf("table_name IN (%s)", strings.Join(tableNames, ", "))
	del := fmt.Sprintf("%s AND %s", mysql.ClearSchemaCopy, tableNamePredicate)
	upd := fmt.Sprintf("%s AND %s", mysql.InsertIntoSchemaCopy, tableNamePredicate)

	// Reload the schema in a transaction.
	_, err = conn.Exec(ctx, "begin", 1, false)
	if err != nil {
		return nil, err
	}
	defer conn.Exec(ctx, "rollback", 1, false)

	_, err = conn.Exec(ctx, del, 1, false)
	if err != nil {
		return nil, err
	}

	_, err = conn.Exec(ctx, upd, 1, false)
	if err != nil {
		return nil, err
	}

	_, err = conn.Exec(ctx, "commit", 1, false)
	if err != nil {
		return nil, err
	}
	return tables, nil
}

func (hs *healthStreamer) getChangedViewNames(ctx context.Context, conn *connpool.DBConn) ([]string, error) {
	if !hs.viewsEnabled {
		return nil, nil
	}
	var changedViews []string
	views := map[string]string{}

	callback := func(qr *sqltypes.Result) error {
		for _, row := range qr.Rows {
			viewName := row[0].ToString()
			lastUpdTime := row[1].ToString()
			views[viewName] = lastUpdTime
		}

		return nil
	}
	alloc := func() *sqltypes.Result { return &sqltypes.Result{} }
	bufferSize := 1000
	err := conn.Stream(ctx, mysql.SelectAllViews, callback, alloc, bufferSize, 0)
	if err != nil {
		return nil, err
	}

	// If no change detected, then return
	if len(views) == 0 && len(hs.views) == 0 {
		return nil, nil
	}

	for viewName, lastUpdTime := range views {
		t, exists := hs.views[viewName]
		if !exists { // new view added
			changedViews = append(changedViews, viewName)
			continue
		}
		if t != lastUpdTime { // view updated
			changedViews = append(changedViews, viewName)
		}
		delete(hs.views, viewName)
	}

	// views deleted
	for viewName := range hs.views {
		changedViews = append(changedViews, viewName)
	}

	// update hs.views with latest view info
	hs.views = views

	return changedViews, nil
}
