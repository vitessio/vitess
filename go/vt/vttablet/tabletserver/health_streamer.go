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
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sidecardb"

	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/vt/dbconfigs"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/history"
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
	unhealthyThreshold atomic.Int64

	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	clients map[chan *querypb.StreamHealthResponse]struct{}
	state   *querypb.StreamHealthResponse
	// isServingPrimary stores if this tablet is currently the serving primary or not.
	isServingPrimary bool

	se      *schema.Engine
	history *history.History

	dbConfig               dbconfigs.Connector
	conns                  *connpool.Pool
	signalWhenSchemaChange bool
	reloadTimeout          time.Duration

	viewsEnabled bool
}

func newHealthStreamer(env tabletenv.Env, alias *topodatapb.TabletAlias, engine *schema.Engine) *healthStreamer {
	var pool *connpool.Pool
	if env.Config().SignalWhenSchemaChange {
		// We need one connection for the reloader.
		pool = connpool.NewPool(env, "", tabletenv.ConnPoolConfig{
			Size:               1,
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		})
	}
	hs := &healthStreamer{
		stats:             env.Stats(),
		degradedThreshold: env.Config().Healthcheck.DegradedThresholdSeconds.Get(),
		clients:           make(map[chan *querypb.StreamHealthResponse]struct{}),

		state: &querypb.StreamHealthResponse{
			Target:      &querypb.Target{},
			TabletAlias: alias,
			RealtimeStats: &querypb.RealtimeStats{
				HealthError: errUnintialized,
			},
		},

		history:                history.New(5),
		conns:                  pool,
		signalWhenSchemaChange: env.Config().SignalWhenSchemaChange,
		reloadTimeout:          env.Config().SchemaChangeReloadTimeout,
		viewsEnabled:           env.Config().EnableViews,
		se:                     engine,
	}
	hs.unhealthyThreshold.Store(env.Config().Healthcheck.UnhealthyThresholdSeconds.Get().Nanoseconds())
	return hs
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
	}
}

func (hs *healthStreamer) Close() {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	if hs.cancel != nil {
		hs.se.UnregisterNotifier("healthStreamer")
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
	case sbm > time.Duration(hs.unhealthyThreshold.Load()):
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

func (hs *healthStreamer) SetUnhealthyThreshold(v time.Duration) {
	hs.unhealthyThreshold.Store(v.Nanoseconds())
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

// MakePrimary tells the healthstreamer that the current tablet is now the primary,
// so it can read and write to the MySQL instance for schema-tracking.
func (hs *healthStreamer) MakePrimary(serving bool) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.isServingPrimary = serving
	// We register for notifications from the schema Engine only when schema tracking is enabled,
	// and we are going to a serving primary state.
	if serving && hs.signalWhenSchemaChange {
		hs.se.RegisterNotifier("healthStreamer", func(full map[string]*schema.Table, created, altered, dropped []*schema.Table) {
			if err := hs.reload(full, created, altered, dropped); err != nil {
				log.Errorf("periodic schema reload failed in health stream: %v", err)
			}
		}, false)
	}
}

// MakeNonPrimary tells the healthstreamer that the current tablet is now not a primary.
func (hs *healthStreamer) MakeNonPrimary() {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.isServingPrimary = false
}

// reload reloads the schema from the underlying mysql for the tables that we get the alert on.
func (hs *healthStreamer) reload(full map[string]*schema.Table, created, altered, dropped []*schema.Table) error {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	// Schema Reload to happen only on primary when it is serving.
	// We can be in a state when the primary is not serving after we have run DemotePrimary. In that case,
	// we don't want to run any queries in MySQL, so we shouldn't reload anything in the healthStreamer.
	if !hs.isServingPrimary {
		return nil
	}

	// add a timeout to prevent unbounded waits
	ctx, cancel := context.WithTimeout(hs.ctx, hs.reloadTimeout)
	defer cancel()

	conn, err := hs.conns.Get(ctx, nil)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	// We create lists to store the tables that have schema changes.
	var tables []string
	var views []string

	// Range over the tables that are created/altered and split them up based on their type.
	for _, table := range append(append(dropped, created...), altered...) {
		tableName := table.Name.String()
		if table.Type == schema.View && hs.viewsEnabled {
			views = append(views, tableName)
		} else {
			tables = append(tables, tableName)
		}
	}

	// Reload the tables and views.
	// This stores the data that is used by VTGates upto v17. So, we can remove this reload of
	// tables and views in v19.
	err = hs.reloadTables(ctx, conn, tables)
	if err != nil {
		return err
	}
	err = hs.reloadViews(ctx, conn, views)
	if err != nil {
		if len(tables) == 0 {
			return err
		}
		// there are some tables, we need to send that in the health stream.
		// making views to nil will prevent any views notification.
		views = nil
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

func (hs *healthStreamer) reloadTables(ctx context.Context, conn *connpool.DBConn, tableNames []string) error {
	if len(tableNames) == 0 {
		return nil
	}
	var escapedTableNames []string
	for _, tableName := range tableNames {
		escapedTblName := sqlparser.String(sqlparser.NewStrLiteral(tableName))
		escapedTableNames = append(escapedTableNames, escapedTblName)
	}

	tableNamePredicate := fmt.Sprintf("table_name IN (%s)", strings.Join(escapedTableNames, ", "))
	del := fmt.Sprintf("%s AND %s", sqlparser.BuildParsedQuery(mysql.ClearSchemaCopy, sidecardb.GetIdentifier()).Query, tableNamePredicate)
	upd := fmt.Sprintf("%s AND %s", sqlparser.BuildParsedQuery(mysql.InsertIntoSchemaCopy, sidecardb.GetIdentifier()).Query, tableNamePredicate)

	// Reload the schema in a transaction.
	_, err := conn.Exec(ctx, "begin", 1, false)
	if err != nil {
		return err
	}
	defer conn.Exec(ctx, "rollback", 1, false)

	_, err = conn.Exec(ctx, del, 1, false)
	if err != nil {
		return err
	}

	_, err = conn.Exec(ctx, upd, 1, false)
	if err != nil {
		return err
	}

	_, err = conn.Exec(ctx, "commit", 1, false)
	if err != nil {
		return err
	}
	return nil
}

type viewDefAndStmt struct {
	def  string
	stmt string
}

func (hs *healthStreamer) reloadViews(ctx context.Context, conn *connpool.DBConn, views []string) error {
	if len(views) == 0 {
		return nil
	}

	/* Retrieve changed views definition */
	viewsDefStmt := map[string]*viewDefAndStmt{}
	callback := func(qr *sqltypes.Result) error {
		for _, row := range qr.Rows {
			viewsDefStmt[row[0].ToString()] = &viewDefAndStmt{def: row[1].ToString()}
		}
		return nil
	}
	alloc := func() *sqltypes.Result { return &sqltypes.Result{} }
	bufferSize := 1000

	viewsBV, err := sqltypes.BuildBindVariable(views)
	if err != nil {
		return err
	}
	bv := map[string]*querypb.BindVariable{"tableNames": viewsBV}

	err = hs.getViewDefinition(ctx, conn, bv, callback, alloc, bufferSize)
	if err != nil {
		return err
	}

	/* Retrieve create statement for views */
	viewsDefStmt, err = hs.getCreateViewStatement(ctx, conn, viewsDefStmt)
	if err != nil {
		return err
	}

	/* update the views copy table */
	err = hs.updateViewsTable(ctx, conn, bv, viewsDefStmt)
	return err
}

func (hs *healthStreamer) getViewDefinition(ctx context.Context, conn *connpool.DBConn, bv map[string]*querypb.BindVariable, callback func(qr *sqltypes.Result) error, alloc func() *sqltypes.Result, bufferSize int) error {
	var viewsDefQuery string
	viewsDefQuery = sqlparser.BuildParsedQuery(mysql.FetchViewDefinition).Query

	stmt, err := sqlparser.Parse(viewsDefQuery)
	if err != nil {
		return err
	}
	viewsDefQuery, err = planbuilder.GenerateFullQuery(stmt).GenerateQuery(bv, nil)
	if err != nil {
		return err
	}
	return conn.Stream(ctx, viewsDefQuery, callback, alloc, bufferSize, 0)
}

func (hs *healthStreamer) getCreateViewStatement(ctx context.Context, conn *connpool.DBConn, viewsDefStmt map[string]*viewDefAndStmt) (map[string]*viewDefAndStmt, error) {
	for k, v := range viewsDefStmt {
		res, err := conn.Exec(ctx, sqlparser.BuildParsedQuery(mysql.FetchCreateStatement, k).Query, 1, false)
		if err != nil {
			return nil, err
		}
		for _, row := range res.Rows {
			v.stmt = row[1].ToString()
		}
	}
	return viewsDefStmt, nil
}

func (hs *healthStreamer) updateViewsTable(ctx context.Context, conn *connpool.DBConn, bv map[string]*querypb.BindVariable, viewsDefStmt map[string]*viewDefAndStmt) error {
	stmt, err := sqlparser.Parse(
		sqlparser.BuildParsedQuery(mysql.DeleteFromViewsTable, sidecardb.GetIdentifier()).Query)
	if err != nil {
		return err
	}
	clearViewQuery, err := planbuilder.GenerateFullQuery(stmt).GenerateQuery(bv, nil)
	if err != nil {
		return err
	}

	stmt, err = sqlparser.Parse(
		sqlparser.BuildParsedQuery(mysql.InsertIntoViewsTable, sidecardb.GetIdentifier()).Query)
	if err != nil {
		return err
	}
	insertViewsParsedQuery := planbuilder.GenerateFullQuery(stmt)

	// Reload the views in a transaction.
	_, err = conn.Exec(ctx, "begin", 1, false)
	if err != nil {
		return err
	}
	defer conn.Exec(ctx, "rollback", 1, false)

	_, err = conn.Exec(ctx, clearViewQuery, 1, false)
	if err != nil {
		return err
	}

	for k, v := range viewsDefStmt {
		bv["table_name"] = sqltypes.StringBindVariable(k)
		bv["create_statement"] = sqltypes.StringBindVariable(v.stmt)
		bv["view_definition"] = sqltypes.StringBindVariable(v.def)
		insertViewQuery, err := insertViewsParsedQuery.GenerateQuery(bv, nil)
		if err != nil {
			return err
		}
		_, err = conn.Exec(ctx, insertViewQuery, 1, false)
		if err != nil {
			return err
		}
	}

	_, err = conn.Exec(ctx, "commit", 1, false)
	return err
}
