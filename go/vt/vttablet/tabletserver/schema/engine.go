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

package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	maps0 "maps"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	maxTableCount         = 10000
	maxPartitionsPerTable = 8192
	maxIndexesPerTable    = 64

	// gtidExecutedOptimizeDataFreeThresholdBytes is the amount of
	// reclaimable free space (information_schema.TABLES.DATA_FREE) on
	// mysql.gtid_executed above which we'll OPTIMIZE the table on a
	// non-primary tablet. This is free space, not total size: a table that
	// is 1 GiB total but only 10 MiB of which is DATA_FREE is left alone,
	// while one that is 300 MiB total with 200 MiB of DATA_FREE triggers
	// OPTIMIZE. The free-space signal tracks bloat accumulation from
	// high-churn replication tracking, which is the failure mode this
	// feature prevents.
	gtidExecutedOptimizeDataFreeThresholdBytes = 128 * 1024 * 1024 // 128 MiB
	// gtidExecutedOptimizeInterval caps how often we'll run OPTIMIZE per tablet.
	gtidExecutedOptimizeInterval = 24 * time.Hour
	// gtidExecutedOptimizeTimeout bounds a single OPTIMIZE invocation. In
	// normal operation it completes in well under a second; 20s is an outlier;
	// 60s means something pathological is happening and we abort so we don't
	// leave a long-running admin statement in flight indefinitely.
	gtidExecutedOptimizeTimeout = 60 * time.Second
	// tabletTypeStabilityCooldown is how long after a transition between
	// PRIMARY and non-PRIMARY we'll defer OPTIMIZE: a reparent (PRS/ERS)
	// flips the type, and we don't want to kick off background admin work
	// while the tablet's role is still settling.
	tabletTypeStabilityCooldown = 5 * time.Minute
)

type notifier func(full map[string]*Table, created, altered, dropped []*Table, udfsChanged bool)

// Engine stores the schema info and performs operations that
// keep itself up-to-date.
type Engine struct {
	env tabletenv.Env
	cp  dbconfigs.Connector

	// mu protects the following fields.
	mu         sync.Mutex
	isOpen     bool
	tables     map[string]*Table
	lastChange int64
	// the position at which the schema was last loaded. it is only used in conjunction with ReloadAt
	reloadAtPos replication.Position
	notifierMu  sync.Mutex
	notifiers   map[string]notifier
	// isServingPrimary stores if this tablet is currently the serving primary or not.
	isServingPrimary bool
	// isPrimaryTablet records whether this tablet is of type PRIMARY,
	// regardless of whether it is currently serving. This is distinct from
	// isServingPrimary, which only flips when the tablet moves between
	// serving and non-serving primary states. Used by the OPTIMIZE paths so
	// we never run background admin work against a primary tablet — even a
	// primary-not-serving one (e.g. during an orderly transition).
	isPrimaryTablet bool
	// tabletTypeLastChangedAt records the wall-clock time at which the
	// tablet most recently transitioned between PRIMARY and non-PRIMARY.
	// Used by the OPTIMIZE paths to avoid kicking off background admin
	// work while a PRS/ERS may still be settling.
	tabletTypeLastChangedAt time.Time
	// gtidExecutedOptimizeLastAt records when the background OPTIMIZE of
	// mysql.gtid_executed last ran successfully on this tablet (throttling).
	gtidExecutedOptimizeLastAt time.Time
	backgroundCtx              context.Context
	backgroundCancel           context.CancelFunc
	// surfacedFreeSpaceTables tracks which user tables we last set on the
	// tableDataFreeBytes gauge, so we can Reset labels for tables that have
	// dropped below the threshold and avoid stale entries.
	surfacedFreeSpaceTables map[string]struct{}
	// schemaCopy stores if the user has requested signals on schema changes. If they have, then we
	// also track the underlying schema and make a copy of it in our MySQL instance.
	schemaCopy bool

	// SkipMetaCheck skips the metadata about the database and table information
	SkipMetaCheck bool

	historian *historian

	conns           *connpool.Pool
	ticks           *timer.Timer
	reloadTimeout   time.Duration
	throttledLogger *logutil.ThrottledLogger

	// dbCreationFailed is for preventing log spam.
	dbCreationFailed bool

	tableFileSizeGauge           *stats.GaugesWithSingleLabel
	tableAllocatedSizeGauge      *stats.GaugesWithSingleLabel
	tableRowsGauge               *stats.GaugesWithSingleLabel
	tableClusteredIndexSizeGauge *stats.GaugesWithSingleLabel
	// tableDataFreeBytes exposes DATA_FREE (bytes reclaimable via OPTIMIZE)
	// per user table for tables whose DATA_FREE exceeds
	// SchemaUserTablesFreeSpacePercentThreshold of the table's total
	// allocated space. Only populated when the threshold is non-zero and a
	// table is currently exceeding it; labels are reset when a table drops
	// below it or is removed.
	tableDataFreeBytes *stats.GaugesWithSingleLabel

	indexCardinalityGauge *stats.GaugesWithMultiLabels
	indexBytesGauge       *stats.GaugesWithMultiLabels

	innoDbReadRowsCounter *stats.Counter
	SchemaReloadTimings   *servenv.TimingsWrapper
}

// NewEngine creates a new Engine.
func NewEngine(env tabletenv.Env) *Engine {
	reloadTime := env.Config().SchemaReloadInterval
	backgroundCtx, backgroundCancel := context.WithCancel(context.Background())
	se := &Engine{
		env: env,
		// We need three connections: one for the reloader, one for
		// the historian, and one for the tracker.
		conns: connpool.NewPool(env, "", tabletenv.ConnPoolConfig{
			Size:        3,
			IdleTimeout: env.Config().OltpReadPool.IdleTimeout,
		}),
		ticks:                   timer.NewTimer(reloadTime),
		throttledLogger:         logutil.NewThrottledLogger("schema-tracker", 1*time.Minute),
		tabletTypeLastChangedAt: time.Now(),
		backgroundCtx:           backgroundCtx,
		backgroundCancel:        backgroundCancel,
	}
	se.schemaCopy = env.Config().SignalWhenSchemaChange
	_ = env.Exporter().NewGaugeDurationFunc("SchemaReloadTime", "vttablet keeps table schemas in its own memory and periodically refreshes it from MySQL. This config controls the reload time.", se.ticks.Interval)
	se.tableFileSizeGauge = env.Exporter().NewGaugesWithSingleLabel("TableFileSize", "tracks table file size", "Table")
	se.tableAllocatedSizeGauge = env.Exporter().NewGaugesWithSingleLabel("TableAllocatedSize", "tracks table allocated size", "Table")
	se.tableRowsGauge = env.Exporter().NewGaugesWithSingleLabel("TableRows", "estimated number of rows in the table", "Table")
	se.tableClusteredIndexSizeGauge = env.Exporter().NewGaugesWithSingleLabel("TableClusteredIndexSize", "byte size of the clustered index (i.e. row data)", "Table")
	se.tableDataFreeBytes = env.Exporter().NewGaugesWithSingleLabel("SchemaTableDataFreeBytes", "DATA_FREE bytes (reclaimable via OPTIMIZE) for user tables whose free-space ratio exceeds --schema-user-tables-free-space-percent-threshold", "Table")
	se.indexCardinalityGauge = env.Exporter().NewGaugesWithMultiLabels("IndexCardinality", "estimated number of unique values in the index", []string{"Table", "Index"})
	se.indexBytesGauge = env.Exporter().NewGaugesWithMultiLabels("IndexBytes", "byte size of the index", []string{"Table", "Index"})
	se.innoDbReadRowsCounter = env.Exporter().NewCounter("InnodbRowsRead", "number of rows read by mysql")
	se.SchemaReloadTimings = env.Exporter().NewTimings("SchemaReload", "time taken to reload the schema", "type")
	se.reloadTimeout = env.Config().SchemaChangeReloadTimeout
	env.Exporter().HandleFunc("/debug/schema", se.handleDebugSchema)
	env.Exporter().HandleFunc("/schemaz", func(w http.ResponseWriter, r *http.Request) {
		// Ensure schema engine is Open. If vttablet came up in a non_serving role,
		// the schema engine may not have been initialized.
		err := se.Open()
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}

		schemazHandler(se.GetSchema(), w, r)
	})
	se.historian = newHistorian(env.Config().TrackSchemaVersions, env.Config().SchemaVersionMaxAgeSeconds, se.conns)
	return se
}

// InitDBConfig must be called before Open.
func (se *Engine) InitDBConfig(cp dbconfigs.Connector) {
	se.cp = cp
}

// syncSidecarDB is called either the first time a primary starts, or
// on subsequent loads, to possibly upgrade to a new Vitess version.
// This is the only entry point into the sidecardb module to get the
// sidecar database to the desired schema for the running Vitess
// version. There is some extra logging in here which can be removed
// in a future version (>v16) once the new schema init functionality
// is stable.
func (se *Engine) syncSidecarDB(ctx context.Context, conn *dbconnpool.DBConnection) error {
	log.Info("In syncSidecarDB")
	defer func(start time.Time) {
		log.Info(fmt.Sprintf("syncSidecarDB took %d ms", time.Since(start).Milliseconds()))
	}(time.Now())

	var exec sidecardb.Exec = func(ctx context.Context, query string, maxRows int, useDB bool) (*sqltypes.Result, error) {
		if useDB {
			_, err := conn.ExecuteFetch(sqlparser.BuildParsedQuery("use %s", sidecar.GetIdentifier()).Query, maxRows, false)
			if err != nil {
				return nil, err
			}
		}
		return conn.ExecuteFetch(query, maxRows, true)
	}
	if err := sidecardb.Init(ctx, se.env.Environment(), exec); err != nil {
		log.Error(fmt.Sprintf("Error in sidecardb.Init: %+v", err))
		if se.env.Config().DB.HasGlobalSettings() {
			log.Warn("Ignoring sidecardb.Init error for unmanaged tablets")
			return nil
		}
		log.Error(fmt.Sprintf("syncSidecarDB error %+v", err))
		return err
	}
	log.Info("syncSidecarDB done")
	return nil
}

// EnsureConnectionAndDB ensures that we can connect to mysql.
// If tablet type is primary and there is no db, then the database is created.
// This function can be called before opening the Engine.
func (se *Engine) EnsureConnectionAndDB(tabletType topodatapb.TabletType, serving bool) error {
	ctx := tabletenv.LocalContext()
	// We use AllPrivs since syncSidecarDB() might need to upgrade the schema
	conn, err := dbconnpool.NewDBConnection(ctx, se.env.Config().DB.AllPrivsWithDB())
	if err == nil {
		se.dbCreationFailed = false
		// upgrade sidecar db if required, for a tablet with an existing database
		// only run DDL updates when a PRIMARY is transitioning to serving state.
		if tabletType == topodatapb.TabletType_PRIMARY && serving {
			if err := se.syncSidecarDB(ctx, conn); err != nil {
				conn.Close()
				return err
			}
		}
		conn.Close()
		return nil
	}
	if tabletType != topodatapb.TabletType_PRIMARY || !serving {
		return err
	}
	if merr, isSQLErr := err.(*sqlerror.SQLError); !isSQLErr || merr.Num != sqlerror.ERBadDb {
		return err
	}

	// We are primary and db is not found. Let's create it.
	// We use allprivs instead of DBA because we want db create to fail if we're read-only.
	conn, err = dbconnpool.NewDBConnection(ctx, se.env.Config().DB.AllPrivsConnector())
	if err != nil {
		return err
	}
	defer conn.Close()

	dbname := se.env.Config().DB.DBName
	_, err = conn.ExecuteFetch(fmt.Sprintf("create database if not exists `%s`", dbname), 1, false)
	if err != nil {
		if !se.dbCreationFailed {
			// This is the first failure.
			log.Error(fmt.Sprintf("db creation failed for %v: %v, will keep retrying", dbname, err))
			se.dbCreationFailed = true
		}
		return err
	}

	log.Info(fmt.Sprintf("db %v created", dbname))
	se.dbCreationFailed = false
	// creates sidecar schema, the first time the database is created
	if err := se.syncSidecarDB(ctx, conn); err != nil {
		return err
	}
	return nil
}

// Open initializes the Engine. Calling Open on an already
// open engine is a no-op.
func (se *Engine) Open() error {
	se.mu.Lock()
	defer se.mu.Unlock()
	if se.isOpen {
		return nil
	}
	se.ensureBackgroundContextLocked()
	log.Info("Schema Engine: opening")

	ctx := tabletenv.LocalContext()

	// The function we're in is supposed to be idempotent, but this conns.Open()
	// call is not itself idempotent. Therefore, if we return for any reason
	// without marking ourselves as open, we need to call conns.Close() so the
	// pools aren't leaked the next time we call Open().
	se.conns.Open(se.cp, se.cp, se.cp)
	defer func() {
		if !se.isOpen {
			se.conns.Close()
		}
	}()

	se.tables = map[string]*Table{
		"dual": NewTable("dual", NoType),
	}
	se.notifiers = make(map[string]notifier)

	if err := se.reload(ctx, false); err != nil {
		return err
	}
	if !se.SkipMetaCheck {
		if err := se.historian.Open(); err != nil {
			return err
		}
	}

	se.ticks.Start(func() {
		// update stats on periodic reloads
		if err := se.reloadAndIncludeStats(ctx); err != nil {
			log.Error(fmt.Sprintf("periodic schema reload failed: %v", err))
		}
	})

	se.isOpen = true
	return nil
}

// IsOpen checks if engine is open
func (se *Engine) IsOpen() bool {
	se.mu.Lock()
	defer se.mu.Unlock()
	return se.isOpen
}

// Close shuts down Engine and is idempotent.
// It can be re-opened after Close.
func (se *Engine) Close() {
	se.mu.Lock()
	if !se.isOpen {
		se.mu.Unlock()
		return
	}

	se.closeLocked()
	log.Info("Schema Engine: closed")
}

// closeLocked closes the schema engine. It is meant to be called after locking the mutex of the schema engine.
// It also unlocks the engine when it returns.
func (se *Engine) closeLocked() {
	if se.backgroundCancel != nil {
		se.backgroundCancel()
	}

	// Close the Timer in a separate go routine because
	// there might be a tick after we have acquired the lock above
	// but before closing the timer, in which case Stop function will wait for the
	// configured function to complete running and that function (ReloadAt) will block
	// on the lock we have already acquired
	wg := sync.WaitGroup{}
	wg.Go(func() {
		se.ticks.Stop()
	})
	se.historian.Close()
	se.conns.Close()

	se.tables = make(map[string]*Table)
	se.lastChange = 0
	se.notifiers = make(map[string]notifier)
	se.isOpen = false

	// Unlock the mutex. If there is a tick blocked on this lock,
	// then it will run and we wait for the Stop function to finish its execution
	se.mu.Unlock()
	wg.Wait()
}

// MakeNonPrimary clears the sequence caches to make sure that
// they don't get accidentally reused after losing primaryship.
func (se *Engine) MakeNonPrimary() {
	// This function is tested through endtoend test.
	se.mu.Lock()
	defer se.mu.Unlock()
	if se.isPrimaryTablet {
		se.tabletTypeLastChangedAt = time.Now()
	}
	se.isPrimaryTablet = false
	se.isServingPrimary = false
	for _, t := range se.tables {
		if t.SequenceInfo != nil {
			t.SequenceInfo.Reset()
		}
	}
}

// MakePrimary tells the schema engine that the current tablet is now the primary,
// so it can read and write to the MySQL instance for schema-tracking.
func (se *Engine) MakePrimary(serving bool) {
	se.mu.Lock()
	defer se.mu.Unlock()
	if !se.isPrimaryTablet {
		se.tabletTypeLastChangedAt = time.Now()
	}
	se.isPrimaryTablet = true
	se.isServingPrimary = serving
}

// shouldAttemptGtidExecutedOptimizeLocked reports whether we should run the
// mysql.gtid_executed OPTIMIZE gating-query+spawn sequence right now. It
// checks that the tablet is not of type PRIMARY (including a
// primary-not-serving tablet mid-transition), that the tablet-role
// stability cooldown has elapsed, and that the per-tablet 24h throttle has
// elapsed. Caller must hold se.mu.
func (se *Engine) shouldAttemptGtidExecutedOptimizeLocked(now time.Time) bool {
	if !se.isGtidExecutedOptimizeEligibleLocked(now) {
		return false
	}
	if !se.gtidExecutedOptimizeLastAt.IsZero() && now.Sub(se.gtidExecutedOptimizeLastAt) < gtidExecutedOptimizeInterval {
		return false
	}
	return true
}

func (se *Engine) isGtidExecutedOptimizeEligibleLocked(now time.Time) bool {
	if se.isPrimaryTablet {
		return false
	}
	if !se.tabletTypeLastChangedAt.IsZero() && now.Sub(se.tabletTypeLastChangedAt) < tabletTypeStabilityCooldown {
		return false
	}
	return true
}

func (se *Engine) ensureBackgroundContextLocked() {
	if se.backgroundCtx != nil && se.backgroundCtx.Err() == nil {
		return
	}
	se.backgroundCtx, se.backgroundCancel = context.WithCancel(context.Background())
}

func (se *Engine) backgroundTimeoutContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	se.mu.Lock()
	backgroundCtx := se.backgroundCtx
	se.mu.Unlock()
	if backgroundCtx == nil {
		backgroundCtx = context.Background()
	}
	return context.WithTimeout(backgroundCtx, timeout)
}

// EnableHistorian forces tracking to be on or off.
// Only used for testing.
func (se *Engine) EnableHistorian(enabled bool) error {
	return se.historian.Enable(enabled)
}

// Reload reloads the schema info from the db.
// Any tables that have changed since the last load are updated.
// The includeStats argument controls whether table size statistics should be
// emitted, as they can be expensive to calculate for a large number of tables
func (se *Engine) Reload(ctx context.Context) error {
	return se.ReloadAt(ctx, replication.Position{})
}

// reloadAndIncludeStats calls the ReloadAtEx function with includeStats set to true.
func (se *Engine) reloadAndIncludeStats(ctx context.Context) error {
	return se.ReloadAtEx(ctx, replication.Position{}, true)
}

// ReloadAt reloads the schema info from the db.
// Any tables that have changed since the last load are updated.
// It maintains the position at which the schema was reloaded and if the same position is provided
// (say by multiple vstreams) it returns the cached schema. In case of a newer or empty pos it always reloads the schema
func (se *Engine) ReloadAt(ctx context.Context, pos replication.Position) error {
	return se.ReloadAtEx(ctx, pos, false)
}

// ReloadAtEx reloads the schema info from the db.
// Any tables that have changed since the last load are updated.
// It maintains the position at which the schema was reloaded and if the same position is provided
// (say by multiple vstreams) it returns the cached schema. In case of a newer or empty pos it always reloads the schema
// The includeStats argument controls whether table size statistics should be
// emitted, as they can be expensive to calculate for a large number of tables
func (se *Engine) ReloadAtEx(ctx context.Context, pos replication.Position, includeStats bool) error {
	se.mu.Lock()
	defer se.mu.Unlock()
	if !se.isOpen {
		log.Warn("Schema reload called for an engine that is not yet open")
		return nil
	}
	if !pos.IsZero() && se.reloadAtPos.AtLeast(pos) {
		log.V(2).Info("ReloadAtEx: found cached schema at " + replication.EncodePosition(pos))
		return nil
	}
	if err := se.reload(ctx, includeStats); err != nil {
		return err
	}
	se.reloadAtPos = pos
	return nil
}

func populateInnoDBStats(ctx context.Context, conn *connpool.Conn) (map[string]*Table, error) {
	innodbTableSizesQuery := conn.BaseShowInnodbTableSizes()
	if innodbTableSizesQuery == "" {
		return nil, nil
	}

	innodbResults, err := conn.Exec(ctx, innodbTableSizesQuery, maxTableCount*maxPartitionsPerTable, false)
	if err != nil {
		return nil, vterrors.Wrapf(err, "in Engine.reload(), reading innodb tables")
	}
	innodbTablesStats := make(map[string]*Table, len(innodbResults.Rows))
	for _, row := range innodbResults.Rows {
		innodbTableName := row[0].ToString() // In the form of encoded `schema/table`
		fileSize, _ := row[1].ToCastUint64()
		allocatedSize, _ := row[2].ToCastUint64()

		if _, ok := innodbTablesStats[innodbTableName]; !ok {
			innodbTablesStats[innodbTableName] = &Table{}
		}
		// There could be multiple appearances of the same table in the result set:
		// A table that has FULLTEXT indexes will appear once for the table itself,
		// with total size of row data, and once for the aggregates size of all
		// FULLTEXT indexes. We aggregate the sizes of all appearances of the same table.
		table := innodbTablesStats[innodbTableName]
		table.FileSize += fileSize
		table.AllocatedSize += allocatedSize

		if originalTableName, _, found := strings.Cut(innodbTableName, "#p#"); found {
			// innodbTableName is encoded any special characters are turned into some @0-f0-f0-f value.
			// Therefore this "#p#" here is a clear indication that we are looking at a partitioned table.
			// We turn `my@002ddb/tbl_part#p#p0` into `my@002ddb/tbl_part`
			// and aggregate the total partition sizes.
			if _, ok := innodbTablesStats[originalTableName]; !ok {
				innodbTablesStats[originalTableName] = &Table{}
			}
			originalTable := innodbTablesStats[originalTableName]
			originalTable.FileSize += fileSize
			originalTable.AllocatedSize += allocatedSize
		}
	}
	// See testing in TestEngineReload
	return innodbTablesStats, nil
}

// gtidExecutedDataFreeQuery reads the reclaimable free space
// (DATA_FREE) of mysql.gtid_executed from information_schema.TABLES.
// Separate from populateInnoDBStats because that query scopes to the
// tablet's user database; mysql.gtid_executed lives in the mysql schema.
const gtidExecutedDataFreeQuery = `select data_free from information_schema.TABLES where table_schema = 'mysql' and table_name = 'gtid_executed'`

// optimizeGtidExecutedStatement is run on non-primary tablets to reclaim
// free space on mysql.gtid_executed. NO_WRITE_TO_BINLOG keeps this admin
// statement off the binlog so it does not propagate to other replicas.
const optimizeGtidExecutedStatement = `OPTIMIZE NO_WRITE_TO_BINLOG TABLE mysql.gtid_executed`

// maybeOptimizeGtidExecutedLocked checks mysql.gtid_executed's reclaimable
// free space (DATA_FREE) and, on a non-primary tablet that meets the
// throttling and stability conditions, kicks off the OPTIMIZE in a
// background goroutine so neither reload nor a subsequent
// MakePrimary/MakeNonPrimary waits on the admin statement. The OPTIMIZE
// itself is bounded by gtidExecutedOptimizeTimeout so we don't leave a
// pathological invocation in flight.
//
// The background goroutine intentionally re-checks isPrimaryTablet under
// se.mu immediately before executing: we do NOT run OPTIMIZE if the tablet
// has been promoted to PRIMARY, even if the state flipped after we spawned.
//
// Caller must hold se.mu — this function is called from reload(), which
// holds se.mu for its entire duration, so we deliberately avoid
// re-acquiring the lock here (Go mutexes are not re-entrant).
func (se *Engine) maybeOptimizeGtidExecutedLocked(ctx context.Context, conn *connpool.Conn) {
	if !se.shouldAttemptGtidExecutedOptimizeLocked(time.Now()) {
		return
	}

	result, err := conn.Exec(ctx, gtidExecutedDataFreeQuery, 1, false)
	if err != nil {
		se.throttledLogger.Warningf("schema engine: reading mysql.gtid_executed DATA_FREE failed: %v", err)
		return
	}
	if len(result.Rows) == 0 {
		// Table not present (e.g. not created yet).
		return
	}
	dataFree, err := result.Rows[0][0].ToCastUint64()
	if err != nil {
		se.throttledLogger.Warningf("schema engine: parsing mysql.gtid_executed DATA_FREE failed: %v", err)
		return
	}
	if dataFree <= gtidExecutedOptimizeDataFreeThresholdBytes {
		return
	}

	// Stamp the start time before spawning so we won't launch again until
	// the 24h throttle expires. The goroutine below also refreshes this
	// stamp on its own failure paths, so a persistently-failing OPTIMIZE
	// does not retry on every schema reload (which would flip
	// super_read_only briefly on each attempt).
	se.gtidExecutedOptimizeLastAt = time.Now()

	go se.runOptimizeGtidExecuted(dataFree)
}

// disableSuperReadOnly turns off @@global.super_read_only if it is currently
// on (which it will be for a healthy non-primary tablet). It returns a restore
// function that the caller MUST invoke via defer to re-enable
// super_read_only regardless of how the OPTIMIZE returns (success, error, or
// panic). The returned restore function is a no-op when super_read_only was
// already off or when the MySQL flavor does not support super_read_only (e.g.
// MariaDB), so deferring it unconditionally is always safe.
//
// The restore opens its own fresh DBA connection rather than reusing the
// OPTIMIZE conn. This matters because the OPTIMIZE conn may have been
// closed by the time restore fires — notably the executeFetchCtx KILL path
// closes the conn after timing out, which would strand the tablet
// super_read_only=OFF if restore depended on that handle. super_read_only
// is a GLOBAL variable, so flipping it from a different session is
// equivalent.
//
// MariaDB and some older MySQL versions don't support super_read_only; in
// that case we return ERUnknownSystemVariable from the initial read and
// treat it as "nothing to do" (no-op restore). Any other failure to read or
// to flip the value returns an error so the caller can skip OPTIMIZE.
func (se *Engine) disableSuperReadOnly(conn *dbconnpool.DBConnection) (restore func(), err error) {
	noop := func() {}
	result, err := conn.ExecuteFetch("SELECT @@global.super_read_only", 1, false)
	if err != nil {
		if sqlErr, ok := err.(*sqlerror.SQLError); ok && sqlErr.Number() == sqlerror.ERUnknownSystemVariable {
			return noop, nil
		}
		return noop, fmt.Errorf("reading @@global.super_read_only: %w", err)
	}
	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		return noop, nil
	}
	wasOn := result.Rows[0][0].ToString()
	if wasOn != "1" && wasOn != "ON" {
		return noop, nil
	}
	if _, err := conn.ExecuteFetch("SET GLOBAL super_read_only = 'OFF'", 1, false); err != nil {
		return noop, fmt.Errorf("disabling super_read_only: %w", err)
	}
	return se.restoreSuperReadOnly, nil
}

// restoreSuperReadOnly re-enables @@global.super_read_only on a fresh
// DBA connection (independent of any connection the caller may have been
// using for OPTIMIZE). Called via defer from disableSuperReadOnly's restore
// closure. Failures are logged loudly but not returned — nothing actionable
// the caller can do at this point except alert.
//
// If the tablet has since been promoted to PRIMARY (e.g. a PRS/ERS landed
// while OPTIMIZE was running), we skip the restore: the promotion flow
// itself turns super_read_only OFF and we must not stomp that back ON and
// leave a freshly-promoted primary read-only.
func (se *Engine) restoreSuperReadOnly() {
	se.mu.Lock()
	promoted := se.isPrimaryTablet
	se.mu.Unlock()
	if promoted {
		log.Warn("schema engine: skipping super_read_only restore; tablet has been promoted to PRIMARY since OPTIMIZE started")
		return
	}

	// SRO restore is safety-critical cleanup: leaving a non-primary tablet
	// with super_read_only=OFF can let stale or misrouted writes land on it
	// after shutdown while MySQL is still up. Derive the timeout from
	// context.Background() rather than se.backgroundCtx so that
	// Engine.Close() — which cancels backgroundCtx — cannot suppress the
	// restore.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := dbconnpool.NewDBConnection(ctx, se.env.Config().DB.DbaWithDB())
	if err != nil {
		log.Warn("schema engine: CRITICAL: failed to open connection to re-enable super_read_only after OPTIMIZE; tablet may now accept writes — investigate immediately",
			slog.Any("error", err))
		return
	}
	defer conn.Close()
	if _, err := conn.ExecuteFetch("SET GLOBAL super_read_only = 'ON'", 1, false); err != nil {
		log.Warn("schema engine: CRITICAL: failed to re-enable super_read_only after OPTIMIZE; tablet may now accept writes — investigate immediately",
			slog.Any("error", err))
	}
}

// executeFetchCtx runs conn.ExecuteFetch while honouring ctx. If ctx expires
// before the query returns, executeFetchCtx closes the underlying connection
// to interrupt ExecuteFetch (this is the primary kill switch — it doesn't
// depend on being able to open another connection) and fires a best-effort
// KILL <conn.ID> asynchronously from a separate connection. If
// ExecuteFetch still hasn't unwound after a bounded secondary wait, we
// return ctx.Err() rather than blocking the caller indefinitely.
//
// This is necessary because mysql.Conn.ExecuteFetch is not context-aware:
// the caller's context.WithTimeout only bounds connection setup, so a
// pathological OPTIMIZE would otherwise run indefinitely (and keep
// super_read_only OFF longer than intended). Pattern modelled on
// (*Mysqld).executeFetchContext in go/vt/mysqlctl/query.go, hardened to
// not hang if KILL itself cannot be issued.
func (se *Engine) executeFetchCtx(ctx context.Context, conn *dbconnpool.DBConnection, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var (
		result  *sqltypes.Result
		execErr error
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		result, execErr = conn.ExecuteFetch(query, maxrows, wantfields)
	}()

	select {
	case <-done:
		return result, execErr
	case <-ctx.Done():
		// select chooses pseudorandomly when both channels are ready — if
		// the query already completed, return its result.
		select {
		case <-done:
			return result, execErr
		default:
		}

		// Context expired. Capture the connection ID for the async KILL,
		// then close the underlying connection directly — this interrupts
		// ExecuteFetch even when KILL cannot be issued (permissions,
		// server unresponsive, pool exhausted, etc.) and is the primary
		// mechanism we rely on for the timeout to actually hold.
		connID := conn.ID()
		log.Warn("schema engine: OPTIMIZE timed out, interrupting in-flight query",
			slog.Int64("connID", connID), slog.String("query", query))
		conn.Close()

		// Best-effort server-side cleanup; do not block on it.
		go se.killMySQLConn(connID)

		// Bounded secondary wait for ExecuteFetch to unwind after the
		// close. If we still don't see it return, propagate ctx.Err()
		// rather than hanging the caller.
		const postCloseGrace = 5 * time.Second
		timer := time.NewTimer(postCloseGrace)
		defer timer.Stop()
		select {
		case <-done:
			if execErr == nil {
				// Query beat the interruption and returned a result —
				// prefer it over the ctx error.
				return result, nil
			}
			return nil, ctx.Err()
		case <-timer.C:
			log.Warn("schema engine: timed out waiting for interrupted query to unwind; returning ctx error",
				slog.Int64("connID", connID), slog.String("query", query),
				slog.Duration("grace", postCloseGrace))
			return nil, ctx.Err()
		}
	}
}

// killMySQLConn opens a standalone DB connection, bounded by a fresh 10s
// timeout (since the caller's context is likely already expired), and
// issues a KILL statement for the given connection ID.
func (se *Engine) killMySQLConn(connID int64) {
	ctx, cancel := se.backgroundTimeoutContext(10 * time.Second)
	defer cancel()
	killConn, err := dbconnpool.NewDBConnection(ctx, se.env.Config().DB.DbaWithDB())
	if err != nil {
		log.Warn("schema engine: failed to open connection to KILL runaway OPTIMIZE",
			slog.Int64("connID", connID), slog.Any("error", err))
		return
	}
	defer killConn.Close()
	if _, err := killConn.ExecuteFetch(fmt.Sprintf("KILL %d", connID), 1, false); err != nil {
		log.Warn("schema engine: failed to KILL runaway OPTIMIZE conn",
			slog.Int64("connID", connID), slog.Any("error", err))
	}
}

// runOptimizeGtidExecuted is the background goroutine spawned by
// maybeOptimizeGtidExecutedLocked. Uses a standalone DB connection (not se.conns)
// so a pathological run doesn't starve the reloader/historian/tracker slots.
// Temporarily disables super_read_only for the duration of OPTIMIZE (a
// non-primary tablet is normally super_read_only so the OPTIMIZE would
// otherwise be rejected) and restores it via defer.
func (se *Engine) runOptimizeGtidExecuted(dataFreeAtSpawn uint64) {
	ctx, cancel := se.backgroundTimeoutContext(gtidExecutedOptimizeTimeout)
	defer cancel()

	checkEligibility := func() bool {
		se.mu.Lock()
		defer se.mu.Unlock()
		if !se.isGtidExecutedOptimizeEligibleLocked(time.Now()) {
			se.gtidExecutedOptimizeLastAt = time.Time{}
			return false
		}
		return true
	}

	if !checkEligibility() {
		return
	}

	conn, err := dbconnpool.NewDBConnection(ctx, se.env.Config().DB.DbaWithDB())
	if err != nil {
		log.Warn("schema engine: failed to open connection for mysql.gtid_executed OPTIMIZE", slog.Any("error", err))
		se.mu.Lock()
		se.gtidExecutedOptimizeLastAt = time.Now()
		se.mu.Unlock()
		return
	}
	defer conn.Close()

	if !checkEligibility() {
		return
	}

	restoreSRO, err := se.disableSuperReadOnly(conn)
	// Always defer restore before running OPTIMIZE: we re-enable
	// super_read_only no matter how the body of this function exits.
	defer restoreSRO()
	if err != nil {
		log.Warn("schema engine: skipping mysql.gtid_executed OPTIMIZE; could not disable super_read_only",
			slog.Any("error", err))
		se.mu.Lock()
		se.gtidExecutedOptimizeLastAt = time.Now()
		se.mu.Unlock()
		return
	}

	if !checkEligibility() {
		return
	}

	start := time.Now()
	if _, err := se.executeFetchCtx(ctx, conn, optimizeGtidExecutedStatement, 100, false); err != nil {
		elapsed := time.Since(start)
		log.Warn("schema engine: OPTIMIZE of mysql.gtid_executed failed",
			slog.Any("error", err),
			slog.Duration("elapsed", elapsed),
			slog.Uint64("dataFreeAtSpawn", dataFreeAtSpawn),
		)
		se.mu.Lock()
		se.gtidExecutedOptimizeLastAt = time.Now()
		se.mu.Unlock()
		return
	}
	log.Info("schema engine: OPTIMIZE of mysql.gtid_executed completed",
		slog.Duration("elapsed", time.Since(start)),
		slog.Uint64("dataFreeAtSpawn", dataFreeAtSpawn),
	)
}

// userTableFreeSpaceQueryFormat selects user tables whose DATA_FREE
// (bytes reclaimable via OPTIMIZE) is more than the given percentage of
// the table's total allocated space (DATA_LENGTH + INDEX_LENGTH +
// DATA_FREE). The predicate is written as
// `data_free * 100 > (data_length + index_length + data_free) * <percent>`
// to keep the comparison in integer arithmetic.
const userTableFreeSpaceQueryFormat = `select table_name, data_free, data_length + index_length + data_free as total_size from information_schema.TABLES where table_schema = database() and data_free * 100 > (data_length + index_length + data_free) * %d`

// updateUserTableFreeSpaceLocked populates the SchemaTableDataFreeBytes
// gauge for user tables where DATA_FREE exceeds
// SchemaUserTablesFreeSpacePercentThreshold of the table's total allocated
// space, and emits a throttled INFO log summarizing the offenders. Runs on
// all tablet types. When the configured threshold is 0 the feature is
// disabled and this function is a no-op (after clearing any stale gauge
// labels from a prior enabled run).
//
// This path is alerting/visibility only — the schema engine does not run
// OPTIMIZE against user tables. Operators use the metric and log to drive
// whatever remediation is appropriate for their environment.
//
// Caller must hold se.mu — this is called from reload(), which holds se.mu
// for its entire duration. The function reads and writes
// se.surfacedFreeSpaceTables under the caller's lock.
func (se *Engine) updateUserTableFreeSpaceLocked(ctx context.Context, conn *connpool.Conn, percentThreshold int) {
	// The flag is viper-dynamic, so even though TabletConfig.Verify() enforces
	// [0, 100] at startup an operator can still push an out-of-range value via
	// the config file at runtime. Clamp-to-disabled here: a negative value
	// would make the SQL predicate match every table (see integer arithmetic
	// in userTableFreeSpaceQueryFormat) and a value > 100 is nonsensical.
	if percentThreshold < 0 || percentThreshold > 100 {
		se.throttledLogger.Warningf("schema engine: --schema-user-tables-free-space-percent-threshold=%d is out of range [0, 100]; treating as disabled", percentThreshold)
		percentThreshold = 0
	}
	if percentThreshold <= 0 {
		// Clear any gauge labels we might have set while previously enabled.
		for tableName := range se.surfacedFreeSpaceTables {
			se.tableDataFreeBytes.Reset(tableName)
		}
		se.surfacedFreeSpaceTables = nil
		return
	}
	result, err := conn.Exec(ctx, fmt.Sprintf(userTableFreeSpaceQueryFormat, percentThreshold), maxTableCount, false)
	if err != nil {
		se.throttledLogger.Warningf("schema engine: reading user table DATA_FREE failed: %v", err)
		return
	}
	current := make(map[string]uint64, len(result.Rows))
	logSummary := make(map[string]string, len(result.Rows))
	for _, row := range result.Rows {
		tableName := row[0].ToString()
		dataFree, err := row[1].ToCastUint64()
		if err != nil {
			continue
		}
		totalSize, err := row[2].ToCastUint64()
		if err != nil {
			continue
		}
		current[tableName] = dataFree
		se.tableDataFreeBytes.Set(tableName, int64(dataFree))
		if totalSize > 0 {
			logSummary[tableName] = fmt.Sprintf("%d/%d bytes (%.1f%%)", dataFree, totalSize, float64(dataFree)/float64(totalSize)*100)
		} else {
			logSummary[tableName] = fmt.Sprintf("%d bytes", dataFree)
		}
	}
	// Zero out labels for tables that were over the threshold previously but
	// no longer are (dropped, truncated, or OPTIMIZEd out).
	for tableName := range se.surfacedFreeSpaceTables {
		if _, stillOver := current[tableName]; !stillOver {
			se.tableDataFreeBytes.Reset(tableName)
		}
	}
	se.surfacedFreeSpaceTables = make(map[string]struct{}, len(current))
	for tableName := range current {
		se.surfacedFreeSpaceTables[tableName] = struct{}{}
	}
	if len(current) > 0 {
		se.throttledLogger.Infof("schema engine: %d user table(s) have reclaimable free space exceeding %d%% of total size: %v",
			len(current), percentThreshold, logSummary)
	}
}

// reload reloads the schema. It can also be used to initialize it.
func (se *Engine) reload(ctx context.Context, includeStats bool) error {
	start := time.Now()
	defer func() {
		se.env.LogError()
		se.SchemaReloadTimings.Record("SchemaReload", start)
	}()

	// if this flag is set, then we don't need table meta information
	if se.SkipMetaCheck {
		return nil
	}

	// add a timeout to prevent unbounded waits
	ctx, cancel := context.WithTimeout(ctx, se.reloadTimeout)
	defer cancel()

	conn, err := se.conns.Get(ctx, nil)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	// curTime will be saved into lastChange after schema is loaded.
	curTime, err := se.mysqlTime(ctx, conn.Conn)
	if err != nil {
		return err
	}

	var innodbTablesStats map[string]*Table
	if includeStats {
		if innodbTablesStats, err = populateInnoDBStats(ctx, conn.Conn); err != nil {
			return err
		}
		// Since the InnoDB table size query is available to us on this MySQL version, we should use it.
		// We therefore don't want to query for table sizes in getTableData()
		includeStats = false

		if err := se.updateTableIndexMetrics(ctx, conn.Conn); err != nil {
			log.Error(fmt.Sprintf("Updating index/table statistics failed, error: %v", err))
		}
		// Surface user tables with reclaimable free space (all tablet types)
		// and consider OPTIMIZing mysql.gtid_executed / surfaced user tables
		// on non-primary tablets. All of this is best-effort and must not
		// fail the reload. These helpers rely on ReloadAtEx holding se.mu
		// for the lifetime of the reload, and do not re-acquire it (Go
		// mutexes are not re-entrant).
		se.updateUserTableFreeSpaceLocked(ctx, conn.Conn, tabletenv.SchemaUserTablesFreeSpacePercentThreshold())
		se.maybeOptimizeGtidExecutedLocked(ctx, conn.Conn)
	}
	tableData, err := getTableData(ctx, conn.Conn, includeStats)
	if err != nil {
		return vterrors.Wrapf(err, "in Engine.reload(), reading tables")
	}

	// On the primary tablet, we also check the data we have stored in our schema tables to see what all needs reloading.
	shouldUseDatabase := se.isServingPrimary && se.schemaCopy

	// changedViews are the views that have changed. We can't use the same createTime logic for views because, MySQL
	// doesn't update the create_time field for views when they are altered. This is annoying, but something we have to work around.
	changedViews, err := getChangedViewNames(ctx, conn.Conn, shouldUseDatabase)
	if err != nil {
		return err
	}
	// mismatchTables stores the tables whose createTime in our cache doesn't match the createTime stored in the database.
	// This can happen if a primary crashed right after a DML succeeded, before it could reload its state. If all the replicas
	// are able to reload their cache before one of them is promoted, then the database information would be out of sync.
	mismatchTables, err := se.getMismatchedTableNames(ctx, conn.Conn, shouldUseDatabase)
	if err != nil {
		return err
	}

	err = se.updateInnoDBRowsRead(ctx, conn.Conn)
	if err != nil {
		return err
	}

	udfsChanged, err := getChangedUserDefinedFunctions(ctx, conn.Conn, shouldUseDatabase)
	if err != nil {
		se.throttledLogger.Errorf("error in getting changed UDFs: %v", err)
	}

	rec := concurrency.AllErrorRecorder{}
	// curTables keeps track of tables in the new snapshot so we can detect what was dropped.
	curTables := map[string]bool{"dual": true}
	// changedTables keeps track of tables that have changed so we can reload their pk info.
	changedTables := make(map[string]*Table)
	// created and altered contain the names of created and altered tables for broadcast.
	var created, altered []*Table
	databaseName := se.cp.DBName()
	for _, row := range tableData.Rows {
		tableName := row[0].ToString()
		var innodbTable *Table
		if innodbTablesStats != nil {
			innodbTableName := fmt.Sprintf("%s/%s", charset.TablenameToFilename(databaseName), charset.TablenameToFilename(tableName))
			innodbTable = innodbTablesStats[innodbTableName]
		}
		curTables[tableName] = true
		createTime, _ := row[2].ToCastInt64()
		var fileSize, allocatedSize uint64

		// For 5.7 flavor, includeStats is ignored, so we don't get the additional columns
		if includeStats && len(row) >= 6 {
			fileSize, _ = row[4].ToCastUint64()
			allocatedSize, _ = row[5].ToCastUint64()
			// publish the size metrics
			se.tableFileSizeGauge.Set(tableName, int64(fileSize))
			se.tableAllocatedSizeGauge.Set(tableName, int64(allocatedSize))
		} else if innodbTable != nil {
			se.tableFileSizeGauge.Set(tableName, int64(innodbTable.FileSize))
			se.tableAllocatedSizeGauge.Set(tableName, int64(innodbTable.AllocatedSize))
		}

		// Table schemas are cached by tabletserver. For each table we cache `information_schema.tables.create_time` (`tbl.CreateTime`).
		// We also record the last time the schema was loaded (`se.lastChange`). Both are in seconds. We reload a table only when:
		//   1. A table's underlying mysql metadata has changed: `se.lastChange >= createTime`. This can happen if a table was directly altered.
		//      Note that we also reload if `se.lastChange == createTime` since it is possible, especially in unit tests,
		//      that a table might be changed multiple times within the same second.
		//
		//   2. A table was swapped in by Online DDL: `createTime != tbl.CreateTime`. When an Online DDL migration is completed the temporary table is
		//      renamed to the table being altered. `se.lastChange` is updated every time the schema is reloaded (default: 30m).
		//      Online DDL can take hours. So it is possible that the `create_time` of the temporary table is before se.lastChange. Hence,
		//      #1 will not identify the renamed table as a changed one.
		//
		//   3. A table's create_time in our database doesn't match the create_time in the cache. This can happen if a primary crashed right after a DML succeeded,
		//      before it could reload its state. If all the replicas are able to reload their cache before one of them is promoted,
		//      then the database information would be out of sync. We check this by consulting the mismatchTables map.
		//
		//   4. A view's definition has changed. We can't use the same createTime logic for views because, MySQL
		//	    doesn't update the create_time field for views when they are altered. This is annoying, but something we have to work around.
		//      We check this by consulting the changedViews map.
		tbl, isInTablesMap := se.tables[tableName]
		_, isInChangedViewMap := changedViews[tableName]
		_, isInMismatchTableMap := mismatchTables[tableName]
		if isInTablesMap && createTime == tbl.CreateTime && createTime < se.lastChange && !isInChangedViewMap && !isInMismatchTableMap {
			if includeStats {
				tbl.FileSize = fileSize
				tbl.AllocatedSize = allocatedSize
			} else if innodbTable != nil {
				tbl.FileSize = innodbTable.FileSize
				tbl.AllocatedSize = innodbTable.AllocatedSize
			}
			continue
		}

		log.V(2).Info("Reading schema for table: " + tableName)
		tableType := row[1].String()
		table, err := LoadTable(conn, se.cp.DBName(), tableName, tableType, row[3].ToString(), se.env.Environment().CollationEnv())
		if err != nil {
			// Non recoverable error:
			rec.RecordError(vterrors.Wrapf(err, "in Engine.reload(), reading table %s", tableName))
			continue
		}
		if includeStats {
			table.FileSize = fileSize
			table.AllocatedSize = allocatedSize
		} else if innodbTable != nil {
			table.FileSize = innodbTable.FileSize
			table.AllocatedSize = innodbTable.AllocatedSize
		}
		table.CreateTime = createTime
		changedTables[tableName] = table
		if isInTablesMap {
			altered = append(altered, table)
		} else {
			created = append(created, table)
		}
	}
	if rec.HasErrors() {
		return rec.Error()
	}

	dropped := se.getDroppedTables(curTables, changedViews, mismatchTables)

	// Populate PK Columns for changed tables.
	if err := se.populatePrimaryKeys(ctx, conn.Conn, changedTables); err != nil {
		return err
	}

	// If this tablet is the primary and schema tracking is required, we should reload the information in our database.
	if shouldUseDatabase {
		// If reloadDataInDB succeeds, then we don't want to prevent sending the broadcast notification.
		// So, we do this step in the end when we can receive no more errors that fail the reload operation.
		err = reloadDataInDB(ctx, conn.Conn, altered, created, dropped, udfsChanged, se.env.Environment().Parser())
		if err != nil {
			log.Error(fmt.Sprintf("error in updating schema information in Engine.reload() - %v", err))
		}
	}

	// Update se.tables
	maps0.Copy(se.tables, changedTables)
	se.lastChange = curTime
	if len(created) > 0 || len(altered) > 0 || len(dropped) > 0 {
		log.Info(fmt.Sprintf("schema engine created %v, altered %v, dropped %v", extractNamesFromTablesList(created), extractNamesFromTablesList(altered), extractNamesFromTablesList(dropped)))
	}
	se.broadcast(created, altered, dropped, udfsChanged)
	return nil
}

func (se *Engine) getDroppedTables(curTables map[string]bool, changedViews map[string]any, mismatchTables map[string]any) []*Table {
	// Compute and handle dropped tables.
	dropped := make(map[string]*Table)
	for tableName, table := range se.tables {
		if !curTables[tableName] {
			dropped[tableName] = table
			delete(se.tables, tableName)
			// We can't actually delete the label from the stats, but we can set it to 0.
			// Many monitoring tools will drop zero-valued metrics.
			se.tableFileSizeGauge.Reset(tableName)
			se.tableAllocatedSizeGauge.Reset(tableName)
		}
	}

	// If we have a view that has changed, but doesn't exist in the current list of tables,
	// then it was dropped before, and we were unable to update our database. So, we need to signal its
	// drop again.
	for viewName := range changedViews {
		_, alreadyExists := dropped[viewName]
		if !curTables[viewName] && !alreadyExists {
			dropped[viewName] = NewTable(viewName, View)
		}
	}

	// If we have a table that has a mismatch, but doesn't exist in the current list of tables,
	// then it was dropped before, and we were unable to update our database. So, we need to signal its
	// drop again.
	for tableName := range mismatchTables {
		_, alreadyExists := dropped[tableName]
		if !curTables[tableName] && !alreadyExists {
			dropped[tableName] = NewTable(tableName, NoType)
		}
	}

	return maps.Values(dropped)
}

func getTableData(ctx context.Context, conn *connpool.Conn, includeStats bool) (*sqltypes.Result, error) {
	var showTablesQuery string
	if includeStats {
		showTablesQuery = conn.BaseShowTablesWithSizes()
	} else {
		showTablesQuery = conn.BaseShowTables()
	}
	return conn.Exec(ctx, showTablesQuery, maxTableCount, false)
}

func (se *Engine) updateInnoDBRowsRead(ctx context.Context, conn *connpool.Conn) error {
	readRowsData, err := conn.Exec(ctx, mysql.ShowRowsRead, 10, false)
	if err != nil {
		return err
	}

	if len(readRowsData.Rows) == 1 && len(readRowsData.Rows[0]) == 2 {
		value, err := readRowsData.Rows[0][1].ToCastInt64()
		if err != nil {
			return err
		}

		se.innoDbReadRowsCounter.Set(value)
	} else {
		log.Warn(fmt.Sprintf("got strange results from 'show status': %v", readRowsData.Rows))
	}
	return nil
}

func (se *Engine) updateTableIndexMetrics(ctx context.Context, conn *connpool.Conn) error {
	if conn.BaseShowIndexSizes() == "" ||
		conn.BaseShowTableRowCountClusteredIndex() == "" ||
		conn.BaseShowIndexSizes() == "" ||
		conn.BaseShowIndexCardinalities() == "" {
		return nil
	}
	// Load all partitions so that we can extract the base table name from tables given as "TABLE#p#PARTITION"
	type partition struct {
		table     string
		partition string
	}

	partitionsResults, err := conn.Exec(ctx, conn.BaseShowPartitions(), 8192*maxTableCount, false)
	if err != nil {
		return err
	}
	partitions := make(map[string]partition)
	for _, row := range partitionsResults.Rows {
		p := partition{
			table:     row[0].ToString(),
			partition: row[1].ToString(),
		}
		key := p.table + "#p#" + p.partition
		partitions[key] = p
	}

	// Load table row counts and clustered index sizes. Results contain one row for every partition
	type table struct {
		table    string
		rows     int64
		rowBytes int64
	}
	tables := make(map[string]table)
	tableStatsResults, err := conn.Exec(ctx, conn.BaseShowTableRowCountClusteredIndex(), maxTableCount*maxPartitionsPerTable, false)
	if err != nil {
		return err
	}
	for _, row := range tableStatsResults.Rows {
		tableName := row[0].ToString()
		rowCount, _ := row[1].ToInt64()
		rowsBytes, _ := row[2].ToInt64()

		if partition, ok := partitions[tableName]; ok {
			tableName = partition.table
		}

		t, ok := tables[tableName]
		if !ok {
			t = table{table: tableName}
		}
		t.rows += rowCount
		t.rowBytes += rowsBytes
		tables[tableName] = t
	}

	type index struct {
		table       string
		index       string
		bytes       int64
		cardinality int64
	}
	indexes := make(map[[2]string]index)

	// Load the byte sizes of all indexes. Results contain one row for every index/partition combination.
	bytesResults, err := conn.Exec(ctx, conn.BaseShowIndexSizes(), maxTableCount*maxIndexesPerTable, false)
	if err != nil {
		return err
	}
	for _, row := range bytesResults.Rows {
		tableName := row[0].ToString()
		indexName := row[1].ToString()
		indexBytes, _ := row[2].ToInt64()

		if partition, ok := partitions[tableName]; ok {
			tableName = partition.table
		}

		key := [2]string{tableName, indexName}
		idx, ok := indexes[key]
		if !ok {
			idx = index{
				table: tableName,
				index: indexName,
			}
		}
		idx.bytes += indexBytes
		indexes[key] = idx
	}

	// Load index cardinalities. Results contain one row for every index (pre-aggregated across partitions).
	cardinalityResults, err := conn.Exec(ctx, conn.BaseShowIndexCardinalities(), maxTableCount*maxPartitionsPerTable, false)
	if err != nil {
		return err
	}
	for _, row := range cardinalityResults.Rows {
		tableName := row[0].ToString()
		indexName := row[1].ToString()
		cardinality, _ := row[2].ToInt64()

		key := [2]string{tableName, indexName}
		idx, ok := indexes[key]
		if !ok {
			idx = index{
				table: tableName,
				index: indexName,
			}
		}
		idx.cardinality = cardinality
		indexes[key] = idx
	}

	se.indexBytesGauge.ResetAll()
	se.indexCardinalityGauge.ResetAll()
	for _, idx := range indexes {
		key := []string{idx.table, idx.index}
		se.indexBytesGauge.Set(key, idx.bytes)
		se.indexCardinalityGauge.Set(key, idx.cardinality)
	}

	se.tableRowsGauge.ResetAll()
	se.tableClusteredIndexSizeGauge.ResetAll()
	for _, tbl := range tables {
		se.tableRowsGauge.Set(tbl.table, tbl.rows)
		se.tableClusteredIndexSizeGauge.Set(tbl.table, tbl.rowBytes)
	}

	return nil
}

func (se *Engine) mysqlTime(ctx context.Context, conn *connpool.Conn) (int64, error) {
	// Keep `SELECT UNIX_TIMESTAMP` is in uppercase because binlog server queries are case sensitive and expect it to be so.
	tm, err := conn.Exec(ctx, "SELECT UNIX_TIMESTAMP()", 1, false)
	if err != nil {
		return 0, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not get MySQL time: %v", err)
	}
	if len(tm.Rows) != 1 || len(tm.Rows[0]) != 1 || tm.Rows[0][0].IsNull() {
		return 0, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "unexpected result for MySQL time: %+v", tm.Rows)
	}
	t, err := tm.Rows[0][0].ToCastInt64()
	if err != nil {
		return 0, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not parse time %v: %v", tm, err)
	}
	return t, nil
}

// populatePrimaryKeys populates the PKColumns for the specified tables.
func (se *Engine) populatePrimaryKeys(ctx context.Context, conn *connpool.Conn, tables map[string]*Table) error {
	pkData, err := conn.Exec(ctx, mysql.BaseShowPrimary, maxTableCount, false)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not get table primary key info: %v", err)
	}
	for _, row := range pkData.Rows {
		tableName := row[0].ToString()
		table, ok := tables[tableName]
		if !ok {
			continue
		}
		colName := row[1].ToString()
		index := table.FindColumn(sqlparser.NewIdentifierCI(colName))
		if index < 0 {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "column %v is listed as primary key, but not present in table %v", colName, tableName)
		}
		table.PKColumns = append(table.PKColumns, index)
	}
	return nil
}

// RegisterVersionEvent is called by the vstream when it encounters a version event (an
// insert into the schema_tracking table). It triggers the historian to load the newer
// rows from the database to update its cache.
func (se *Engine) RegisterVersionEvent() error {
	return se.historian.RegisterVersionEvent()
}

// RefreshHistorianForStreamStart performs the strict historian refresh used during stream
// startup. It returns schema_version read errors so the caller can abort startup instead
// of proceeding with stale historian state.
func (se *Engine) RefreshHistorianForStreamStart(ctx context.Context) error {
	return se.historian.RefreshForStreamStart(ctx)
}

// GetTableForPos makes a best-effort attempt to return a table's schema at a specific
// GTID/position. If it cannot get the table schema for the given GTID/position then it
// returns the latest table schema that is available in the database -- the table schema
// for the "current" GTID/position (updating the cache entry). If the table is not found
// in the cache, it will reload the cache from the database in case the table was created
// after the last schema reload or the cache has not yet been initialized. This function
// makes the schema cache a read-through cache for VReplication purposes.
func (se *Engine) GetTableForPos(ctx context.Context, tableName sqlparser.IdentifierCS, gtid string) (*binlogdatapb.MinimalTable, error) {
	mt, err := se.historian.GetTableForPos(tableName, gtid)
	if err != nil {
		log.Info("GetTableForPos returned error: " + err.Error())
		return nil, err
	}
	if mt != nil {
		return mt, nil
	}
	// We got nothing from the historian, which typically means that it's not enabled.
	se.mu.Lock()
	defer se.mu.Unlock()
	tableNameStr := tableName.String()
	if st, ok := se.tables[tableNameStr]; ok && tableNameStr != "dual" { // No need to refresh dual
		// Test Engines (NewEngineForTests()) don't have a conns pool and are not
		// supposed to talk to the database, so don't update the cache entry in that
		// case.
		if se.conns == nil {
			return newMinimalTable(st), nil
		}
		// We have the table in our cache. Let's be sure that our table definition is
		// up-to-date for the "current" position.
		conn, err := se.conns.Get(ctx, nil)
		if err != nil {
			return nil, err
		}
		defer conn.Recycle()
		cst := *st       // Make a copy
		cst.Fields = nil // We're going to refresh the columns/fields
		if err := fetchColumns(&cst, conn, se.cp.DBName(), tableNameStr); err != nil {
			return nil, err
		}
		// Update the PK columns for the table as well as they may have changed.
		cst.PKColumns = nil // We're going to repopulate the PK columns
		if err := se.populatePrimaryKeys(ctx, conn.Conn, map[string]*Table{tableNameStr: &cst}); err != nil {
			return nil, err
		}
		se.tables[tableNameStr] = &cst
		return newMinimalTable(&cst), nil
	}
	// It's expected that internal tables are not found within VReplication workflows.
	// No need to refresh the cache for internal tables.
	if schema.IsInternalOperationTableName(tableNameStr) {
		log.Info(fmt.Sprintf("internal table %v found in vttablet schema: skipping for GTID search", tableNameStr))
		return nil, nil
	}
	// We don't currently have the non-internal table in the cache. This can happen when
	// a table was created after the last schema reload (which happens at least every
	// --queryserver-config-schema-reload-time).
	// Whatever the reason, we should ensure that our cache is able to get the latest
	// table schema for the "current" position IF the table exists in the database.
	// In order to ensure this, we need to reload the latest schema so that our cache
	// is up to date. This effectively turns our in-memory cache into a read-through
	// cache for VReplication related needs (this function is only used by vstreamers).
	// This adds an additional cost, but for VReplication it should be rare that we are
	// trying to replicate a table that doesn't actually exist.
	// This also allows us to perform a just-in-time initialization of the cache if
	// a vstreamer is the first one to access it.
	if se.conns != nil { // Test Engines (NewEngineForTests()) don't have a conns pool
		if err := se.reload(ctx, false); err != nil {
			return nil, err
		}
		if st, ok := se.tables[tableNameStr]; ok {
			return newMinimalTable(st), nil
		}
	}

	log.Info(fmt.Sprintf("table %v not found in vttablet schema, current tables: %v", tableNameStr, se.tables))
	return nil, fmt.Errorf("table %v not found in vttablet schema", tableNameStr)
}

// RegisterNotifier registers the function for schema change notification.
// It also causes an immediate notification to the caller. The notified
// function must not change the map or its contents. The only exception
// is the sequence table where the values can be changed using the lock.
func (se *Engine) RegisterNotifier(name string, f notifier, runNotifier bool) {
	if !se.isOpen {
		return
	}

	se.notifierMu.Lock()
	defer se.notifierMu.Unlock()

	se.notifiers[name] = f
	var created []*Table
	for _, table := range se.tables {
		created = append(created, table)
	}
	if runNotifier {
		s := maps.Clone(se.tables)
		f(s, created, nil, nil, true)
	}
}

// UnregisterNotifier unregisters the notifier function.
func (se *Engine) UnregisterNotifier(name string) {
	if !se.isOpen {
		log.Info("schema Engine is not open")
		return
	}

	log.Info("schema Engine - acquiring notifierMu lock")
	se.notifierMu.Lock()
	log.Info("schema Engine - acquired notifierMu lock")
	defer se.notifierMu.Unlock()

	delete(se.notifiers, name)
	log.Info("schema Engine - finished UnregisterNotifier")
}

// broadcast must be called while holding a lock on se.mu.
func (se *Engine) broadcast(created, altered, dropped []*Table, udfsChanged bool) {
	if !se.isOpen {
		return
	}

	se.notifierMu.Lock()
	defer se.notifierMu.Unlock()
	s := maps.Clone(se.tables)
	for _, f := range se.notifiers {
		f(s, created, altered, dropped, udfsChanged)
	}
}

// BroadcastForTesting is meant to be a testing function that triggers a broadcast call.
func (se *Engine) BroadcastForTesting(created, altered, dropped []*Table, udfsChanged bool) {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.broadcast(created, altered, dropped, udfsChanged)
}

// GetTable returns the info for a table.
func (se *Engine) GetTable(tableName sqlparser.IdentifierCS) *Table {
	se.mu.Lock()
	defer se.mu.Unlock()
	return se.tables[tableName.String()]
}

// GetSchema returns the current schema. The Tables are a
// shared data structure and must be treated as read-only.
func (se *Engine) GetSchema() map[string]*Table {
	se.mu.Lock()
	defer se.mu.Unlock()
	tables := maps.Clone(se.tables)
	return tables
}

// MarshalMinimalSchema returns a protobuf encoded binlogdata.MinimalSchema
func (se *Engine) MarshalMinimalSchema() ([]byte, error) {
	se.mu.Lock()
	defer se.mu.Unlock()
	dbSchema := &binlogdatapb.MinimalSchema{
		Tables: make([]*binlogdatapb.MinimalTable, 0, len(se.tables)),
	}
	for _, table := range se.tables {
		dbSchema.Tables = append(dbSchema.Tables, newMinimalTable(table))
	}
	return dbSchema.MarshalVT()
}

func newMinimalTable(st *Table) *binlogdatapb.MinimalTable {
	table := &binlogdatapb.MinimalTable{
		Name:   st.Name.String(),
		Fields: st.Fields,
	}
	pkc := make([]int64, len(st.PKColumns))
	for i, pk := range st.PKColumns {
		pkc[i] = int64(pk)
	}
	table.PKColumns = pkc
	return table
}

// GetConnection returns a connection from the pool
func (se *Engine) GetConnection(ctx context.Context) (*connpool.PooledConn, error) {
	return se.conns.Get(ctx, nil)
}

func (se *Engine) handleDebugSchema(response http.ResponseWriter, request *http.Request) {
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}
	se.handleHTTPSchema(response)
}

func (se *Engine) handleHTTPSchema(response http.ResponseWriter) {
	// Ensure schema engine is Open. If vttablet came up in a non_serving role,
	// the schema engine may not have been initialized.
	err := se.Open()
	if err != nil {
		response.Write([]byte(err.Error()))
		return
	}

	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	b, err := json.MarshalIndent(se.GetSchema(), "", " ")
	if err != nil {
		response.Write([]byte(err.Error()))
		return
	}
	buf := bytes.NewBuffer(nil)
	json.HTMLEscape(buf, b)
	response.Write(buf.Bytes())
}

// Test methods. Do not use in non-test code.

// NewEngineForTests creates a new engine, that can't query the
// database, and will not send notifications. It starts opened, and
// doesn't reload.  Use SetTableForTests to set table schema.
func NewEngineForTests() *Engine {
	se := &Engine{
		isOpen:    true,
		tables:    make(map[string]*Table),
		historian: newHistorian(false, 0, nil),
		env:       tabletenv.NewEnv(vtenv.NewTestEnv(), tabletenv.NewDefaultConfig(), "SchemaEngineForTests"),
		notifiers: make(map[string]notifier),
	}
	return se
}

// SetTableForTests puts a Table in the map directly.
func (se *Engine) SetTableForTests(table *Table) {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.tables[table.Name.String()] = table
}

func (se *Engine) GetDBConnector() dbconfigs.Connector {
	return se.cp
}

func (se *Engine) Environment() *vtenv.Environment {
	return se.env.Environment()
}

func extractNamesFromTablesList(tables []*Table) []string {
	var tableNames []string
	for _, table := range tables {
		tableNames = append(tableNames, table.Name.String())
	}
	return tableNames
}

func (se *Engine) ResetSequences(tables []string) error {
	se.mu.Lock()
	defer se.mu.Unlock()
	for _, tableName := range tables {
		if table, ok := se.tables[tableName]; ok {
			if table.SequenceInfo != nil {
				log.Info(fmt.Sprintf("Resetting sequence info for table %s: %+v", tableName, table.SequenceInfo))
				table.SequenceInfo.Reset()
			}
		} else {
			return vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "table %v not found in schema", tableName)
		}
	}
	return nil
}
