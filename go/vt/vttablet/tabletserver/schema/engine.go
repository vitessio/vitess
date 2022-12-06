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
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sidecardb"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"context"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const maxTableCount = 10000

type notifier func(full map[string]*Table, created, altered, dropped []string)

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
	reloadTime time.Duration
	//the position at which the schema was last loaded. it is only used in conjunction with ReloadAt
	reloadAtPos mysql.Position
	notifierMu  sync.Mutex
	notifiers   map[string]notifier

	// SkipMetaCheck skips the metadata about the database and table information
	SkipMetaCheck bool

	historian *historian

	conns *connpool.Pool
	ticks *timer.Timer

	// dbCreationFailed is for preventing log spam.
	dbCreationFailed bool

	tableFileSizeGauge      *stats.GaugesWithSingleLabel
	tableAllocatedSizeGauge *stats.GaugesWithSingleLabel
	innoDbReadRowsCounter   *stats.Counter
}

// NewEngine creates a new Engine.
func NewEngine(env tabletenv.Env) *Engine {
	reloadTime := env.Config().SchemaReloadIntervalSeconds.Get()
	se := &Engine{
		env: env,
		// We need three connections: one for the reloader, one for
		// the historian, and one for the tracker.
		conns: connpool.NewPool(env, "", tabletenv.ConnPoolConfig{
			Size:               3,
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		}),
		ticks:      timer.NewTimer(reloadTime),
		reloadTime: reloadTime,
	}
	_ = env.Exporter().NewGaugeDurationFunc("SchemaReloadTime", "vttablet keeps table schemas in its own memory and periodically refreshes it from MySQL. This config controls the reload time.", se.ticks.Interval)
	se.tableFileSizeGauge = env.Exporter().NewGaugesWithSingleLabel("TableFileSize", "tracks table file size", "Table")
	se.tableAllocatedSizeGauge = env.Exporter().NewGaugesWithSingleLabel("TableAllocatedSize", "tracks table allocated size", "Table")
	se.innoDbReadRowsCounter = env.Exporter().NewCounter("InnodbRowsRead", "number of rows read by mysql")

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
	se.historian = newHistorian(env.Config().TrackSchemaVersions, se.conns)
	return se
}

// InitDBConfig must be called before Open.
func (se *Engine) InitDBConfig(cp dbconfigs.Connector) {
	se.cp = cp
}

// syncVTDatabase is called either the first time a primary starts or on consequent loads to possibly upgrade to a
// new Vitess version. This is the only entry point into the sidecardb module to get the _vt database to the desired
// schema for the running Vitess version.
// There is some extra logging in here which can be removed in a future version (>v16) once the new schema init
// functionality is stable.
func (se *Engine) syncVTDatabase(ctx context.Context, conn *dbconnpool.DBConnection, dbaConn *dbconnpool.DBConnection) error {
	log.Infof("In syncVTDatabase")
	defer func(start time.Time) {
		log.Infof("syncVTDatabase took %d ms", time.Since(start).Milliseconds())
	}(time.Now())

	var exec sidecardb.Exec = func(ctx context.Context, query string, maxRows int, wantFields bool, useVT bool) (*sqltypes.Result, error) {
		if useVT {
			_, err := conn.ExecuteFetch(sidecardb.UseVTDatabaseQuery, maxRows, wantFields)
			if err != nil {
				return nil, err
			}
		}
		return conn.ExecuteFetch(query, maxRows, wantFields)
	}
	var rsroHook sidecardb.ReSetSuperReadOnlyHook = func(ctx context.Context) (err error) {
		log.Infof("resetting hook for super read only...")
		if _, err := dbaConn.ExecuteFetch("SET GLOBAL super_read_only='ON'", 1, false); err != nil {
			log.Infof("Not able to set super_read_only user... This can cause errant GTIDs in future.")
			return err
		}
		return nil
	}
	var sroHook sidecardb.SetSuperReadOnlyHook = func(ctx context.Context) (needsReset bool, err error) {
		if !dbaConn.IsMariaDB() {
			if err := dbaConn.WriteComQuery("SELECT @@global.super_read_only"); err != nil {
				log.Infof("Not able to select super_read_only.")
				return false, err
			}
			res, _, _, err := dbaConn.ReadQueryResult(1, false)
			if err == nil && len(res.Rows) == 1 {
				sro := res.Rows[0][0].ToString()
				if sro == "1" || sro == "ON" {
					log.Infof("setting super read only to false...")
					if _, err = dbaConn.ExecuteFetch("SET GLOBAL read_only='OFF'", 1, false); err != nil {
						return false, err
					}
					return true, nil
				}
			}
		}
		return false, nil
	}
	log.Infof("before sidecardb.Init")
	if err := sidecardb.Init(ctx, exec, sroHook, rsroHook); err != nil {
		log.Errorf("Error in sidecardb.Init: %+v", err)
		// temporary logging, to be deleted in v17
		if strings.Contains(err.Error(), "--read-only") {
			rs, _ := conn.ExecuteFetch("SHOW GLOBAL VARIABLES LIKE '%read_only%'", 100, false)
			log.Infof("Readonly variable values: %+v", rs.Rows)
			log.Infof("Got read-only error from _vt database init %s.\n%s", err.Error(), debug.Stack())
		}
		if se.env.Config().DB.HasGlobalSettings() {
			log.Warning("ignoring sidecardb Init error for unmanaged tablets")
			return nil
		}
		log.Errorf("syncVTDatabase error %+v", err)
		return err
	}
	log.Infof("syncVTDatabase done")
	return nil
}

// EnsureConnectionAndDB ensures that we can connect to mysql.
// If tablet type is primary and there is no db, then the database is created.
// This function can be called before opening the Engine.
func (se *Engine) EnsureConnectionAndDB(tabletType topodatapb.TabletType) error {
	ctx := tabletenv.LocalContext()
	conn, err := dbconnpool.NewDBConnection(ctx, se.env.Config().DB.AllPrivsWithDB())
	if err == nil {
		log.Infof("inside EnsureConnectionAndDB with TabletType %v...", tabletType)
		se.dbCreationFailed = false
		// upgrade _vt if required, for a tablet with an existing database
		if tabletType == topodatapb.TabletType_PRIMARY {
			dbaConn, err := dbconnpool.NewDBConnection(ctx, se.env.Config().DB.DbaConnector())
			if err != nil {
				log.Info("Not able to get connection with all privileges...")
				return err
			}
			defer dbaConn.Close()
			if err := se.syncVTDatabase(ctx, conn, dbaConn); err != nil {
				conn.Close()
				return err
			}
		}
		conn.Close()
		return nil
	}
	if tabletType != topodatapb.TabletType_PRIMARY {
		return err
	}
	if merr, isSQLErr := err.(*mysql.SQLError); !isSQLErr || merr.Num != mysql.ERBadDb {
		return err
	}

	// We are primary and db is not found. Let's create it.
	// We use allprivs instead of DBA because we want db create to fail if we're read-only.
	conn, err = dbconnpool.NewDBConnection(ctx, se.env.Config().DB.AllPrivsConnector())
	if err != nil {
		return err
	}
	defer conn.Close()
	dbaConn, err := dbconnpool.NewDBConnection(ctx, se.env.Config().DB.DbaConnector())
	if err != nil {
		log.Info("Not able to get connection with all privileges...")
		return err
	}
	defer dbaConn.Close()
	_, err = dbaConn.ExecuteFetch("SET GLOBAL read_only='OFF'", 1, false)
	if err != nil {
		log.Errorf("Error trying to switch off read-only for DB.")
		return err
	}
	dbname := se.env.Config().DB.DBName
	_, err = conn.ExecuteFetch(fmt.Sprintf("create database if not exists `%s`", dbname), 1, false)
	if err != nil {
		if !se.dbCreationFailed {
			// This is the first failure.
			log.Errorf("db creation failed for %v: %v, will keep retrying", dbname, err)
			se.dbCreationFailed = true
		}
		return err
	}
	log.Infof("db %v created", dbname)
	se.dbCreationFailed = false
	if err := se.syncVTDatabase(ctx, conn, dbaConn); err != nil {
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
		"dual": NewTable("dual"),
	}
	se.notifiers = make(map[string]notifier)

	if err := se.reload(ctx, true); err != nil {
		return err
	}
	if !se.SkipMetaCheck {
		if err := se.historian.Open(); err != nil {
			return err
		}
	}

	se.ticks.Start(func() {
		if err := se.Reload(ctx); err != nil {
			log.Errorf("periodic schema reload failed: %v", err)
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
	// Close the Timer in a separate go routine because
	// there might be a tick after we have acquired the lock above
	// but before closing the timer, in which case Stop function will wait for the
	// configured function to complete running and that function (ReloadAt) will block
	// on the lock we have already acquired
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		se.ticks.Stop()
		wg.Done()
	}()
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
	for _, t := range se.tables {
		if t.SequenceInfo != nil {
			t.SequenceInfo.Lock()
			t.SequenceInfo.NextVal = 0
			t.SequenceInfo.LastVal = 0
			t.SequenceInfo.Unlock()
		}
	}
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
	return se.ReloadAt(ctx, mysql.Position{})
}

// ReloadAt reloads the schema info from the db.
// Any tables that have changed since the last load are updated.
// It maintains the position at which the schema was reloaded and if the same position is provided
// (say by multiple vstreams) it returns the cached schema. In case of a newer or empty pos it always reloads the schema
func (se *Engine) ReloadAt(ctx context.Context, pos mysql.Position) error {
	return se.ReloadAtEx(ctx, pos, true)
}

// ReloadAtEx reloads the schema info from the db.
// Any tables that have changed since the last load are updated.
// It maintains the position at which the schema was reloaded and if the same position is provided
// (say by multiple vstreams) it returns the cached schema. In case of a newer or empty pos it always reloads the schema
// The includeStats argument controls whether table size statistics should be
// emitted, as they can be expensive to calculate for a large number of tables
func (se *Engine) ReloadAtEx(ctx context.Context, pos mysql.Position, includeStats bool) error {
	se.mu.Lock()
	defer se.mu.Unlock()
	if !se.isOpen {
		log.Warning("Schema reload called for an engine that is not yet open")
		return nil
	}
	if !pos.IsZero() && se.reloadAtPos.AtLeast(pos) {
		log.V(2).Infof("ReloadAtEx: found cached schema at %s", mysql.EncodePosition(pos))
		return nil
	}
	if err := se.reload(ctx, includeStats); err != nil {
		return err
	}
	se.reloadAtPos = pos
	return nil
}

// reload reloads the schema. It can also be used to initialize it.
func (se *Engine) reload(ctx context.Context, includeStats bool) error {
	defer func() {
		se.env.LogError()
	}()

	conn, err := se.conns.Get(ctx, nil)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	// curTime will be saved into lastChange after schema is loaded.
	curTime, err := se.mysqlTime(ctx, conn)
	if err != nil {
		return err
	}
	// if this flag is set, then we don't need table meta information
	if se.SkipMetaCheck {
		return nil
	}

	var showTablesQuery string
	if includeStats {
		showTablesQuery = conn.BaseShowTablesWithSizes()
	} else {
		showTablesQuery = conn.BaseShowTables()
	}
	tableData, err := conn.Exec(ctx, showTablesQuery, maxTableCount, false)
	if err != nil {
		return err
	}

	err = se.updateInnoDBRowsRead(ctx, conn)
	if err != nil {
		return err
	}

	rec := concurrency.AllErrorRecorder{}
	// curTables keeps track of tables in the new snapshot so we can detect what was dropped.
	curTables := map[string]bool{"dual": true}
	// changedTables keeps track of tables that have changed so we can reload their pk info.
	changedTables := make(map[string]*Table)
	// created and altered contain the names of created and altered tables for broadcast.
	var created, altered []string
	for _, row := range tableData.Rows {
		tableName := row[0].ToString()
		curTables[tableName] = true
		createTime, _ := evalengine.ToInt64(row[2])
		var fileSize, allocatedSize uint64

		if includeStats {
			fileSize, _ = evalengine.ToUint64(row[4])
			allocatedSize, _ = evalengine.ToUint64(row[5])
			// publish the size metrics
			se.tableFileSizeGauge.Set(tableName, int64(fileSize))
			se.tableAllocatedSizeGauge.Set(tableName, int64(allocatedSize))
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
		tbl, isInTablesMap := se.tables[tableName]
		if isInTablesMap && createTime == tbl.CreateTime && createTime < se.lastChange {
			if includeStats {
				tbl.FileSize = fileSize
				tbl.AllocatedSize = allocatedSize
			}
			continue
		}

		log.V(2).Infof("Reading schema for table: %s", tableName)
		table, err := LoadTable(conn, se.cp.DBName(), tableName, row[3].ToString())
		if err != nil {
			rec.RecordError(err)
			continue
		}
		if includeStats {
			table.FileSize = fileSize
			table.AllocatedSize = allocatedSize
		}
		table.CreateTime = createTime
		changedTables[tableName] = table
		if isInTablesMap {
			altered = append(altered, tableName)
		} else {
			created = append(created, tableName)
		}
	}
	if rec.HasErrors() {
		return rec.Error()
	}

	// Compute and handle dropped tables.
	var dropped []string
	for tableName := range se.tables {
		if !curTables[tableName] {
			dropped = append(dropped, tableName)
			delete(se.tables, tableName)
			// We can't actually delete the label from the stats, but we can set it to 0.
			// Many monitoring tools will drop zero-valued metrics.
			se.tableFileSizeGauge.Reset(tableName)
			se.tableAllocatedSizeGauge.Reset(tableName)
		}
	}

	// Populate PKColumns for changed tables.
	if err := se.populatePrimaryKeys(ctx, conn, changedTables); err != nil {
		return err
	}

	// Update se.tables
	for k, t := range changedTables {
		se.tables[k] = t
	}
	se.lastChange = curTime
	if len(created) > 0 || len(altered) > 0 || len(dropped) > 0 {
		log.Infof("schema engine created %v, altered %v, dropped %v", created, altered, dropped)
	}
	se.broadcast(created, altered, dropped)
	return nil
}

func (se *Engine) updateInnoDBRowsRead(ctx context.Context, conn *connpool.DBConn) error {
	readRowsData, err := conn.Exec(ctx, mysql.ShowRowsRead, 10, false)
	if err != nil {
		return err
	}

	if len(readRowsData.Rows) == 1 && len(readRowsData.Rows[0]) == 2 {
		value, err := evalengine.ToInt64(readRowsData.Rows[0][1])
		if err != nil {
			return err
		}

		se.innoDbReadRowsCounter.Set(value)
	} else {
		log.Warningf("got strange results from 'show status': %v", readRowsData.Rows)
	}
	return nil
}

func (se *Engine) mysqlTime(ctx context.Context, conn *connpool.DBConn) (int64, error) {
	// Keep `SELECT UNIX_TIMESTAMP` is in uppercase because binlog server queries are case sensitive and expect it to be so.
	tm, err := conn.Exec(ctx, "SELECT UNIX_TIMESTAMP()", 1, false)
	if err != nil {
		return 0, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not get MySQL time: %v", err)
	}
	if len(tm.Rows) != 1 || len(tm.Rows[0]) != 1 || tm.Rows[0][0].IsNull() {
		return 0, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "unexpected result for MySQL time: %+v", tm.Rows)
	}
	t, err := evalengine.ToInt64(tm.Rows[0][0])
	if err != nil {
		return 0, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not parse time %v: %v", tm, err)
	}
	return t, nil
}

// populatePrimaryKeys populates the PKColumns for the specified tables.
func (se *Engine) populatePrimaryKeys(ctx context.Context, conn *connpool.DBConn, tables map[string]*Table) error {
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

// RegisterVersionEvent is called by the vstream when it encounters a version event (an insert into _vt.schema_tracking)
// It triggers the historian to load the newer rows from the database to update its cache
func (se *Engine) RegisterVersionEvent() error {
	return se.historian.RegisterVersionEvent()
}

// GetTableForPos returns a best-effort schema for a specific gtid
func (se *Engine) GetTableForPos(tableName sqlparser.IdentifierCS, gtid string) (*binlogdatapb.MinimalTable, error) {
	mt, err := se.historian.GetTableForPos(tableName, gtid)
	if err != nil {
		log.Infof("GetTableForPos returned error: %s", err.Error())
		return nil, err
	}
	if mt != nil {
		return mt, nil
	}
	se.mu.Lock()
	defer se.mu.Unlock()
	tableNameStr := tableName.String()
	st, ok := se.tables[tableNameStr]
	if !ok {
		if schema.IsInternalOperationTableName(tableNameStr) {
			log.Infof("internal table %v found in vttablet schema: skipping for GTID search", tableNameStr)
		} else {
			log.Infof("table %v not found in vttablet schema, current tables: %v", tableNameStr, se.tables)
			return nil, fmt.Errorf("table %v not found in vttablet schema", tableNameStr)
		}
	}
	return newMinimalTable(st), nil
}

// RegisterNotifier registers the function for schema change notification.
// It also causes an immediate notification to the caller. The notified
// function must not change the map or its contents. The only exception
// is the sequence table where the values can be changed using the lock.
func (se *Engine) RegisterNotifier(name string, f notifier) {
	if !se.isOpen {
		return
	}

	se.notifierMu.Lock()
	defer se.notifierMu.Unlock()

	se.notifiers[name] = f
	var created []string
	for tableName := range se.tables {
		created = append(created, tableName)
	}
	f(se.tables, created, nil, nil)
}

// UnregisterNotifier unregisters the notifier function.
func (se *Engine) UnregisterNotifier(name string) {
	if !se.isOpen {
		log.Infof("schema Engine is not open")
		return
	}

	log.Infof("schema Engine - acquiring notifierMu lock")
	se.notifierMu.Lock()
	log.Infof("schema Engine - acquired notifierMu lock")
	defer se.notifierMu.Unlock()

	delete(se.notifiers, name)
	log.Infof("schema Engine - finished UnregisterNotifier")
}

// broadcast must be called while holding a lock on se.mu.
func (se *Engine) broadcast(created, altered, dropped []string) {
	if !se.isOpen {
		return
	}

	se.notifierMu.Lock()
	defer se.notifierMu.Unlock()
	s := make(map[string]*Table, len(se.tables))
	for k, v := range se.tables {
		s[k] = v
	}
	for _, f := range se.notifiers {
		f(s, created, altered, dropped)
	}
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
	tables := make(map[string]*Table, len(se.tables))
	for k, v := range se.tables {
		tables[k] = v
	}
	return tables
}

// GetConnection returns a connection from the pool
func (se *Engine) GetConnection(ctx context.Context) (*connpool.DBConn, error) {
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
		historian: newHistorian(false, nil),
	}
	return se
}

// SetTableForTests puts a Table in the map directly.
func (se *Engine) SetTableForTests(table *Table) {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.tables[table.Name.String()] = table
}
