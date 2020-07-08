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
	"sync"
	"time"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"golang.org/x/net/context"

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

	historian *historian

	conns *connpool.Pool
	ticks *timer.Timer
}

// NewEngine creates a new Engine.
func NewEngine(env tabletenv.Env) *Engine {
	reloadTime := time.Duration(env.Config().SchemaReloadIntervalSeconds * 1e9)
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

// Open initializes the Engine. Calling Open on an already
// open engine is a no-op.
func (se *Engine) Open() error {
	se.mu.Lock()
	defer se.mu.Unlock()
	if se.isOpen {
		return nil
	}

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

	if err := se.reload(ctx); err != nil {
		return err
	}
	if err := se.historian.Open(); err != nil {
		return err
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
	defer se.mu.Unlock()
	if !se.isOpen {
		return
	}

	se.ticks.Stop()
	se.historian.Close()
	se.conns.Close()

	se.tables = make(map[string]*Table)
	se.lastChange = 0
	se.notifiers = make(map[string]notifier)
	se.isOpen = false
}

// MakeNonMaster clears the sequence caches to make sure that
// they don't get accidentally reused after losing mastership.
func (se *Engine) MakeNonMaster() {
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
func (se *Engine) Reload(ctx context.Context) error {
	return se.ReloadAt(ctx, mysql.Position{})
}

// ReloadAt reloads the schema info from the db.
// Any tables that have changed since the last load are updated.
// It maintains the position at which the schema was reloaded and if the same position is provided
// (say by multiple vstreams) it returns the cached schema. In case of a newer or empty pos it always reloads the schema
func (se *Engine) ReloadAt(ctx context.Context, pos mysql.Position) error {
	se.mu.Lock()
	defer se.mu.Unlock()
	if !se.isOpen {
		log.Warning("Schema reload called for an engine that is not yet open")
		return nil
	}
	if !pos.IsZero() && se.reloadAtPos.AtLeast(pos) {
		log.V(2).Infof("ReloadAt: found cached schema at %s", mysql.EncodePosition(pos))
		return nil
	}
	if err := se.reload(ctx); err != nil {
		return err
	}
	se.reloadAtPos = pos
	return nil
}

// reload reloads the schema. It can also be used to initialize it.
func (se *Engine) reload(ctx context.Context) error {
	start := time.Now()
	defer func() {
		log.Infof("Time taken to load the schema: %v", time.Since(start))
		se.env.LogError()
	}()

	conn, err := se.conns.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	// curTime will be saved into lastChange after schema is loaded.
	curTime, err := se.mysqlTime(ctx, conn)
	if err != nil {
		return err
	}
	tableData, err := conn.Exec(ctx, mysql.BaseShowTables, maxTableCount, false)
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
		if _, ok := se.tables[tableName]; ok && createTime < se.lastChange {
			continue
		}
		log.V(2).Infof("Reading schema for table: %s", tableName)
		table, err := LoadTable(conn, tableName, row[1].ToString(), row[3].ToString())
		if err != nil {
			rec.RecordError(err)
			continue
		}
		changedTables[tableName] = table
		if _, ok := se.tables[tableName]; ok {
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
		if curTables[tableName] {
			continue
		}
		dropped = append(dropped, tableName)
		delete(se.tables, tableName)
	}

	// Populate PKColumns for changed tables.
	if err := se.populatePrimaryKeys(ctx, conn, changedTables); err != nil {
		return err
	}

	// Update se.tables and se.lastChange
	for k, t := range changedTables {
		se.tables[k] = t
	}
	se.lastChange = curTime
	se.broadcast(created, altered, dropped)
	return nil
}

func (se *Engine) mysqlTime(ctx context.Context, conn *connpool.DBConn) (int64, error) {
	tm, err := conn.Exec(ctx, "select unix_timestamp()", 1, false)
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
		index := table.FindColumn(sqlparser.NewColIdent(colName))
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
func (se *Engine) GetTableForPos(tableName sqlparser.TableIdent, gtid string) (*binlogdatapb.MinimalTable, error) {
	mt, err := se.historian.GetTableForPos(tableName, gtid)
	if err != nil {
		return nil, err
	}
	if mt != nil {
		return mt, nil
	}
	se.mu.Lock()
	defer se.mu.Unlock()
	st, ok := se.tables[tableName.String()]
	if !ok {
		return nil, fmt.Errorf("table %v not found in vttablet schema", tableName.String())
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
		return
	}

	se.notifierMu.Lock()
	defer se.notifierMu.Unlock()

	delete(se.notifiers, name)
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
func (se *Engine) GetTable(tableName sqlparser.TableIdent) *Table {
	se.mu.Lock()
	defer se.mu.Unlock()
	return se.tables[tableName.String()]
}

// GetSchema returns the current The Tables are a shared
// data structure and must be treated as read-only.
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
	return se.conns.Get(ctx)
}

func (se *Engine) handleDebugSchema(response http.ResponseWriter, request *http.Request) {
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}
	se.handleHTTPSchema(response, request)
}

func (se *Engine) handleHTTPSchema(response http.ResponseWriter, request *http.Request) {
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
		isOpen: true,
		tables: make(map[string]*Table),
	}
	return se
}

// SetTableForTests puts a Table in the map directly.
func (se *Engine) SetTableForTests(table *Table) {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.tables[table.Name.String()] = table
}
