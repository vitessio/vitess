// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/mysqlconn"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/timer"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/connpool"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

const maxTableCount = 10000

type notifier func(full map[string]*Table, created, altered, dropped []string)

// Engine stores the schema info and performs operations that
// keep itself up-to-date.
type Engine struct {
	// mu protects the following fields.
	mu         sync.Mutex
	isOpen     bool
	tables     map[string]*Table
	lastChange int64
	reloadTime time.Duration
	notifiers  map[string]notifier

	// The following fields have their own synchronization
	// and do not require locking mu.
	conns *connpool.Pool
	ticks *timer.Timer
}

var schemaOnce sync.Once

// NewEngine creates a new Engine.
func NewEngine(checker connpool.MySQLChecker, config tabletenv.TabletConfig) *Engine {
	reloadTime := time.Duration(config.SchemaReloadTime * 1e9)
	idleTimeout := time.Duration(config.IdleTimeout * 1e9)
	se := &Engine{
		conns:      connpool.New("", 3, idleTimeout, checker),
		ticks:      timer.NewTimer(reloadTime),
		reloadTime: reloadTime,
	}
	schemaOnce.Do(func() {
		stats.Publish("SchemaReloadTime", stats.DurationFunc(se.ticks.Interval))
		_ = stats.NewMultiCountersFunc("TableRows", []string{"Table"}, se.getTableRows)
		_ = stats.NewMultiCountersFunc("DataLength", []string{"Table"}, se.getDataLength)
		_ = stats.NewMultiCountersFunc("IndexLength", []string{"Table"}, se.getIndexLength)
		_ = stats.NewMultiCountersFunc("DataFree", []string{"Table"}, se.getDataFree)
		_ = stats.NewMultiCountersFunc("MaxDataLength", []string{"Table"}, se.getMaxDataLength)

		http.Handle("/debug/schema", se)
		http.HandleFunc("/schemaz", func(w http.ResponseWriter, r *http.Request) {
			schemazHandler(se.GetSchema(), w, r)
		})
	})
	return se
}

// Open initializes the Engine. Calling Open on an already
// open engine is a no-op.
func (se *Engine) Open(dbaParams *sqldb.ConnParams) error {
	se.mu.Lock()
	defer se.mu.Unlock()
	if se.isOpen {
		return nil
	}
	start := time.Now()
	defer log.Infof("Time taken to load the schema: %v", time.Now().Sub(start))
	ctx := tabletenv.LocalContext()
	se.conns.Open(dbaParams, dbaParams)

	conn, err := se.conns.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	curTime, err := se.mysqlTime(ctx, conn)
	if err != nil {
		return err
	}

	tableData, err := conn.Exec(ctx, mysqlconn.BaseShowTables, maxTableCount, false)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not get table list: %v", err)
	}

	tables := make(map[string]*Table, len(tableData.Rows)+1)
	tables["dual"] = NewTable("dual")
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	for _, row := range tableData.Rows {
		wg.Add(1)
		go func(row []sqltypes.Value) {
			defer func() {
				tabletenv.LogError()
				wg.Done()
			}()

			tableName := row[0].String()
			conn, err := se.conns.Get(ctx)
			if err != nil {
				log.Errorf("Engine.Open: connection error while reading table %s: %v", tableName, err)
				return
			}
			defer conn.Recycle()

			table, err := LoadTable(
				conn,
				tableName,
				row[1].String(), // table_type
				row[3].String(), // table_comment
			)
			if err != nil {
				tabletenv.InternalErrors.Add("Schema", 1)
				log.Errorf("Engine.Open: failed to load table %s: %v", tableName, err)
				// Skip over the table that had an error and move on to the next one
				return
			}
			table.SetMysqlStats(row[4], row[5], row[6], row[7], row[8])
			mu.Lock()
			tables[tableName] = table
			mu.Unlock()
		}(row)
	}
	wg.Wait()

	// Fail if we can't load the schema for any tables, but we know that some tables exist. This points to a configuration problem.
	if len(tableData.Rows) != 0 && len(tables) == 1 { // len(tables) is always at least 1 because of the "dual" table
		return vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not get schema for any tables")
	}
	se.tables = tables
	se.lastChange = curTime
	se.ticks.Start(func() {
		if err := se.Reload(ctx); err != nil {
			log.Errorf("periodic schema reload failed: %v", err)
		}
	})
	se.notifiers = make(map[string]notifier)
	se.isOpen = true
	return nil
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
	se.conns.Close()
	se.tables = make(map[string]*Table)
	se.notifiers = make(map[string]notifier)
	se.isOpen = false
}

// Reload reloads the schema info from the db.
// Any tables that have changed since the last load are updated.
// This is a no-op if the Engine is closed.
func (se *Engine) Reload(ctx context.Context) error {
	se.mu.Lock()
	defer se.mu.Unlock()
	if !se.isOpen {
		return nil
	}
	defer tabletenv.LogError()

	curTime, tableData, err := func() (int64, *sqltypes.Result, error) {
		conn, err := se.conns.Get(ctx)
		if err != nil {
			return 0, nil, err
		}
		defer conn.Recycle()
		curTime, err := se.mysqlTime(ctx, conn)
		if err != nil {
			return 0, nil, err
		}
		tableData, err := conn.Exec(ctx, mysqlconn.BaseShowTables, maxTableCount, false)
		if err != nil {
			return 0, nil, err
		}
		return curTime, tableData, nil
	}()
	if err != nil {
		return fmt.Errorf("could not get table list for reload: %v", err)
	}

	// Reload any tables that have changed. We try every table even if some fail,
	// but we return success only if all tables succeed.
	// The following section requires us to hold mu.
	rec := concurrency.AllErrorRecorder{}
	curTables := map[string]bool{"dual": true}
	for _, row := range tableData.Rows {
		tableName := row[0].String()
		curTables[tableName] = true
		createTime, _ := row[2].ParseInt64()
		// Check if we know about the table or it has been recreated.
		if _, ok := se.tables[tableName]; !ok || createTime >= se.lastChange {
			func() {
				// Unlock so TableWasCreatedOrAltered can lock.
				se.mu.Unlock()
				defer se.mu.Lock()
				log.Infof("Reloading schema for table: %s", tableName)
				rec.RecordError(se.TableWasCreatedOrAltered(ctx, tableName))
			}()
			// In case someone closed se when lock was released.
			if !se.isOpen {
				return nil
			}
			continue
		}
		// Only update table_rows, data_length, index_length, max_data_length
		se.tables[tableName].SetMysqlStats(row[4], row[5], row[6], row[7], row[8])
	}
	se.lastChange = curTime

	// Handle table drops
	var dropped []string
	for tableName := range se.tables {
		if curTables[tableName] {
			continue
		}
		delete(se.tables, tableName)
		dropped = append(dropped, tableName)
	}
	// We only need to broadcast dropped tables because
	// TableWasCreatedOrAltered will broadcast the other changes.
	if len(dropped) > 0 {
		se.broadcast(nil, nil, dropped)
	}
	return rec.Error()
}

func (se *Engine) mysqlTime(ctx context.Context, conn *connpool.DBConn) (int64, error) {
	tm, err := conn.Exec(ctx, "select unix_timestamp()", 1, false)
	if err != nil {
		return 0, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not get MySQL time: %v", err)
	}
	if len(tm.Rows) != 1 || len(tm.Rows[0]) != 1 || tm.Rows[0][0].IsNull() {
		return 0, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "unexpected result for MySQL time: %+v", tm.Rows)
	}
	t, err := strconv.ParseInt(tm.Rows[0][0].String(), 10, 64)
	if err != nil {
		return 0, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not parse time %+v: %v", tm, err)
	}
	return t, nil
}

// TableWasCreatedOrAltered must be called if a DDL was applied to that table.
func (se *Engine) TableWasCreatedOrAltered(ctx context.Context, tableName string) error {
	se.mu.Lock()
	defer se.mu.Unlock()
	if !se.isOpen {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "DDL called on closed schema")
	}

	conn, err := se.conns.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()
	tableData, err := conn.Exec(ctx, mysqlconn.BaseShowTablesForTable(tableName), 1, false)
	if err != nil {
		tabletenv.InternalErrors.Add("Schema", 1)
		return vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "TableWasCreatedOrAltered: information_schema query failed for table %s: %v", tableName, err)
	}
	if len(tableData.Rows) != 1 {
		// This can happen if DDLs race with each other.
		return nil
	}
	row := tableData.Rows[0]
	table, err := LoadTable(
		conn,
		tableName,
		row[1].String(), // table_type
		row[3].String(), // table_comment
	)
	if err != nil {
		tabletenv.InternalErrors.Add("Schema", 1)
		return vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "TableWasCreatedOrAltered: failed to load table %s: %v", tableName, err)
	}
	// table_rows, data_length, index_length, max_data_length
	table.SetMysqlStats(row[4], row[5], row[6], row[7], row[8])

	var created, altered []string
	if _, ok := se.tables[tableName]; ok {
		// If the table already exists, we overwrite it with the latest info.
		// This also means that the query cache needs to be cleared.
		// Otherwise, the query plans may not be in sync with the schema.
		log.Infof("Updating table %s", tableName)
		altered = append(altered, tableName)
	} else {
		created = append(created, tableName)
	}
	se.tables[tableName] = table
	log.Infof("Initialized table: %s, type: %s", tableName, TypeNames[table.Type])
	se.broadcast(created, altered, nil)
	return nil
}

// RegisterNotifier registers the function for schema change notification.
// It also causes an immediate notification to the caller. The notified
// function must not change the map or its contents. The only exception
// is the sequence table where the values can be changed using the lock.
func (se *Engine) RegisterNotifier(name string, f notifier) {
	se.mu.Lock()
	defer se.mu.Unlock()
	if !se.isOpen {
		return
	}

	se.notifiers[name] = f
	var created []string
	for tableName := range se.tables {
		created = append(created, tableName)
	}
	f(se.tables, created, nil, nil)
}

// UnregisterNotifier unregisters the notifier function.
func (se *Engine) UnregisterNotifier(name string) {
	se.mu.Lock()
	defer se.mu.Unlock()
	if !se.isOpen {
		return
	}

	delete(se.notifiers, name)
}

// broadcast must be called while holding a lock on se.mu.
func (se *Engine) broadcast(created, altered, dropped []string) {
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
// data strucutre and must be treated as read-only.
func (se *Engine) GetSchema() map[string]*Table {
	se.mu.Lock()
	defer se.mu.Unlock()
	tables := make(map[string]*Table, len(se.tables))
	for k, v := range se.tables {
		tables[k] = v
	}
	return tables
}

// SetReloadTime changes how often the schema is reloaded. This
// call also triggers an immediate reload.
func (se *Engine) SetReloadTime(reloadTime time.Duration) {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.ticks.Trigger()
	se.ticks.SetInterval(reloadTime)
	se.reloadTime = reloadTime
}

// ReloadTime returns schema info reload time.
func (se *Engine) ReloadTime() time.Duration {
	se.mu.Lock()
	defer se.mu.Unlock()
	return se.reloadTime
}

func (se *Engine) getTableRows() map[string]int64 {
	se.mu.Lock()
	defer se.mu.Unlock()
	tstats := make(map[string]int64)
	for k, v := range se.tables {
		tstats[k] = v.TableRows.Get()
	}
	return tstats
}

func (se *Engine) getDataLength() map[string]int64 {
	se.mu.Lock()
	defer se.mu.Unlock()
	tstats := make(map[string]int64)
	for k, v := range se.tables {
		tstats[k] = v.DataLength.Get()
	}
	return tstats
}

func (se *Engine) getIndexLength() map[string]int64 {
	se.mu.Lock()
	defer se.mu.Unlock()
	tstats := make(map[string]int64)
	for k, v := range se.tables {
		tstats[k] = v.IndexLength.Get()
	}
	return tstats
}

func (se *Engine) getDataFree() map[string]int64 {
	se.mu.Lock()
	defer se.mu.Unlock()
	tstats := make(map[string]int64)
	for k, v := range se.tables {
		tstats[k] = v.DataFree.Get()
	}
	return tstats
}

func (se *Engine) getMaxDataLength() map[string]int64 {
	se.mu.Lock()
	defer se.mu.Unlock()
	tstats := make(map[string]int64)
	for k, v := range se.tables {
		tstats[k] = v.MaxDataLength.Get()
	}
	return tstats
}

func (se *Engine) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}
	se.handleHTTPSchema(response, request)
}

func (se *Engine) handleHTTPSchema(response http.ResponseWriter, request *http.Request) {
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
