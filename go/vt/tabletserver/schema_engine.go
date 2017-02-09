// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

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
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/timer"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tabletserver/connpool"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletenv"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

const maxTableCount = 10000

type notifier func(full map[string]*schema.Table, created, altered, dropped []string)

// SchemaEngine stores the schema info and performs operations that
// keep itself up-to-date.
type SchemaEngine struct {
	isOpen bool

	mu         sync.Mutex
	tables     map[string]*schema.Table
	lastChange int64
	reloadTime time.Duration
	strictMode sync2.AtomicBool

	// The following vars have their own synchronization.
	conns *connpool.Pool
	ticks *timer.Timer

	notifiers map[string]notifier
}

var schemaOnce sync.Once

// NewSchemaEngine creates a new SchemaEngine.
func NewSchemaEngine(checker MySQLChecker, config tabletenv.TabletConfig) *SchemaEngine {
	reloadTime := time.Duration(config.SchemaReloadTime * 1e9)
	idleTimeout := time.Duration(config.IdleTimeout * 1e9)
	se := &SchemaEngine{
		conns:      connpool.New("", 3, idleTimeout, checker),
		ticks:      timer.NewTimer(reloadTime),
		reloadTime: reloadTime,
		strictMode: sync2.NewAtomicBool(config.StrictMode),
	}
	schemaOnce.Do(func() {
		stats.Publish("SchemaReloadTime", stats.DurationFunc(se.ticks.Interval))
		_ = stats.NewMultiCountersFunc("TableRows", []string{"Table"}, se.getTableRows)
		_ = stats.NewMultiCountersFunc("DataLength", []string{"Table"}, se.getDataLength)
		_ = stats.NewMultiCountersFunc("IndexLength", []string{"Table"}, se.getIndexLength)
		_ = stats.NewMultiCountersFunc("DataFree", []string{"Table"}, se.getDataFree)
		_ = stats.NewMultiCountersFunc("MaxDataLength", []string{"Table"}, se.getMaxDataLength)

		http.Handle("/debug/schema", se)
	})
	return se
}

// Open initializes the SchemaEngine.
// The state transition is Open->Operations->Close.
// Synchronization between the above operations is
// the responsibility of the caller.
// Close is idempotent.
func (se *SchemaEngine) Open(dbaParams *sqldb.ConnParams) error {
	if se.isOpen {
		return nil
	}
	start := time.Now()
	defer log.Infof("Time taken to load the schema: %v", time.Now().Sub(start))
	ctx := localContext()
	se.conns.Open(dbaParams, dbaParams)

	conn, err := se.conns.Get(ctx)
	if err != nil {
		return tabletenv.NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, err)
	}
	defer conn.Recycle()

	curTime, err := se.mysqlTime(ctx, conn)
	if err != nil {
		return err
	}

	if se.strictMode.Get() {
		if err := conn.VerifyMode(); err != nil {
			return tabletenv.NewTabletError(vtrpcpb.ErrorCode_UNKNOWN_ERROR, err.Error())
		}
	}

	tableData, err := conn.Exec(ctx, mysqlconn.BaseShowTables, maxTableCount, false)
	if err != nil {
		return tabletenv.PrefixTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, err, "Could not get table list: ")
	}

	tables := make(map[string]*schema.Table, len(tableData.Rows)+1)
	tables["dual"] = schema.NewTable("dual")
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	for _, row := range tableData.Rows {
		wg.Add(1)
		go func(row []sqltypes.Value) {
			defer wg.Done()

			tableName := row[0].String()
			conn, err := se.conns.Get(ctx)
			if err != nil {
				log.Errorf("SchemaEngine.Open: connection error while reading table %s: %v", tableName, err)
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
				log.Errorf("SchemaEngine.Open: failed to load table %s: %v", tableName, err)
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
		return tabletenv.NewTabletError(vtrpcpb.ErrorCode_UNKNOWN_ERROR, "could not get schema for any tables")
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

// Close shuts down SchemaEngine. It can be re-opened after Close.
func (se *SchemaEngine) Close() {
	if !se.isOpen {
		return
	}
	se.ticks.Stop()
	se.conns.Close()
	se.tables = nil
	se.notifiers = nil
	se.isOpen = false
}

// Reload reloads the schema info from the db.
// Any tables that have changed since the last load are updated.
// This is a no-op if the SchemaEngine is closed.
func (se *SchemaEngine) Reload(ctx context.Context) error {
	defer tabletenv.LogError()

	curTime, tableData, err := func() (int64, *sqltypes.Result, error) {
		conn, err := se.conns.Get(ctx)
		if err != nil {
			return 0, nil, tabletenv.NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, err)
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
	se.mu.Lock()
	defer se.mu.Unlock()
	curTables := map[string]bool{"dual": true}
	for _, row := range tableData.Rows {
		tableName := row[0].String()
		curTables[tableName] = true
		createTime, _ := row[2].ParseInt64()
		// Check if we know about the table or it has been recreated.
		if _, ok := se.tables[tableName]; !ok || createTime >= se.lastChange {
			func() {
				// Unlock so CreateOrAlterTable can lock.
				se.mu.Unlock()
				defer se.mu.Lock()
				log.Infof("Reloading schema for table: %s", tableName)
				rec.RecordError(se.CreateOrAlterTable(ctx, tableName))
			}()
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
	// CreateOrAlterTable will broadcast the other changes.
	if len(dropped) > 0 {
		se.broadcast(nil, nil, dropped)
	}
	return rec.Error()
}

func (se *SchemaEngine) mysqlTime(ctx context.Context, conn *connpool.DBConn) (int64, error) {
	tm, err := conn.Exec(ctx, "select unix_timestamp()", 1, false)
	if err != nil {
		return 0, tabletenv.PrefixTabletError(vtrpcpb.ErrorCode_UNKNOWN_ERROR, err, "Could not get MySQL time: ")
	}
	if len(tm.Rows) != 1 || len(tm.Rows[0]) != 1 || tm.Rows[0][0].IsNull() {
		return 0, tabletenv.NewTabletError(vtrpcpb.ErrorCode_UNKNOWN_ERROR, "Unexpected result for MySQL time: %+v", tm.Rows)
	}
	t, err := strconv.ParseInt(tm.Rows[0][0].String(), 10, 64)
	if err != nil {
		return 0, tabletenv.NewTabletError(vtrpcpb.ErrorCode_UNKNOWN_ERROR, "Could not parse time %+v: %v", tm, err)
	}
	return t, nil
}

// CreateOrAlterTable must be called if a DDL was applied to that table.
func (se *SchemaEngine) CreateOrAlterTable(ctx context.Context, tableName string) error {
	conn, err := se.conns.Get(ctx)
	if err != nil {
		return tabletenv.NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, err)
	}
	defer conn.Recycle()
	tableData, err := conn.Exec(ctx, mysqlconn.BaseShowTablesForTable(tableName), 1, false)
	if err != nil {
		tabletenv.InternalErrors.Add("Schema", 1)
		return tabletenv.PrefixTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, err,
			fmt.Sprintf("CreateOrAlterTable: information_schema query failed for table %s: ", tableName))
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
		return tabletenv.PrefixTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, err,
			fmt.Sprintf("CreateOrAlterTable: failed to load table %s: ", tableName))
	}
	// table_rows, data_length, index_length, max_data_length
	table.SetMysqlStats(row[4], row[5], row[6], row[7], row[8])

	// Need to acquire lock now.
	se.mu.Lock()
	defer se.mu.Unlock()
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
	log.Infof("Initialized table: %s, type: %s", tableName, schema.TypeNames[table.Type])
	se.broadcast(created, altered, nil)
	return nil
}

// DropTable must be called if a table was dropped.
func (se *SchemaEngine) DropTable(tableName sqlparser.TableIdent) {
	se.mu.Lock()
	defer se.mu.Unlock()

	delete(se.tables, tableName.String())
	log.Infof("Table %s forgotten", tableName)
	se.broadcast(nil, nil, []string{tableName.String()})
}

// RegisterNotifier registers the function for schema change notification.
// It also causes an immediate notification to the caller. The notified
// function must not change the map or its contents. The only exception
// is the sequence table where the values can be changed using the lock.
func (se *SchemaEngine) RegisterNotifier(name string, f notifier) {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.notifiers[name] = f
	var created []string
	for tableName := range se.tables {
		created = append(created, tableName)
	}
	f(se.tables, created, nil, nil)
}

// UnregisterNotifier unregisters the notifier function.
func (se *SchemaEngine) UnregisterNotifier(name string) {
	se.mu.Lock()
	defer se.mu.Unlock()
	delete(se.notifiers, name)
}

// broadcast must be called while holding a lock on se.mu.
func (se *SchemaEngine) broadcast(created, altered, dropped []string) {
	s := make(map[string]*schema.Table, len(se.tables))
	for k, v := range se.tables {
		s[k] = v
	}
	for _, f := range se.notifiers {
		f(s, created, altered, dropped)
	}
}

// GetTable returns the info for a table.
func (se *SchemaEngine) GetTable(tableName sqlparser.TableIdent) *schema.Table {
	se.mu.Lock()
	defer se.mu.Unlock()
	return se.tables[tableName.String()]
}

// GetSchema returns the current schema. The Tables are a shared
// data strucutre and must be treated as read-only.
func (se *SchemaEngine) GetSchema() map[string]*schema.Table {
	se.mu.Lock()
	defer se.mu.Unlock()
	tables := make(map[string]*schema.Table, len(se.tables))
	for k, v := range se.tables {
		tables[k] = v
	}
	return tables
}

// SetReloadTime changes how often the schema is reloaded. This
// call also triggers an immediate reload.
func (se *SchemaEngine) SetReloadTime(reloadTime time.Duration) {
	se.ticks.Trigger()
	se.ticks.SetInterval(reloadTime)
	se.mu.Lock()
	defer se.mu.Unlock()
	se.reloadTime = reloadTime
}

// ReloadTime returns schema info reload time.
func (se *SchemaEngine) ReloadTime() time.Duration {
	se.mu.Lock()
	defer se.mu.Unlock()
	return se.reloadTime
}

func (se *SchemaEngine) getTableRows() map[string]int64 {
	se.mu.Lock()
	defer se.mu.Unlock()
	tstats := make(map[string]int64)
	for k, v := range se.tables {
		tstats[k] = v.TableRows.Get()
	}
	return tstats
}

func (se *SchemaEngine) getDataLength() map[string]int64 {
	se.mu.Lock()
	defer se.mu.Unlock()
	tstats := make(map[string]int64)
	for k, v := range se.tables {
		tstats[k] = v.DataLength.Get()
	}
	return tstats
}

func (se *SchemaEngine) getIndexLength() map[string]int64 {
	se.mu.Lock()
	defer se.mu.Unlock()
	tstats := make(map[string]int64)
	for k, v := range se.tables {
		tstats[k] = v.IndexLength.Get()
	}
	return tstats
}

func (se *SchemaEngine) getDataFree() map[string]int64 {
	se.mu.Lock()
	defer se.mu.Unlock()
	tstats := make(map[string]int64)
	for k, v := range se.tables {
		tstats[k] = v.DataFree.Get()
	}
	return tstats
}

func (se *SchemaEngine) getMaxDataLength() map[string]int64 {
	se.mu.Lock()
	defer se.mu.Unlock()
	tstats := make(map[string]int64)
	for k, v := range se.tables {
		tstats[k] = v.MaxDataLength.Get()
	}
	return tstats
}

func (se *SchemaEngine) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}
	se.handleHTTPSchema(response, request)
}

func (se *SchemaEngine) handleHTTPSchema(response http.ResponseWriter, request *http.Request) {
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
