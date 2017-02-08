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
	"github.com/youtube/vitess/go/cache"
	"github.com/youtube/vitess/go/mysqlconn"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/timer"
	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tableacl"
	"github.com/youtube/vitess/go/vt/tabletserver/connpool"
	"github.com/youtube/vitess/go/vt/tabletserver/planbuilder"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletenv"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

const maxTableCount = 10000

//_______________________________________________

// ExecPlan wraps the planbuilder's exec plan to enforce additional rules
// and track stats.
type ExecPlan struct {
	*planbuilder.ExecPlan
	TableInfo  *TableInfo
	Fields     []*querypb.Field
	Rules      *QueryRules
	Authorized *tableacl.ACLResult

	mu         sync.Mutex
	QueryCount int64
	Time       time.Duration
	MysqlTime  time.Duration
	RowCount   int64
	ErrorCount int64
}

// Size allows ExecPlan to be in cache.LRUCache.
func (*ExecPlan) Size() int {
	return 1
}

// AddStats updates the stats for the current ExecPlan.
func (ep *ExecPlan) AddStats(queryCount int64, duration, mysqlTime time.Duration, rowCount, errorCount int64) {
	ep.mu.Lock()
	ep.QueryCount += queryCount
	ep.Time += duration
	ep.MysqlTime += mysqlTime
	ep.RowCount += rowCount
	ep.ErrorCount += errorCount
	ep.mu.Unlock()
}

// Stats returns the current stats of ExecPlan.
func (ep *ExecPlan) Stats() (queryCount int64, duration, mysqlTime time.Duration, rowCount, errorCount int64) {
	ep.mu.Lock()
	queryCount = ep.QueryCount
	duration = ep.Time
	mysqlTime = ep.MysqlTime
	rowCount = ep.RowCount
	errorCount = ep.ErrorCount
	ep.mu.Unlock()
	return
}

//_______________________________________________

type notifier func(full map[string]*TableInfo, created, altered, dropped []string)

// SchemaEngine stores the schema info and performs operations that
// keep itself up-to-date.
type SchemaEngine struct {
	isOpen bool

	mu         sync.Mutex
	tables     map[string]*TableInfo
	lastChange int64
	reloadTime time.Duration
	strictMode sync2.AtomicBool

	// The following vars are either read-only or have
	// their own synchronization.
	queries          *cache.LRUCache
	conns            *connpool.Pool
	ticks            *timer.Timer
	queryRuleSources *QueryRuleInfo

	notifiers map[string]notifier
}

var schemaOnce sync.Once

// NewSchemaEngine creates a new SchemaEngine.
func NewSchemaEngine(checker MySQLChecker, config tabletenv.TabletConfig) *SchemaEngine {
	reloadTime := time.Duration(config.SchemaReloadTime * 1e9)
	idleTimeout := time.Duration(config.IdleTimeout * 1e9)
	se := &SchemaEngine{
		queries:          cache.NewLRUCache(int64(config.QueryCacheSize)),
		conns:            connpool.New("", 3, idleTimeout, checker),
		ticks:            timer.NewTimer(reloadTime),
		reloadTime:       reloadTime,
		strictMode:       sync2.NewAtomicBool(config.StrictMode),
		queryRuleSources: NewQueryRuleInfo(),
	}
	schemaOnce.Do(func() {
		stats.Publish("QueryCacheLength", stats.IntFunc(se.queries.Length))
		stats.Publish("QueryCacheSize", stats.IntFunc(se.queries.Size))
		stats.Publish("QueryCacheCapacity", stats.IntFunc(se.queries.Capacity))
		stats.Publish("QueryCacheOldest", stats.StringFunc(func() string {
			return fmt.Sprintf("%v", se.queries.Oldest())
		}))
		stats.Publish("SchemaReloadTime", stats.DurationFunc(se.ticks.Interval))
		_ = stats.NewMultiCountersFunc("QueryCounts", []string{"Table", "Plan"}, se.getQueryCount)
		_ = stats.NewMultiCountersFunc("QueryTimesNs", []string{"Table", "Plan"}, se.getQueryTime)
		_ = stats.NewMultiCountersFunc("QueryRowCounts", []string{"Table", "Plan"}, se.getQueryRowCount)
		_ = stats.NewMultiCountersFunc("QueryErrorCounts", []string{"Table", "Plan"}, se.getQueryErrorCount)
		_ = stats.NewMultiCountersFunc("TableRows", []string{"Table"}, se.getTableRows)
		_ = stats.NewMultiCountersFunc("DataLength", []string{"Table"}, se.getDataLength)
		_ = stats.NewMultiCountersFunc("IndexLength", []string{"Table"}, se.getIndexLength)
		_ = stats.NewMultiCountersFunc("DataFree", []string{"Table"}, se.getDataFree)
		_ = stats.NewMultiCountersFunc("MaxDataLength", []string{"Table"}, se.getMaxDataLength)

		endpoints := []string{
			"/debug/tablet_plans",
			"/debug/query_stats",
			"/debug/schema",
			"/debug/query_rules",
		}
		for _, ep := range endpoints {
			http.Handle(ep, se)
		}
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

	tables := make(map[string]*TableInfo, len(tableData.Rows)+1)
	tables["dual"] = &TableInfo{Table: schema.NewTable("dual")}
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

			tableInfo, err := NewTableInfo(
				conn,
				tableName,
				row[1].String(), // table_type
				row[3].String(), // table_comment
			)
			if err != nil {
				tabletenv.InternalErrors.Add("Schema", 1)
				log.Errorf("SchemaEngine.Open: failed to create TableInfo for table %s: %v", tableName, err)
				// Skip over the table that had an error and move on to the next one
				return
			}
			tableInfo.SetMysqlStats(row[4], row[5], row[6], row[7], row[8])
			mu.Lock()
			tables[tableName] = tableInfo
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
	se.queries.Clear()
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

// ClearQueryPlanCache should be called if query plan cache is potentially obsolete
func (se *SchemaEngine) ClearQueryPlanCache() {
	se.queries.Clear()
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
	tableInfo, err := NewTableInfo(
		conn,
		tableName,
		row[1].String(), // table_type
		row[3].String(), // table_comment
	)
	if err != nil {
		tabletenv.InternalErrors.Add("Schema", 1)
		return tabletenv.PrefixTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, err,
			fmt.Sprintf("CreateOrAlterTable: failed to create TableInfo for table %s: ", tableName))
	}
	// table_rows, data_length, index_length, max_data_length
	tableInfo.SetMysqlStats(row[4], row[5], row[6], row[7], row[8])

	// Need to acquire lock now.
	se.mu.Lock()
	defer se.mu.Unlock()
	var created, altered []string
	if _, ok := se.tables[tableName]; ok {
		// If the table already exists, we overwrite it with the latest info.
		// This also means that the query cache needs to be cleared.
		// Otherwise, the query plans may not be in sync with the schema.
		se.queries.Clear()
		log.Infof("Updating table %s", tableName)
		altered = append(altered, tableName)
	} else {
		created = append(created, tableName)
	}
	se.tables[tableName] = tableInfo
	log.Infof("Initialized table: %s, type: %s", tableName, schema.TypeNames[tableInfo.Type])
	se.broadcast(created, altered, nil)
	return nil
}

// DropTable must be called if a table was dropped.
func (se *SchemaEngine) DropTable(tableName sqlparser.TableIdent) {
	se.mu.Lock()
	defer se.mu.Unlock()

	delete(se.tables, tableName.String())
	se.queries.Clear()
	log.Infof("Table %s forgotten", tableName)
	se.broadcast(nil, nil, []string{tableName.String()})
}

// RegisterNotifier registers the function for schema change notification.
// It also causes an immediate notification to the caller.
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
	for _, f := range se.notifiers {
		f(se.tables, created, altered, dropped)
	}
}

// GetPlan returns the ExecPlan that for the query. Plans are cached in a cache.LRUCache.
func (se *SchemaEngine) GetPlan(ctx context.Context, logStats *tabletenv.LogStats, sql string) (*ExecPlan, error) {
	span := trace.NewSpanFromContext(ctx)
	span.StartLocal("SchemaEngine.GetPlan")
	defer span.Finish()

	// Fastpath if plan already exists.
	if plan := se.getQuery(sql); plan != nil {
		return plan, nil
	}

	// TODO(sougou): It's not correct to hold this lock here because the code
	// below runs queries against MySQL. But if we don't hold the lock, there
	// are other race conditions where identical queries will end up building
	// plans and compete with populating the query cache. In other words, we
	// need a more elaborate scheme that blocks less, but still prevents these
	// race conditions.
	se.mu.Lock()
	defer se.mu.Unlock()
	// Recheck. A plan might have been built by someone else.
	if plan := se.getQuery(sql); plan != nil {
		return plan, nil
	}

	var tableInfo *TableInfo
	GetTable := func(tableName sqlparser.TableIdent) (table *schema.Table, ok bool) {
		tableInfo, ok = se.tables[tableName.String()]
		if !ok {
			return nil, false
		}
		return tableInfo.Table, true
	}
	splan, err := planbuilder.GetExecPlan(sql, GetTable)
	if err != nil {
		return nil, tabletenv.PrefixTabletError(vtrpcpb.ErrorCode_UNKNOWN_ERROR, err, "")
	}
	plan := &ExecPlan{ExecPlan: splan, TableInfo: tableInfo}
	plan.Rules = se.queryRuleSources.filterByPlan(sql, plan.PlanID, plan.TableName.String())
	plan.Authorized = tableacl.Authorized(plan.TableName.String(), plan.PlanID.MinRole())
	if plan.PlanID.IsSelect() {
		if plan.FieldQuery == nil {
			log.Warningf("Cannot cache field info: %s", sql)
		} else {
			conn, err := se.conns.Get(ctx)
			if err != nil {
				return nil, tabletenv.NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, err)
			}
			defer conn.Recycle()

			sql := plan.FieldQuery.Query
			start := time.Now()
			r, err := conn.Exec(ctx, sql, 1, true)
			logStats.AddRewrittenSQL(sql, start)
			if err != nil {
				return nil, tabletenv.PrefixTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, err, "Error fetching fields: ")
			}
			plan.Fields = r.Fields
		}
	} else if plan.PlanID == planbuilder.PlanDDL || plan.PlanID == planbuilder.PlanSet {
		return plan, nil
	}
	se.queries.Set(sql, plan)
	return plan, nil
}

// GetStreamPlan is similar to GetPlan, but doesn't use the cache
// and doesn't enforce a limit. It just returns the parsed query.
func (se *SchemaEngine) GetStreamPlan(sql string) (*ExecPlan, error) {
	var tableInfo *TableInfo
	GetTable := func(tableName sqlparser.TableIdent) (table *schema.Table, ok bool) {
		se.mu.Lock()
		defer se.mu.Unlock()
		tableInfo, ok = se.tables[tableName.String()]
		if !ok {
			return nil, false
		}
		return tableInfo.Table, true
	}
	splan, err := planbuilder.GetStreamExecPlan(sql, GetTable)
	if err != nil {
		return nil, tabletenv.PrefixTabletError(vtrpcpb.ErrorCode_BAD_INPUT, err, "")
	}
	plan := &ExecPlan{ExecPlan: splan, TableInfo: tableInfo}
	plan.Rules = se.queryRuleSources.filterByPlan(sql, plan.PlanID, plan.TableName.String())
	plan.Authorized = tableacl.Authorized(plan.TableName.String(), plan.PlanID.MinRole())
	return plan, nil
}

// GetTable returns the TableInfo for a table.
func (se *SchemaEngine) GetTable(tableName sqlparser.TableIdent) *TableInfo {
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
		tables[k] = v.Table
	}
	return tables
}

// getQuery fetches the plan and makes it the most recent.
func (se *SchemaEngine) getQuery(sql string) *ExecPlan {
	if cacheResult, ok := se.queries.Get(sql); ok {
		return cacheResult.(*ExecPlan)
	}
	return nil
}

// peekQuery fetches the plan without changing the LRU order.
func (se *SchemaEngine) peekQuery(sql string) *ExecPlan {
	if cacheResult, ok := se.queries.Peek(sql); ok {
		return cacheResult.(*ExecPlan)
	}
	return nil
}

// SetQueryCacheCap sets the query cache capacity.
func (se *SchemaEngine) SetQueryCacheCap(size int) {
	if size <= 0 {
		size = 1
	}
	se.queries.SetCapacity(int64(size))
}

// QueryCacheCap returns the capacity of the query cache.
func (se *SchemaEngine) QueryCacheCap() int {
	return int(se.queries.Capacity())
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

func (se *SchemaEngine) getQueryCount() map[string]int64 {
	f := func(plan *ExecPlan) int64 {
		queryCount, _, _, _, _ := plan.Stats()
		return queryCount
	}
	return se.getQueryStats(f)
}

func (se *SchemaEngine) getQueryTime() map[string]int64 {
	f := func(plan *ExecPlan) int64 {
		_, time, _, _, _ := plan.Stats()
		return int64(time)
	}
	return se.getQueryStats(f)
}

func (se *SchemaEngine) getQueryRowCount() map[string]int64 {
	f := func(plan *ExecPlan) int64 {
		_, _, _, rowCount, _ := plan.Stats()
		return rowCount
	}
	return se.getQueryStats(f)
}

func (se *SchemaEngine) getQueryErrorCount() map[string]int64 {
	f := func(plan *ExecPlan) int64 {
		_, _, _, _, errorCount := plan.Stats()
		return errorCount
	}
	return se.getQueryStats(f)
}

type queryStatsFunc func(*ExecPlan) int64

func (se *SchemaEngine) getQueryStats(f queryStatsFunc) map[string]int64 {
	keys := se.queries.Keys()
	qstats := make(map[string]int64)
	for _, v := range keys {
		if plan := se.peekQuery(v); plan != nil {
			table := plan.TableName
			if table.IsEmpty() {
				table = sqlparser.NewTableIdent("Join")
			}
			planType := plan.PlanID.String()
			data := f(plan)
			qstats[table.String()+"."+planType] += data
		}
	}
	return qstats
}

type perQueryStats struct {
	Query      string
	Table      string
	Plan       planbuilder.PlanType
	QueryCount int64
	Time       time.Duration
	MysqlTime  time.Duration
	RowCount   int64
	ErrorCount int64
}

func (se *SchemaEngine) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}
	switch request.URL.Path {
	case "/debug/tablet_plans":
		se.handleHTTPQueryPlans(response, request)
	case "/debug/query_stats":
		se.handleHTTPQueryStats(response, request)
	case "/debug/schema":
		se.handleHTTPSchema(response, request)
	case "/debug/query_rules":
		se.handleHTTPQueryRules(response, request)
	default:
		response.WriteHeader(http.StatusNotFound)
	}
}

func (se *SchemaEngine) handleHTTPQueryPlans(response http.ResponseWriter, request *http.Request) {
	keys := se.queries.Keys()
	response.Header().Set("Content-Type", "text/plain")
	response.Write([]byte(fmt.Sprintf("Length: %d\n", len(keys))))
	for _, v := range keys {
		response.Write([]byte(fmt.Sprintf("%#v\n", v)))
		if plan := se.peekQuery(v); plan != nil {
			if b, err := json.MarshalIndent(plan.ExecPlan, "", "  "); err != nil {
				response.Write([]byte(err.Error()))
			} else {
				response.Write(b)
			}
			response.Write(([]byte)("\n\n"))
		}
	}
}

func (se *SchemaEngine) handleHTTPQueryStats(response http.ResponseWriter, request *http.Request) {
	keys := se.queries.Keys()
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	qstats := make([]perQueryStats, 0, len(keys))
	for _, v := range keys {
		if plan := se.peekQuery(v); plan != nil {
			var pqstats perQueryStats
			pqstats.Query = unicoded(v)
			pqstats.Table = plan.TableName.String()
			pqstats.Plan = plan.PlanID
			pqstats.QueryCount, pqstats.Time, pqstats.MysqlTime, pqstats.RowCount, pqstats.ErrorCount = plan.Stats()
			qstats = append(qstats, pqstats)
		}
	}
	if b, err := json.MarshalIndent(qstats, "", "  "); err != nil {
		response.Write([]byte(err.Error()))
	} else {
		response.Write(b)
	}
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

func (se *SchemaEngine) handleHTTPQueryRules(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	b, err := json.MarshalIndent(se.queryRuleSources, "", " ")
	if err != nil {
		response.Write([]byte(err.Error()))
		return
	}
	buf := bytes.NewBuffer(nil)
	json.HTMLEscape(buf, b)
	response.Write(buf.Bytes())
}
