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
	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/cache"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/timer"
	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/concurrency"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tableacl"
	"github.com/youtube/vitess/go/vt/tabletserver/planbuilder"
	"golang.org/x/net/context"
)

const baseShowTables = "SELECT table_name, table_type, unix_timestamp(create_time), table_comment, table_rows, data_length, index_length, data_free, max_data_length FROM information_schema.tables WHERE table_schema = database()"

const maxTableCount = 10000

const (
	debugQueryPlansKey = "query_plans"
	debugQueryStatsKey = "query_stats"
	debugSchemaKey     = "schema"
	debugQueryRulesKey = "query_rules"
)

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

type notifier func(map[string]*schema.Table)

// SchemaInfo stores the schema info and performs operations that
// keep itself up-to-date.
type SchemaInfo struct {
	isOpen bool

	mu         sync.Mutex
	tables     map[string]*TableInfo
	lastChange int64
	reloadTime time.Duration

	// The following vars are either read-only or have
	// their own synchronization.
	queries           *cache.LRUCache
	connPool          *ConnPool
	ticks             *timer.Timer
	endpoints         map[string]string
	queryRuleSources  *QueryRuleInfo
	queryServiceStats *QueryServiceStats

	// Notification vars for broadcasting schema changes.
	notifyMutex sync.Mutex
	notifiers   map[string]notifier
}

// NewSchemaInfo creates a new SchemaInfo.
func NewSchemaInfo(
	statsPrefix string,
	checker MySQLChecker,
	queryCacheSize int,
	reloadTime time.Duration,
	idleTimeout time.Duration,
	endpoints map[string]string,
	enablePublishStats bool,
	queryServiceStats *QueryServiceStats) *SchemaInfo {
	si := &SchemaInfo{
		queries:           cache.NewLRUCache(int64(queryCacheSize)),
		connPool:          NewConnPool("", 3, idleTimeout, enablePublishStats, queryServiceStats, checker),
		ticks:             timer.NewTimer(reloadTime),
		endpoints:         endpoints,
		reloadTime:        reloadTime,
		queryRuleSources:  NewQueryRuleInfo(),
		queryServiceStats: queryServiceStats,
	}
	if enablePublishStats {
		stats.Publish(statsPrefix+"QueryCacheLength", stats.IntFunc(si.queries.Length))
		stats.Publish(statsPrefix+"QueryCacheSize", stats.IntFunc(si.queries.Size))
		stats.Publish(statsPrefix+"QueryCacheCapacity", stats.IntFunc(si.queries.Capacity))
		stats.Publish(statsPrefix+"QueryCacheOldest", stats.StringFunc(func() string {
			return fmt.Sprintf("%v", si.queries.Oldest())
		}))
		stats.Publish(statsPrefix+"SchemaReloadTime", stats.DurationFunc(si.ticks.Interval))
		_ = stats.NewMultiCountersFunc(statsPrefix+"QueryCounts", []string{"Table", "Plan"}, si.getQueryCount)
		_ = stats.NewMultiCountersFunc(statsPrefix+"QueryTimesNs", []string{"Table", "Plan"}, si.getQueryTime)
		_ = stats.NewMultiCountersFunc(statsPrefix+"QueryRowCounts", []string{"Table", "Plan"}, si.getQueryRowCount)
		_ = stats.NewMultiCountersFunc(statsPrefix+"QueryErrorCounts", []string{"Table", "Plan"}, si.getQueryErrorCount)
		_ = stats.NewMultiCountersFunc(statsPrefix+"TableRows", []string{"Table"}, si.getTableRows)
		_ = stats.NewMultiCountersFunc(statsPrefix+"DataLength", []string{"Table"}, si.getDataLength)
		_ = stats.NewMultiCountersFunc(statsPrefix+"IndexLength", []string{"Table"}, si.getIndexLength)
		_ = stats.NewMultiCountersFunc(statsPrefix+"DataFree", []string{"Table"}, si.getDataFree)
		_ = stats.NewMultiCountersFunc(statsPrefix+"MaxDataLength", []string{"Table"}, si.getMaxDataLength)
	}
	for _, ep := range endpoints {
		http.Handle(ep, si)
	}
	return si
}

// Open initializes the SchemaInfo.
// The state transition is Open->Operations->Close.
// Synchronization between the above operations is
// the responsibility of the caller.
// Close is idempotent.
func (si *SchemaInfo) Open(dbaParams *sqldb.ConnParams, strictMode bool) {
	if si.isOpen {
		return
	}
	ctx := localContext()
	si.connPool.Open(dbaParams, dbaParams)
	// Get time first because it needs a connection from the pool.
	curTime := si.mysqlTime(ctx)

	conn := getOrPanic(ctx, si.connPool)
	defer conn.Recycle()

	if strictMode {
		if err := conn.VerifyMode(); err != nil {
			panic(NewTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, err.Error()))
		}
	}

	tableData, err := conn.Exec(ctx, baseShowTables, maxTableCount, false)
	if err != nil {
		panic(PrefixTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, err, "Could not get table list: "))
	}

	tables := make(map[string]*TableInfo, len(tableData.Rows)+1)
	tables["dual"] = &TableInfo{Table: schema.NewTable("dual")}
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	for _, row := range tableData.Rows {
		wg.Add(1)
		go func(row []sqltypes.Value) {
			defer wg.Done()

			conn := getOrPanic(ctx, si.connPool)
			defer conn.Recycle()

			tableName := row[0].String()
			tableInfo, err := NewTableInfo(
				conn,
				tableName,
				row[1].String(), // table_type
				row[3].String(), // table_comment
			)
			if err != nil {
				si.queryServiceStats.InternalErrors.Add("Schema", 1)
				log.Errorf("SchemaInfo.Open: failed to create TableInfo for table %s: %v", tableName, err)
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
		panic(NewTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, "could not get schema for any tables"))
	}
	si.tables = tables
	si.lastChange = curTime
	si.ticks.Start(func() {
		if err := si.Reload(ctx); err != nil {
			log.Errorf("periodic schema reload failed: %v", err)
		}
	})
	si.notifiers = make(map[string]notifier)
	si.isOpen = true
}

// Close shuts down SchemaInfo. It can be re-opened after Close.
func (si *SchemaInfo) Close() {
	if !si.isOpen {
		return
	}
	si.ticks.Stop()
	si.connPool.Close()
	si.queries.Clear()
	si.tables = nil
	si.notifiers = nil
	si.isOpen = false
}

// Reload reloads the schema info from the db.
// Any tables that have changed since the last load are updated.
// This is a no-op if the SchemaInfo is closed.
func (si *SchemaInfo) Reload(ctx context.Context) error {
	defer logError(si.queryServiceStats)
	// Get time first because it needs a connection from the pool.
	curTime := si.mysqlTime(ctx)

	var tableData *sqltypes.Result
	var err error
	func() {
		conn := getOrPanic(ctx, si.connPool)
		defer conn.Recycle()
		tableData, err = conn.Exec(ctx, baseShowTables, maxTableCount, false)
	}()
	if err != nil {
		return fmt.Errorf("could not get table list for reload: %v", err)
	}

	// Reload any tables that have changed. We try every table even if some fail,
	// but we return success only if all tables succeed.
	// The following section requires us to hold mu.
	rec := concurrency.AllErrorRecorder{}
	schemaChanged := false
	si.mu.Lock()
	defer si.mu.Unlock()
	for _, row := range tableData.Rows {
		tableName := row[0].String()
		createTime, _ := row[2].ParseInt64()
		// Check if we know about the table or it has been recreated.
		if _, ok := si.tables[tableName]; !ok || createTime >= si.lastChange {
			schemaChanged = true
			func() {
				// Unlock so CreateOrUpdateTable can lock.
				si.mu.Unlock()
				defer si.mu.Lock()
				log.Infof("Reloading schema for table: %s", tableName)
				rec.RecordError(si.CreateOrUpdateTable(ctx, tableName))
			}()
			continue
		}
		// Only update table_rows, data_length, index_length, max_data_length
		si.tables[tableName].SetMysqlStats(row[4], row[5], row[6], row[7], row[8])
	}
	si.lastChange = curTime
	if schemaChanged {
		si.broadcast()
	}
	return rec.Error()
}

func (si *SchemaInfo) mysqlTime(ctx context.Context) int64 {
	conn := getOrPanic(ctx, si.connPool)
	defer conn.Recycle()
	tm, err := conn.Exec(ctx, "select unix_timestamp()", 1, false)
	if err != nil {
		panic(PrefixTabletError(vtrpcpb.ErrorCode_UNKNOWN_ERROR, err, "Could not get MySQL time: "))
	}
	if len(tm.Rows) != 1 || len(tm.Rows[0]) != 1 || tm.Rows[0][0].IsNull() {
		panic(NewTabletError(vtrpcpb.ErrorCode_UNKNOWN_ERROR, "Unexpected result for MySQL time: %+v", tm.Rows))
	}
	t, err := strconv.ParseInt(tm.Rows[0][0].String(), 10, 64)
	if err != nil {
		panic(NewTabletError(vtrpcpb.ErrorCode_UNKNOWN_ERROR, "Could not parse time %+v: %v", tm, err))
	}
	return t
}

// ClearQueryPlanCache should be called if query plan cache is potentially obsolete
func (si *SchemaInfo) ClearQueryPlanCache() {
	si.queries.Clear()
}

// CreateOrUpdateTable must be called if a DDL was applied to that table.
func (si *SchemaInfo) CreateOrUpdateTable(ctx context.Context, tableName string) error {
	conn := getOrPanic(ctx, si.connPool)
	defer conn.Recycle()
	tableData, err := conn.Exec(ctx, fmt.Sprintf("%s and table_name = '%s'", baseShowTables, tableName), 1, false)
	if err != nil {
		si.queryServiceStats.InternalErrors.Add("Schema", 1)
		return PrefixTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, err,
			fmt.Sprintf("CreateOrUpdateTable: information_schema query failed for table %s: ", tableName))
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
		si.queryServiceStats.InternalErrors.Add("Schema", 1)
		return PrefixTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, err,
			fmt.Sprintf("CreateOrUpdateTable: failed to create TableInfo for table %s: ", tableName))
	}
	// table_rows, data_length, index_length, max_data_length
	tableInfo.SetMysqlStats(row[4], row[5], row[6], row[7], row[8])

	// Need to acquire lock now.
	si.mu.Lock()
	defer si.mu.Unlock()
	if _, ok := si.tables[tableName]; ok {
		// If the table already exists, we overwrite it with the latest info.
		// This also means that the query cache needs to be cleared.
		// Otherwise, the query plans may not be in sync with the schema.
		si.queries.Clear()
		log.Infof("Updating table %s", tableName)
	}
	si.tables[tableName] = tableInfo
	log.Infof("Initialized table: %s, type: %s", tableName, schema.TypeNames[tableInfo.Type])
	si.broadcast()
	return nil
}

// DropTable must be called if a table was dropped.
func (si *SchemaInfo) DropTable(tableName sqlparser.TableIdent) {
	si.mu.Lock()
	defer si.mu.Unlock()

	delete(si.tables, tableName.String())
	si.queries.Clear()
	log.Infof("Table %s forgotten", tableName)
	si.broadcast()
}

// RegisterNotifier registers the function for schema change notification.
func (si *SchemaInfo) RegisterNotifier(name string, f notifier) {
	si.notifyMutex.Lock()
	defer si.notifyMutex.Unlock()
	si.notifiers[name] = f
}

// UnregisterNotifier unregisters the notifier function.
func (si *SchemaInfo) UnregisterNotifier(name string) {
	si.notifyMutex.Lock()
	defer si.notifyMutex.Unlock()
	delete(si.notifiers, name)
}

// broadcast must be called while holding a lock on si.mu.
func (si *SchemaInfo) broadcast() {
	schema := make(map[string]*schema.Table, len(si.tables))
	for k, v := range si.tables {
		schema[k] = v.Table
	}
	si.notifyMutex.Lock()
	defer si.notifyMutex.Unlock()
	for _, f := range si.notifiers {
		f(schema)
	}
}

// GetPlan returns the ExecPlan that for the query. Plans are cached in a cache.LRUCache.
func (si *SchemaInfo) GetPlan(ctx context.Context, logStats *LogStats, sql string) *ExecPlan {
	span := trace.NewSpanFromContext(ctx)
	span.StartLocal("SchemaInfo.GetPlan")
	defer span.Finish()

	// Fastpath if plan already exists.
	if plan := si.getQuery(sql); plan != nil {
		return plan
	}

	// TODO(sougou): It's not correct to hold this lock here because the code
	// below runs queries against MySQL. But if we don't hold the lock, there
	// are other race conditions where identical queries will end up building
	// plans and compete with populating the query cache. In other words, we
	// need a more elaborate scheme that blocks less, but still prevents these
	// race conditions.
	si.mu.Lock()
	defer si.mu.Unlock()
	// Recheck. A plan might have been built by someone else.
	if plan := si.getQuery(sql); plan != nil {
		return plan
	}

	var tableInfo *TableInfo
	GetTable := func(tableName sqlparser.TableIdent) (table *schema.Table, ok bool) {
		tableInfo, ok = si.tables[tableName.String()]
		if !ok {
			return nil, false
		}
		return tableInfo.Table, true
	}
	splan, err := planbuilder.GetExecPlan(sql, GetTable)
	if err != nil {
		panic(PrefixTabletError(vtrpcpb.ErrorCode_UNKNOWN_ERROR, err, ""))
	}
	plan := &ExecPlan{ExecPlan: splan, TableInfo: tableInfo}
	plan.Rules = si.queryRuleSources.filterByPlan(sql, plan.PlanID, plan.TableName.String())
	plan.Authorized = tableacl.Authorized(plan.TableName.String(), plan.PlanID.MinRole())
	if plan.PlanID.IsSelect() {
		if plan.FieldQuery == nil {
			log.Warningf("Cannot cache field info: %s", sql)
		} else {
			conn := getOrPanic(ctx, si.connPool)
			defer conn.Recycle()
			sql := plan.FieldQuery.Query
			start := time.Now()
			r, err := conn.Exec(ctx, sql, 1, true)
			logStats.AddRewrittenSQL(sql, start)
			if err != nil {
				panic(PrefixTabletError(vtrpcpb.ErrorCode_UNKNOWN_ERROR, err, "Error fetching fields: "))
			}
			plan.Fields = r.Fields
		}
	} else if plan.PlanID == planbuilder.PlanDDL || plan.PlanID == planbuilder.PlanSet {
		return plan
	}
	si.queries.Set(sql, plan)
	return plan
}

// GetStreamPlan is similar to GetPlan, but doesn't use the cache
// and doesn't enforce a limit. It just returns the parsed query.
func (si *SchemaInfo) GetStreamPlan(sql string) *ExecPlan {
	var tableInfo *TableInfo
	GetTable := func(tableName sqlparser.TableIdent) (table *schema.Table, ok bool) {
		si.mu.Lock()
		defer si.mu.Unlock()
		tableInfo, ok = si.tables[tableName.String()]
		if !ok {
			return nil, false
		}
		return tableInfo.Table, true
	}
	splan, err := planbuilder.GetStreamExecPlan(sql, GetTable)
	if err != nil {
		panic(PrefixTabletError(vtrpcpb.ErrorCode_UNKNOWN_ERROR, err, ""))
	}
	plan := &ExecPlan{ExecPlan: splan, TableInfo: tableInfo}
	plan.Rules = si.queryRuleSources.filterByPlan(sql, plan.PlanID, plan.TableName.String())
	plan.Authorized = tableacl.Authorized(plan.TableName.String(), plan.PlanID.MinRole())
	return plan
}

// GetTable returns the TableInfo for a table.
func (si *SchemaInfo) GetTable(tableName sqlparser.TableIdent) *TableInfo {
	si.mu.Lock()
	defer si.mu.Unlock()
	return si.tables[tableName.String()]
}

// GetSchema returns a copy of the schema.
func (si *SchemaInfo) GetSchema() map[string]*schema.Table {
	si.mu.Lock()
	defer si.mu.Unlock()
	tables := make(map[string]*schema.Table, len(si.tables))
	for k, v := range si.tables {
		tables[k] = v.Table
	}
	return tables
}

// getQuery fetches the plan and makes it the most recent.
func (si *SchemaInfo) getQuery(sql string) *ExecPlan {
	if cacheResult, ok := si.queries.Get(sql); ok {
		return cacheResult.(*ExecPlan)
	}
	return nil
}

// peekQuery fetches the plan without changing the LRU order.
func (si *SchemaInfo) peekQuery(sql string) *ExecPlan {
	if cacheResult, ok := si.queries.Peek(sql); ok {
		return cacheResult.(*ExecPlan)
	}
	return nil
}

// SetQueryCacheCap sets the query cache capacity.
func (si *SchemaInfo) SetQueryCacheCap(size int) {
	if size <= 0 {
		panic(NewTabletError(vtrpcpb.ErrorCode_BAD_INPUT, "cache size %v out of range", size))
	}
	si.queries.SetCapacity(int64(size))
}

// QueryCacheCap returns the capacity of the query cache.
func (si *SchemaInfo) QueryCacheCap() int {
	return int(si.queries.Capacity())
}

// SetReloadTime changes how often the schema is reloaded. This
// call also triggers an immediate reload.
func (si *SchemaInfo) SetReloadTime(reloadTime time.Duration) {
	si.ticks.Trigger()
	si.ticks.SetInterval(reloadTime)
	si.mu.Lock()
	defer si.mu.Unlock()
	si.reloadTime = reloadTime
}

// ReloadTime returns schema info reload time.
func (si *SchemaInfo) ReloadTime() time.Duration {
	si.mu.Lock()
	defer si.mu.Unlock()
	return si.reloadTime
}

func (si *SchemaInfo) getTableRows() map[string]int64 {
	si.mu.Lock()
	defer si.mu.Unlock()
	tstats := make(map[string]int64)
	for k, v := range si.tables {
		tstats[k] = v.TableRows.Get()
	}
	return tstats
}

func (si *SchemaInfo) getDataLength() map[string]int64 {
	si.mu.Lock()
	defer si.mu.Unlock()
	tstats := make(map[string]int64)
	for k, v := range si.tables {
		tstats[k] = v.DataLength.Get()
	}
	return tstats
}

func (si *SchemaInfo) getIndexLength() map[string]int64 {
	si.mu.Lock()
	defer si.mu.Unlock()
	tstats := make(map[string]int64)
	for k, v := range si.tables {
		tstats[k] = v.IndexLength.Get()
	}
	return tstats
}

func (si *SchemaInfo) getDataFree() map[string]int64 {
	si.mu.Lock()
	defer si.mu.Unlock()
	tstats := make(map[string]int64)
	for k, v := range si.tables {
		tstats[k] = v.DataFree.Get()
	}
	return tstats
}

func (si *SchemaInfo) getMaxDataLength() map[string]int64 {
	si.mu.Lock()
	defer si.mu.Unlock()
	tstats := make(map[string]int64)
	for k, v := range si.tables {
		tstats[k] = v.MaxDataLength.Get()
	}
	return tstats
}

func (si *SchemaInfo) getQueryCount() map[string]int64 {
	f := func(plan *ExecPlan) int64 {
		queryCount, _, _, _, _ := plan.Stats()
		return queryCount
	}
	return si.getQueryStats(f)
}

func (si *SchemaInfo) getQueryTime() map[string]int64 {
	f := func(plan *ExecPlan) int64 {
		_, time, _, _, _ := plan.Stats()
		return int64(time)
	}
	return si.getQueryStats(f)
}

func (si *SchemaInfo) getQueryRowCount() map[string]int64 {
	f := func(plan *ExecPlan) int64 {
		_, _, _, rowCount, _ := plan.Stats()
		return rowCount
	}
	return si.getQueryStats(f)
}

func (si *SchemaInfo) getQueryErrorCount() map[string]int64 {
	f := func(plan *ExecPlan) int64 {
		_, _, _, _, errorCount := plan.Stats()
		return errorCount
	}
	return si.getQueryStats(f)
}

type queryStatsFunc func(*ExecPlan) int64

func (si *SchemaInfo) getQueryStats(f queryStatsFunc) map[string]int64 {
	keys := si.queries.Keys()
	qstats := make(map[string]int64)
	for _, v := range keys {
		if plan := si.peekQuery(v); plan != nil {
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

func (si *SchemaInfo) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}
	if ep, ok := si.endpoints[debugQueryPlansKey]; ok && request.URL.Path == ep {
		si.handleHTTPQueryPlans(response, request)
	} else if ep, ok := si.endpoints[debugQueryStatsKey]; ok && request.URL.Path == ep {
		si.handleHTTPQueryStats(response, request)
	} else if ep, ok := si.endpoints[debugSchemaKey]; ok && request.URL.Path == ep {
		si.handleHTTPSchema(response, request)
	} else if ep, ok := si.endpoints[debugQueryRulesKey]; ok && request.URL.Path == ep {
		si.handleHTTPQueryRules(response, request)
	} else {
		response.WriteHeader(http.StatusNotFound)
	}
}

func (si *SchemaInfo) handleHTTPQueryPlans(response http.ResponseWriter, request *http.Request) {
	keys := si.queries.Keys()
	response.Header().Set("Content-Type", "text/plain")
	response.Write([]byte(fmt.Sprintf("Length: %d\n", len(keys))))
	for _, v := range keys {
		response.Write([]byte(fmt.Sprintf("%#v\n", v)))
		if plan := si.peekQuery(v); plan != nil {
			if b, err := json.MarshalIndent(plan.ExecPlan, "", "  "); err != nil {
				response.Write([]byte(err.Error()))
			} else {
				response.Write(b)
			}
			response.Write(([]byte)("\n\n"))
		}
	}
}

func (si *SchemaInfo) handleHTTPQueryStats(response http.ResponseWriter, request *http.Request) {
	keys := si.queries.Keys()
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	qstats := make([]perQueryStats, 0, len(keys))
	for _, v := range keys {
		if plan := si.peekQuery(v); plan != nil {
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

func (si *SchemaInfo) handleHTTPSchema(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	b, err := json.MarshalIndent(si.GetSchema(), "", " ")
	if err != nil {
		response.Write([]byte(err.Error()))
		return
	}
	buf := bytes.NewBuffer(nil)
	json.HTMLEscape(buf, b)
	response.Write(buf.Bytes())
}

func (si *SchemaInfo) handleHTTPQueryRules(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	b, err := json.MarshalIndent(si.queryRuleSources, "", " ")
	if err != nil {
		response.Write([]byte(err.Error()))
		return
	}
	buf := bytes.NewBuffer(nil)
	json.HTMLEscape(buf, b)
	response.Write(buf.Bytes())
}
