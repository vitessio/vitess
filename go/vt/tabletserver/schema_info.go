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
	"github.com/youtube/vitess/go/mysqlconn"
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
	"github.com/youtube/vitess/go/vt/tabletserver/connpool"
	"github.com/youtube/vitess/go/vt/tabletserver/planbuilder"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletenv"
	"golang.org/x/net/context"
)

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

type notifier func(map[string]*TableInfo)

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
	queries          *cache.LRUCache
	conns            *connpool.Pool
	ticks            *timer.Timer
	endpoints        map[string]string
	queryRuleSources *QueryRuleInfo

	notifiers map[string]notifier
}

var schemaOnce sync.Once

// NewSchemaInfo creates a new SchemaInfo.
func NewSchemaInfo(
	checker MySQLChecker,
	queryCacheSize int,
	reloadTime time.Duration,
	idleTimeout time.Duration,
	endpoints map[string]string) *SchemaInfo {
	si := &SchemaInfo{
		queries:          cache.NewLRUCache(int64(queryCacheSize)),
		conns:            connpool.New("", 3, idleTimeout, checker),
		ticks:            timer.NewTimer(reloadTime),
		endpoints:        endpoints,
		reloadTime:       reloadTime,
		queryRuleSources: NewQueryRuleInfo(),
	}
	schemaOnce.Do(func() {
		stats.Publish("QueryCacheLength", stats.IntFunc(si.queries.Length))
		stats.Publish("QueryCacheSize", stats.IntFunc(si.queries.Size))
		stats.Publish("QueryCacheCapacity", stats.IntFunc(si.queries.Capacity))
		stats.Publish("QueryCacheOldest", stats.StringFunc(func() string {
			return fmt.Sprintf("%v", si.queries.Oldest())
		}))
		stats.Publish("SchemaReloadTime", stats.DurationFunc(si.ticks.Interval))
		_ = stats.NewMultiCountersFunc("QueryCounts", []string{"Table", "Plan"}, si.getQueryCount)
		_ = stats.NewMultiCountersFunc("QueryTimesNs", []string{"Table", "Plan"}, si.getQueryTime)
		_ = stats.NewMultiCountersFunc("QueryRowCounts", []string{"Table", "Plan"}, si.getQueryRowCount)
		_ = stats.NewMultiCountersFunc("QueryErrorCounts", []string{"Table", "Plan"}, si.getQueryErrorCount)
		_ = stats.NewMultiCountersFunc("TableRows", []string{"Table"}, si.getTableRows)
		_ = stats.NewMultiCountersFunc("DataLength", []string{"Table"}, si.getDataLength)
		_ = stats.NewMultiCountersFunc("IndexLength", []string{"Table"}, si.getIndexLength)
		_ = stats.NewMultiCountersFunc("DataFree", []string{"Table"}, si.getDataFree)
		_ = stats.NewMultiCountersFunc("MaxDataLength", []string{"Table"}, si.getMaxDataLength)

		for _, ep := range endpoints {
			http.Handle(ep, si)
		}
	})
	return si
}

// Open initializes the SchemaInfo.
// The state transition is Open->Operations->Close.
// Synchronization between the above operations is
// the responsibility of the caller.
// Close is idempotent.
func (si *SchemaInfo) Open(dbaParams *sqldb.ConnParams, strictMode bool) error {
	if si.isOpen {
		return nil
	}
	ctx := localContext()
	si.conns.Open(dbaParams, dbaParams)

	conn, err := si.conns.Get(ctx)
	if err != nil {
		return tabletenv.NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, err)
	}
	defer conn.Recycle()

	curTime, err := si.mysqlTime(ctx, conn)
	if err != nil {
		return err
	}

	if strictMode {
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
			conn, err := si.conns.Get(ctx)
			if err != nil {
				log.Errorf("SchemaInfo.Open: connection error while reading table %s: %v", tableName, err)
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
		return tabletenv.NewTabletError(vtrpcpb.ErrorCode_UNKNOWN_ERROR, "could not get schema for any tables")
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
	return nil
}

// Close shuts down SchemaInfo. It can be re-opened after Close.
func (si *SchemaInfo) Close() {
	if !si.isOpen {
		return
	}
	si.ticks.Stop()
	si.conns.Close()
	si.queries.Clear()
	si.tables = nil
	si.notifiers = nil
	si.isOpen = false
}

// Reload reloads the schema info from the db.
// Any tables that have changed since the last load are updated.
// This is a no-op if the SchemaInfo is closed.
func (si *SchemaInfo) Reload(ctx context.Context) error {
	defer tabletenv.LogError()

	curTime, tableData, err := func() (int64, *sqltypes.Result, error) {
		conn, err := si.conns.Get(ctx)
		if err != nil {
			return 0, nil, tabletenv.NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, err)
		}
		defer conn.Recycle()
		curTime, err := si.mysqlTime(ctx, conn)
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

func (si *SchemaInfo) mysqlTime(ctx context.Context, conn *connpool.DBConn) (int64, error) {
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
func (si *SchemaInfo) ClearQueryPlanCache() {
	si.queries.Clear()
}

// CreateOrUpdateTable must be called if a DDL was applied to that table.
func (si *SchemaInfo) CreateOrUpdateTable(ctx context.Context, tableName string) error {
	conn, err := si.conns.Get(ctx)
	if err != nil {
		return tabletenv.NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, err)
	}
	defer conn.Recycle()
	tableData, err := conn.Exec(ctx, mysqlconn.BaseShowTablesForTable(tableName), 1, false)
	if err != nil {
		tabletenv.InternalErrors.Add("Schema", 1)
		return tabletenv.PrefixTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, err,
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
		tabletenv.InternalErrors.Add("Schema", 1)
		return tabletenv.PrefixTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, err,
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
// It also causes an immediate notification to the caller.
func (si *SchemaInfo) RegisterNotifier(name string, f notifier) {
	si.mu.Lock()
	defer si.mu.Unlock()
	si.notifiers[name] = f
	f(si.tables)
}

// UnregisterNotifier unregisters the notifier function.
func (si *SchemaInfo) UnregisterNotifier(name string) {
	si.mu.Lock()
	defer si.mu.Unlock()
	delete(si.notifiers, name)
}

// broadcast must be called while holding a lock on si.mu.
func (si *SchemaInfo) broadcast() {
	for _, f := range si.notifiers {
		f(si.tables)
	}
}

// GetPlan returns the ExecPlan that for the query. Plans are cached in a cache.LRUCache.
func (si *SchemaInfo) GetPlan(ctx context.Context, logStats *tabletenv.LogStats, sql string) (*ExecPlan, error) {
	span := trace.NewSpanFromContext(ctx)
	span.StartLocal("SchemaInfo.GetPlan")
	defer span.Finish()

	// Fastpath if plan already exists.
	if plan := si.getQuery(sql); plan != nil {
		return plan, nil
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
		return plan, nil
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
		return nil, tabletenv.PrefixTabletError(vtrpcpb.ErrorCode_UNKNOWN_ERROR, err, "")
	}
	plan := &ExecPlan{ExecPlan: splan, TableInfo: tableInfo}
	plan.Rules = si.queryRuleSources.filterByPlan(sql, plan.PlanID, plan.TableName.String())
	plan.Authorized = tableacl.Authorized(plan.TableName.String(), plan.PlanID.MinRole())
	if plan.PlanID.IsSelect() {
		if plan.FieldQuery == nil {
			log.Warningf("Cannot cache field info: %s", sql)
		} else {
			conn, err := si.conns.Get(ctx)
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
	si.queries.Set(sql, plan)
	return plan, nil
}

// GetStreamPlan is similar to GetPlan, but doesn't use the cache
// and doesn't enforce a limit. It just returns the parsed query.
func (si *SchemaInfo) GetStreamPlan(sql string) (*ExecPlan, error) {
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
		return nil, tabletenv.PrefixTabletError(vtrpcpb.ErrorCode_BAD_INPUT, err, "")
	}
	plan := &ExecPlan{ExecPlan: splan, TableInfo: tableInfo}
	plan.Rules = si.queryRuleSources.filterByPlan(sql, plan.PlanID, plan.TableName.String())
	plan.Authorized = tableacl.Authorized(plan.TableName.String(), plan.PlanID.MinRole())
	return plan, nil
}

// GetTable returns the TableInfo for a table.
func (si *SchemaInfo) GetTable(tableName sqlparser.TableIdent) *TableInfo {
	si.mu.Lock()
	defer si.mu.Unlock()
	return si.tables[tableName.String()]
}

// GetSchema returns the current schema. The Tables are a shared
// data strucutre and must be treated as read-only.
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
		size = 1
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
