/*
Copyright 2017 Google Inc.

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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/cache"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
	tacl "vitess.io/vitess/go/vt/tableacl/acl"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/txserializer"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

//_______________________________________________

// TabletPlan wraps the planbuilder's exec plan to enforce additional rules
// and track stats.
type TabletPlan struct {
	*planbuilder.Plan
	Fields           []*querypb.Field
	Rules            *rules.Rules
	LegacyAuthorized *tableacl.ACLResult
	Authorized       []*tableacl.ACLResult

	mu         sync.Mutex
	QueryCount int64
	Time       time.Duration
	MysqlTime  time.Duration
	RowCount   int64
	ErrorCount int64
}

// Size allows TabletPlan to be in cache.LRUCache.
func (*TabletPlan) Size() int {
	return 1
}

// AddStats updates the stats for the current TabletPlan.
func (ep *TabletPlan) AddStats(queryCount int64, duration, mysqlTime time.Duration, rowCount, errorCount int64) {
	ep.mu.Lock()
	ep.QueryCount += queryCount
	ep.Time += duration
	ep.MysqlTime += mysqlTime
	ep.RowCount += rowCount
	ep.ErrorCount += errorCount
	ep.mu.Unlock()
}

// Stats returns the current stats of TabletPlan.
func (ep *TabletPlan) Stats() (queryCount int64, duration, mysqlTime time.Duration, rowCount, errorCount int64) {
	ep.mu.Lock()
	queryCount = ep.QueryCount
	duration = ep.Time
	mysqlTime = ep.MysqlTime
	rowCount = ep.RowCount
	errorCount = ep.ErrorCount
	ep.mu.Unlock()
	return
}

// buildAuthorized builds 'Authorized', which is the runtime part for 'Permissions'.
func (ep *TabletPlan) buildAuthorized() {
	ep.Authorized = make([]*tableacl.ACLResult, len(ep.Permissions))
	for i, perm := range ep.Permissions {
		ep.Authorized[i] = tableacl.Authorized(perm.TableName, perm.Role)
	}
}

//_______________________________________________

// QueryEngine implements the core functionality of tabletserver.
// It assumes that no requests will be sent to it before Open is
// called and succeeds.
// Shutdown is done in the following order:
//
// Close: There should be no more pending queries when this
// function is called.
type QueryEngine struct {
	se        *schema.Engine
	dbconfigs *dbconfigs.DBConfigs

	// mu protects the following fields.
	mu               sync.RWMutex
	tables           map[string]*schema.Table
	plans            *cache.LRUCache
	queryRuleSources *rules.Map

	queryStatsMu sync.RWMutex
	queryStats   map[string]*QueryStats

	// Pools
	conns       *connpool.Pool
	streamConns *connpool.Pool

	// Services
	consolidator *sync2.Consolidator
	// txSerializer protects vttablet from applications which try to concurrently
	// UPDATE (or DELETE) a "hot" row (or range of rows).
	// Such queries would be serialized by MySQL anyway. This serializer prevents
	// that we start more than one transaction per hot row (range).
	// For implementation details, please see BeginExecute() in tabletserver.go.
	txSerializer *txserializer.TxSerializer
	streamQList  *QueryList

	// Vars
	connTimeout        sync2.AtomicDuration
	queryPoolWaiters   sync2.AtomicInt64
	queryPoolWaiterCap sync2.AtomicInt64
	binlogFormat       connpool.BinlogFormat
	autoCommit         sync2.AtomicBool
	maxResultSize      sync2.AtomicInt64
	warnResultSize     sync2.AtomicInt64
	maxDMLRows         sync2.AtomicInt64
	passthroughDMLs    sync2.AtomicBool
	allowUnsafeDMLs    bool
	streamBufferSize   sync2.AtomicInt64
	// tableaclExemptCount count the number of accesses allowed
	// based on membership in the superuser ACL
	tableaclExemptCount  sync2.AtomicInt64
	strictTableACL       bool
	enableTableACLDryRun bool
	// TODO(sougou) There are two acl packages. Need to rename.
	exemptACL tacl.ACL

	strictTransTables bool

	enableConsolidator bool

	// Loggers
	accessCheckerLogger *logutil.ThrottledLogger
}

var (
	qeOnce sync.Once
)

// NewQueryEngine creates a new QueryEngine.
// This is a singleton class.
// You must call this only once.
func NewQueryEngine(checker connpool.MySQLChecker, se *schema.Engine, config tabletenv.TabletConfig) *QueryEngine {
	qe := &QueryEngine{
		se:                 se,
		tables:             make(map[string]*schema.Table),
		plans:              cache.NewLRUCache(int64(config.QueryPlanCacheSize)),
		queryRuleSources:   rules.NewMap(),
		queryPoolWaiterCap: sync2.NewAtomicInt64(int64(config.QueryPoolWaiterCap)),
		queryStats:         make(map[string]*QueryStats),
	}

	qe.conns = connpool.New(
		config.PoolNamePrefix+"ConnPool",
		config.PoolSize,
		time.Duration(config.IdleTimeout*1e9),
		0,
		checker,
	)
	qe.connTimeout.Set(time.Duration(config.QueryPoolTimeout * 1e9))

	qe.streamConns = connpool.New(
		config.PoolNamePrefix+"StreamConnPool",
		config.StreamPoolSize,
		time.Duration(config.IdleTimeout*1e9),
		0,
		checker,
	)
	qe.enableConsolidator = config.EnableConsolidator
	qe.consolidator = sync2.NewConsolidator()
	qe.txSerializer = txserializer.New(config.EnableHotRowProtectionDryRun,
		config.HotRowProtectionMaxQueueSize,
		config.HotRowProtectionMaxGlobalQueueSize,
		config.HotRowProtectionConcurrentTransactions)
	qe.streamQList = NewQueryList()

	qe.autoCommit.Set(config.EnableAutoCommit)
	qe.strictTableACL = config.StrictTableACL
	qe.enableTableACLDryRun = config.EnableTableACLDryRun

	qe.strictTransTables = config.EnforceStrictTransTables

	if config.TableACLExemptACL != "" {
		if f, err := tableacl.GetCurrentAclFactory(); err == nil {
			if exemptACL, err := f.New([]string{config.TableACLExemptACL}); err == nil {
				log.Infof("Setting Table ACL exempt rule for %v", config.TableACLExemptACL)
				qe.exemptACL = exemptACL
			} else {
				log.Infof("Cannot build exempt ACL for table ACL: %v", err)
			}
		} else {
			log.Infof("Cannot get current ACL Factory: %v", err)
		}
	}

	qe.maxResultSize = sync2.NewAtomicInt64(int64(config.MaxResultSize))
	qe.warnResultSize = sync2.NewAtomicInt64(int64(config.WarnResultSize))
	qe.maxDMLRows = sync2.NewAtomicInt64(int64(config.MaxDMLRows))
	qe.streamBufferSize = sync2.NewAtomicInt64(int64(config.StreamBufferSize))

	qe.passthroughDMLs = sync2.NewAtomicBool(config.PassthroughDMLs)
	qe.allowUnsafeDMLs = config.AllowUnsafeDMLs
	planbuilder.PassthroughDMLs = config.PassthroughDMLs

	qe.accessCheckerLogger = logutil.NewThrottledLogger("accessChecker", 1*time.Second)

	qeOnce.Do(func() {
		stats.NewGaugeFunc("MaxResultSize", "Query engine max result size", qe.maxResultSize.Get)
		stats.NewGaugeFunc("WarnResultSize", "Query engine warn result size", qe.warnResultSize.Get)
		stats.NewGaugeFunc("MaxDMLRows", "Query engine max DML rows", qe.maxDMLRows.Get)
		stats.NewGaugeFunc("StreamBufferSize", "Query engine stream buffer size", qe.streamBufferSize.Get)
		stats.NewCounterFunc("TableACLExemptCount", "Query engine table ACL exempt count", qe.tableaclExemptCount.Get)
		stats.NewGaugeFunc("QueryPoolWaiters", "Query engine query pool waiters", qe.queryPoolWaiters.Get)

		stats.NewGaugeFunc("QueryCacheLength", "Query engine query cache length", qe.plans.Length)
		stats.NewGaugeFunc("QueryCacheSize", "Query engine query cache size", qe.plans.Size)
		stats.NewGaugeFunc("QueryCacheCapacity", "Query engine query cache capacity", qe.plans.Capacity)
		stats.NewCounterFunc("QueryCacheEvictions", "Query engine query cache evictions", qe.plans.Evictions)
		stats.Publish("QueryCacheOldest", stats.StringFunc(func() string {
			return fmt.Sprintf("%v", qe.plans.Oldest())
		}))
		_ = stats.NewCountersFuncWithMultiLabels("QueryCounts", "query counts", []string{"Table", "Plan"}, qe.getQueryCount)
		_ = stats.NewCountersFuncWithMultiLabels("QueryTimesNs", "query times in ns", []string{"Table", "Plan"}, qe.getQueryTime)
		_ = stats.NewCountersFuncWithMultiLabels("QueryRowCounts", "query row counts", []string{"Table", "Plan"}, qe.getQueryRowCount)
		_ = stats.NewCountersFuncWithMultiLabels("QueryErrorCounts", "query error counts", []string{"Table", "Plan"}, qe.getQueryErrorCount)

		http.Handle("/debug/hotrows", qe.txSerializer)

		endpoints := []string{
			"/debug/tablet_plans",
			"/debug/query_stats",
			"/debug/query_rules",
			"/debug/consolidations",
			"/debug/acl",
		}
		for _, ep := range endpoints {
			http.Handle(ep, qe)
		}
	})

	return qe
}

// InitDBConfig must be called before Open.
func (qe *QueryEngine) InitDBConfig(dbcfgs *dbconfigs.DBConfigs) {
	qe.dbconfigs = dbcfgs
}

// Open must be called before sending requests to QueryEngine.
func (qe *QueryEngine) Open() error {
	qe.conns.Open(qe.dbconfigs.AppWithDB(), qe.dbconfigs.DbaWithDB(), qe.dbconfigs.AppDebugWithDB())

	conn, err := qe.conns.Get(tabletenv.LocalContext())
	if err != nil {
		qe.conns.Close()
		return err
	}
	qe.binlogFormat, err = conn.VerifyMode(qe.strictTransTables)
	conn.Recycle()

	if err != nil {
		qe.conns.Close()
		return err
	}

	qe.streamConns.Open(qe.dbconfigs.AppWithDB(), qe.dbconfigs.DbaWithDB(), qe.dbconfigs.AppDebugWithDB())
	qe.se.RegisterNotifier("qe", qe.schemaChanged)
	return nil
}

// Close must be called to shut down QueryEngine.
// You must ensure that no more queries will be sent
// before calling Close.
func (qe *QueryEngine) Close() {
	// Close in reverse order of Open.
	qe.se.UnregisterNotifier("qe")
	qe.plans.Clear()
	qe.tables = make(map[string]*schema.Table)
	qe.streamConns.Close()
	qe.conns.Close()
}

// GetPlan returns the TabletPlan that for the query. Plans are cached in a cache.LRUCache.
func (qe *QueryEngine) GetPlan(ctx context.Context, logStats *tabletenv.LogStats, sql string, skipQueryPlanCache bool) (*TabletPlan, error) {
	span := trace.NewSpanFromContext(ctx)
	span.StartLocal("QueryEngine.GetPlan")
	defer span.Finish()

	if plan := qe.getQuery(sql); plan != nil {
		return plan, nil
	}

	// Obtain read lock to prevent schema from changing while
	// we build a plan. The read lock allows multiple identical
	// queries to build the same plan. One of them will win by
	// updating the query cache and prevent future races. Due to
	// this, query stats reporting may not be accurate, but it's
	// acceptable because those numbers are best effort.
	qe.mu.RLock()
	defer qe.mu.RUnlock()
	statement, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	splan, err := planbuilder.Build(statement, qe.tables)
	if err != nil {
		return nil, err
	}
	plan := &TabletPlan{Plan: splan}
	plan.Rules = qe.queryRuleSources.FilterByPlan(sql, plan.PlanID, plan.TableName().String())
	plan.LegacyAuthorized = tableacl.Authorized(plan.TableName().String(), plan.PlanID.MinRole())
	plan.buildAuthorized()
	if plan.PlanID.IsSelect() {
		if plan.FieldQuery != nil {
			conn, err := qe.getQueryConn(ctx)
			if err != nil {
				return nil, err
			}
			defer conn.Recycle()

			sql := plan.FieldQuery.Query
			start := time.Now()
			r, err := conn.Exec(ctx, sql, 1, true)
			logStats.AddRewrittenSQL(sql, start)
			if err != nil {
				return nil, err
			}
			plan.Fields = r.Fields
		}
	} else if plan.PlanID == planbuilder.PlanDDL || plan.PlanID == planbuilder.PlanSet {
		return plan, nil
	}
	if !skipQueryPlanCache && !sqlparser.SkipQueryPlanCacheDirective(statement) {
		qe.plans.Set(sql, plan)
	}
	return plan, nil
}

// getQueryConn returns a connection from the query pool using either
// the conn pool timeout if configured, or the original context query timeout
func (qe *QueryEngine) getQueryConn(ctx context.Context) (*connpool.DBConn, error) {
	waiterCount := qe.queryPoolWaiters.Add(1)
	defer qe.queryPoolWaiters.Add(-1)

	if waiterCount > qe.queryPoolWaiterCap.Get() {
		return nil, vterrors.New(vtrpcpb.Code_RESOURCE_EXHAUSTED, "query pool waiter count exceeded")
	}

	timeout := qe.connTimeout.Get()
	if timeout != 0 {
		ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		conn, err := qe.conns.Get(ctxTimeout)
		if err != nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "query pool wait time exceeded")
		}
		return conn, err
	}
	return qe.conns.Get(ctx)
}

// GetStreamPlan is similar to GetPlan, but doesn't use the cache
// and doesn't enforce a limit. It just returns the parsed query.
func (qe *QueryEngine) GetStreamPlan(sql string) (*TabletPlan, error) {
	qe.mu.RLock()
	defer qe.mu.RUnlock()
	splan, err := planbuilder.BuildStreaming(sql, qe.tables)
	if err != nil {
		return nil, err
	}
	plan := &TabletPlan{Plan: splan}
	plan.Rules = qe.queryRuleSources.FilterByPlan(sql, plan.PlanID, plan.TableName().String())
	plan.LegacyAuthorized = tableacl.Authorized(plan.TableName().String(), plan.PlanID.MinRole())
	plan.buildAuthorized()
	return plan, nil
}

// GetMessageStreamPlan builds a plan for Message streaming.
func (qe *QueryEngine) GetMessageStreamPlan(name string) (*TabletPlan, error) {
	qe.mu.RLock()
	defer qe.mu.RUnlock()
	splan, err := planbuilder.BuildMessageStreaming(name, qe.tables)
	if err != nil {
		return nil, err
	}
	plan := &TabletPlan{Plan: splan}
	plan.Rules = qe.queryRuleSources.FilterByPlan("stream from "+name, plan.PlanID, plan.TableName().String())
	plan.LegacyAuthorized = tableacl.Authorized(plan.TableName().String(), plan.PlanID.MinRole())
	plan.buildAuthorized()
	return plan, nil
}

// ClearQueryPlanCache should be called if query plan cache is potentially obsolete
func (qe *QueryEngine) ClearQueryPlanCache() {
	qe.plans.Clear()
}

// IsMySQLReachable returns true if we can connect to MySQL.
func (qe *QueryEngine) IsMySQLReachable() bool {
	conn, err := dbconnpool.NewDBConnection(qe.dbconfigs.AppWithDB(), tabletenv.MySQLStats)
	if err != nil {
		if mysql.IsConnErr(err) {
			return false
		}
		log.Warningf("checking MySQL, unexpected error: %v", err)
		return true
	}
	conn.Close()
	return true
}

func (qe *QueryEngine) schemaChanged(tables map[string]*schema.Table, created, altered, dropped []string) {
	qe.mu.Lock()
	defer qe.mu.Unlock()
	qe.tables = tables
	if len(altered) != 0 || len(dropped) != 0 {
		qe.plans.Clear()
	}
}

// getQuery fetches the plan and makes it the most recent.
func (qe *QueryEngine) getQuery(sql string) *TabletPlan {
	if cacheResult, ok := qe.plans.Get(sql); ok {
		return cacheResult.(*TabletPlan)
	}
	return nil
}

// peekQuery fetches the plan without changing the LRU order.
func (qe *QueryEngine) peekQuery(sql string) *TabletPlan {
	if cacheResult, ok := qe.plans.Peek(sql); ok {
		return cacheResult.(*TabletPlan)
	}
	return nil
}

// SetQueryPlanCacheCap sets the query plan cache capacity.
func (qe *QueryEngine) SetQueryPlanCacheCap(size int) {
	if size <= 0 {
		size = 1
	}
	qe.plans.SetCapacity(int64(size))
}

// QueryPlanCacheCap returns the capacity of the query cache.
func (qe *QueryEngine) QueryPlanCacheCap() int {
	return int(qe.plans.Capacity())
}

// QueryStats tracks query stats for export per planName/tableName
type QueryStats struct {
	mu         sync.Mutex
	queryCount int64
	time       time.Duration
	mysqlTime  time.Duration
	rowCount   int64
	errorCount int64
}

// AddStats adds the given stats for the planName.tableName
func (qe *QueryEngine) AddStats(planName, tableName string, queryCount int64, duration, mysqlTime time.Duration, rowCount, errorCount int64) {
	key := tableName + "." + planName

	qe.queryStatsMu.RLock()
	stats, ok := qe.queryStats[key]
	qe.queryStatsMu.RUnlock()

	if !ok {
		// Check again with the write lock held and
		// create a new record only if none exists
		qe.queryStatsMu.Lock()
		if stats, ok = qe.queryStats[key]; !ok {
			stats = &QueryStats{}
			qe.queryStats[key] = stats
		}
		qe.queryStatsMu.Unlock()
	}

	stats.mu.Lock()
	stats.queryCount += queryCount
	stats.time += duration
	stats.mysqlTime += mysqlTime
	stats.rowCount += rowCount
	stats.errorCount += errorCount
	stats.mu.Unlock()
}

func (qe *QueryEngine) getQueryCount() map[string]int64 {
	qstats := make(map[string]int64)
	qe.queryStatsMu.RLock()
	defer qe.queryStatsMu.RUnlock()
	for k, qs := range qe.queryStats {
		qs.mu.Lock()
		qstats[k] = qs.queryCount
		qs.mu.Unlock()
	}
	return qstats
}

func (qe *QueryEngine) getQueryTime() map[string]int64 {
	qstats := make(map[string]int64)
	qe.queryStatsMu.RLock()
	defer qe.queryStatsMu.RUnlock()
	for k, qs := range qe.queryStats {
		qs.mu.Lock()
		qstats[k] = int64(qs.time)
		qs.mu.Unlock()
	}
	return qstats
}

func (qe *QueryEngine) getQueryRowCount() map[string]int64 {
	qstats := make(map[string]int64)
	qe.queryStatsMu.RLock()
	defer qe.queryStatsMu.RUnlock()
	for k, qs := range qe.queryStats {
		qs.mu.Lock()
		qstats[k] = qs.rowCount
		qs.mu.Unlock()
	}
	return qstats
}

func (qe *QueryEngine) getQueryErrorCount() map[string]int64 {
	qstats := make(map[string]int64)
	qe.queryStatsMu.RLock()
	defer qe.queryStatsMu.RUnlock()
	for k, qs := range qe.queryStats {
		qs.mu.Lock()
		qstats[k] = qs.errorCount
		qs.mu.Unlock()
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

func (qe *QueryEngine) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}
	switch request.URL.Path {
	case "/debug/tablet_plans":
		qe.handleHTTPQueryPlans(response, request)
	case "/debug/query_stats":
		qe.handleHTTPQueryStats(response, request)
	case "/debug/query_rules":
		qe.handleHTTPQueryRules(response, request)
	case "/debug/acl":
		qe.handleHTTPAclJSON(response, request)
	case "/debug/consolidations":
		qe.handleHTTPConsolidations(response, request)
	default:
		response.WriteHeader(http.StatusNotFound)
	}
}

func (qe *QueryEngine) handleHTTPQueryPlans(response http.ResponseWriter, request *http.Request) {
	keys := qe.plans.Keys()
	response.Header().Set("Content-Type", "text/plain")
	response.Write([]byte(fmt.Sprintf("Length: %d\n", len(keys))))
	for _, v := range keys {
		response.Write([]byte(fmt.Sprintf("%#v\n", sqlparser.TruncateForUI(v))))
		if plan := qe.peekQuery(v); plan != nil {
			if b, err := json.MarshalIndent(plan.Plan, "", "  "); err != nil {
				response.Write([]byte(err.Error()))
			} else {
				response.Write(b)
			}
			response.Write(([]byte)("\n\n"))
		}
	}
}

func (qe *QueryEngine) handleHTTPQueryStats(response http.ResponseWriter, request *http.Request) {
	keys := qe.plans.Keys()
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	qstats := make([]perQueryStats, 0, len(keys))
	for _, v := range keys {
		if plan := qe.peekQuery(v); plan != nil {
			var pqstats perQueryStats
			pqstats.Query = unicoded(sqlparser.TruncateForUI(v))
			pqstats.Table = plan.TableName().String()
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

func (qe *QueryEngine) handleHTTPQueryRules(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	b, err := json.MarshalIndent(qe.queryRuleSources, "", " ")
	if err != nil {
		response.Write([]byte(err.Error()))
		return
	}
	buf := bytes.NewBuffer(nil)
	json.HTMLEscape(buf, b)
	response.Write(buf.Bytes())
}

func (qe *QueryEngine) handleHTTPAclJSON(response http.ResponseWriter, request *http.Request) {
	aclConfig := tableacl.GetCurrentConfig()
	if aclConfig == nil {
		response.WriteHeader(http.StatusNotFound)
		return
	}
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	b, err := json.MarshalIndent(aclConfig, "", " ")
	if err != nil {
		response.Write([]byte(err.Error()))
		return
	}
	buf := bytes.NewBuffer(nil)
	json.HTMLEscape(buf, b)
	response.Write(buf.Bytes())
}

// ServeHTTP lists the most recent, cached queries and their count.
func (qe *QueryEngine) handleHTTPConsolidations(response http.ResponseWriter, request *http.Request) {
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}
	items := qe.consolidator.Items()
	response.Header().Set("Content-Type", "text/plain")
	if items == nil {
		response.Write([]byte("empty\n"))
		return
	}
	response.Write([]byte(fmt.Sprintf("Length: %d\n", len(items))))
	for _, v := range items {
		var query string
		if *streamlog.RedactDebugUIQueries {
			query, _ = sqlparser.RedactSQLQuery(v.Query)
		} else {
			query = v.Query
		}
		response.Write([]byte(fmt.Sprintf("%v: %s\n", v.Count, query)))
	}
}
