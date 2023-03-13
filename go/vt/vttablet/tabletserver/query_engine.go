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

package tabletserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/cache"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
	tacl "vitess.io/vitess/go/vt/tableacl/acl"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/txserializer"
)

// _______________________________________________

// TabletPlan wraps the planbuilder's exec plan to enforce additional rules
// and track stats.
type TabletPlan struct {
	*planbuilder.Plan
	Original   string
	Rules      *rules.Rules
	Authorized []*tableacl.ACLResult

	QueryCount   uint64
	Time         uint64
	MysqlTime    uint64
	RowsAffected uint64
	RowsReturned uint64
	ErrorCount   uint64
}

// AddStats updates the stats for the current TabletPlan.
func (ep *TabletPlan) AddStats(queryCount uint64, duration, mysqlTime time.Duration, rowsAffected, rowsReturned, errorCount uint64) {
	atomic.AddUint64(&ep.QueryCount, queryCount)
	atomic.AddUint64(&ep.Time, uint64(duration))
	atomic.AddUint64(&ep.MysqlTime, uint64(mysqlTime))
	atomic.AddUint64(&ep.RowsAffected, rowsAffected)
	atomic.AddUint64(&ep.RowsReturned, rowsReturned)
	atomic.AddUint64(&ep.ErrorCount, errorCount)
}

// Stats returns the current stats of TabletPlan.
func (ep *TabletPlan) Stats() (queryCount uint64, duration, mysqlTime time.Duration, rowsAffected, rowsReturned, errorCount uint64) {
	queryCount = atomic.LoadUint64(&ep.QueryCount)
	duration = time.Duration(atomic.LoadUint64(&ep.Time))
	mysqlTime = time.Duration(atomic.LoadUint64(&ep.MysqlTime))
	rowsAffected = atomic.LoadUint64(&ep.RowsAffected)
	rowsReturned = atomic.LoadUint64(&ep.RowsReturned)
	errorCount = atomic.LoadUint64(&ep.ErrorCount)
	return
}

// buildAuthorized builds 'Authorized', which is the runtime part for 'Permissions'.
func (ep *TabletPlan) buildAuthorized() {
	ep.Authorized = make([]*tableacl.ACLResult, len(ep.Permissions))
	for i, perm := range ep.Permissions {
		ep.Authorized[i] = tableacl.Authorized(perm.TableName, perm.Role)
	}
}

func (ep *TabletPlan) IsValid(hasReservedCon, hasSysSettings bool) error {
	if !ep.NeedsReservedConn {
		return nil
	}
	return isValid(ep.PlanID, hasReservedCon, hasSysSettings)
}

func isValid(planType planbuilder.PlanType, hasReservedCon bool, hasSysSettings bool) error {
	switch planType {
	case planbuilder.PlanSelectLockFunc, planbuilder.PlanDDL:
		if hasReservedCon {
			return nil
		}
	case planbuilder.PlanSet:
		if hasReservedCon || hasSysSettings {
			return nil
		}
	}
	return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "%s not allowed without reserved connection", planType.String())
}

// _______________________________________________

// QueryEngine implements the core functionality of tabletserver.
// It assumes that no requests will be sent to it before Open is
// called and succeeds.
// Shutdown is done in the following order:
//
// Close: There should be no more pending queries when this
// function is called.
type QueryEngine struct {
	isOpen bool
	env    tabletenv.Env
	se     *schema.Engine

	// mu protects the following fields.
	mu               sync.RWMutex
	tables           map[string]*schema.Table
	plans            cache.Cache
	queryRuleSources *rules.Map

	// Pools
	conns       *connpool.Pool
	streamConns *connpool.Pool

	// Services
	consolidator       *sync2.Consolidator
	streamConsolidator *StreamConsolidator
	// txSerializer protects vttablet from applications which try to concurrently
	// UPDATE (or DELETE) a "hot" row (or range of rows).
	// Such queries would be serialized by MySQL anyway. This serializer prevents
	// that we start more than one transaction per hot row (range).
	// For implementation details, please see BeginExecute() in tabletserver.go.
	txSerializer *txserializer.TxSerializer

	// Vars
	maxResultSize    atomic.Int64
	warnResultSize   atomic.Int64
	streamBufferSize atomic.Int64
	// tableaclExemptCount count the number of accesses allowed
	// based on membership in the superuser ACL
	tableaclExemptCount  atomic.Int64
	strictTableACL       bool
	enableTableACLDryRun bool
	// TODO(sougou) There are two acl packages. Need to rename.
	exemptACL tacl.ACL

	strictTransTables bool

	consolidatorMode atomic.Value

	// stats
	// Note: queryErrorCountsWithCode is similar to queryErrorCounts except it contains error code as an additional dimension
	queryCounts, queryTimes, queryErrorCounts, queryErrorCountsWithCode, queryRowsAffected, queryRowsReturned *stats.CountersWithMultiLabels

	// Loggers
	accessCheckerLogger *logutil.ThrottledLogger
}

// NewQueryEngine creates a new QueryEngine.
// This is a singleton class.
// You must call this only once.
func NewQueryEngine(env tabletenv.Env, se *schema.Engine) *QueryEngine {
	config := env.Config()
	cacheCfg := &cache.Config{
		MaxEntries:     int64(config.QueryCacheSize),
		MaxMemoryUsage: config.QueryCacheMemory,
		LFU:            config.QueryCacheLFU,
	}

	qe := &QueryEngine{
		env:              env,
		se:               se,
		tables:           make(map[string]*schema.Table),
		plans:            cache.NewDefaultCacheImpl(cacheCfg),
		queryRuleSources: rules.NewMap(),
	}

	qe.conns = connpool.NewPool(env, "ConnPool", config.OltpReadPool)
	qe.streamConns = connpool.NewPool(env, "StreamConnPool", config.OlapReadPool)
	qe.consolidatorMode.Store(config.Consolidator)
	qe.consolidator = sync2.NewConsolidator()
	if config.ConsolidatorStreamTotalSize > 0 && config.ConsolidatorStreamQuerySize > 0 {
		log.Infof("Stream consolidator is enabled with query size set to %d and total size set to %d.",
			config.ConsolidatorStreamQuerySize, config.ConsolidatorStreamTotalSize)
		qe.streamConsolidator = NewStreamConsolidator(config.ConsolidatorStreamTotalSize, config.ConsolidatorStreamQuerySize, returnStreamResult)
	} else {
		log.Info("Stream consolidator is not enabled.")
	}
	qe.txSerializer = txserializer.New(env)

	qe.strictTableACL = config.StrictTableACL
	qe.enableTableACLDryRun = config.EnableTableACLDryRun

	qe.strictTransTables = config.EnforceStrictTransTables

	if config.TableACLExemptACL != "" {
		if f, err := tableacl.GetCurrentACLFactory(); err == nil {
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

	qe.maxResultSize.Store(int64(config.Oltp.MaxRows))
	qe.warnResultSize.Store(int64(config.Oltp.WarnRows))
	qe.streamBufferSize.Store(int64(config.StreamBufferSize))

	planbuilder.PassthroughDMLs = config.PassthroughDML

	qe.accessCheckerLogger = logutil.NewThrottledLogger("accessChecker", 1*time.Second)

	env.Exporter().NewGaugeFunc("MaxResultSize", "Query engine max result size", qe.maxResultSize.Load)
	env.Exporter().NewGaugeFunc("WarnResultSize", "Query engine warn result size", qe.warnResultSize.Load)
	env.Exporter().NewGaugeFunc("StreamBufferSize", "Query engine stream buffer size", qe.streamBufferSize.Load)
	env.Exporter().NewCounterFunc("TableACLExemptCount", "Query engine table ACL exempt count", qe.tableaclExemptCount.Load)

	env.Exporter().NewGaugeFunc("QueryCacheLength", "Query engine query cache length", func() int64 {
		return int64(qe.plans.Len())
	})
	env.Exporter().NewGaugeFunc("QueryCacheSize", "Query engine query cache size", qe.plans.UsedCapacity)
	env.Exporter().NewGaugeFunc("QueryCacheCapacity", "Query engine query cache capacity", qe.plans.MaxCapacity)
	env.Exporter().NewCounterFunc("QueryCacheEvictions", "Query engine query cache evictions", qe.plans.Evictions)
	qe.queryCounts = env.Exporter().NewCountersWithMultiLabels("QueryCounts", "query counts", []string{"Table", "Plan"})
	qe.queryTimes = env.Exporter().NewCountersWithMultiLabels("QueryTimesNs", "query times in ns", []string{"Table", "Plan"})
	qe.queryRowsAffected = env.Exporter().NewCountersWithMultiLabels("QueryRowsAffected", "query rows affected", []string{"Table", "Plan"})
	qe.queryRowsReturned = env.Exporter().NewCountersWithMultiLabels("QueryRowsReturned", "query rows returned", []string{"Table", "Plan"})
	qe.queryErrorCounts = env.Exporter().NewCountersWithMultiLabels("QueryErrorCounts", "query error counts", []string{"Table", "Plan"})
	qe.queryErrorCountsWithCode = env.Exporter().NewCountersWithMultiLabels("QueryErrorCountsWithCode", "query error counts with error code", []string{"Table", "Plan", "Code"})

	env.Exporter().HandleFunc("/debug/hotrows", qe.txSerializer.ServeHTTP)
	env.Exporter().HandleFunc("/debug/tablet_plans", qe.handleHTTPQueryPlans)
	env.Exporter().HandleFunc("/debug/query_stats", qe.handleHTTPQueryStats)
	env.Exporter().HandleFunc("/debug/query_rules", qe.handleHTTPQueryRules)
	env.Exporter().HandleFunc("/debug/consolidations", qe.handleHTTPConsolidations)
	env.Exporter().HandleFunc("/debug/acl", qe.handleHTTPAclJSON)

	return qe
}

// Open must be called before sending requests to QueryEngine.
func (qe *QueryEngine) Open() error {
	if qe.isOpen {
		return nil
	}
	log.Info("Query Engine: opening")

	qe.conns.Open(qe.env.Config().DB.AppWithDB(), qe.env.Config().DB.DbaWithDB(), qe.env.Config().DB.AppDebugWithDB())

	conn, err := qe.conns.Get(tabletenv.LocalContext(), nil)
	if err != nil {
		qe.conns.Close()
		return err
	}
	err = conn.VerifyMode(qe.strictTransTables)
	// Recycle needs to happen before error check.
	// Otherwise, qe.conns.Close will hang.
	conn.Recycle()

	if err != nil {
		qe.conns.Close()
		return err
	}

	qe.streamConns.Open(qe.env.Config().DB.AppWithDB(), qe.env.Config().DB.DbaWithDB(), qe.env.Config().DB.AppDebugWithDB())
	qe.se.RegisterNotifier("qe", qe.schemaChanged)
	qe.isOpen = true
	return nil
}

// Close must be called to shut down QueryEngine.
// You must ensure that no more queries will be sent
// before calling Close.
func (qe *QueryEngine) Close() {
	if !qe.isOpen {
		return
	}
	// Close in reverse order of Open.
	qe.se.UnregisterNotifier("qe")
	qe.plans.Clear()
	qe.tables = make(map[string]*schema.Table)
	qe.streamConns.Close()
	qe.conns.Close()
	qe.isOpen = false
	log.Info("Query Engine: closed")
}

// GetPlan returns the TabletPlan that for the query. Plans are cached in a cache.LRUCache.
func (qe *QueryEngine) GetPlan(ctx context.Context, logStats *tabletenv.LogStats, sql string, skipQueryPlanCache bool) (*TabletPlan, error) {
	span, _ := trace.NewSpan(ctx, "QueryEngine.GetPlan")
	defer span.Finish()
	if !skipQueryPlanCache {
		if plan := qe.getQuery(sql); plan != nil {
			logStats.CachedPlan = true
			return plan, nil
		}
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
	splan, err := planbuilder.Build(statement, qe.tables, qe.env.Config().DB.DBName, qe.env.Config().EnableViews)
	if err != nil {
		return nil, err
	}
	plan := &TabletPlan{Plan: splan, Original: sql}
	plan.Rules = qe.queryRuleSources.FilterByPlan(sql, plan.PlanID, plan.TableNames()...)
	plan.buildAuthorized()
	if plan.PlanID == planbuilder.PlanDDL || plan.PlanID == planbuilder.PlanSet {
		return plan, nil
	}
	if !skipQueryPlanCache && !sqlparser.SkipQueryPlanCacheDirective(statement) {
		qe.plans.Set(sql, plan)
	}
	return plan, nil
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
	plan := &TabletPlan{Plan: splan, Original: sql}
	plan.Rules = qe.queryRuleSources.FilterByPlan(sql, plan.PlanID, plan.TableName().String())
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
	plan.buildAuthorized()
	return plan, nil
}

// GetConnSetting returns system settings for the connection.
func (qe *QueryEngine) GetConnSetting(ctx context.Context, settings []string) (*pools.Setting, error) {
	span, _ := trace.NewSpan(ctx, "QueryEngine.GetConnSetting")
	defer span.Finish()

	var keyBuilder strings.Builder
	for _, q := range settings {
		keyBuilder.WriteString(q)
	}

	// try to get the connSetting from the cache
	cacheKey := keyBuilder.String()
	if plan := qe.getConnSetting(cacheKey); plan != nil {
		return plan, nil
	}

	// build the setting queries
	query, resetQuery, err := planbuilder.BuildSettingQuery(settings)
	if err != nil {
		return nil, err
	}
	connSetting := pools.NewSetting(query, resetQuery)

	// store the connSetting in the cache
	qe.plans.Set(cacheKey, connSetting)

	return connSetting, nil
}

// ClearQueryPlanCache should be called if query plan cache is potentially obsolete
func (qe *QueryEngine) ClearQueryPlanCache() {
	qe.plans.Clear()
}

// IsMySQLReachable returns an error if it cannot connect to MySQL.
// This can be called before opening the QueryEngine.
func (qe *QueryEngine) IsMySQLReachable() error {
	conn, err := dbconnpool.NewDBConnection(context.TODO(), qe.env.Config().DB.AppWithDB())
	if err != nil {
		if mysql.IsTooManyConnectionsErr(err) {
			return nil
		}
		return err
	}
	conn.Close()
	return nil
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
	cacheResult, ok := qe.plans.Get(sql)
	if !ok {
		return nil
	}
	plan, ok := cacheResult.(*TabletPlan)
	if ok {
		return plan
	}
	return nil
}

func (qe *QueryEngine) getConnSetting(key string) *pools.Setting {
	cacheResult, ok := qe.plans.Get(key)
	if !ok {
		return nil
	}
	plan, ok := cacheResult.(*pools.Setting)
	if ok {
		return plan
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
	return int(qe.plans.MaxCapacity())
}

// QueryPlanCacheLen returns the length (size in entries) of the query cache
func (qe *QueryEngine) QueryPlanCacheLen() int {
	qe.plans.Wait()
	return qe.plans.Len()
}

// AddStats adds the given stats for the planName.tableName
func (qe *QueryEngine) AddStats(planType planbuilder.PlanType, tableName string, queryCount int64, duration, mysqlTime time.Duration, rowsAffected, rowsReturned, errorCount int64, errorCode string) {
	// table names can contain "." characters, replace them!
	keys := []string{tableName, planType.String()}
	qe.queryCounts.Add(keys, queryCount)
	qe.queryTimes.Add(keys, int64(duration))
	qe.queryErrorCounts.Add(keys, errorCount)
	// queryErrorCountsWithCode is similar to queryErrorCounts except we have an additional dimension
	// of error code.
	if errorCount > 0 {
		errorKeys := []string{tableName, planType.String(), errorCode}
		qe.queryErrorCountsWithCode.Add(errorKeys, errorCount)
	}

	// For certain plan types like select, we only want to add their metrics to rows returned
	// But there are special cases like `SELECT ... INTO OUTFILE ''` which return positive rows affected
	// So we check if it is positive and add that too.
	switch planType {
	case planbuilder.PlanSelect, planbuilder.PlanSelectStream, planbuilder.PlanSelectImpossible, planbuilder.PlanShow, planbuilder.PlanOtherRead:
		qe.queryRowsReturned.Add(keys, rowsReturned)
		if rowsAffected > 0 {
			qe.queryRowsAffected.Add(keys, rowsAffected)
		}
	default:
		qe.queryRowsAffected.Add(keys, rowsAffected)
		if rowsReturned > 0 {
			qe.queryRowsReturned.Add(keys, rowsReturned)
		}
	}
}

type perQueryStats struct {
	Query        string
	Table        string
	Plan         planbuilder.PlanType
	QueryCount   uint64
	Time         time.Duration
	MysqlTime    time.Duration
	RowsAffected uint64
	RowsReturned uint64
	ErrorCount   uint64
}

func (qe *QueryEngine) handleHTTPQueryPlans(response http.ResponseWriter, request *http.Request) {
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}

	response.Header().Set("Content-Type", "text/plain")
	qe.plans.ForEach(func(value any) bool {
		plan := value.(*TabletPlan)
		response.Write([]byte(fmt.Sprintf("%#v\n", sqlparser.TruncateForUI(plan.Original))))
		if b, err := json.MarshalIndent(plan.Plan, "", "  "); err != nil {
			response.Write([]byte(err.Error()))
		} else {
			response.Write(b)
		}
		response.Write(([]byte)("\n\n"))
		return true
	})
}

func (qe *QueryEngine) handleHTTPQueryStats(response http.ResponseWriter, request *http.Request) {
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	var qstats []perQueryStats
	qe.plans.ForEach(func(value any) bool {
		plan := value.(*TabletPlan)

		var pqstats perQueryStats
		pqstats.Query = unicoded(sqlparser.TruncateForUI(plan.Original))
		pqstats.Table = plan.TableName().String()
		pqstats.Plan = plan.PlanID
		pqstats.QueryCount, pqstats.Time, pqstats.MysqlTime, pqstats.RowsAffected, pqstats.RowsReturned, pqstats.ErrorCount = plan.Stats()

		qstats = append(qstats, pqstats)
		return true
	})
	if b, err := json.MarshalIndent(qstats, "", "  "); err != nil {
		response.Write([]byte(err.Error()))
	} else {
		response.Write(b)
	}
}

func (qe *QueryEngine) handleHTTPQueryRules(response http.ResponseWriter, request *http.Request) {
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}
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
	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}
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
		if streamlog.GetRedactDebugUIQueries() {
			query, _ = sqlparser.RedactSQLQuery(v.Query)
		} else {
			query = v.Query
		}
		response.Write([]byte(fmt.Sprintf("%v: %s\n", v.Count, query)))
	}
}

// unicoded returns a valid UTF-8 string that json won't reject
func unicoded(in string) (out string) {
	for i, v := range in {
		if v == 0xFFFD {
			return in[:i]
		}
	}
	return in
}
