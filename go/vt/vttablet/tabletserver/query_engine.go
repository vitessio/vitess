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
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/cache/theine"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/pools/smartconnpool"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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
	case planbuilder.PlanSelectLockFunc, planbuilder.PlanDDL, planbuilder.PlanFlush:
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

type PlanCacheKey = theine.StringKey
type PlanCache = theine.Store[PlanCacheKey, *TabletPlan]

type SettingsCacheKey = theine.StringKey
type SettingsCache = theine.Store[SettingsCacheKey, *smartconnpool.Setting]

type currentSchema struct {
	tables map[string]*schema.Table
	epoch  uint32
}

// QueryEngine implements the core functionality of tabletserver.
// It assumes that no requests will be sent to it before Open is
// called and succeeds.
// Shutdown is done in the following order:
//
// Close: There should be no more pending queries when this
// function is called.
type QueryEngine struct {
	isOpen atomic.Bool
	env    tabletenv.Env
	se     *schema.Engine

	// mu protects the following fields.
	schemaMu sync.Mutex
	epoch    uint32
	schema   atomic.Pointer[currentSchema]

	plans            *PlanCache
	settings         *SettingsCache
	queryRuleSources *rules.Map

	// Pools
	conns       *connpool.Pool
	streamConns *connpool.Pool

	// Services
	consolidator       sync2.Consolidator
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
	queryCounts, queryCountsWithTabletType, queryTimes, queryErrorCounts, queryErrorCountsWithCode, queryRowsAffected, queryRowsReturned *stats.CountersWithMultiLabels
	queryCacheHits, queryCacheMisses                                                                                                     *stats.CounterFunc

	// stats flags
	enablePerWorkloadTableMetrics bool

	// Loggers
	accessCheckerLogger *logutil.ThrottledLogger
}

// NewQueryEngine creates a new QueryEngine.
// This is a singleton class.
// You must call this only once.
func NewQueryEngine(env tabletenv.Env, se *schema.Engine) *QueryEngine {
	config := env.Config()

	qe := &QueryEngine{
		env:                           env,
		se:                            se,
		queryRuleSources:              rules.NewMap(),
		enablePerWorkloadTableMetrics: config.EnablePerWorkloadTableMetrics,
	}

	// Cache for query plans: user configured size with a doorkeeper by default to prevent one-off queries
	// from thrashing the cache.
	qe.plans = theine.NewStore[PlanCacheKey, *TabletPlan](config.QueryCacheMemory, config.QueryCacheDoorkeeper)

	// cache for connection settings: default to 1/4th of the size for the query cache and do
	// not use a doorkeeper because custom connection settings are rarely one-off and we always
	// want to cache them
	var settingsCacheMemory = config.QueryCacheMemory / 4
	qe.settings = theine.NewStore[SettingsCacheKey, *smartconnpool.Setting](settingsCacheMemory, false)

	qe.schema.Store(&currentSchema{
		tables: make(map[string]*schema.Table),
		epoch:  0,
	})

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
	env.Exporter().NewGaugeFunc("QueryCacheSize", "Query engine query cache size", func() int64 {
		return int64(qe.plans.UsedCapacity())
	})
	env.Exporter().NewGaugeFunc("QueryCacheCapacity", "Query engine query cache capacity", func() int64 {
		return int64(qe.plans.MaxCapacity())
	})
	env.Exporter().NewCounterFunc("QueryCacheEvictions", "Query engine query cache evictions", func() int64 {
		return qe.plans.Metrics.Evicted()
	})
	qe.queryCacheHits = env.Exporter().NewCounterFunc("QueryCacheHits", "Query engine query cache hits", func() int64 {
		return qe.plans.Metrics.Hits()
	})
	qe.queryCacheMisses = env.Exporter().NewCounterFunc("QueryCacheMisses", "Query engine query cache misses", func() int64 {
		return qe.plans.Metrics.Misses()
	})

	labels := []string{"Table", "Plan"}
	if config.EnablePerWorkloadTableMetrics {
		labels = []string{"Table", "Plan", "Workload"}
	}

	qe.queryCounts = env.Exporter().NewCountersWithMultiLabels("QueryCounts", "query counts", labels)
	qe.queryCountsWithTabletType = env.Exporter().NewCountersWithMultiLabels("QueryCountsWithTabletType", "query counts with tablet type labels", []string{"Table", "Plan", "TabletType"})
	qe.queryTimes = env.Exporter().NewCountersWithMultiLabels("QueryTimesNs", "query times in ns", labels)
	qe.queryRowsAffected = env.Exporter().NewCountersWithMultiLabels("QueryRowsAffected", "query rows affected", labels)
	qe.queryRowsReturned = env.Exporter().NewCountersWithMultiLabels("QueryRowsReturned", "query rows returned", labels)
	qe.queryErrorCounts = env.Exporter().NewCountersWithMultiLabels("QueryErrorCounts", "query error counts", labels)
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
	if qe.isOpen.Load() {
		return nil
	}
	log.Info("Query Engine: opening")

	config := qe.env.Config()

	qe.conns.Open(config.DB.AppWithDB(), config.DB.DbaWithDB(), config.DB.AppDebugWithDB())

	conn, err := qe.conns.Get(tabletenv.LocalContext(), nil)
	if err != nil {
		qe.conns.Close()
		return err
	}
	err = conn.Conn.VerifyMode(qe.strictTransTables)
	// Recycle needs to happen before error check.
	// Otherwise, qe.conns.Close will hang.
	conn.Recycle()

	if err != nil {
		qe.conns.Close()
		return err
	}

	qe.streamConns.Open(config.DB.AppWithDB(), config.DB.DbaWithDB(), config.DB.AppDebugWithDB())
	qe.se.RegisterNotifier("qe", qe.schemaChanged, true)
	qe.plans.EnsureOpen()
	qe.settings.EnsureOpen()
	qe.isOpen.Store(true)
	return nil
}

// Close must be called to shut down QueryEngine.
// You must ensure that no more queries will be sent
// before calling Close.
func (qe *QueryEngine) Close() {
	if !qe.isOpen.Swap(false) {
		return
	}
	// Close in reverse order of Open.
	qe.se.UnregisterNotifier("qe")

	qe.plans.Close()
	qe.settings.Close()

	qe.streamConns.Close()
	qe.conns.Close()
	log.Info("Query Engine: closed")
}

var errNoCache = errors.New("plan should not be cached")

func (qe *QueryEngine) getPlan(curSchema *currentSchema, sql string) (*TabletPlan, error) {
	statement, err := qe.env.Environment().Parser().Parse(sql)
	if err != nil {
		return nil, err
	}
	splan, err := planbuilder.Build(qe.env.Environment(), statement, curSchema.tables, qe.env.Config().DB.DBName, qe.env.Config().EnableViews)
	if err != nil {
		return nil, err
	}
	plan := &TabletPlan{Plan: splan, Original: sql}
	plan.Rules = qe.queryRuleSources.FilterByPlan(sql, plan.PlanID, plan.TableNames()...)
	plan.buildAuthorized()
	if sqlparser.CachePlan(statement) {
		return plan, nil
	}

	return plan, errNoCache
}

// GetPlan returns the TabletPlan that for the query. Plans are cached in an LRU cache.
func (qe *QueryEngine) GetPlan(ctx context.Context, logStats *tabletenv.LogStats, sql string, skipQueryPlanCache bool) (*TabletPlan, error) {
	span, _ := trace.NewSpan(ctx, "QueryEngine.GetPlan")
	defer span.Finish()

	var plan *TabletPlan
	var err error

	curSchema := qe.schema.Load()

	if skipQueryPlanCache {
		plan, err = qe.getPlan(curSchema, sql)
	} else {
		plan, logStats.CachedPlan, err = qe.plans.GetOrLoad(PlanCacheKey(sql), curSchema.epoch, func() (*TabletPlan, error) {
			return qe.getPlan(curSchema, sql)
		})
	}

	if errors.Is(err, errNoCache) {
		err = nil
	}
	return plan, err
}

func (qe *QueryEngine) getStreamPlan(curSchema *currentSchema, sql string) (*TabletPlan, error) {
	statement, err := qe.env.Environment().Parser().Parse(sql)
	if err != nil {
		return nil, err
	}

	splan, err := planbuilder.BuildStreaming(statement, curSchema.tables)

	if err != nil {
		return nil, err
	}

	plan := &TabletPlan{Plan: splan, Original: sql}
	plan.Rules = qe.queryRuleSources.FilterByPlan(sql, plan.PlanID, plan.TableName().String())
	plan.buildAuthorized()

	if sqlparser.CachePlan(statement) {
		return plan, nil
	}

	return plan, errNoCache
}

// GetStreamPlan returns the TabletPlan that for the query. Plans are cached in an LRU cache.
func (qe *QueryEngine) GetStreamPlan(ctx context.Context, logStats *tabletenv.LogStats, sql string, skipQueryPlanCache bool) (*TabletPlan, error) {
	span, _ := trace.NewSpan(ctx, "QueryEngine.GetStreamPlan")
	defer span.Finish()

	var plan *TabletPlan
	var err error

	curSchema := qe.schema.Load()

	if skipQueryPlanCache {
		plan, err = qe.getStreamPlan(curSchema, sql)
	} else {
		plan, logStats.CachedPlan, err = qe.plans.GetOrLoad(PlanCacheKey(qe.getStreamPlanCacheKey(sql)), curSchema.epoch, func() (*TabletPlan, error) {
			return qe.getStreamPlan(curSchema, sql)
		})
	}

	if errors.Is(err, errNoCache) {
		err = nil
	}
	return plan, err
}

// gets key used to cache stream query plan
func (qe *QueryEngine) getStreamPlanCacheKey(sql string) string {
	return "__STREAM__" + sql
}

// GetMessageStreamPlan builds a plan for Message streaming.
func (qe *QueryEngine) GetMessageStreamPlan(name string) (*TabletPlan, error) {
	splan, err := planbuilder.BuildMessageStreaming(name, qe.schema.Load().tables)
	if err != nil {
		return nil, err
	}
	plan := &TabletPlan{Plan: splan}
	plan.Rules = qe.queryRuleSources.FilterByPlan("stream from "+name, plan.PlanID, plan.TableName().String())
	plan.buildAuthorized()
	return plan, nil
}

// GetConnSetting returns system settings for the connection.
func (qe *QueryEngine) GetConnSetting(ctx context.Context, settings []string) (*smartconnpool.Setting, error) {
	span, _ := trace.NewSpan(ctx, "QueryEngine.GetConnSetting")
	defer span.Finish()

	var buf strings.Builder
	for _, q := range settings {
		_, _ = buf.WriteString(q)
		_ = buf.WriteByte(';')
	}

	cacheKey := SettingsCacheKey(buf.String())
	connSetting, _, err := qe.settings.GetOrLoad(cacheKey, 0, func() (*smartconnpool.Setting, error) {
		// build the setting queries
		query, resetQuery, err := planbuilder.BuildSettingQuery(settings, qe.env.Environment().Parser())
		if err != nil {
			return nil, err
		}
		return smartconnpool.NewSetting(query, resetQuery), nil
	})
	return connSetting, err
}

// ClearQueryPlanCache should be called if query plan cache is potentially obsolete
func (qe *QueryEngine) ClearQueryPlanCache() {
	qe.schemaMu.Lock()
	defer qe.schemaMu.Unlock()

	qe.epoch++

	current := qe.schema.Load()
	qe.schema.Store(&currentSchema{
		tables: current.tables,
		epoch:  qe.epoch,
	})
}

func (qe *QueryEngine) ForEachPlan(each func(plan *TabletPlan) bool) {
	curSchema := qe.schema.Load()
	qe.plans.Range(curSchema.epoch, func(_ PlanCacheKey, plan *TabletPlan) bool {
		return each(plan)
	})
}

// IsMySQLReachable returns an error if it cannot connect to MySQL.
// This can be called before opening the QueryEngine.
func (qe *QueryEngine) IsMySQLReachable() error {
	conn, err := dbconnpool.NewDBConnection(context.TODO(), qe.env.Config().DB.AppWithDB())
	if err != nil {
		if sqlerror.IsTooManyConnectionsErr(err) {
			return nil
		}
		return err
	}
	conn.Close()
	return nil
}

func (qe *QueryEngine) schemaChanged(tables map[string]*schema.Table, created, altered, dropped []*schema.Table) {
	qe.schemaMu.Lock()
	defer qe.schemaMu.Unlock()

	if len(altered) != 0 || len(dropped) != 0 {
		qe.epoch++
	}

	qe.schema.Store(&currentSchema{
		tables: tables,
		epoch:  qe.epoch,
	})
}

// QueryPlanCacheCap returns the capacity of the query cache.
func (qe *QueryEngine) QueryPlanCacheCap() int {
	return qe.plans.MaxCapacity()
}

// QueryPlanCacheLen returns the length (size in entries) of the query cache
func (qe *QueryEngine) QueryPlanCacheLen() (count int) {
	qe.ForEachPlan(func(plan *TabletPlan) bool {
		count++
		return true
	})
	return
}

// AddStats adds the given stats for the planName.tableName
func (qe *QueryEngine) AddStats(planType planbuilder.PlanType, tableName, workload string, tabletType topodata.TabletType, queryCount int64, duration, mysqlTime time.Duration, rowsAffected, rowsReturned, errorCount int64, errorCode string) {
	// table names can contain "." characters, replace them!
	keys := []string{tableName, planType.String()}
	// Only use the workload as a label if that's enabled in the configuration.
	if qe.enablePerWorkloadTableMetrics {
		keys = append(keys, workload)
	}
	qe.queryCounts.Add(keys, queryCount)
	qe.queryTimes.Add(keys, int64(duration))
	qe.queryErrorCounts.Add(keys, errorCount)

	qe.queryCountsWithTabletType.Add([]string{tableName, planType.String(), tabletType.String()}, queryCount)

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
	qe.ForEachPlan(func(plan *TabletPlan) bool {
		response.Write([]byte(fmt.Sprintf("%#v\n", qe.env.Environment().Parser().TruncateForUI(plan.Original))))
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
	qe.ForEachPlan(func(plan *TabletPlan) bool {
		var pqstats perQueryStats
		pqstats.Query = unicoded(qe.env.Environment().Parser().TruncateForUI(plan.Original))
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
			query, _ = qe.env.Environment().Parser().RedactSQLQuery(v.Query)
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
