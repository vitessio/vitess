// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/cache"
	"github.com/youtube/vitess/go/mysqlconn"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tableacl"
	tacl "github.com/youtube/vitess/go/vt/tableacl/acl"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/connpool"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/rules"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/txserializer"
	"github.com/youtube/vitess/go/vt/utils"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

//_______________________________________________

// TabletPlan wraps the planbuilder's exec plan to enforce additional rules
// and track stats.
type TabletPlan struct {
	*planbuilder.Plan
	Fields     []*querypb.Field
	Rules      *rules.Rules
	Authorized *tableacl.ACLResult

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
	dbconfigs dbconfigs.DBConfigs

	// mu protects the following fields.
	mu               sync.RWMutex
	tables           map[string]*schema.Table
	queries          *cache.LRUCache
	queryRuleSources *rules.Map

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
	strictMode       sync2.AtomicBool
	autoCommit       sync2.AtomicBool
	maxResultSize    sync2.AtomicInt64
	maxDMLRows       sync2.AtomicInt64
	streamBufferSize sync2.AtomicInt64
	// tableaclExemptCount count the number of accesses allowed
	// based on membership in the superuser ACL
	tableaclExemptCount  sync2.AtomicInt64
	strictTableACL       bool
	enableTableACLDryRun bool
	// TODO(sougou) There are two acl packages. Need to rename.
	exemptACL tacl.ACL

	// Loggers
	accessCheckerLogger *logutil.ThrottledLogger
}

var (
	qeOnce sync.Once
)

// NewQueryEngine creates a new QueryEngine.
// This is a singleton class.
// You must call this only once.
func NewQueryEngine(checker MySQLChecker, se *schema.Engine, config tabletenv.TabletConfig) *QueryEngine {
	qe := &QueryEngine{
		se:               se,
		tables:           make(map[string]*schema.Table),
		queries:          cache.NewLRUCache(int64(config.QueryCacheSize)),
		queryRuleSources: rules.NewMap(),
	}

	qe.conns = connpool.New(
		config.PoolNamePrefix+"ConnPool",
		config.PoolSize,
		time.Duration(config.IdleTimeout*1e9),
		checker,
	)
	qe.streamConns = connpool.New(
		config.PoolNamePrefix+"StreamConnPool",
		config.StreamPoolSize,
		time.Duration(config.IdleTimeout*1e9),
		checker,
	)

	qe.consolidator = sync2.NewConsolidator()
	qe.txSerializer = txserializer.New(config.EnableHotRowProtectionDryRun,
		config.HotRowProtectionMaxQueueSize, config.HotRowProtectionMaxGlobalQueueSize)
	qe.streamQList = NewQueryList()

	qe.strictMode.Set(config.StrictMode)
	qe.autoCommit.Set(config.EnableAutoCommit)
	qe.strictTableACL = config.StrictTableACL
	qe.enableTableACLDryRun = config.EnableTableACLDryRun

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
	qe.maxDMLRows = sync2.NewAtomicInt64(int64(config.MaxDMLRows))
	qe.streamBufferSize = sync2.NewAtomicInt64(int64(config.StreamBufferSize))

	qe.accessCheckerLogger = logutil.NewThrottledLogger("accessChecker", 1*time.Second)

	qeOnce.Do(func() {
		stats.Publish("MaxResultSize", stats.IntFunc(qe.maxResultSize.Get))
		stats.Publish("MaxDMLRows", stats.IntFunc(qe.maxDMLRows.Get))
		stats.Publish("StreamBufferSize", stats.IntFunc(qe.streamBufferSize.Get))
		stats.Publish("TableACLExemptCount", stats.IntFunc(qe.tableaclExemptCount.Get))

		stats.Publish("QueryCacheLength", stats.IntFunc(qe.queries.Length))
		stats.Publish("QueryCacheSize", stats.IntFunc(qe.queries.Size))
		stats.Publish("QueryCacheCapacity", stats.IntFunc(qe.queries.Capacity))
		stats.Publish("QueryCacheOldest", stats.StringFunc(func() string {
			return fmt.Sprintf("%v", qe.queries.Oldest())
		}))
		_ = stats.NewMultiCountersFunc("QueryCounts", []string{"Table", "Plan"}, qe.getQueryCount)
		_ = stats.NewMultiCountersFunc("QueryTimesNs", []string{"Table", "Plan"}, qe.getQueryTime)
		_ = stats.NewMultiCountersFunc("QueryRowCounts", []string{"Table", "Plan"}, qe.getQueryRowCount)
		_ = stats.NewMultiCountersFunc("QueryErrorCounts", []string{"Table", "Plan"}, qe.getQueryErrorCount)

		http.Handle("/debug/consolidations", qe.consolidator)
		http.Handle("/debug/hotrows", qe.txSerializer)

		endpoints := []string{
			"/debug/tablet_plans",
			"/debug/query_stats",
			"/debug/query_rules",
		}
		for _, ep := range endpoints {
			http.Handle(ep, qe)
		}
	})

	return qe
}

// Open must be called before sending requests to QueryEngine.
func (qe *QueryEngine) Open(dbconfigs dbconfigs.DBConfigs) error {
	qe.dbconfigs = dbconfigs
	qe.conns.Open(&qe.dbconfigs.App, &qe.dbconfigs.Dba)

	if qe.strictMode.Get() {
		conn, err := qe.conns.Get(tabletenv.LocalContext())
		if err != nil {
			qe.conns.Close()
			return err
		}
		err = conn.VerifyMode()
		conn.Recycle()

		if err != nil {
			qe.conns.Close()
			return err
		}
	}

	qe.streamConns.Open(&qe.dbconfigs.App, &qe.dbconfigs.Dba)
	qe.se.RegisterNotifier("qe", qe.schemaChanged)
	return nil
}

// Close must be called to shut down QueryEngine.
// You must ensure that no more queries will be sent
// before calling Close.
func (qe *QueryEngine) Close() {
	// Close in reverse order of Open.
	qe.se.UnregisterNotifier("qe")
	qe.queries.Clear()
	qe.tables = make(map[string]*schema.Table)
	qe.streamConns.Close()
	qe.conns.Close()
}

// GetPlan returns the TabletPlan that for the query. Plans are cached in a cache.LRUCache.
func (qe *QueryEngine) GetPlan(ctx context.Context, logStats *tabletenv.LogStats, sql string) (*TabletPlan, error) {
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
	splan, err := planbuilder.Build(sql, qe.tables)
	if err != nil {
		// TODO(sougou): Inspect to see if Build can return coded error.
		return nil, vterrors.New(vtrpcpb.Code_UNKNOWN, err.Error())
	}
	plan := &TabletPlan{Plan: splan}
	plan.Rules = qe.queryRuleSources.FilterByPlan(sql, plan.PlanID, plan.TableName().String())
	plan.Authorized = tableacl.Authorized(plan.TableName().String(), plan.PlanID.MinRole())
	if plan.PlanID.IsSelect() {
		if plan.FieldQuery == nil {
			log.Warningf("Cannot cache field info: %s", sql)
		} else {
			conn, err := qe.conns.Get(ctx)
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
	qe.queries.Set(sql, plan)
	return plan, nil
}

// GetStreamPlan is similar to GetPlan, but doesn't use the cache
// and doesn't enforce a limit. It just returns the parsed query.
func (qe *QueryEngine) GetStreamPlan(sql string) (*TabletPlan, error) {
	qe.mu.RLock()
	defer qe.mu.RUnlock()
	splan, err := planbuilder.BuildStreaming(sql, qe.tables)
	if err != nil {
		// TODO(sougou): Inspect to see if BuildStreaming can return coded error.
		return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, err.Error())
	}
	plan := &TabletPlan{Plan: splan}
	plan.Rules = qe.queryRuleSources.FilterByPlan(sql, plan.PlanID, plan.TableName().String())
	plan.Authorized = tableacl.Authorized(plan.TableName().String(), plan.PlanID.MinRole())
	return plan, nil
}

// ClearQueryPlanCache should be called if query plan cache is potentially obsolete
func (qe *QueryEngine) ClearQueryPlanCache() {
	qe.queries.Clear()
}

// IsMySQLReachable returns true if we can connect to MySQL.
func (qe *QueryEngine) IsMySQLReachable() bool {
	conn, err := dbconnpool.NewDBConnection(&qe.dbconfigs.App, tabletenv.MySQLStats)
	if err != nil {
		if mysqlconn.IsConnErr(err) {
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
		qe.queries.Clear()
	}
}

// getQuery fetches the plan and makes it the most recent.
func (qe *QueryEngine) getQuery(sql string) *TabletPlan {
	if cacheResult, ok := qe.queries.Get(sql); ok {
		return cacheResult.(*TabletPlan)
	}
	return nil
}

// peekQuery fetches the plan without changing the LRU order.
func (qe *QueryEngine) peekQuery(sql string) *TabletPlan {
	if cacheResult, ok := qe.queries.Peek(sql); ok {
		return cacheResult.(*TabletPlan)
	}
	return nil
}

// SetQueryCacheCap sets the query cache capacity.
func (qe *QueryEngine) SetQueryCacheCap(size int) {
	if size <= 0 {
		size = 1
	}
	qe.queries.SetCapacity(int64(size))
}

// QueryCacheCap returns the capacity of the query cache.
func (qe *QueryEngine) QueryCacheCap() int {
	return int(qe.queries.Capacity())
}

func (qe *QueryEngine) getQueryCount() map[string]int64 {
	f := func(plan *TabletPlan) int64 {
		queryCount, _, _, _, _ := plan.Stats()
		return queryCount
	}
	return qe.getQueryStats(f)
}

func (qe *QueryEngine) getQueryTime() map[string]int64 {
	f := func(plan *TabletPlan) int64 {
		_, time, _, _, _ := plan.Stats()
		return int64(time)
	}
	return qe.getQueryStats(f)
}

func (qe *QueryEngine) getQueryRowCount() map[string]int64 {
	f := func(plan *TabletPlan) int64 {
		_, _, _, rowCount, _ := plan.Stats()
		return rowCount
	}
	return qe.getQueryStats(f)
}

func (qe *QueryEngine) getQueryErrorCount() map[string]int64 {
	f := func(plan *TabletPlan) int64 {
		_, _, _, _, errorCount := plan.Stats()
		return errorCount
	}
	return qe.getQueryStats(f)
}

type queryStatsFunc func(*TabletPlan) int64

func (qe *QueryEngine) getQueryStats(f queryStatsFunc) map[string]int64 {
	keys := qe.queries.Keys()
	qstats := make(map[string]int64)
	for _, v := range keys {
		if plan := qe.peekQuery(v); plan != nil {
			table := plan.TableName()
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
	default:
		response.WriteHeader(http.StatusNotFound)
	}
}

func (qe *QueryEngine) handleHTTPQueryPlans(response http.ResponseWriter, request *http.Request) {
	keys := qe.queries.Keys()
	response.Header().Set("Content-Type", "text/plain")
	response.Write([]byte(fmt.Sprintf("Length: %d\n", len(keys))))
	for _, v := range keys {
		response.Write([]byte(fmt.Sprintf("%#v\n", utils.TruncateQuery(v))))
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
	keys := qe.queries.Keys()
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	qstats := make([]perQueryStats, 0, len(keys))
	for _, v := range keys {
		if plan := qe.peekQuery(v); plan != nil {
			var pqstats perQueryStats
			pqstats.Query = unicoded(utils.TruncateQuery(v))
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
