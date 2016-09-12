// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"net/http"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/logutil"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/tableacl"
	"github.com/youtube/vitess/go/vt/tableacl/acl"
)

// QueryEngine implements the core functionality of tabletserver.
// It assumes that no requests will be sent to it before Open is
// called and succeeds.
// Shutdown is done in the following order:
//
// WaitForTxEmpty: There should be no more new calls to Begin
// once this function is called. This will return when there
// are no more pending transactions.
//
// Close: There should be no more pending queries when this
// function is called.
//
// Functions of QueryEngine do not return errors. They instead
// panic with NewTabletError as the error type.
// TODO(sougou): Switch to error return scheme.
type QueryEngine struct {
	schemaInfo *SchemaInfo
	config     Config
	dbconfigs  dbconfigs.DBConfigs

	// Pools
	connPool       *ConnPool
	streamConnPool *ConnPool

	// Services
	txPool       *TxPool
	consolidator *sync2.Consolidator
	streamQList  *QueryList
	twoPC        *TwoPC

	// Vars
	strictMode       sync2.AtomicInt64
	autoCommit       sync2.AtomicInt64
	maxResultSize    sync2.AtomicInt64
	maxDMLRows       sync2.AtomicInt64
	streamBufferSize sync2.AtomicInt64
	// tableaclExemptCount count the number of accesses allowed
	// based on membership in the superuser ACL
	tableaclExemptCount  sync2.AtomicInt64
	tableaclAllowed      *stats.MultiCounters
	tableaclDenied       *stats.MultiCounters
	tableaclPseudoDenied *stats.MultiCounters
	strictTableAcl       bool
	enableTableAclDryRun bool
	exemptACL            acl.ACL

	// Loggers
	accessCheckerLogger *logutil.ThrottledLogger

	// Stats
	queryServiceStats *QueryServiceStats
}

// Helper method for conn pools to convert errors
func getOrPanic(ctx context.Context, pool *ConnPool) *DBConn {
	conn, err := pool.Get(ctx)
	if err == nil {
		return conn
	}
	if err == ErrConnPoolClosed {
		panic(ErrConnPoolClosed)
	}
	// If there's a problem with getting a connection out of the pool, that is
	// probably not due to the query itself. The query might succeed on a different
	// tablet.
	panic(NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, err))
}

// NewQueryEngine creates a new QueryEngine.
// This is a singleton class.
// You must call this only once.
func NewQueryEngine(checker MySQLChecker, config Config) *QueryEngine {
	qe := &QueryEngine{config: config}
	qe.queryServiceStats = NewQueryServiceStats(config.StatsPrefix, config.EnablePublishStats)
	qe.schemaInfo = NewSchemaInfo(
		config.StatsPrefix,
		checker,
		config.QueryCacheSize,
		time.Duration(config.SchemaReloadTime*1e9),
		time.Duration(config.IdleTimeout*1e9),
		map[string]string{
			debugQueryPlansKey: config.DebugURLPrefix + "/query_plans",
			debugQueryStatsKey: config.DebugURLPrefix + "/query_stats",
			debugSchemaKey:     config.DebugURLPrefix + "/schema",
			debugQueryRulesKey: config.DebugURLPrefix + "/query_rules",
		},
		config.EnablePublishStats,
		qe.queryServiceStats,
	)

	qe.connPool = NewConnPool(
		config.PoolNamePrefix+"ConnPool",
		config.PoolSize,
		time.Duration(config.IdleTimeout*1e9),
		config.EnablePublishStats,
		qe.queryServiceStats,
		checker,
	)
	qe.streamConnPool = NewConnPool(
		config.PoolNamePrefix+"StreamConnPool",
		config.StreamPoolSize,
		time.Duration(config.IdleTimeout*1e9),
		config.EnablePublishStats,
		qe.queryServiceStats,
		checker,
	)

	qe.txPool = NewTxPool(
		config.PoolNamePrefix+"TransactionPool",
		config.StatsPrefix,
		config.TransactionCap,
		time.Duration(config.TransactionTimeout*1e9),
		time.Duration(config.IdleTimeout*1e9),
		config.EnablePublishStats,
		qe.queryServiceStats,
		checker,
	)
	// twoPC depends on txPool for some of its operations.
	qe.twoPC = NewTwoPC(qe.txPool)
	qe.consolidator = sync2.NewConsolidator()
	http.Handle(config.DebugURLPrefix+"/consolidations", qe.consolidator)
	qe.streamQList = NewQueryList()

	if config.StrictMode {
		qe.strictMode.Set(1)
	}
	if config.EnableAutoCommit {
		qe.autoCommit.Set(1)
	}
	qe.strictTableAcl = config.StrictTableAcl
	qe.enableTableAclDryRun = config.EnableTableAclDryRun

	if config.TableAclExemptACL != "" {
		if f, err := tableacl.GetCurrentAclFactory(); err == nil {
			if exemptACL, err := f.New([]string{config.TableAclExemptACL}); err == nil {
				log.Infof("Setting Table ACL exempt rule for %v", config.TableAclExemptACL)
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

	var tableACLAllowedName string
	var tableACLDeniedName string
	var tableACLPseudoDeniedName string
	if config.EnablePublishStats {
		stats.Publish(config.StatsPrefix+"MaxResultSize", stats.IntFunc(qe.maxResultSize.Get))
		stats.Publish(config.StatsPrefix+"MaxDMLRows", stats.IntFunc(qe.maxDMLRows.Get))
		stats.Publish(config.StatsPrefix+"StreamBufferSize", stats.IntFunc(qe.streamBufferSize.Get))
		stats.Publish(config.StatsPrefix+"TableACLExemptCount", stats.IntFunc(qe.tableaclExemptCount.Get))
		tableACLAllowedName = "TableACLAllowed"
		tableACLDeniedName = "TableACLDenied"
		tableACLPseudoDeniedName = "TableACLPseudoDenied"
	}

	qe.tableaclAllowed = stats.NewMultiCounters(tableACLAllowedName, []string{"TableName", "TableGroup", "PlanID", "Username"})
	qe.tableaclDenied = stats.NewMultiCounters(tableACLDeniedName, []string{"TableName", "TableGroup", "PlanID", "Username"})
	qe.tableaclPseudoDenied = stats.NewMultiCounters(tableACLPseudoDeniedName, []string{"TableName", "TableGroup", "PlanID", "Username"})

	return qe
}

// Open must be called before sending requests to QueryEngine.
func (qe *QueryEngine) Open(dbconfigs dbconfigs.DBConfigs) {
	qe.dbconfigs = dbconfigs
	appParams := dbconfigs.App
	// Create dba params based on App connection params
	// and Dba credentials.
	dbaParams := dbconfigs.App
	if dbconfigs.Dba.Uname != "" {
		dbaParams.Uname = dbconfigs.Dba.Uname
		dbaParams.Pass = dbconfigs.Dba.Pass
	}

	strictMode := false
	if qe.strictMode.Get() != 0 {
		strictMode = true
	}

	start := time.Now()
	qe.schemaInfo.Open(&dbaParams, strictMode)
	log.Infof("Time taken to load the schema: %v", time.Now().Sub(start))

	qe.connPool.Open(&appParams, &dbaParams)
	qe.streamConnPool.Open(&appParams, &dbaParams)
	qe.txPool.Open(&appParams, &dbaParams)
	qe.twoPC.Open(qe.dbconfigs.SidecarDBName, &dbaParams)
}

// IsMySQLReachable returns true if we can connect to MySQL.
func (qe *QueryEngine) IsMySQLReachable() bool {
	conn, err := dbconnpool.NewDBConnection(&qe.dbconfigs.App, qe.queryServiceStats.MySQLStats)
	if err != nil {
		if IsConnErr(err) {
			return false
		}
		log.Warningf("checking MySQL, unexpected error: %v", err)
		return true
	}
	conn.Close()
	return true
}

// WaitForTxEmpty must be called before calling Close.
// Before calling WaitForTxEmpty, you must ensure that there
// will be no more calls to Begin.
func (qe *QueryEngine) WaitForTxEmpty() {
	qe.txPool.WaitForEmpty()
}

// Close must be called to shut down QueryEngine.
// You must ensure that no more queries will be sent
// before calling Close.
func (qe *QueryEngine) Close() {
	// Close in reverse order of Open.
	qe.twoPC.Close()
	qe.txPool.Close()
	qe.streamConnPool.Close()
	qe.connPool.Close()
	qe.schemaInfo.Close()
}
