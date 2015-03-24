// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"net/http"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqldbconn"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"golang.org/x/net/context"
)

// spotCheckMultiplier determines the precision of the
// spot check ratio: 1e6 == 6 digits
const spotCheckMultiplier = 1e6

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
	dbconfigs  *dbconfigs.DBConfigs

	// Pools
	cachePool      *CachePool
	connPool       *ConnPool
	streamConnPool *ConnPool

	// Services
	txPool       *TxPool
	consolidator *sync2.Consolidator
	invalidator  *RowcacheInvalidator
	streamQList  *QueryList
	tasks        sync.WaitGroup

	// Vars
	queryTimeout     sync2.AtomicDuration
	spotCheckFreq    sync2.AtomicInt64
	strictMode       sync2.AtomicInt64
	maxResultSize    sync2.AtomicInt64
	maxDMLRows       sync2.AtomicInt64
	streamBufferSize sync2.AtomicInt64
	strictTableAcl   bool

	// loggers
	accessCheckerLogger *logutil.ThrottledLogger

	newSqlDBConn sqldbconn.NewSqlDBConnFunc
}

type compiledPlan struct {
	Query string
	*ExecPlan
	BindVars      map[string]interface{}
	TransactionID int64
}

var (
	// stats are globals to allow anybody to set them
	mysqlStats     *stats.Timings
	queryStats     *stats.Timings
	waitStats      *stats.Timings
	killStats      *stats.Counters
	infoErrors     *stats.Counters
	errorStats     *stats.Counters
	internalErrors *stats.Counters
	resultStats    *stats.Histogram
	spotCheckCount *stats.Int
	qpsRates       *stats.Rates

	resultBuckets = []int64{0, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000}

	connPoolClosedErr = NewTabletError(ErrFatal, "connection pool is closed")
)

// CacheInvalidator provides the abstraction needed for an instant invalidation
// vs. delayed invalidation in the case of in-transaction dmls
type CacheInvalidator interface {
	Delete(key string)
}

// Helper method for conn pools to convert errors
func getOrPanic(ctx context.Context, pool *ConnPool) *DBConn {
	conn, err := pool.Get(ctx)
	if err == nil {
		return conn
	}
	if err == ErrConnPoolClosed {
		panic(connPoolClosedErr)
	}
	panic(NewTabletErrorSql(ErrFatal, err))
}

// NewQueryEngine creates a new QueryEngine.
// This is a singleton class.
// You must call this only once.
func NewQueryEngine(config Config, newSqlDBConn sqldbconn.NewSqlDBConnFunc) *QueryEngine {
	qe := &QueryEngine{newSqlDBConn: newSqlDBConn}
	qe.schemaInfo = NewSchemaInfo(
		config.QueryCacheSize,
		qe.newSqlDBConn,
		time.Duration(config.SchemaReloadTime*1e9),
		time.Duration(config.IdleTimeout*1e9),
	)

	mysqlStats = stats.NewTimings("Mysql")

	// Pools
	qe.cachePool = NewCachePool(
		"Rowcache",
		config.RowCache,
		time.Duration(config.IdleTimeout*1e9),
	)
	qe.connPool = NewConnPool(
		"ConnPool",
		qe.newSqlDBConn,
		config.PoolSize,
		time.Duration(config.IdleTimeout*1e9),
	)
	qe.streamConnPool = NewConnPool(
		"StreamConnPool",
		qe.newSqlDBConn,
		config.StreamPoolSize,
		time.Duration(config.IdleTimeout*1e9),
	)

	// Services
	qe.txPool = NewTxPool(
		"TransactionPool",
		qe.newSqlDBConn,
		config.TransactionCap,
		time.Duration(config.TransactionTimeout*1e9),
		time.Duration(config.TxPoolTimeout*1e9),
		time.Duration(config.IdleTimeout*1e9),
	)
	qe.consolidator = sync2.NewConsolidator()
	http.Handle("/debug/consolidations", qe.consolidator)
	qe.invalidator = NewRowcacheInvalidator(qe)
	qe.streamQList = NewQueryList()

	// Vars
	qe.queryTimeout.Set(time.Duration(config.QueryTimeout * 1e9))
	qe.spotCheckFreq = sync2.AtomicInt64(config.SpotCheckRatio * spotCheckMultiplier)
	if config.StrictMode {
		qe.strictMode.Set(1)
	}
	qe.strictTableAcl = config.StrictTableAcl
	qe.maxResultSize = sync2.AtomicInt64(config.MaxResultSize)
	qe.maxDMLRows = sync2.AtomicInt64(config.MaxDMLRows)
	qe.streamBufferSize = sync2.AtomicInt64(config.StreamBufferSize)

	// loggers
	qe.accessCheckerLogger = logutil.NewThrottledLogger("accessChecker", 1*time.Second)

	// Stats
	stats.Publish("MaxResultSize", stats.IntFunc(qe.maxResultSize.Get))
	stats.Publish("MaxDMLRows", stats.IntFunc(qe.maxDMLRows.Get))
	stats.Publish("StreamBufferSize", stats.IntFunc(qe.streamBufferSize.Get))
	stats.Publish("QueryTimeout", stats.DurationFunc(qe.queryTimeout.Get))
	queryStats = stats.NewTimings("Queries")
	qpsRates = stats.NewRates("QPS", queryStats, 15, 60*time.Second)
	waitStats = stats.NewTimings("Waits")
	killStats = stats.NewCounters("Kills")
	infoErrors = stats.NewCounters("InfoErrors")
	errorStats = stats.NewCounters("Errors")
	internalErrors = stats.NewCounters("InternalErrors")
	resultStats = stats.NewHistogram("Results", resultBuckets)
	stats.Publish("RowcacheSpotCheckRatio", stats.FloatFunc(func() float64 {
		return float64(qe.spotCheckFreq.Get()) / spotCheckMultiplier
	}))
	spotCheckCount = stats.NewInt("RowcacheSpotCheckCount")

	return qe
}

// Open must be called before sending requests to QueryEngine.
func (qe *QueryEngine) Open(dbconfigs *dbconfigs.DBConfigs, schemaOverrides []SchemaOverride, mysqld *mysqlctl.Mysqld) {
	qe.dbconfigs = dbconfigs
	appParams := dbconfigs.App.ConnectionParams
	// Create dba params based on App connection params
	// and Dba credentials.
	dbaParams := dbconfigs.App.ConnectionParams
	if dbconfigs.Dba.Uname != "" {
		dbaParams.Uname = dbconfigs.Dba.Uname
		dbaParams.Pass = dbconfigs.Dba.Pass
	}

	strictMode := false
	if qe.strictMode.Get() != 0 {
		strictMode = true
	}
	if !strictMode && dbconfigs.App.EnableRowcache {
		panic(NewTabletError(ErrFatal, "Rowcache cannot be enabled when queryserver-config-strict-mode is false"))
	}
	if dbconfigs.App.EnableRowcache {
		qe.cachePool.Open()
		log.Infof("rowcache is enabled")
	} else {
		// Invalidator should not be enabled if rowcache is not enabled.
		dbconfigs.App.EnableInvalidator = false
		log.Infof("rowcache is not enabled")
	}

	start := time.Now()
	// schemaInfo depends on cachePool. Every table that has a rowcache
	// points to the cachePool.
	qe.schemaInfo.Open(&appParams, &dbaParams, schemaOverrides, qe.cachePool, strictMode)
	log.Infof("Time taken to load the schema: %v", time.Now().Sub(start))

	// Start the invalidator only after schema is loaded.
	// This will allow qe to find the table info
	// for the invalidation events that will start coming
	// immediately.
	if dbconfigs.App.EnableInvalidator {
		qe.invalidator.Open(dbconfigs.App.DbName, mysqld)
	}
	qe.connPool.Open(&appParams, &dbaParams)
	qe.streamConnPool.Open(&appParams, &dbaParams)
	qe.txPool.Open(&appParams, &dbaParams)
}

// Launch launches the specified function inside a goroutine.
// If Close or WaitForTxEmpty is called while a goroutine is running,
// QueryEngine will not return until the existing functions have completed.
// This functionality allows us to launch tasks with the assurance that
// the QueryEngine will not be closed underneath us.
func (qe *QueryEngine) Launch(f func()) {
	qe.tasks.Add(1)
	go func() {
		defer func() {
			qe.tasks.Done()
			if x := recover(); x != nil {
				internalErrors.Add("Task", 1)
				log.Errorf("task error: %v", x)
			}
		}()
		f()
	}()
}

// CheckMySQL returns true if we can connect to MySQL.
func (qe *QueryEngine) CheckMySQL() bool {
	conn, err := dbconnpool.NewDBConnection(&qe.dbconfigs.App.ConnectionParams,
		qe.newSqlDBConn, mysqlStats)
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
	qe.tasks.Wait()
	// Close in reverse order of Open.
	qe.txPool.Close()
	qe.streamConnPool.Close()
	qe.connPool.Close()
	qe.invalidator.Close()
	qe.schemaInfo.Close()
	qe.cachePool.Close()
	qe.dbconfigs = nil
}
