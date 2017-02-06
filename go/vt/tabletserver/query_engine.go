// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"net/http"
	"sync"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tableacl"
	"github.com/youtube/vitess/go/vt/tableacl/acl"
	"github.com/youtube/vitess/go/vt/tabletserver/connpool"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletenv"
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
type QueryEngine struct {
	schemaInfo *SchemaInfo
	dbconfigs  dbconfigs.DBConfigs

	// Pools
	conns       *connpool.Pool
	streamConns *connpool.Pool

	// Services
	consolidator *sync2.Consolidator
	streamQList  *QueryList

	// Vars
	strictMode       sync2.AtomicInt64
	autoCommit       sync2.AtomicInt64
	maxResultSize    sync2.AtomicInt64
	maxDMLRows       sync2.AtomicInt64
	streamBufferSize sync2.AtomicInt64
	// tableaclExemptCount count the number of accesses allowed
	// based on membership in the superuser ACL
	tableaclExemptCount  sync2.AtomicInt64
	strictTableAcl       bool
	enableTableAclDryRun bool
	exemptACL            acl.ACL

	// Loggers
	accessCheckerLogger *logutil.ThrottledLogger
}

var (
	qeOnce sync.Once
)

// NewQueryEngine creates a new QueryEngine.
// This is a singleton class.
// You must call this only once.
func NewQueryEngine(checker MySQLChecker) *QueryEngine {
	qe := &QueryEngine{}
	qe.schemaInfo = NewSchemaInfo(
		checker,
		tabletenv.Config.QueryCacheSize,
		time.Duration(tabletenv.Config.SchemaReloadTime*1e9),
		time.Duration(tabletenv.Config.IdleTimeout*1e9),
		map[string]string{
			debugQueryPlansKey: tabletenv.Config.DebugURLPrefix + "/query_plans",
			debugQueryStatsKey: tabletenv.Config.DebugURLPrefix + "/query_stats",
			debugSchemaKey:     tabletenv.Config.DebugURLPrefix + "/schema",
			debugQueryRulesKey: tabletenv.Config.DebugURLPrefix + "/query_rules",
		},
	)

	qe.conns = connpool.New(
		tabletenv.Config.PoolNamePrefix+"ConnPool",
		tabletenv.Config.PoolSize,
		time.Duration(tabletenv.Config.IdleTimeout*1e9),
		checker,
	)
	qe.streamConns = connpool.New(
		tabletenv.Config.PoolNamePrefix+"StreamConnPool",
		tabletenv.Config.StreamPoolSize,
		time.Duration(tabletenv.Config.IdleTimeout*1e9),
		checker,
	)

	qe.consolidator = sync2.NewConsolidator()
	http.Handle(tabletenv.Config.DebugURLPrefix+"/consolidations", qe.consolidator)
	qe.streamQList = NewQueryList()

	if tabletenv.Config.StrictMode {
		qe.strictMode.Set(1)
	}
	if tabletenv.Config.EnableAutoCommit {
		qe.autoCommit.Set(1)
	}
	qe.strictTableAcl = tabletenv.Config.StrictTableAcl
	qe.enableTableAclDryRun = tabletenv.Config.EnableTableAclDryRun

	if tabletenv.Config.TableAclExemptACL != "" {
		if f, err := tableacl.GetCurrentAclFactory(); err == nil {
			if exemptACL, err := f.New([]string{tabletenv.Config.TableAclExemptACL}); err == nil {
				log.Infof("Setting Table ACL exempt rule for %v", tabletenv.Config.TableAclExemptACL)
				qe.exemptACL = exemptACL
			} else {
				log.Infof("Cannot build exempt ACL for table ACL: %v", err)
			}
		} else {
			log.Infof("Cannot get current ACL Factory: %v", err)
		}
	}

	qe.maxResultSize = sync2.NewAtomicInt64(int64(tabletenv.Config.MaxResultSize))
	qe.maxDMLRows = sync2.NewAtomicInt64(int64(tabletenv.Config.MaxDMLRows))
	qe.streamBufferSize = sync2.NewAtomicInt64(int64(tabletenv.Config.StreamBufferSize))

	qe.accessCheckerLogger = logutil.NewThrottledLogger("accessChecker", 1*time.Second)

	qeOnce.Do(func() {
		stats.Publish("MaxResultSize", stats.IntFunc(qe.maxResultSize.Get))
		stats.Publish("MaxDMLRows", stats.IntFunc(qe.maxDMLRows.Get))
		stats.Publish("StreamBufferSize", stats.IntFunc(qe.streamBufferSize.Get))
		stats.Publish("TableACLExemptCount", stats.IntFunc(qe.tableaclExemptCount.Get))
	})

	return qe
}

// Open must be called before sending requests to QueryEngine.
func (qe *QueryEngine) Open(dbconfigs dbconfigs.DBConfigs) error {
	qe.dbconfigs = dbconfigs

	strictMode := false
	if qe.strictMode.Get() != 0 {
		strictMode = true
	}

	start := time.Now()
	if err := qe.schemaInfo.Open(&qe.dbconfigs.Dba, strictMode); err != nil {
		return err
	}
	log.Infof("Time taken to load the schema: %v", time.Now().Sub(start))

	qe.conns.Open(&qe.dbconfigs.App, &qe.dbconfigs.Dba)
	qe.streamConns.Open(&qe.dbconfigs.App, &qe.dbconfigs.Dba)
	return nil
}

// IsMySQLReachable returns true if we can connect to MySQL.
func (qe *QueryEngine) IsMySQLReachable() bool {
	conn, err := dbconnpool.NewDBConnection(&qe.dbconfigs.App, tabletenv.MySQLStats)
	if err != nil {
		if tabletenv.IsConnErr(err) {
			return false
		}
		log.Warningf("checking MySQL, unexpected error: %v", err)
		return true
	}
	conn.Close()
	return true
}

// Close must be called to shut down QueryEngine.
// You must ensure that no more queries will be sent
// before calling Close.
func (qe *QueryEngine) Close() {
	// Close in reverse order of Open.
	qe.streamConns.Close()
	qe.conns.Close()
	qe.schemaInfo.Close()
}
