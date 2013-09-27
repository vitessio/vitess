// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"flag"
	"io/ioutil"
	"net/http"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/jscfg"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	rpcproto "github.com/youtube/vitess/go/rpcwrap/proto"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

var (
	queryLogHandler    = flag.String("query-log-stream-handler", "/debug/querylog", "URL handler for streaming queries log")
	txLogHandler       = flag.String("transaction-log-stream-handler", "/debug/txlog", "URL handler for streaming transactions log")
	qsConfigFile       = flag.String("queryserver-config-file", "", "config file name for the query service")
	customRules        = flag.String("customrules", "", "custom query rules file")
	spotCheckRatio     = flag.Float64("spot-check-ratio", 0.0, "rowcache spot check frequency")
	streamExecThrottle = flag.Int("queryserver-config-stream-exec-throttle", 8, "Maximum number of simultaneous streaming requests that can wait for results")
	streamWaitTimeout  = flag.Float64("queryserver-config-stream-exec-timeout", 4*60, "Timeout for stream-exec-throttle")
)

type Config struct {
	PoolSize           int
	StreamPoolSize     int
	TransactionCap     int
	TransactionTimeout float64
	MaxResultSize      int
	StreamBufferSize   int
	QueryCacheSize     int
	SchemaReloadTime   float64
	QueryTimeout       float64
	IdleTimeout        float64
	RowCache           []string
	SpotCheckRatio     float64
	StreamExecThrottle int
	StreamWaitTimeout  float64
}

// DefaultQSConfig is the default value for the query service config.
//
// The value for StreamBufferSize was chosen after trying out a few of
// them. Too small buffers force too many packets to be sent. Too big
// buffers force the clients to read them in multiple chunks and make
// memory copies.  so with the encoding overhead, this seems to work
// great (the overhead makes the final packets on the wire about twice
// bigger than this).
var DefaultQsConfig = Config{
	PoolSize:           16,
	StreamPoolSize:     750,
	TransactionCap:     20,
	TransactionTimeout: 30,
	MaxResultSize:      10000,
	QueryCacheSize:     5000,
	SchemaReloadTime:   30 * 60,
	QueryTimeout:       0,
	IdleTimeout:        30 * 60,
	StreamBufferSize:   32 * 1024,
	RowCache:           nil,
	SpotCheckRatio:     0,
	StreamExecThrottle: 8,
	StreamWaitTimeout:  4 * 60,
}

var SqlQueryRpcService *SqlQuery

func RegisterQueryService(config Config) {
	if SqlQueryRpcService != nil {
		log.Warningf("RPC service already up %v", SqlQueryRpcService)
		return
	}
	SqlQueryRpcService = NewSqlQuery(config)
	proto.RegisterAuthenticated(SqlQueryRpcService)
	http.HandleFunc("/debug/health", healthCheck)
}

// AllowQueries can take an indefinite amount of time to return because
// it keeps retrying until it obtains a valid connection to the database.
func AllowQueries(dbconfig dbconfigs.DBConfig, schemaOverrides []SchemaOverride, qrs *QueryRules) {
	defer logError()
	SqlQueryRpcService.allowQueries(dbconfig, schemaOverrides, qrs)
}

// DisallowQueries can take a long time to return (not indefinite) because
// it has to wait for queries & transactions to be completed or killed,
// and also for house keeping goroutines to be terminated.
func DisallowQueries() {
	defer logError()
	SqlQueryRpcService.disallowQueries()
}

// Reload the schema. If the query service is not running, nothing will happen
func ReloadSchema() {
	defer logError()
	SqlQueryRpcService.qe.schemaInfo.triggerReload()
}

func GetSessionId() int64 {
	return SqlQueryRpcService.sessionId
}

func IsCachePoolAvailable() bool {
	return !SqlQueryRpcService.qe.cachePool.IsClosed()
}

func InvalidateForDml(cacheInvalidate *proto.CacheInvalidate) {
	SqlQueryRpcService.InvalidateForDml(cacheInvalidate)
}

func InvalidateForDDL(ddlInvalidate *proto.DDLInvalidate) {
	SqlQueryRpcService.InvalidateForDDL(ddlInvalidate)
}

func SetQueryRules(qrs *QueryRules) {
	SqlQueryRpcService.qe.schemaInfo.SetRules(qrs)
}

func GetQueryRules() (qrs *QueryRules) {
	return SqlQueryRpcService.qe.schemaInfo.GetRules()
}

// IsHealthy returns nil if the query service is healthy (able to
// connect to the database and serving traffic) or an error explaining
// the unhealthiness otherwise.
func IsHealthy() error {
	return SqlQueryRpcService.Execute(
		new(rpcproto.Context),
		&proto.Query{Sql: "select 1 from dual", SessionId: SqlQueryRpcService.sessionId},
		new(mproto.QueryResult),
	)
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	if err := IsHealthy(); err != nil {
		w.Write([]byte("notok"))
	}
	w.Write([]byte("ok"))
}

// InitQueryService registers the query service, after loading any
// necessary config files. It also starts any relevant streaming logs.
func InitQueryService() {
	SqlQueryLogger.ServeLogs(*queryLogHandler)
	TxLogger.ServeLogs(*txLogHandler)

	qsConfig := DefaultQsConfig
	if *qsConfigFile != "" {
		if err := jscfg.ReadJson(*qsConfigFile, &qsConfig); err != nil {
			log.Fatalf("cannot load qsconfig file: %v", err)
		}
	}
	qsConfig.SpotCheckRatio = *spotCheckRatio
	// TODO(liguo): Merge into your CL
	qsConfig.StreamExecThrottle = *streamExecThrottle
	qsConfig.StreamWaitTimeout = *streamWaitTimeout

	RegisterQueryService(qsConfig)
}

// LoadCustomRules returns custom rules as specified by the command
// line flags.
func LoadCustomRules() (qrs *QueryRules) {
	if *customRules == "" {
		return NewQueryRules()
	}

	data, err := ioutil.ReadFile(*customRules)
	if err != nil {
		log.Fatalf("Error reading file %v: %v", *customRules, err)
	}

	qrs = NewQueryRules()
	err = qrs.UnmarshalJSON(data)
	if err != nil {
		log.Fatalf("Error unmarshaling query rules %v", err)
	}
	return qrs
}
