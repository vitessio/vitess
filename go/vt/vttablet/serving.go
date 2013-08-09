package vttablet

import (
	"flag"
	"os"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/relog"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/rpcwrap/jsonrpc"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	ts "github.com/youtube/vitess/go/vt/tabletserver"
)

var (
	queryLog        = flag.String("debug-querylog-file", "", "for testing: log all queries to this file")
	queryLogHandler = flag.String("query-log-stream-handler", "/debug/querylog", "URL handler for streaming queries log")
	txLogHandler    = flag.String("transaction-log-stream-handler", "/debug/txlog", "URL handler for streaming transactions log")
	qsConfigFile    = flag.String("queryserver-config-file", "", "config file name for the query service")
)

// DefaultQSConfig is the default value for the query service config.
//
// The value for StreamBufferSize was chosen after trying out a few of
// them. Too small buffers force too many packets to be sent. Too big
// buffers force the clients to read them in multiple chunks and make
// memory copies.  so with the encoding overhead, this seems to work
// great (the overhead makes the final packets on the wire about twice
// bigger than this).
var DefaultQsConfig = ts.Config{
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
}

func ServeAuthRPC() {
	bsonrpc.ServeAuthRPC()
	jsonrpc.ServeAuthRPC()
}

func ServeRPC() {
	jsonrpc.ServeHTTP()
	jsonrpc.ServeRPC()
	bsonrpc.ServeHTTP()
	bsonrpc.ServeRPC()
}

func InitQueryService(dbcfgs dbconfigs.DBConfigs) {
	ts.SqlQueryLogger.ServeLogs(*queryLogHandler)
	ts.TxLogger.ServeLogs(*txLogHandler)

	qsConfig := DefaultQsConfig
	if *qsConfigFile != "" {
		if err := jscfg.ReadJson(*qsConfigFile, &qsConfig); err != nil {
			log.Fatalf("cannot load qsconfig file: %v", err)
		}
	}

	ts.RegisterQueryService(qsConfig)

	if *queryLog != "" {
		if f, err := os.OpenFile(*queryLog, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644); err == nil {
			ts.QueryLogger = relog.New(f, "", relog.DEBUG)
		} else {
			log.Fatalf("Error opening file %v: %v", *queryLog, err)
		}
	}
}
