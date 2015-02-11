// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/acl"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/streamlog"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"golang.org/x/net/context"
)

var (
	queryLogHandler = flag.String("query-log-stream-handler", "/debug/querylog", "URL handler for streaming queries log")
	txLogHandler    = flag.String("transaction-log-stream-handler", "/debug/txlog", "URL handler for streaming transactions log")

	checkMySLQThrottler = sync2.NewSemaphore(1, 0)
)

func init() {
	flag.IntVar(&qsConfig.PoolSize, "queryserver-config-pool-size", DefaultQsConfig.PoolSize, "query server pool size")
	flag.IntVar(&qsConfig.StreamPoolSize, "queryserver-config-stream-pool-size", DefaultQsConfig.StreamPoolSize, "query server stream pool size")
	flag.IntVar(&qsConfig.TransactionCap, "queryserver-config-transaction-cap", DefaultQsConfig.TransactionCap, "query server transaction cap")
	flag.Float64Var(&qsConfig.TransactionTimeout, "queryserver-config-transaction-timeout", DefaultQsConfig.TransactionTimeout, "query server transaction timeout")
	flag.IntVar(&qsConfig.MaxResultSize, "queryserver-config-max-result-size", DefaultQsConfig.MaxResultSize, "query server max result size")
	flag.IntVar(&qsConfig.MaxDMLRows, "queryserver-config-max-dml-rows", DefaultQsConfig.MaxDMLRows, "query server max dml rows per statement")
	flag.IntVar(&qsConfig.StreamBufferSize, "queryserver-config-stream-buffer-size", DefaultQsConfig.StreamBufferSize, "query server stream buffer size")
	flag.IntVar(&qsConfig.QueryCacheSize, "queryserver-config-query-cache-size", DefaultQsConfig.QueryCacheSize, "query server query cache size")
	flag.Float64Var(&qsConfig.SchemaReloadTime, "queryserver-config-schema-reload-time", DefaultQsConfig.SchemaReloadTime, "query server schema reload time")
	flag.Float64Var(&qsConfig.QueryTimeout, "queryserver-config-query-timeout", DefaultQsConfig.QueryTimeout, "query server query timeout")
	flag.Float64Var(&qsConfig.TxPoolTimeout, "queryserver-config-txpool-timeout", DefaultQsConfig.TxPoolTimeout, "query server transaction pool timeout")
	flag.Float64Var(&qsConfig.IdleTimeout, "queryserver-config-idle-timeout", DefaultQsConfig.IdleTimeout, "query server idle timeout")
	flag.Float64Var(&qsConfig.SpotCheckRatio, "queryserver-config-spot-check-ratio", DefaultQsConfig.SpotCheckRatio, "query server rowcache spot check frequency")
	flag.BoolVar(&qsConfig.StrictMode, "queryserver-config-strict-mode", DefaultQsConfig.StrictMode, "allow only predictable DMLs and enforces MySQL's STRICT_TRANS_TABLES")
	flag.BoolVar(&qsConfig.StrictTableAcl, "queryserver-config-strict-table-acl", DefaultQsConfig.StrictTableAcl, "only allow queries that pass table acl checks")
	flag.StringVar(&qsConfig.RowCache.Binary, "rowcache-bin", DefaultQsConfig.RowCache.Binary, "rowcache binary file")
	flag.IntVar(&qsConfig.RowCache.Memory, "rowcache-memory", DefaultQsConfig.RowCache.Memory, "rowcache max memory usage in MB")
	flag.StringVar(&qsConfig.RowCache.Socket, "rowcache-socket", DefaultQsConfig.RowCache.Socket, "rowcache socket path to listen on")
	flag.IntVar(&qsConfig.RowCache.TcpPort, "rowcache-port", DefaultQsConfig.RowCache.TcpPort, "rowcache tcp port to listen on")
	flag.IntVar(&qsConfig.RowCache.Connections, "rowcache-connections", DefaultQsConfig.RowCache.Connections, "rowcache max simultaneous connections")
	flag.IntVar(&qsConfig.RowCache.Threads, "rowcache-threads", DefaultQsConfig.RowCache.Threads, "rowcache number of threads")
	flag.BoolVar(&qsConfig.RowCache.LockPaged, "rowcache-lock-paged", DefaultQsConfig.RowCache.LockPaged, "whether rowcache locks down paged memory")
}

// RowCacheConfig encapsulates the configuration for RowCache
type RowCacheConfig struct {
	Binary      string
	Memory      int
	Socket      string
	TcpPort     int
	Connections int
	Threads     int
	LockPaged   bool
}

// GetSubprocessFlags returns the flags to use to call memcached
func (c *RowCacheConfig) GetSubprocessFlags() []string {
	cmd := []string{}
	if c.Binary == "" {
		return cmd
	}
	cmd = append(cmd, c.Binary)
	if c.Memory > 0 {
		// memory is given in bytes and rowcache expects in MBs
		cmd = append(cmd, "-m", strconv.Itoa(c.Memory/1000000))
	}
	if c.Socket != "" {
		cmd = append(cmd, "-s", c.Socket)
	}
	if c.TcpPort > 0 {
		cmd = append(cmd, "-p", strconv.Itoa(c.TcpPort))
	}
	if c.Connections > 0 {
		cmd = append(cmd, "-c", strconv.Itoa(c.Connections))
	}
	if c.Threads > 0 {
		cmd = append(cmd, "-t", strconv.Itoa(c.Threads))
	}
	if c.LockPaged {
		cmd = append(cmd, "-k")
	}
	return cmd
}

// Config contains all the configuration for query service
type Config struct {
	PoolSize           int
	StreamPoolSize     int
	TransactionCap     int
	TransactionTimeout float64
	MaxResultSize      int
	MaxDMLRows         int
	StreamBufferSize   int
	QueryCacheSize     int
	SchemaReloadTime   float64
	QueryTimeout       float64
	TxPoolTimeout      float64
	IdleTimeout        float64
	RowCache           RowCacheConfig
	SpotCheckRatio     float64
	StrictMode         bool
	StrictTableAcl     bool
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
	MaxDMLRows:         500,
	QueryCacheSize:     5000,
	SchemaReloadTime:   30 * 60,
	QueryTimeout:       0,
	TxPoolTimeout:      1,
	IdleTimeout:        30 * 60,
	StreamBufferSize:   32 * 1024,
	RowCache:           RowCacheConfig{Memory: -1, TcpPort: -1, Connections: -1, Threads: -1},
	SpotCheckRatio:     0,
	StrictMode:         true,
	StrictTableAcl:     false,
}

var qsConfig Config

// QueryServiceControl is the interface implemented by the controller
// for the query service.
type QueryServiceControl interface {
	// Register registers this query service with the RPC layer.
	Register()

	// AddStatusPart adds the status part to the status page
	AddStatusPart()

	// AllowQueries enables queries.
	AllowQueries(*dbconfigs.DBConfigs, []SchemaOverride, *mysqlctl.Mysqld) error

	// DisallowQueries shuts down the query service.
	DisallowQueries()

	// IsServing returns true if the query service is running
	IsServing() bool

	// IsHealthy returns the health status of the QueryService
	IsHealthy() error

	// ReloadSchema makes the quey service reload its schema cache
	ReloadSchema()

	// SetQueryRules sets the query rules for this QueryService
	SetQueryRules(ruleSource string, qrs *QueryRules) error

	// SqlQuery returns the SqlQuery object used by this QueryServiceControl
	SqlQuery() *SqlQuery
}

// TestQueryServiceControl is a fake version of QueryServiceControl
type TestQueryServiceControl struct {
	// QueryServiceEnabled is a state variable
	QueryServiceEnabled bool

	// AllowQueriesError is the return value for AllowQueries
	AllowQueriesError error

	// IsHealthy is the return value for IsHealthy
	IsHealthyError error

	// ReloadSchemaCount counts how many times ReloadSchema was called
	ReloadSchemaCount int
}

// NewTestQueryServiceControl returns an implementation of QueryServiceControl
// that is entirely fake
func NewTestQueryServiceControl() *TestQueryServiceControl {
	return &TestQueryServiceControl{
		QueryServiceEnabled: false,
		AllowQueriesError:   nil,
		IsHealthyError:      nil,
		ReloadSchemaCount:   0,
	}
}

// Register is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) Register() {
}

// AddStatusPart is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) AddStatusPart() {
}

// AllowQueries is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) AllowQueries(*dbconfigs.DBConfigs, []SchemaOverride, *mysqlctl.Mysqld) error {
	tqsc.QueryServiceEnabled = tqsc.AllowQueriesError == nil
	return tqsc.AllowQueriesError
}

// DisallowQueries is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) DisallowQueries() {
	tqsc.QueryServiceEnabled = false
}

// IsServing is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) IsServing() bool {
	return tqsc.QueryServiceEnabled
}

// IsHealthy is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) IsHealthy() error {
	return tqsc.IsHealthyError
}

// ReloadSchema is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) ReloadSchema() {
	tqsc.ReloadSchemaCount++
}

// SetQueryRules is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) SetQueryRules(ruleSource string, qrs *QueryRules) error {
	return nil
}

// SqlQuery is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) SqlQuery() *SqlQuery {
	return nil
}

// realQueryServiceControl implements QueryServiceControl for real
type realQueryServiceControl struct {
	sqlQueryRPCService *SqlQuery
}

// NewQueryServiceControl returns a real implementation of QueryServiceControl
func NewQueryServiceControl() QueryServiceControl {
	return &realQueryServiceControl{
		sqlQueryRPCService: NewSqlQuery(qsConfig),
	}
}

// registration service for all server protocols

// QueryServiceControlRegisterFunction is a callback type to be called when we
// Register() a QueryServiceControl
type QueryServiceControlRegisterFunction func(QueryServiceControl)

// QueryServiceControlRegisterFunctions is an array of all the
// QueryServiceControlRegisterFunction that will be called upon
// Register() on a QueryServiceControl
var QueryServiceControlRegisterFunctions []QueryServiceControlRegisterFunction

// Register is part of the QueryServiceControl interface
func (rqsc *realQueryServiceControl) Register() {
	rqsc.registerCheckMySQL()
	for _, f := range QueryServiceControlRegisterFunctions {
		f(rqsc)
	}
	rqsc.registerDebugHealthHandler()
	rqsc.registerQueryzHandler()
	rqsc.registerSchemazHandler()
	rqsc.registerStreamQueryzHandlers()
}

// AllowQueries starts the query service.
func (rqsc *realQueryServiceControl) AllowQueries(dbconfigs *dbconfigs.DBConfigs, schemaOverrides []SchemaOverride, mysqld *mysqlctl.Mysqld) error {
	return rqsc.sqlQueryRPCService.allowQueries(dbconfigs, schemaOverrides, mysqld)
}

// DisallowQueries can take a long time to return (not indefinite) because
// it has to wait for queries & transactions to be completed or killed,
// and also for house keeping goroutines to be terminated.
func (rqsc *realQueryServiceControl) DisallowQueries() {
	defer logError()
	rqsc.sqlQueryRPCService.disallowQueries()
}

// IsServing is part of the QueryServiceControl interface
func (rqsc *realQueryServiceControl) IsServing() bool {
	return rqsc.sqlQueryRPCService.GetState() == "SERVING"
}

// Reload the schema. If the query service is not running, nothing will happen
func (rqsc *realQueryServiceControl) ReloadSchema() {
	defer logError()
	rqsc.sqlQueryRPCService.qe.schemaInfo.triggerReload()
}

// checkMySQL verifies that MySQL is still reachable by connecting to it.
// If it's not reachable, it shuts down the query service.
// This function rate-limits the check to no more than once per second.
// FIXME(alainjobart) this global variable is accessed from many parts
// of this library, this needs refactoring, probably using an interface.
var checkMySQL = func() {}

func (rqsc *realQueryServiceControl) registerCheckMySQL() {
	checkMySQL = func() {
		if !checkMySLQThrottler.TryAcquire() {
			return
		}
		defer func() {
			time.Sleep(1 * time.Second)
			checkMySLQThrottler.Release()
		}()
		defer logError()
		if rqsc.sqlQueryRPCService.checkMySQL() {
			return
		}
		log.Infof("Check MySQL failed. Shutting down query service")
		rqsc.DisallowQueries()
	}
}

// SetQueryRules is the tabletserver level API to write current query rules
func (rqsc *realQueryServiceControl) SetQueryRules(ruleSource string, qrs *QueryRules) error {
	err := QueryRuleSources.SetRules(ruleSource, qrs)
	if err != nil {
		return err
	}
	rqsc.sqlQueryRPCService.qe.schemaInfo.ClearQueryPlanCache()
	return nil
}

// SqlQuery is part of the QueryServiceControl interface
func (rqsc *realQueryServiceControl) SqlQuery() *SqlQuery {
	return rqsc.sqlQueryRPCService
}

// IsHealthy returns nil if the query service is healthy (able to
// connect to the database and serving traffic) or an error explaining
// the unhealthiness otherwise.
func (rqsc *realQueryServiceControl) IsHealthy() error {
	return rqsc.sqlQueryRPCService.Execute(
		context.Background(),
		&proto.Query{
			Sql:       "select 1 from dual",
			SessionId: rqsc.sqlQueryRPCService.sessionId,
		},
		new(mproto.QueryResult),
	)
}

func (rqsc *realQueryServiceControl) registerDebugHealthHandler() {
	http.HandleFunc("/debug/health", func(w http.ResponseWriter, r *http.Request) {
		if err := acl.CheckAccessHTTP(r, acl.MONITORING); err != nil {
			acl.SendError(w, err)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		if err := rqsc.IsHealthy(); err != nil {
			w.Write([]byte("not ok"))
			return
		}
		w.Write([]byte("ok"))
	})
}

func buildFmter(logger *streamlog.StreamLogger) func(url.Values, interface{}) string {
	type formatter interface {
		Format(url.Values) string
	}

	return func(params url.Values, val interface{}) string {
		fmter, ok := val.(formatter)
		if !ok {
			return fmt.Sprintf("Error: unexpected value of type %T in %s!", val, logger.Name())
		}
		return fmter.Format(params)
	}
}

// InitQueryService registers the query service, after loading any
// necessary config files. It also starts any relevant streaming logs.
func InitQueryService(qsc QueryServiceControl) {
	SqlQueryLogger.ServeLogs(*queryLogHandler, buildFmter(SqlQueryLogger))
	TxLogger.ServeLogs(*txLogHandler, buildFmter(TxLogger))
	qsc.Register()
}
