// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"flag"
	"fmt"
	"net/url"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/streamlog"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	queryLogHandler = flag.String("query-log-stream-handler", "/debug/querylog", "URL handler for streaming queries log")
	txLogHandler    = flag.String("transaction-log-stream-handler", "/debug/txlog", "URL handler for streaming transactions log")
)

func init() {
	flag.IntVar(&qsConfig.PoolSize, "queryserver-config-pool-size", DefaultQsConfig.PoolSize, "query server connection pool size, connection pool is used by regular queries (non streaming, not in a transaction)")
	flag.IntVar(&qsConfig.StreamPoolSize, "queryserver-config-stream-pool-size", DefaultQsConfig.StreamPoolSize, "query server stream pool size, stream pool is used by stream queries: queries that return results to client in a streaming fashion")
	flag.IntVar(&qsConfig.TransactionCap, "queryserver-config-transaction-cap", DefaultQsConfig.TransactionCap, "query server transaction cap is the maximum number of transactions allowed to happen at any given point of a time for a single vttablet. E.g. by setting transaction cap to 100, there are at most 100 transactions will be processed by a vttablet and the 101th transaction will be blocked (and fail if it cannot get connection within specified timeout)")
	flag.Float64Var(&qsConfig.TransactionTimeout, "queryserver-config-transaction-timeout", DefaultQsConfig.TransactionTimeout, "query server transaction timeout (in seconds), a transaction will be killed if it takes longer than this value")
	flag.Float64Var(&qsConfig.TxShutDownGracePeriod, "transaction_shutdown_grace_period", DefaultQsConfig.TxShutDownGracePeriod, "how long to wait (in seconds) for transactions to complete during graceful shutdown.")
	flag.IntVar(&qsConfig.MaxResultSize, "queryserver-config-max-result-size", DefaultQsConfig.MaxResultSize, "query server max result size, maximum number of rows allowed to return from vttablet for non-streaming queries.")
	flag.IntVar(&qsConfig.MaxDMLRows, "queryserver-config-max-dml-rows", DefaultQsConfig.MaxDMLRows, "query server max dml rows per statement, maximum number of rows allowed to return at a time for an upadte or delete with either 1) an equality where clauses on primary keys, or 2) a subselect statement. For update and delete statements in above two categories, vttablet will split the original query into multiple small queries based on this configuration value. ")
	flag.IntVar(&qsConfig.StreamBufferSize, "queryserver-config-stream-buffer-size", DefaultQsConfig.StreamBufferSize, "query server stream buffer size, the maximum number of bytes sent from vttablet for each stream call.")
	flag.IntVar(&qsConfig.QueryCacheSize, "queryserver-config-query-cache-size", DefaultQsConfig.QueryCacheSize, "query server query cache size, maximum number of queries to be cached. vttablet analyzes every incoming query and generate a query plan, these plans are being cached in a lru cache. This config controls the capacity of the lru cache.")
	flag.Float64Var(&qsConfig.SchemaReloadTime, "queryserver-config-schema-reload-time", DefaultQsConfig.SchemaReloadTime, "query server schema reload time, how often vttablet reloads schemas from underlying MySQL instance in seconds. vttablet keeps table schemas in its own memory and periodically refreshes it from MySQL. This config controls the reload time.")
	flag.Float64Var(&qsConfig.QueryTimeout, "queryserver-config-query-timeout", DefaultQsConfig.QueryTimeout, "query server query timeout (in seconds), this is the query timeout in vttablet side. If a query takes more than this timeout, it will be killed.")
	flag.Float64Var(&qsConfig.TxPoolTimeout, "queryserver-config-txpool-timeout", DefaultQsConfig.TxPoolTimeout, "query server transaction pool timeout, it is how long vttablet waits if tx pool is full")
	flag.Float64Var(&qsConfig.IdleTimeout, "queryserver-config-idle-timeout", DefaultQsConfig.IdleTimeout, "query server idle timeout (in seconds), vttablet manages various mysql connection pools. This config means if a connection has not been used in given idle timeout, this connection will be removed from pool. This effectively manages number of connection objects and optimize the pool performance.")
	flag.BoolVar(&qsConfig.StrictMode, "queryserver-config-strict-mode", DefaultQsConfig.StrictMode, "allow only predictable DMLs and enforces MySQL's STRICT_TRANS_TABLES")
	// tableacl related configurations.
	flag.BoolVar(&qsConfig.StrictTableAcl, "queryserver-config-strict-table-acl", DefaultQsConfig.StrictTableAcl, "only allow queries that pass table acl checks")
	flag.BoolVar(&qsConfig.EnableTableAclDryRun, "queryserver-config-enable-table-acl-dry-run", DefaultQsConfig.EnableTableAclDryRun, "If this flag is enabled, tabletserver will emit monitoring metrics and let the request pass regardless of table acl check results")
	flag.StringVar(&qsConfig.TableAclExemptACL, "queryserver-config-acl-exempt-acl", DefaultQsConfig.TableAclExemptACL, "an acl that exempt from table acl checking (this acl is free to access any vitess tables).")
	flag.BoolVar(&qsConfig.TerseErrors, "queryserver-config-terse-errors", DefaultQsConfig.TerseErrors, "prevent bind vars from escaping in returned errors")
	flag.BoolVar(&qsConfig.EnablePublishStats, "queryserver-config-enable-publish-stats", DefaultQsConfig.EnablePublishStats, "set this flag to true makes queryservice publish monitoring stats")
	flag.StringVar(&qsConfig.StatsPrefix, "stats-prefix", DefaultQsConfig.StatsPrefix, "prefix for variable names exported via expvar")
	flag.StringVar(&qsConfig.DebugURLPrefix, "debug-url-prefix", DefaultQsConfig.DebugURLPrefix, "debug url prefix, vttablet will report various system debug pages and this config controls the prefix of these debug urls")
	flag.StringVar(&qsConfig.PoolNamePrefix, "pool-name-prefix", DefaultQsConfig.PoolNamePrefix, "pool name prefix, vttablet has several pools and each of them has a name. This config specifies the prefix of these pool names")
	flag.BoolVar(&qsConfig.WatchReplication, "watch_replication_stream", false, "When enabled, vttablet will stream the MySQL replication stream from the local server, and use it to support the include_event_token ExecuteOptions.")
	flag.BoolVar(&qsConfig.EnableAutoCommit, "enable-autocommit", DefaultQsConfig.EnableAutoCommit, "if the flag is on, a DML outsides a transaction will be auto committed.")
	flag.BoolVar(&qsConfig.TwoPCEnable, "twopc_enable", DefaultQsConfig.TwoPCEnable, "if the flag is on, 2pc is enabled. Other 2pc flags must be supplied.")
	flag.StringVar(&qsConfig.TwoPCCoordinatorAddress, "twopc_coordinator_address", DefaultQsConfig.TwoPCCoordinatorAddress, "address of the (VTGate) process(es) that will be used to notify of abandoned transactions.")
	flag.Float64Var(&qsConfig.TwoPCAbandonAge, "twopc_abandon_age", DefaultQsConfig.TwoPCAbandonAge, "time in seconds. Any unresolved transaction older than this time will be sent to the coordinator to be resolved.")
}

// Init must be called after flag.Parse, and before doing any other operations.
func Init() {
	StatsLogger.ServeLogs(*queryLogHandler, buildFmter(StatsLogger))
	TxLogger.ServeLogs(*txLogHandler, buildFmter(TxLogger))
}

// Config contains all the configuration for query service
type Config struct {
	PoolSize                int
	StreamPoolSize          int
	TransactionCap          int
	TransactionTimeout      float64
	TxShutDownGracePeriod   float64
	MaxResultSize           int
	MaxDMLRows              int
	StreamBufferSize        int
	QueryCacheSize          int
	SchemaReloadTime        float64
	QueryTimeout            float64
	TxPoolTimeout           float64
	IdleTimeout             float64
	StrictMode              bool
	StrictTableAcl          bool
	TerseErrors             bool
	EnablePublishStats      bool
	EnableAutoCommit        bool
	EnableTableAclDryRun    bool
	StatsPrefix             string
	DebugURLPrefix          string
	PoolNamePrefix          string
	TableAclExemptACL       string
	WatchReplication        bool
	TwoPCEnable             bool
	TwoPCCoordinatorAddress string
	TwoPCAbandonAge         float64
}

// DefaultQsConfig is the default value for the query service config.
// The value for StreamBufferSize was chosen after trying out a few of
// them. Too small buffers force too many packets to be sent. Too big
// buffers force the clients to read them in multiple chunks and make
// memory copies.  so with the encoding overhead, this seems to work
// great (the overhead makes the final packets on the wire about twice
// bigger than this).
var DefaultQsConfig = Config{
	PoolSize:                16,
	StreamPoolSize:          200,
	TransactionCap:          20,
	TransactionTimeout:      30,
	TxShutDownGracePeriod:   0,
	MaxResultSize:           10000,
	MaxDMLRows:              500,
	QueryCacheSize:          5000,
	SchemaReloadTime:        30 * 60,
	QueryTimeout:            30,
	TxPoolTimeout:           1,
	IdleTimeout:             30 * 60,
	StreamBufferSize:        32 * 1024,
	StrictMode:              true,
	StrictTableAcl:          false,
	TerseErrors:             false,
	EnablePublishStats:      true,
	EnableAutoCommit:        false,
	EnableTableAclDryRun:    false,
	StatsPrefix:             "",
	DebugURLPrefix:          "/debug",
	PoolNamePrefix:          "",
	TableAclExemptACL:       "",
	WatchReplication:        false,
	TwoPCEnable:             false,
	TwoPCCoordinatorAddress: "",
	TwoPCAbandonAge:         0,
}

var qsConfig Config

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

// NewServer creates a new TabletServer based on the command line flags.
func NewServer() *TabletServer {
	return NewTabletServer(qsConfig)
}

// Controller defines the control interface for TabletServer.
type Controller interface {
	// Register registers this query service with the RPC layer.
	Register()

	// AddStatusPart adds the status part to the status page
	AddStatusPart()

	// InitDBConfig sets up the db config vars.
	InitDBConfig(querypb.Target, dbconfigs.DBConfigs, mysqlctl.MysqlDaemon) error

	// SetServingType transitions the query service to the required serving type.
	// Returns true if the state of QueryService or the tablet type changed.
	SetServingType(tabletType topodatapb.TabletType, serving bool, alsoAllow []topodatapb.TabletType) (bool, error)

	// EnterLameduck causes tabletserver to enter the lameduck state.
	EnterLameduck()

	// IsServing returns true if the query service is running
	IsServing() bool

	// IsHealthy returns the health status of the QueryService
	IsHealthy() error

	// ClearQueryPlanCache clears internal query plan cache
	ClearQueryPlanCache()

	// ReloadSchema makes the quey service reload its schema cache
	ReloadSchema(ctx context.Context) error

	// RegisterQueryRuleSource adds a query rule source
	RegisterQueryRuleSource(ruleSource string)

	// RegisterQueryRuleSource removes a query rule source
	UnRegisterQueryRuleSource(ruleSource string)

	// SetQueryRules sets the query rules for this QueryService
	SetQueryRules(ruleSource string, qrs *QueryRules) error

	// QueryService returns the QueryService object used by this Controller
	QueryService() queryservice.QueryService

	QueryServiceStats() *QueryServiceStats

	// BroadcastHealth sends the current health to all listeners
	BroadcastHealth(terTimestamp int64, stats *querypb.RealtimeStats)
}

// Ensure TabletServer satisfies Controller interface.
var _ Controller = (*TabletServer)(nil)
