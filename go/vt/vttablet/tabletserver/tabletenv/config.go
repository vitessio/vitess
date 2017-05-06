/*
Copyright 2017 Google Inc.

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

package tabletenv

import (
	"errors"
	"flag"
	"fmt"
	"net/url"

	"github.com/golang/protobuf/proto"

	"time"

	"github.com/youtube/vitess/go/flagutil"
	"github.com/youtube/vitess/go/streamlog"
	"github.com/youtube/vitess/go/vt/throttler"
)

var (
	queryLogHandler = flag.String("query-log-stream-handler", "/debug/querylog", "URL handler for streaming queries log")
	txLogHandler    = flag.String("transaction-log-stream-handler", "/debug/txlog", "URL handler for streaming transactions log")

	// TxLogger can be used to enable logging of transactions.
	// Call TxLogger.ServeLogs in your main program to enable logging.
	// The log format can be inferred by looking at TxConnection.Format.
	TxLogger = streamlog.New("TxLog", 10)

	// StatsLogger is the main stream logger object
	StatsLogger = streamlog.New("TabletServer", 50)
)

func init() {
	flag.IntVar(&Config.PoolSize, "queryserver-config-pool-size", DefaultQsConfig.PoolSize, "query server connection pool size, connection pool is used by regular queries (non streaming, not in a transaction)")
	flag.IntVar(&Config.StreamPoolSize, "queryserver-config-stream-pool-size", DefaultQsConfig.StreamPoolSize, "query server stream connection pool size, stream pool is used by stream queries: queries that return results to client in a streaming fashion")
	flag.IntVar(&Config.MessagePoolSize, "queryserver-config-message-conn-pool-size", DefaultQsConfig.MessagePoolSize, "query server message connection pool size, message pool is used by message managers: recommended value is one per message table")
	flag.IntVar(&Config.TransactionCap, "queryserver-config-transaction-cap", DefaultQsConfig.TransactionCap, "query server transaction cap is the maximum number of transactions allowed to happen at any given point of a time for a single vttablet. E.g. by setting transaction cap to 100, there are at most 100 transactions will be processed by a vttablet and the 101th transaction will be blocked (and fail if it cannot get connection within specified timeout)")
	flag.Float64Var(&Config.TransactionTimeout, "queryserver-config-transaction-timeout", DefaultQsConfig.TransactionTimeout, "query server transaction timeout (in seconds), a transaction will be killed if it takes longer than this value")
	flag.Float64Var(&Config.TxShutDownGracePeriod, "transaction_shutdown_grace_period", DefaultQsConfig.TxShutDownGracePeriod, "how long to wait (in seconds) for transactions to complete during graceful shutdown.")
	flag.IntVar(&Config.MaxResultSize, "queryserver-config-max-result-size", DefaultQsConfig.MaxResultSize, "query server max result size, maximum number of rows allowed to return from vttablet for non-streaming queries.")
	flag.IntVar(&Config.MaxDMLRows, "queryserver-config-max-dml-rows", DefaultQsConfig.MaxDMLRows, "query server max dml rows per statement, maximum number of rows allowed to return at a time for an upadte or delete with either 1) an equality where clauses on primary keys, or 2) a subselect statement. For update and delete statements in above two categories, vttablet will split the original query into multiple small queries based on this configuration value. ")
	flag.IntVar(&Config.StreamBufferSize, "queryserver-config-stream-buffer-size", DefaultQsConfig.StreamBufferSize, "query server stream buffer size, the maximum number of bytes sent from vttablet for each stream call.")
	flag.IntVar(&Config.QueryCacheSize, "queryserver-config-query-cache-size", DefaultQsConfig.QueryCacheSize, "query server query cache size, maximum number of queries to be cached. vttablet analyzes every incoming query and generate a query plan, these plans are being cached in a lru cache. This config controls the capacity of the lru cache.")
	flag.Float64Var(&Config.SchemaReloadTime, "queryserver-config-schema-reload-time", DefaultQsConfig.SchemaReloadTime, "query server schema reload time, how often vttablet reloads schemas from underlying MySQL instance in seconds. vttablet keeps table schemas in its own memory and periodically refreshes it from MySQL. This config controls the reload time.")
	flag.Float64Var(&Config.QueryTimeout, "queryserver-config-query-timeout", DefaultQsConfig.QueryTimeout, "query server query timeout (in seconds), this is the query timeout in vttablet side. If a query takes more than this timeout, it will be killed.")
	flag.Float64Var(&Config.TxPoolTimeout, "queryserver-config-txpool-timeout", DefaultQsConfig.TxPoolTimeout, "query server transaction pool timeout, it is how long vttablet waits if tx pool is full")
	flag.Float64Var(&Config.IdleTimeout, "queryserver-config-idle-timeout", DefaultQsConfig.IdleTimeout, "query server idle timeout (in seconds), vttablet manages various mysql connection pools. This config means if a connection has not been used in given idle timeout, this connection will be removed from pool. This effectively manages number of connection objects and optimize the pool performance.")
	// tableacl related configurations.
	flag.BoolVar(&Config.StrictTableACL, "queryserver-config-strict-table-acl", DefaultQsConfig.StrictTableACL, "only allow queries that pass table acl checks")
	flag.BoolVar(&Config.EnableTableACLDryRun, "queryserver-config-enable-table-acl-dry-run", DefaultQsConfig.EnableTableACLDryRun, "If this flag is enabled, tabletserver will emit monitoring metrics and let the request pass regardless of table acl check results")
	flag.StringVar(&Config.TableACLExemptACL, "queryserver-config-acl-exempt-acl", DefaultQsConfig.TableACLExemptACL, "an acl that exempt from table acl checking (this acl is free to access any vitess tables).")
	flag.BoolVar(&Config.TerseErrors, "queryserver-config-terse-errors", DefaultQsConfig.TerseErrors, "prevent bind vars from escaping in returned errors")
	flag.StringVar(&Config.PoolNamePrefix, "pool-name-prefix", DefaultQsConfig.PoolNamePrefix, "pool name prefix, vttablet has several pools and each of them has a name. This config specifies the prefix of these pool names")
	flag.BoolVar(&Config.WatchReplication, "watch_replication_stream", false, "When enabled, vttablet will stream the MySQL replication stream from the local server, and use it to support the include_event_token ExecuteOptions.")
	flag.BoolVar(&Config.EnableAutoCommit, "enable-autocommit", DefaultQsConfig.EnableAutoCommit, "if the flag is on, a DML outsides a transaction will be auto committed. This flag is deprecated and is unsafe. Instead, use the VTGate provided autocommit feature.")
	flag.BoolVar(&Config.TwoPCEnable, "twopc_enable", DefaultQsConfig.TwoPCEnable, "if the flag is on, 2pc is enabled. Other 2pc flags must be supplied.")
	flag.StringVar(&Config.TwoPCCoordinatorAddress, "twopc_coordinator_address", DefaultQsConfig.TwoPCCoordinatorAddress, "address of the (VTGate) process(es) that will be used to notify of abandoned transactions.")
	flag.Float64Var(&Config.TwoPCAbandonAge, "twopc_abandon_age", DefaultQsConfig.TwoPCAbandonAge, "time in seconds. Any unresolved transaction older than this time will be sent to the coordinator to be resolved.")
	flag.BoolVar(&Config.EnableTxThrottler, "enable-tx-throttler", DefaultQsConfig.EnableTxThrottler, "If true replication-lag-based throttling on transactions will be enabled.")
	flag.StringVar(&Config.TxThrottlerConfig, "tx-throttler-config", DefaultQsConfig.TxThrottlerConfig, "The configuration of the transaction throttler as a text formatted throttlerdata.Configuration protocol buffer message")
	flagutil.StringListVar(&Config.TxThrottlerHealthCheckCells, "tx-throttler-healthcheck-cells", DefaultQsConfig.TxThrottlerHealthCheckCells, "A comma-separated list of cells. Only tabletservers running in these cells will be monitored for replication lag by the transaction throttler.")

	flag.BoolVar(&Config.EnableHotRowProtection, "enable_hot_row_protection", DefaultQsConfig.EnableHotRowProtection, "If true, incoming transactions for the same row (range) will be queued and cannot consume all txpool slots.")
	flag.BoolVar(&Config.EnableHotRowProtectionDryRun, "enable_hot_row_protection_dry_run", DefaultQsConfig.EnableHotRowProtectionDryRun, "If true, hot row protection is not enforced but logs if transactions would have been queued.")
	flag.IntVar(&Config.HotRowProtectionMaxQueueSize, "hot_row_protection_max_queue_size", DefaultQsConfig.HotRowProtectionMaxQueueSize, "Maximum number of BeginExecute RPCs which will be queued for the same row (range).")
	flag.IntVar(&Config.HotRowProtectionMaxGlobalQueueSize, "hot_row_protection_max_global_queue_size", DefaultQsConfig.HotRowProtectionMaxGlobalQueueSize, "Global queue limit across all row (ranges). Useful to prevent that the queue can grow unbounded.")

	flag.BoolVar(&Config.HeartbeatEnable, "heartbeat_enable", DefaultQsConfig.HeartbeatEnable, "If true, vttablet records (if master) or checks (if replica) the current time of a replication heartbeat in the table _vt.heartbeat. The result is used to inform the serving state of the vttablet via healthchecks.")
	flag.DurationVar(&Config.HeartbeatInterval, "heartbeat_interval", DefaultQsConfig.HeartbeatInterval, "How frequently to read and write replication heartbeat.")

	flag.BoolVar(&Config.EnforceStrictTransTables, "enforce_strict_trans_tables", DefaultQsConfig.EnforceStrictTransTables, "If true, vttablet requires MySQL to run with STRICT_TRANS_TABLES on. It is recommended to not turn this flag off. Otherwise MySQL may alter your supplied values before saving them to the database.")
}

// Init must be called after flag.Parse, and before doing any other operations.
func Init() {
	StatsLogger.ServeLogs(*queryLogHandler, buildFmter(StatsLogger))
	TxLogger.ServeLogs(*txLogHandler, buildFmter(TxLogger))
}

// TabletConfig contains all the configuration for query service
type TabletConfig struct {
	PoolSize                int
	StreamPoolSize          int
	MessagePoolSize         int
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
	StrictTableACL          bool
	TerseErrors             bool
	EnableAutoCommit        bool
	EnableTableACLDryRun    bool
	PoolNamePrefix          string
	TableACLExemptACL       string
	WatchReplication        bool
	TwoPCEnable             bool
	TwoPCCoordinatorAddress string
	TwoPCAbandonAge         float64

	EnableTxThrottler           bool
	TxThrottlerConfig           string
	TxThrottlerHealthCheckCells []string

	EnableHotRowProtection             bool
	EnableHotRowProtectionDryRun       bool
	HotRowProtectionMaxQueueSize       int
	HotRowProtectionMaxGlobalQueueSize int

	HeartbeatEnable   bool
	HeartbeatInterval time.Duration

	EnforceStrictTransTables bool
}

// DefaultQsConfig is the default value for the query service config.
// The value for StreamBufferSize was chosen after trying out a few of
// them. Too small buffers force too many packets to be sent. Too big
// buffers force the clients to read them in multiple chunks and make
// memory copies.  so with the encoding overhead, this seems to work
// great (the overhead makes the final packets on the wire about twice
// bigger than this).
var DefaultQsConfig = TabletConfig{
	PoolSize:                16,
	StreamPoolSize:          200,
	MessagePoolSize:         5,
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
	StrictTableACL:          false,
	TerseErrors:             false,
	EnableAutoCommit:        false,
	EnableTableACLDryRun:    false,
	PoolNamePrefix:          "",
	TableACLExemptACL:       "",
	WatchReplication:        false,
	TwoPCEnable:             false,
	TwoPCCoordinatorAddress: "",
	TwoPCAbandonAge:         0,

	EnableTxThrottler:           false,
	TxThrottlerConfig:           defaultTxThrottlerConfig(),
	TxThrottlerHealthCheckCells: []string{},

	EnableHotRowProtection:       false,
	EnableHotRowProtectionDryRun: false,
	// Default value is the same as TransactionCap.
	HotRowProtectionMaxQueueSize:       20,
	HotRowProtectionMaxGlobalQueueSize: 1000,

	HeartbeatEnable:   false,
	HeartbeatInterval: 1 * time.Second,

	EnforceStrictTransTables: true,
}

// defaultTxThrottlerConfig formats the default throttlerdata.Configuration
// object in text format. It uses the object returned by
// throttler.DefaultMaxReplicationLagModuleConfig().Configuration and overrides some of its
// fields. It panics on error.
func defaultTxThrottlerConfig() string {
	// Take throttler.DefaultMaxReplicationLagModuleConfig and override some fields.
	config := throttler.DefaultMaxReplicationLagModuleConfig().Configuration
	// TODO(erez): Make DefaultMaxReplicationLagModuleConfig() return a MaxReplicationLagSec of 10
	// and remove this line.
	config.MaxReplicationLagSec = 10
	return proto.MarshalTextString(&config)
}

// Config contains all the current config values. It's read-only,
// except for tests.
var Config TabletConfig

// VerifyConfig checks "Config" for contradicting flags.
func VerifyConfig() error {
	if actual, dryRun := Config.EnableHotRowProtection, Config.EnableHotRowProtectionDryRun; actual && dryRun {
		return errors.New("only one of two flags allowed: -enable_hot_row_protection or -enable_hot_row_protection_dry_run")
	}
	if v := Config.HotRowProtectionMaxQueueSize; v <= 0 {
		return fmt.Errorf("-hot_row_protection_max_queue_size must be > 0 (specified value: %v)", v)
	}
	if v := Config.HotRowProtectionMaxGlobalQueueSize; v <= 0 {
		return fmt.Errorf("-hot_row_protection_max_global_queue_size must be > 0 (specified value: %v)", v)
	}
	if globalSize, size := Config.HotRowProtectionMaxGlobalQueueSize, Config.HotRowProtectionMaxQueueSize; globalSize < size {
		return fmt.Errorf("global queue size must be >= per row (range) queue size: -hot_row_protection_max_global_queue_size < hot_row_protection_max_queue_size (%v < %v)", globalSize, size)
	}
	return nil
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
