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
	"time"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/throttler"
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
	flag.IntVar(&Config.MessagePostponeCap, "queryserver-config-message-postpone-cap", DefaultQsConfig.MessagePostponeCap, "query server message postpone cap is the maximum number of messages that can be postponed at any given time. Set this number to substantially lower than transaction cap, so that the transaction pool isn't exhausted by the message subsystem.")
	flag.IntVar(&Config.FoundRowsPoolSize, "client-found-rows-pool-size", DefaultQsConfig.FoundRowsPoolSize, "size of a special pool that will be used if the client requests that statements be executed with the CLIENT_FOUND_ROWS option of MySQL.")
	flag.Float64Var(&Config.TransactionTimeout, "queryserver-config-transaction-timeout", DefaultQsConfig.TransactionTimeout, "query server transaction timeout (in seconds), a transaction will be killed if it takes longer than this value")
	flag.Float64Var(&Config.TxShutDownGracePeriod, "transaction_shutdown_grace_period", DefaultQsConfig.TxShutDownGracePeriod, "how long to wait (in seconds) for transactions to complete during graceful shutdown.")
	flag.IntVar(&Config.MaxResultSize, "queryserver-config-max-result-size", DefaultQsConfig.MaxResultSize, "query server max result size, maximum number of rows allowed to return from vttablet for non-streaming queries.")
	flag.IntVar(&Config.WarnResultSize, "queryserver-config-warn-result-size", DefaultQsConfig.WarnResultSize, "query server result size warning threshold, warn if number of rows returned from vttablet for non-streaming queries exceeds this")
	flag.IntVar(&Config.MaxDMLRows, "queryserver-config-max-dml-rows", DefaultQsConfig.MaxDMLRows, "query server max dml rows per statement, maximum number of rows allowed to return at a time for an update or delete with either 1) an equality where clauses on primary keys, or 2) a subselect statement. For update and delete statements in above two categories, vttablet will split the original query into multiple small queries based on this configuration value. ")
	flag.BoolVar(&Config.PassthroughDMLs, "queryserver-config-passthrough-dmls", DefaultQsConfig.PassthroughDMLs, "query server pass through all dml statements without rewriting")
	flag.BoolVar(&Config.AllowUnsafeDMLs, "queryserver-config-allowunsafe-dmls", DefaultQsConfig.AllowUnsafeDMLs, "query server allow unsafe dml statements")

	flag.IntVar(&Config.StreamBufferSize, "queryserver-config-stream-buffer-size", DefaultQsConfig.StreamBufferSize, "query server stream buffer size, the maximum number of bytes sent from vttablet for each stream call. It's recommended to keep this value in sync with vtgate's stream_buffer_size.")
	flag.IntVar(&Config.QueryPlanCacheSize, "queryserver-config-query-cache-size", DefaultQsConfig.QueryPlanCacheSize, "query server query cache size, maximum number of queries to be cached. vttablet analyzes every incoming query and generate a query plan, these plans are being cached in a lru cache. This config controls the capacity of the lru cache.")
	flag.Float64Var(&Config.SchemaReloadTime, "queryserver-config-schema-reload-time", DefaultQsConfig.SchemaReloadTime, "query server schema reload time, how often vttablet reloads schemas from underlying MySQL instance in seconds. vttablet keeps table schemas in its own memory and periodically refreshes it from MySQL. This config controls the reload time.")
	flag.Float64Var(&Config.QueryTimeout, "queryserver-config-query-timeout", DefaultQsConfig.QueryTimeout, "query server query timeout (in seconds), this is the query timeout in vttablet side. If a query takes more than this timeout, it will be killed.")
	flag.Float64Var(&Config.QueryPoolTimeout, "queryserver-config-query-pool-timeout", DefaultQsConfig.QueryPoolTimeout, "query server query pool timeout (in seconds), it is how long vttablet waits for a connection from the query pool. If set to 0 (default) then the overall query timeout is used instead.")
	flag.Float64Var(&Config.TxPoolTimeout, "queryserver-config-txpool-timeout", DefaultQsConfig.TxPoolTimeout, "query server transaction pool timeout, it is how long vttablet waits if tx pool is full")
	flag.Float64Var(&Config.IdleTimeout, "queryserver-config-idle-timeout", DefaultQsConfig.IdleTimeout, "query server idle timeout (in seconds), vttablet manages various mysql connection pools. This config means if a connection has not been used in given idle timeout, this connection will be removed from pool. This effectively manages number of connection objects and optimize the pool performance.")
	flag.BoolVar(&Config.EnableFastPool, "queryserver-config-enable-fast-pool", DefaultQsConfig.EnableFastPool, "Enable the experimental fast pool.")
	flag.IntVar(&Config.QueryPoolMinActive, "queryserver-config-pool-min-active", DefaultQsConfig.QueryPoolMinActive, "(fast pool only) query server pool minimum active connections: will enforce at least this many active connections.")
	flag.IntVar(&Config.TxPoolMinActive, "queryserver-config-txpool-min-active", DefaultQsConfig.TxPoolMinActive, "(fast pool only) query server transaction pool minimum active connections: will enforce at least this many active connections")
	flag.IntVar(&Config.QueryPoolWaiterCap, "queryserver-config-query-pool-waiter-cap", DefaultQsConfig.QueryPoolWaiterCap, "query server query pool waiter limit, this is the maximum number of queries that can be queued waiting to get a connection")
	flag.IntVar(&Config.TxPoolWaiterCap, "queryserver-config-txpool-waiter-cap", DefaultQsConfig.TxPoolWaiterCap, "query server transaction pool waiter limit, this is the maximum number of transactions that can be queued waiting to get a connection")
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
	flag.IntVar(&Config.HotRowProtectionConcurrentTransactions, "hot_row_protection_concurrent_transactions", DefaultQsConfig.HotRowProtectionConcurrentTransactions, "Number of concurrent transactions let through to the txpool/MySQL for the same hot row. Should be > 1 to have enough 'ready' transactions in MySQL and benefit from a pipelining effect.")

	flag.BoolVar(&Config.EnableTransactionLimit, "enable_transaction_limit", DefaultQsConfig.EnableTransactionLimit, "If true, limit on number of transactions open at the same time will be enforced for all users. User trying to open a new transaction after exhausting their limit will receive an error immediately, regardless of whether there are available slots or not.")
	flag.BoolVar(&Config.EnableTransactionLimitDryRun, "enable_transaction_limit_dry_run", DefaultQsConfig.EnableTransactionLimitDryRun, "If true, limit on number of transactions open at the same time will be tracked for all users, but not enforced.")
	flag.Float64Var(&Config.TransactionLimitPerUser, "transaction_limit_per_user", DefaultQsConfig.TransactionLimitPerUser, "Maximum number of transactions a single user is allowed to use at any time, represented as fraction of -transaction_cap.")
	flag.BoolVar(&Config.TransactionLimitByUsername, "transaction_limit_by_username", DefaultQsConfig.TransactionLimitByUsername, "Include VTGateCallerID.username when considering who the user is for the purpose of transaction limit.")
	flag.BoolVar(&Config.TransactionLimitByPrincipal, "transaction_limit_by_principal", DefaultQsConfig.TransactionLimitByPrincipal, "Include CallerID.principal when considering who the user is for the purpose of transaction limit.")
	flag.BoolVar(&Config.TransactionLimitByComponent, "transaction_limit_by_component", DefaultQsConfig.TransactionLimitByComponent, "Include CallerID.component when considering who the user is for the purpose of transaction limit.")
	flag.BoolVar(&Config.TransactionLimitBySubcomponent, "transaction_limit_by_subcomponent", DefaultQsConfig.TransactionLimitBySubcomponent, "Include CallerID.subcomponent when considering who the user is for the purpose of transaction limit.")

	flag.BoolVar(&Config.HeartbeatEnable, "heartbeat_enable", DefaultQsConfig.HeartbeatEnable, "If true, vttablet records (if master) or checks (if replica) the current time of a replication heartbeat in the table _vt.heartbeat. The result is used to inform the serving state of the vttablet via healthchecks.")
	flag.DurationVar(&Config.HeartbeatInterval, "heartbeat_interval", DefaultQsConfig.HeartbeatInterval, "How frequently to read and write replication heartbeat.")

	flag.BoolVar(&Config.EnforceStrictTransTables, "enforce_strict_trans_tables", DefaultQsConfig.EnforceStrictTransTables, "If true, vttablet requires MySQL to run with STRICT_TRANS_TABLES or STRICT_ALL_TABLES on. It is recommended to not turn this flag off. Otherwise MySQL may alter your supplied values before saving them to the database.")
	flag.BoolVar(&Config.EnableConsolidator, "enable-consolidator", DefaultQsConfig.EnableConsolidator, "This option enables the query consolidator.")
}

// Init must be called after flag.Parse, and before doing any other operations.
func Init() {
	switch *streamlog.QueryLogFormat {
	case streamlog.QueryLogFormatText:
	case streamlog.QueryLogFormatJSON:
	default:
		log.Exitf("Invalid querylog-format value %v: must be either text or json", *streamlog.QueryLogFormat)
	}

	if *queryLogHandler != "" {
		StatsLogger.ServeLogs(*queryLogHandler, streamlog.GetFormatter(StatsLogger))
	}

	if *txLogHandler != "" {
		TxLogger.ServeLogs(*txLogHandler, streamlog.GetFormatter(TxLogger))
	}
}

// TabletConfig contains all the configuration for query service
type TabletConfig struct {
	PoolSize                int
	StreamPoolSize          int
	MessagePoolSize         int
	TransactionCap          int
	MessagePostponeCap      int
	FoundRowsPoolSize       int
	TransactionTimeout      float64
	TxShutDownGracePeriod   float64
	MaxResultSize           int
	WarnResultSize          int
	MaxDMLRows              int
	PassthroughDMLs         bool
	AllowUnsafeDMLs         bool
	StreamBufferSize        int
	QueryPlanCacheSize      int
	SchemaReloadTime        float64
	QueryTimeout            float64
	QueryPoolTimeout        float64
	TxPoolTimeout           float64
	IdleTimeout             float64
	EnableFastPool          bool
	QueryPoolMinActive      int
	TxPoolMinActive         int
	QueryPoolWaiterCap      int
	TxPoolWaiterCap         int
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

	EnableHotRowProtection                 bool
	EnableHotRowProtectionDryRun           bool
	HotRowProtectionMaxQueueSize           int
	HotRowProtectionMaxGlobalQueueSize     int
	HotRowProtectionConcurrentTransactions int

	TransactionLimitConfig

	HeartbeatEnable   bool
	HeartbeatInterval time.Duration

	EnforceStrictTransTables bool
	EnableConsolidator       bool
}

// TransactionLimitConfig captures configuration of transaction pool slots
// limiter configuration.
type TransactionLimitConfig struct {
	EnableTransactionLimit         bool
	EnableTransactionLimitDryRun   bool
	TransactionLimitPerUser        float64
	TransactionLimitByUsername     bool
	TransactionLimitByPrincipal    bool
	TransactionLimitByComponent    bool
	TransactionLimitBySubcomponent bool
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
	MessagePostponeCap:      4,
	FoundRowsPoolSize:       20,
	TransactionTimeout:      30,
	TxShutDownGracePeriod:   0,
	MaxResultSize:           10000,
	WarnResultSize:          0,
	MaxDMLRows:              500,
	PassthroughDMLs:         false,
	AllowUnsafeDMLs:         false,
	QueryPlanCacheSize:      5000,
	SchemaReloadTime:        30 * 60,
	QueryTimeout:            30,
	QueryPoolTimeout:        0,
	TxPoolTimeout:           1,
	IdleTimeout:             30 * 60,
	EnableFastPool:          false,
	QueryPoolMinActive:      0,
	TxPoolMinActive:         0,
	QueryPoolWaiterCap:      50000,
	TxPoolWaiterCap:         50000,
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
	// Allow more than 1 transaction for the same hot row through to have enough
	// of them ready in MySQL and profit from a pipelining effect.
	HotRowProtectionConcurrentTransactions: 5,

	TransactionLimitConfig: defaultTransactionLimitConfig(),

	HeartbeatEnable:   false,
	HeartbeatInterval: 1 * time.Second,

	EnforceStrictTransTables: true,
	EnableConsolidator:       true,
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

func defaultTransactionLimitConfig() TransactionLimitConfig {
	return TransactionLimitConfig{
		EnableTransactionLimit:       false,
		EnableTransactionLimitDryRun: false,

		// Single user can use up to 40% of transaction pool slots. Enough to
		// accommodate 2 misbehaving users.
		TransactionLimitPerUser: 0.4,

		TransactionLimitByUsername:     true,
		TransactionLimitByPrincipal:    true,
		TransactionLimitByComponent:    false,
		TransactionLimitBySubcomponent: false,
	}
}

// verifyTransactionLimitConfig checks TransactionLimitConfig for sanity
func (c *TabletConfig) verifyTransactionLimitConfig() error {
	actual, dryRun := c.EnableTransactionLimit, c.EnableTransactionLimitDryRun
	if actual && dryRun {
		return errors.New("only one of two flags allowed: -enable_transaction_limit or -enable_transaction_limit_dry_run")
	}

	// Skip other checks if this is not enabled
	if !actual && !dryRun {
		return nil
	}

	var (
		byUser      = c.TransactionLimitByUsername
		byPrincipal = c.TransactionLimitByPrincipal
		byComp      = c.TransactionLimitByComponent
		bySubcomp   = c.TransactionLimitBySubcomponent
	)
	if byAny := byUser || byPrincipal || byComp || bySubcomp; !byAny {
		return errors.New("no user discriminating fields selected for transaction limiter, everyone would share single chunk of transaction pool. Override with at least one of -transaction_limit_by flags set to true")
	}
	if v := c.TransactionLimitPerUser; v <= 0 || v >= 1 {
		return fmt.Errorf("-transaction_limit_per_user should be a fraction within range (0, 1) (specified value: %v)", v)
	}
	if limit := int(c.TransactionLimitPerUser * float64(c.TransactionCap)); limit == 0 {
		return fmt.Errorf("effective transaction limit per user is 0 due to rounding, increase -transaction_limit_per_user")
	}
	return nil
}

// Config contains all the current config values. It's read-only,
// except for tests.
var Config TabletConfig

// VerifyConfig checks "Config" for contradicting flags.
func VerifyConfig() error {
	if err := Config.verifyTransactionLimitConfig(); err != nil {
		return err
	}
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
	if v := Config.HotRowProtectionConcurrentTransactions; v <= 0 {
		return fmt.Errorf("-hot_row_protection_concurrent_transactions must be > 0 (specified value: %v)", v)
	}
	return nil
}

func (c *TabletConfig) PoolImpl() pools.Impl {
	if c.EnableFastPool {
		return pools.FastImpl
	} else {
		return pools.ResourceImpl
	}
}
