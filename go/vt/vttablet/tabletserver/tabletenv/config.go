/*
Copyright 2019 The Vitess Authors.

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
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/throttler"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	throttlerdatapb "vitess.io/vitess/go/vt/proto/throttlerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// These constants represent values for various config parameters.
const (
	Enable       = "enable"
	Disable      = "disable"
	Dryrun       = "dryRun"
	NotOnPrimary = "notOnPrimary"
	Polling      = "polling"
	Heartbeat    = "heartbeat"
)

var (
	currentConfig TabletConfig

	// TxLogger can be used to enable logging of transactions.
	// Call TxLogger.ServeLogs in your main program to enable logging.
	// The log format can be inferred by looking at TxConnection.Format.
	TxLogger = streamlog.New[any]("TxLog", 10)

	// StatsLogger is the main stream logger object
	StatsLogger = streamlog.New[*LogStats]("TabletServer", 50)

	// The following vars are used for custom initialization of Tabletconfig.
	enableHotRowProtection       bool
	enableHotRowProtectionDryRun bool
	enableConsolidator           bool
	enableConsolidatorReplicas   bool
	enableHeartbeat              bool
	heartbeatInterval            time.Duration
	heartbeatOnDemandDuration    time.Duration
	healthCheckInterval          time.Duration
	degradedThreshold            time.Duration
	unhealthyThreshold           time.Duration
	transitionGracePeriod        time.Duration
	enableReplicationReporter    bool
)

func init() {
	currentConfig = *NewDefaultConfig()
	currentConfig.DB = &dbconfigs.GlobalDBConfigs
	servenv.OnParseFor("vtcombo", registerTabletEnvFlags)
	servenv.OnParseFor("vttablet", registerTabletEnvFlags)
}

var (
	queryLogHandler = "/debug/querylog"
	txLogHandler    = "/debug/txlog"
)

type TxThrottlerConfigFlag struct {
	*throttlerdatapb.Configuration
}

func NewTxThrottlerConfigFlag() *TxThrottlerConfigFlag {
	return &TxThrottlerConfigFlag{&throttlerdatapb.Configuration{}}
}

func (t *TxThrottlerConfigFlag) Get() *throttlerdatapb.Configuration {
	return t.Configuration
}

func (t *TxThrottlerConfigFlag) Set(arg string) error {
	return prototext.Unmarshal([]byte(arg), t.Configuration)
}

func (t *TxThrottlerConfigFlag) Type() string { return "string" }

// RegisterTabletEnvFlags is a public API to register tabletenv flags for use by test cases that expect
// some flags to be set with default values
func RegisterTabletEnvFlags(fs *pflag.FlagSet) {
	registerTabletEnvFlags(fs)
}

func registerTabletEnvFlags(fs *pflag.FlagSet) {
	fs.StringVar(&queryLogHandler, "query-log-stream-handler", queryLogHandler, "URL handler for streaming queries log")
	fs.StringVar(&txLogHandler, "transaction-log-stream-handler", txLogHandler, "URL handler for streaming transactions log")

	fs.IntVar(&currentConfig.OltpReadPool.Size, "queryserver-config-pool-size", defaultConfig.OltpReadPool.Size, "query server read pool size, connection pool is used by regular queries (non streaming, not in a transaction)")
	fs.IntVar(&currentConfig.OlapReadPool.Size, "queryserver-config-stream-pool-size", defaultConfig.OlapReadPool.Size, "query server stream connection pool size, stream pool is used by stream queries: queries that return results to client in a streaming fashion")
	fs.IntVar(&currentConfig.TxPool.Size, "queryserver-config-transaction-cap", defaultConfig.TxPool.Size, "query server transaction cap is the maximum number of transactions allowed to happen at any given point of a time for a single vttablet. E.g. by setting transaction cap to 100, there are at most 100 transactions will be processed by a vttablet and the 101th transaction will be blocked (and fail if it cannot get connection within specified timeout)")
	fs.IntVar(&currentConfig.MessagePostponeParallelism, "queryserver-config-message-postpone-cap", defaultConfig.MessagePostponeParallelism, "query server message postpone cap is the maximum number of messages that can be postponed at any given time. Set this number to substantially lower than transaction cap, so that the transaction pool isn't exhausted by the message subsystem.")
	fs.DurationVar(&currentConfig.Oltp.TxTimeout, "queryserver-config-transaction-timeout", defaultConfig.Oltp.TxTimeout, "query server transaction timeout, a transaction will be killed if it takes longer than this value")
	fs.DurationVar(&currentConfig.GracePeriods.Shutdown, "shutdown_grace_period", defaultConfig.GracePeriods.Shutdown, "how long to wait for queries and transactions to complete during graceful shutdown.")
	fs.IntVar(&currentConfig.Oltp.MaxRows, "queryserver-config-max-result-size", defaultConfig.Oltp.MaxRows, "query server max result size, maximum number of rows allowed to return from vttablet for non-streaming queries.")
	fs.IntVar(&currentConfig.Oltp.WarnRows, "queryserver-config-warn-result-size", defaultConfig.Oltp.WarnRows, "query server result size warning threshold, warn if number of rows returned from vttablet for non-streaming queries exceeds this")
	fs.BoolVar(&currentConfig.PassthroughDML, "queryserver-config-passthrough-dmls", defaultConfig.PassthroughDML, "query server pass through all dml statements without rewriting")

	fs.IntVar(&currentConfig.StreamBufferSize, "queryserver-config-stream-buffer-size", defaultConfig.StreamBufferSize, "query server stream buffer size, the maximum number of bytes sent from vttablet for each stream call. It's recommended to keep this value in sync with vtgate's stream_buffer_size.")

	fs.Int64Var(&currentConfig.QueryCacheMemory, "queryserver-config-query-cache-memory", defaultConfig.QueryCacheMemory, "query server query cache size in bytes, maximum amount of memory to be used for caching. vttablet analyzes every incoming query and generate a query plan, these plans are being cached in a lru cache. This config controls the capacity of the lru cache.")

	fs.DurationVar(&currentConfig.SchemaReloadInterval, "queryserver-config-schema-reload-time", defaultConfig.SchemaReloadInterval, "query server schema reload time, how often vttablet reloads schemas from underlying MySQL instance. vttablet keeps table schemas in its own memory and periodically refreshes it from MySQL. This config controls the reload time.")
	fs.DurationVar(&currentConfig.SchemaChangeReloadTimeout, "schema-change-reload-timeout", defaultConfig.SchemaChangeReloadTimeout, "query server schema change reload timeout, this is how long to wait for the signaled schema reload operation to complete before giving up")
	fs.BoolVar(&currentConfig.SignalWhenSchemaChange, "queryserver-config-schema-change-signal", defaultConfig.SignalWhenSchemaChange, "query server schema signal, will signal connected vtgates that schema has changed whenever this is detected. VTGates will need to have -schema_change_signal enabled for this to work")
	fs.DurationVar(&currentConfig.Olap.TxTimeout, "queryserver-config-olap-transaction-timeout", defaultConfig.Olap.TxTimeout, "query server transaction timeout (in seconds), after which a transaction in an OLAP session will be killed")
	fs.DurationVar(&currentConfig.Oltp.QueryTimeout, "queryserver-config-query-timeout", defaultConfig.Oltp.QueryTimeout, "query server query timeout, this is the query timeout in vttablet side. If a query takes more than this timeout, it will be killed.")
	fs.DurationVar(&currentConfig.OltpReadPool.Timeout, "queryserver-config-query-pool-timeout", defaultConfig.OltpReadPool.Timeout, "query server query pool timeout, it is how long vttablet waits for a connection from the query pool. If set to 0 (default) then the overall query timeout is used instead.")
	fs.DurationVar(&currentConfig.OlapReadPool.Timeout, "queryserver-config-stream-pool-timeout", defaultConfig.OlapReadPool.Timeout, "query server stream pool timeout, it is how long vttablet waits for a connection from the stream pool. If set to 0 (default) then there is no timeout.")
	fs.DurationVar(&currentConfig.TxPool.Timeout, "queryserver-config-txpool-timeout", defaultConfig.TxPool.Timeout, "query server transaction pool timeout, it is how long vttablet waits if tx pool is full")
	fs.DurationVar(&currentConfig.OltpReadPool.IdleTimeout, "queryserver-config-idle-timeout", defaultConfig.OltpReadPool.IdleTimeout, "query server idle timeout, vttablet manages various mysql connection pools. This config means if a connection has not been used in given idle timeout, this connection will be removed from pool. This effectively manages number of connection objects and optimize the pool performance.")
	fs.DurationVar(&currentConfig.OltpReadPool.MaxLifetime, "queryserver-config-pool-conn-max-lifetime", defaultConfig.OltpReadPool.MaxLifetime, "query server connection max lifetime, vttablet manages various mysql connection pools. This config means if a connection has lived at least this long, it connection will be removed from pool upon the next time it is returned to the pool.")

	var unused int
	fs.IntVar(&unused, "queryserver-config-query-pool-waiter-cap", 0, "query server query pool waiter limit, this is the maximum number of queries that can be queued waiting to get a connection")
	fs.IntVar(&unused, "queryserver-config-stream-pool-waiter-cap", 0, "query server stream pool waiter limit, this is the maximum number of streaming queries that can be queued waiting to get a connection")
	fs.IntVar(&unused, "queryserver-config-txpool-waiter-cap", 0, "query server transaction pool waiter limit, this is the maximum number of transactions that can be queued waiting to get a connection")
	fs.MarkDeprecated("queryserver-config-query-pool-waiter-cap", "The new connection pool in v19 does not limit waiter capacity. This flag will be removed in a future release.")
	fs.MarkDeprecated("queryserver-config-stream-pool-waiter-cap", "The new connection pool in v19 does not limit waiter capacity. This flag will be removed in a future release.")
	fs.MarkDeprecated("queryserver-config-txpool-waiter-cap", "The new connection pool in v19 does not limit waiter capacity. This flag will be removed in a future release.")

	// tableacl related configurations.
	fs.BoolVar(&currentConfig.StrictTableACL, "queryserver-config-strict-table-acl", defaultConfig.StrictTableACL, "only allow queries that pass table acl checks")
	fs.BoolVar(&currentConfig.EnableTableACLDryRun, "queryserver-config-enable-table-acl-dry-run", defaultConfig.EnableTableACLDryRun, "If this flag is enabled, tabletserver will emit monitoring metrics and let the request pass regardless of table acl check results")
	fs.StringVar(&currentConfig.TableACLExemptACL, "queryserver-config-acl-exempt-acl", defaultConfig.TableACLExemptACL, "an acl that exempt from table acl checking (this acl is free to access any vitess tables).")
	fs.BoolVar(&currentConfig.TerseErrors, "queryserver-config-terse-errors", defaultConfig.TerseErrors, "prevent bind vars from escaping in client error messages")
	fs.IntVar(&currentConfig.TruncateErrorLen, "queryserver-config-truncate-error-len", defaultConfig.TruncateErrorLen, "truncate errors sent to client if they are longer than this value (0 means do not truncate)")
	fs.BoolVar(&currentConfig.AnnotateQueries, "queryserver-config-annotate-queries", defaultConfig.AnnotateQueries, "prefix queries to MySQL backend with comment indicating vtgate principal (user) and target tablet type")
	fs.BoolVar(&currentConfig.WatchReplication, "watch_replication_stream", false, "When enabled, vttablet will stream the MySQL replication stream from the local server, and use it to update schema when it sees a DDL.")
	fs.BoolVar(&currentConfig.TrackSchemaVersions, "track_schema_versions", false, "When enabled, vttablet will store versions of schemas at each position that a DDL is applied and allow retrieval of the schema corresponding to a position")
	fs.Int64Var(&currentConfig.SchemaVersionMaxAgeSeconds, "schema-version-max-age-seconds", 0, "max age of schema version records to kept in memory by the vreplication historian")
	fs.BoolVar(&currentConfig.TwoPCEnable, "twopc_enable", defaultConfig.TwoPCEnable, "if the flag is on, 2pc is enabled. Other 2pc flags must be supplied.")
	fs.StringVar(&currentConfig.TwoPCCoordinatorAddress, "twopc_coordinator_address", defaultConfig.TwoPCCoordinatorAddress, "address of the (VTGate) process(es) that will be used to notify of abandoned transactions.")
	SecondsVar(fs, &currentConfig.TwoPCAbandonAge, "twopc_abandon_age", defaultConfig.TwoPCAbandonAge, "time in seconds. Any unresolved transaction older than this time will be sent to the coordinator to be resolved.")
	// Tx throttler config
	flagutil.DualFormatBoolVar(fs, &currentConfig.EnableTxThrottler, "enable_tx_throttler", defaultConfig.EnableTxThrottler, "If true replication-lag-based throttling on transactions will be enabled.")
	flagutil.DualFormatVar(fs, currentConfig.TxThrottlerConfig, "tx_throttler_config", "The configuration of the transaction throttler as a text-formatted throttlerdata.Configuration protocol buffer message.")
	flagutil.DualFormatStringListVar(fs, &currentConfig.TxThrottlerHealthCheckCells, "tx_throttler_healthcheck_cells", defaultConfig.TxThrottlerHealthCheckCells, "A comma-separated list of cells. Only tabletservers running in these cells will be monitored for replication lag by the transaction throttler.")
	fs.IntVar(&currentConfig.TxThrottlerDefaultPriority, "tx-throttler-default-priority", defaultConfig.TxThrottlerDefaultPriority, "Default priority assigned to queries that lack priority information")
	fs.Var(currentConfig.TxThrottlerTabletTypes, "tx-throttler-tablet-types", "A comma-separated list of tablet types. Only tablets of this type are monitored for replication lag by the transaction throttler. Supported types are replica and/or rdonly.")
	fs.BoolVar(&currentConfig.TxThrottlerDryRun, "tx-throttler-dry-run", defaultConfig.TxThrottlerDryRun, "If present, the transaction throttler only records metrics about requests received and throttled, but does not actually throttle any requests.")
	fs.DurationVar(&currentConfig.TxThrottlerTopoRefreshInterval, "tx-throttler-topo-refresh-interval", time.Minute*5, "The rate that the transaction throttler will refresh the topology to find cells.")

	fs.BoolVar(&enableHotRowProtection, "enable_hot_row_protection", false, "If true, incoming transactions for the same row (range) will be queued and cannot consume all txpool slots.")
	fs.BoolVar(&enableHotRowProtectionDryRun, "enable_hot_row_protection_dry_run", false, "If true, hot row protection is not enforced but logs if transactions would have been queued.")
	fs.IntVar(&currentConfig.HotRowProtection.MaxQueueSize, "hot_row_protection_max_queue_size", defaultConfig.HotRowProtection.MaxQueueSize, "Maximum number of BeginExecute RPCs which will be queued for the same row (range).")
	fs.IntVar(&currentConfig.HotRowProtection.MaxGlobalQueueSize, "hot_row_protection_max_global_queue_size", defaultConfig.HotRowProtection.MaxGlobalQueueSize, "Global queue limit across all row (ranges). Useful to prevent that the queue can grow unbounded.")
	fs.IntVar(&currentConfig.HotRowProtection.MaxConcurrency, "hot_row_protection_concurrent_transactions", defaultConfig.HotRowProtection.MaxConcurrency, "Number of concurrent transactions let through to the txpool/MySQL for the same hot row. Should be > 1 to have enough 'ready' transactions in MySQL and benefit from a pipelining effect.")

	fs.BoolVar(&currentConfig.EnableTransactionLimit, "enable_transaction_limit", defaultConfig.EnableTransactionLimit, "If true, limit on number of transactions open at the same time will be enforced for all users. User trying to open a new transaction after exhausting their limit will receive an error immediately, regardless of whether there are available slots or not.")
	fs.BoolVar(&currentConfig.EnableTransactionLimitDryRun, "enable_transaction_limit_dry_run", defaultConfig.EnableTransactionLimitDryRun, "If true, limit on number of transactions open at the same time will be tracked for all users, but not enforced.")
	fs.Float64Var(&currentConfig.TransactionLimitPerUser, "transaction_limit_per_user", defaultConfig.TransactionLimitPerUser, "Maximum number of transactions a single user is allowed to use at any time, represented as fraction of -transaction_cap.")
	fs.BoolVar(&currentConfig.TransactionLimitByUsername, "transaction_limit_by_username", defaultConfig.TransactionLimitByUsername, "Include VTGateCallerID.username when considering who the user is for the purpose of transaction limit.")
	fs.BoolVar(&currentConfig.TransactionLimitByPrincipal, "transaction_limit_by_principal", defaultConfig.TransactionLimitByPrincipal, "Include CallerID.principal when considering who the user is for the purpose of transaction limit.")
	fs.BoolVar(&currentConfig.TransactionLimitByComponent, "transaction_limit_by_component", defaultConfig.TransactionLimitByComponent, "Include CallerID.component when considering who the user is for the purpose of transaction limit.")
	fs.BoolVar(&currentConfig.TransactionLimitBySubcomponent, "transaction_limit_by_subcomponent", defaultConfig.TransactionLimitBySubcomponent, "Include CallerID.subcomponent when considering who the user is for the purpose of transaction limit.")

	fs.BoolVar(&enableHeartbeat, "heartbeat_enable", false, "If true, vttablet records (if master) or checks (if replica) the current time of a replication heartbeat in the sidecar database's heartbeat table. The result is used to inform the serving state of the vttablet via healthchecks.")
	fs.DurationVar(&heartbeatInterval, "heartbeat_interval", 1*time.Second, "How frequently to read and write replication heartbeat.")
	fs.DurationVar(&heartbeatOnDemandDuration, "heartbeat_on_demand_duration", 0, "If non-zero, heartbeats are only written upon consumer request, and only run for up to given duration following the request. Frequent requests can keep the heartbeat running consistently; when requests are infrequent heartbeat may completely stop between requests")

	fs.BoolVar(&currentConfig.EnforceStrictTransTables, "enforce_strict_trans_tables", defaultConfig.EnforceStrictTransTables, "If true, vttablet requires MySQL to run with STRICT_TRANS_TABLES or STRICT_ALL_TABLES on. It is recommended to not turn this flag off. Otherwise MySQL may alter your supplied values before saving them to the database.")
	flagutil.DualFormatBoolVar(fs, &enableConsolidator, "enable_consolidator", true, "This option enables the query consolidator.")
	flagutil.DualFormatBoolVar(fs, &enableConsolidatorReplicas, "enable_consolidator_replicas", false, "This option enables the query consolidator only on replicas.")
	fs.Int64Var(&currentConfig.ConsolidatorStreamQuerySize, "consolidator-stream-query-size", defaultConfig.ConsolidatorStreamQuerySize, "Configure the stream consolidator query size in bytes. Setting to 0 disables the stream consolidator.")
	fs.Int64Var(&currentConfig.ConsolidatorStreamTotalSize, "consolidator-stream-total-size", defaultConfig.ConsolidatorStreamTotalSize, "Configure the stream consolidator total size in bytes. Setting to 0 disables the stream consolidator.")

	fs.DurationVar(&healthCheckInterval, "health_check_interval", defaultConfig.Healthcheck.Interval, "Interval between health checks")
	fs.DurationVar(&degradedThreshold, "degraded_threshold", defaultConfig.Healthcheck.DegradedThreshold, "replication lag after which a replica is considered degraded")
	fs.DurationVar(&unhealthyThreshold, "unhealthy_threshold", defaultConfig.Healthcheck.UnhealthyThreshold, "replication lag after which a replica is considered unhealthy")
	fs.DurationVar(&transitionGracePeriod, "serving_state_grace_period", 0, "how long to pause after broadcasting health to vtgate, before enforcing a new serving state")

	fs.BoolVar(&enableReplicationReporter, "enable_replication_reporter", false, "Use polling to track replication lag.")
	fs.BoolVar(&currentConfig.EnableOnlineDDL, "queryserver_enable_online_ddl", true, "Enable online DDL.")
	fs.BoolVar(&currentConfig.SanitizeLogMessages, "sanitize_log_messages", false, "Remove potentially sensitive information in tablet INFO, WARNING, and ERROR log messages such as query parameters.")
	fs.BoolVar(&currentConfig.EnableSettingsPool, "queryserver-enable-settings-pool", true, "Enable pooling of connections with modified system settings")

	fs.Int64Var(&currentConfig.RowStreamer.MaxInnoDBTrxHistLen, "vreplication_copy_phase_max_innodb_history_list_length", 1000000, "The maximum InnoDB transaction history that can exist on a vstreamer (source) before starting another round of copying rows. This helps to limit the impact on the source tablet.")
	fs.Int64Var(&currentConfig.RowStreamer.MaxMySQLReplLagSecs, "vreplication_copy_phase_max_mysql_replication_lag", 43200, "The maximum MySQL replication lag (in seconds) that can exist on a vstreamer (source) before starting another round of copying rows. This helps to limit the impact on the source tablet.")

	fs.BoolVar(&currentConfig.EnableViews, "queryserver-enable-views", false, "Enable views support in vttablet.")

	fs.BoolVar(&currentConfig.EnablePerWorkloadTableMetrics, "enable-per-workload-table-metrics", defaultConfig.EnablePerWorkloadTableMetrics, "If true, query counts and query error metrics include a label that identifies the workload")
}

var (
	queryLogHandlerOnce sync.Once
	txLogHandlerOnce    sync.Once
)

// Init must be called after flag.Parse, and before doing any other operations.
func Init() {
	// IdleTimeout is only initialized for OltpReadPool , but the other pools need to inherit the value.
	// TODO(sougou): Make a decision on whether this should be global or per-pool.
	currentConfig.OlapReadPool.IdleTimeout = currentConfig.OltpReadPool.IdleTimeout
	currentConfig.TxPool.IdleTimeout = currentConfig.OltpReadPool.IdleTimeout
	currentConfig.OlapReadPool.MaxLifetime = currentConfig.OltpReadPool.MaxLifetime
	currentConfig.TxPool.MaxLifetime = currentConfig.OltpReadPool.MaxLifetime

	if enableHotRowProtection {
		if enableHotRowProtectionDryRun {
			currentConfig.HotRowProtection.Mode = Dryrun
		} else {
			currentConfig.HotRowProtection.Mode = Enable
		}
	} else {
		currentConfig.HotRowProtection.Mode = Disable
	}

	switch {
	case enableConsolidatorReplicas:
		currentConfig.Consolidator = NotOnPrimary
	case enableConsolidator:
		currentConfig.Consolidator = Enable
	default:
		currentConfig.Consolidator = Disable
	}

	if heartbeatInterval == 0 {
		heartbeatInterval = defaultConfig.ReplicationTracker.HeartbeatInterval
	}
	if heartbeatInterval > time.Second {
		heartbeatInterval = time.Second
	}
	if heartbeatOnDemandDuration < 0 {
		heartbeatOnDemandDuration = 0
	}
	currentConfig.ReplicationTracker.HeartbeatInterval = heartbeatInterval
	currentConfig.ReplicationTracker.HeartbeatOnDemand = heartbeatOnDemandDuration

	switch {
	case enableHeartbeat:
		currentConfig.ReplicationTracker.Mode = Heartbeat
	case enableReplicationReporter:
		currentConfig.ReplicationTracker.Mode = Polling
	default:
		currentConfig.ReplicationTracker.Mode = Disable
	}

	currentConfig.Healthcheck.Interval = healthCheckInterval
	currentConfig.Healthcheck.DegradedThreshold = degradedThreshold
	currentConfig.Healthcheck.UnhealthyThreshold = unhealthyThreshold
	currentConfig.GracePeriods.Transition = transitionGracePeriod

	switch streamlog.GetQueryLogFormat() {
	case streamlog.QueryLogFormatText:
	case streamlog.QueryLogFormatJSON:
	default:
		log.Exitf("Invalid querylog-format value %v: must be either text or json", streamlog.GetQueryLogFormat())
	}

	if queryLogHandler != "" {
		queryLogHandlerOnce.Do(func() {
			StatsLogger.ServeLogs(queryLogHandler, streamlog.GetFormatter(StatsLogger))
		})
	}

	if txLogHandler != "" {
		txLogHandlerOnce.Do(func() {
			TxLogger.ServeLogs(txLogHandler, streamlog.GetFormatter(TxLogger))
		})
	}
}

// TabletConfig contains all the configuration for query service
type TabletConfig struct {
	DB *dbconfigs.DBConfigs `json:"db,omitempty"`

	OltpReadPool ConnPoolConfig `json:"oltpReadPool,omitempty"`
	OlapReadPool ConnPoolConfig `json:"olapReadPool,omitempty"`
	TxPool       ConnPoolConfig `json:"txPool,omitempty"`

	Olap             OlapConfig             `json:"olap,omitempty"`
	Oltp             OltpConfig             `json:"oltp,omitempty"`
	HotRowProtection HotRowProtectionConfig `json:"hotRowProtection,omitempty"`

	Healthcheck  HealthcheckConfig  `json:"healthcheck,omitempty"`
	GracePeriods GracePeriodsConfig `json:"gracePeriods,omitempty"`

	ReplicationTracker ReplicationTrackerConfig `json:"replicationTracker,omitempty"`

	// Consolidator can be enable, disable, or notOnPrimary. Default is enable.
	Consolidator                     string        `json:"consolidator,omitempty"`
	PassthroughDML                   bool          `json:"passthroughDML,omitempty"`
	StreamBufferSize                 int           `json:"streamBufferSize,omitempty"`
	ConsolidatorStreamTotalSize      int64         `json:"consolidatorStreamTotalSize,omitempty"`
	ConsolidatorStreamQuerySize      int64         `json:"consolidatorStreamQuerySize,omitempty"`
	QueryCacheMemory                 int64         `json:"queryCacheMemory,omitempty"`
	QueryCacheDoorkeeper             bool          `json:"queryCacheDoorkeeper,omitempty"`
	SchemaReloadInterval             time.Duration `json:"schemaReloadIntervalSeconds,omitempty"`
	SignalSchemaChangeReloadInterval time.Duration `json:"signalSchemaChangeReloadIntervalSeconds,omitempty"`
	SchemaChangeReloadTimeout        time.Duration `json:"schemaChangeReloadTimeout,omitempty"`
	WatchReplication                 bool          `json:"watchReplication,omitempty"`
	TrackSchemaVersions              bool          `json:"trackSchemaVersions,omitempty"`
	SchemaVersionMaxAgeSeconds       int64         `json:"schemaVersionMaxAgeSeconds,omitempty"`
	TerseErrors                      bool          `json:"terseErrors,omitempty"`
	TruncateErrorLen                 int           `json:"truncateErrorLen,omitempty"`
	AnnotateQueries                  bool          `json:"annotateQueries,omitempty"`
	MessagePostponeParallelism       int           `json:"messagePostponeParallelism,omitempty"`
	SignalWhenSchemaChange           bool          `json:"signalWhenSchemaChange,omitempty"`

	ExternalConnections map[string]*dbconfigs.DBConfigs `json:"externalConnections,omitempty"`

	SanitizeLogMessages     bool    `json:"-"`
	StrictTableACL          bool    `json:"-"`
	EnableTableACLDryRun    bool    `json:"-"`
	TableACLExemptACL       string  `json:"-"`
	TwoPCEnable             bool    `json:"-"`
	TwoPCCoordinatorAddress string  `json:"-"`
	TwoPCAbandonAge         Seconds `json:"-"`

	EnableTxThrottler              bool                          `json:"-"`
	TxThrottlerConfig              *TxThrottlerConfigFlag        `json:"-"`
	TxThrottlerHealthCheckCells    []string                      `json:"-"`
	TxThrottlerDefaultPriority     int                           `json:"-"`
	TxThrottlerTabletTypes         *topoproto.TabletTypeListFlag `json:"-"`
	TxThrottlerTopoRefreshInterval time.Duration                 `json:"-"`
	TxThrottlerDryRun              bool                          `json:"-"`

	EnableTableGC bool `json:"-"` // can be turned off programmatically by tests

	TransactionLimitConfig `json:"-"`

	EnforceStrictTransTables bool `json:"-"`
	EnableOnlineDDL          bool `json:"-"`
	EnableSettingsPool       bool `json:"-"`

	RowStreamer RowStreamerConfig `json:"rowStreamer,omitempty"`

	EnableViews bool `json:"-"`

	EnablePerWorkloadTableMetrics bool `json:"-"`
}

func (cfg *TabletConfig) MarshalJSON() ([]byte, error) {
	type TCProxy TabletConfig

	tmp := struct {
		TCProxy
		SchemaReloadInterval             string `json:"schemaReloadIntervalSeconds,omitempty"`
		SignalSchemaChangeReloadInterval string `json:"signalSchemaChangeReloadIntervalSeconds,omitempty"`
		SchemaChangeReloadTimeout        string `json:"schemaChangeReloadTimeout,omitempty"`
	}{
		TCProxy: TCProxy(*cfg),
	}

	if d := cfg.SchemaReloadInterval; d != 0 {
		tmp.SchemaReloadInterval = d.String()
	}

	if d := cfg.SignalSchemaChangeReloadInterval; d != 0 {
		tmp.SignalSchemaChangeReloadInterval = d.String()
	}

	if d := cfg.SchemaChangeReloadTimeout; d != 0 {
		tmp.SchemaChangeReloadTimeout = d.String()
	}

	return json.Marshal(&tmp)
}

func (cfg *TabletConfig) UnmarshalJSON(data []byte) (err error) {
	type TCProxy TabletConfig

	var tmp struct {
		TCProxy
		SchemaReloadInterval             string `json:"schemaReloadIntervalSeconds,omitempty"`
		SignalSchemaChangeReloadInterval string `json:"signalSchemaChangeReloadIntervalSeconds,omitempty"`
		SchemaChangeReloadTimeout        string `json:"schemaChangeReloadTimeout,omitempty"`
	}

	tmp.TCProxy = TCProxy(*cfg)

	if err = json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	*cfg = TabletConfig(tmp.TCProxy)

	if tmp.SchemaReloadInterval != "" {
		cfg.SchemaReloadInterval, err = time.ParseDuration(tmp.SchemaReloadInterval)
		if err != nil {
			return err
		}
	} else {
		cfg.SchemaReloadInterval = 0
	}

	if tmp.SignalSchemaChangeReloadInterval != "" {
		cfg.SignalSchemaChangeReloadInterval, err = time.ParseDuration(tmp.SignalSchemaChangeReloadInterval)
		if err != nil {
			return err
		}
	} else {
		cfg.SignalSchemaChangeReloadInterval = 0
	}

	if tmp.SchemaChangeReloadTimeout != "" {
		cfg.SchemaChangeReloadTimeout, err = time.ParseDuration(tmp.SchemaChangeReloadTimeout)
		if err != nil {
			return err
		}
	} else {
		cfg.SchemaChangeReloadTimeout = 0
	}

	return nil
}

// ConnPoolConfig contains the config for a conn pool.
type ConnPoolConfig struct {
	Size               int           `json:"size,omitempty"`
	Timeout            time.Duration `json:"timeoutSeconds,omitempty"`
	IdleTimeout        time.Duration `json:"idleTimeoutSeconds,omitempty"`
	MaxLifetime        time.Duration `json:"maxLifetimeSeconds,omitempty"`
	PrefillParallelism int           `json:"prefillParallelism,omitempty"`
}

func (cfg *ConnPoolConfig) MarshalJSON() ([]byte, error) {
	type Proxy ConnPoolConfig

	tmp := struct {
		Proxy
		Timeout     string `json:"timeoutSeconds,omitempty"`
		IdleTimeout string `json:"idleTimeoutSeconds,omitempty"`
		MaxLifetime string `json:"maxLifetimeSeconds,omitempty"`
	}{
		Proxy: Proxy(*cfg),
	}

	if d := cfg.Timeout; d != 0 {
		tmp.Timeout = d.String()
	}

	if d := cfg.IdleTimeout; d != 0 {
		tmp.IdleTimeout = d.String()
	}

	if d := cfg.MaxLifetime; d != 0 {
		tmp.MaxLifetime = d.String()
	}

	return json.Marshal(&tmp)
}

func (cfg *ConnPoolConfig) UnmarshalJSON(data []byte) (err error) {
	var tmp struct {
		Size               int    `json:"size,omitempty"`
		Timeout            string `json:"timeoutSeconds,omitempty"`
		IdleTimeout        string `json:"idleTimeoutSeconds,omitempty"`
		MaxLifetime        string `json:"maxLifetimeSeconds,omitempty"`
		PrefillParallelism int    `json:"prefillParallelism,omitempty"`
	}

	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	if tmp.Timeout != "" {
		cfg.Timeout, err = time.ParseDuration(tmp.Timeout)
		if err != nil {
			return err
		}
	}

	if tmp.IdleTimeout != "" {
		cfg.IdleTimeout, err = time.ParseDuration(tmp.IdleTimeout)
		if err != nil {
			return err
		}
	}

	if tmp.MaxLifetime != "" {
		cfg.MaxLifetime, err = time.ParseDuration(tmp.MaxLifetime)
		if err != nil {
			return err
		}
	}

	cfg.Size = tmp.Size
	cfg.PrefillParallelism = tmp.PrefillParallelism

	return nil
}

// OlapConfig contains the config for olap settings.
type OlapConfig struct {
	TxTimeout time.Duration `json:"txTimeoutSeconds,omitempty"`
}

func (cfg *OlapConfig) MarshalJSON() ([]byte, error) {
	type Proxy OlapConfig

	tmp := struct {
		Proxy
		TxTimeoutSeconds string `json:"txTimeoutSeconds,omitempty"`
	}{
		Proxy: Proxy(*cfg),
	}

	if d := cfg.TxTimeout; d != 0 {
		tmp.TxTimeoutSeconds = d.String()
	}

	return json.Marshal(&tmp)
}

func (cfg *OlapConfig) UnmarshalJSON(data []byte) (err error) {
	var tmp struct {
		TxTimeout string `json:"txTimeoutSeconds,omitempty"`
	}

	if err = json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	if tmp.TxTimeout != "" {
		cfg.TxTimeout, err = time.ParseDuration(tmp.TxTimeout)
		if err != nil {
			return err
		}
	}

	return nil
}

// OltpConfig contains the config for oltp settings.
type OltpConfig struct {
	QueryTimeout time.Duration `json:"queryTimeoutSeconds,omitempty"`
	TxTimeout    time.Duration `json:"txTimeoutSeconds,omitempty"`
	MaxRows      int           `json:"maxRows,omitempty"`
	WarnRows     int           `json:"warnRows,omitempty"`
}

func (cfg *OltpConfig) MarshalJSON() ([]byte, error) {
	type Proxy OltpConfig

	tmp := struct {
		Proxy
		QueryTimeout string `json:"queryTimeoutSeconds,omitempty"`
		TxTimeout    string `json:"txTimeoutSeconds,omitempty"`
	}{
		Proxy: Proxy(*cfg),
	}

	if d := cfg.QueryTimeout; d != 0 {
		tmp.QueryTimeout = d.String()
	}

	if d := cfg.TxTimeout; d != 0 {
		tmp.TxTimeout = d.String()
	}

	return json.Marshal(&tmp)
}

func (cfg *OltpConfig) UnmarshalJSON(data []byte) (err error) {
	var tmp struct {
		OltpConfig
		QueryTimeout string `json:"queryTimeoutSeconds,omitempty"`
		TxTimeout    string `json:"txTimeoutSeconds,omitempty"`
	}

	if err = json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	if tmp.QueryTimeout != "" {
		cfg.QueryTimeout, err = time.ParseDuration(tmp.QueryTimeout)
		if err != nil {
			return err
		}
	}

	if tmp.TxTimeout != "" {
		cfg.TxTimeout, err = time.ParseDuration(tmp.TxTimeout)
		if err != nil {
			return err
		}
	}

	return nil
}

// HotRowProtectionConfig contains the config for hot row protection.
type HotRowProtectionConfig struct {
	// Mode can be disable, dryRun or enable. Default is disable.
	Mode               string `json:"mode,omitempty"`
	MaxQueueSize       int    `json:"maxQueueSize,omitempty"`
	MaxGlobalQueueSize int    `json:"maxGlobalQueueSize,omitempty"`
	MaxConcurrency     int    `json:"maxConcurrency,omitempty"`
}

// HealthcheckConfig contains the config for healthcheck.
type HealthcheckConfig struct {
	Interval           time.Duration
	DegradedThreshold  time.Duration
	UnhealthyThreshold time.Duration
}

func (cfg *HealthcheckConfig) MarshalJSON() ([]byte, error) {
	var tmp struct {
		IntervalSeconds           string `json:"intervalSeconds,omitempty"`
		DegradedThresholdSeconds  string `json:"degradedThresholdSeconds,omitempty"`
		UnhealthyThresholdSeconds string `json:"unhealthyThresholdSeconds,omitempty"`
	}

	if d := cfg.Interval; d != 0 {
		tmp.IntervalSeconds = d.String()
	}

	if d := cfg.DegradedThreshold; d != 0 {
		tmp.DegradedThresholdSeconds = d.String()
	}

	if d := cfg.UnhealthyThreshold; d != 0 {
		tmp.UnhealthyThresholdSeconds = d.String()
	}

	return json.Marshal(&tmp)
}

func (cfg *HealthcheckConfig) UnmarshalJSON(data []byte) (err error) {
	var tmp struct {
		Interval           string `json:"intervalSeconds,omitempty"`
		DegradedThreshold  string `json:"degradedThresholdSeconds,omitempty"`
		UnhealthyThreshold string `json:"unhealthyThresholdSeconds,omitempty"`
	}

	if err = json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	if tmp.Interval != "" {
		cfg.Interval, err = time.ParseDuration(tmp.Interval)
		if err != nil {
			return err
		}
	}

	if tmp.DegradedThreshold != "" {
		cfg.DegradedThreshold, err = time.ParseDuration(tmp.DegradedThreshold)
		if err != nil {
			return err
		}
	}

	if tmp.UnhealthyThreshold != "" {
		cfg.UnhealthyThreshold, err = time.ParseDuration(tmp.UnhealthyThreshold)
		if err != nil {
			return err
		}
	}

	return nil
}

// GracePeriodsConfig contains various grace periods.
// TODO(sougou): move lameduck here?
type GracePeriodsConfig struct {
	Shutdown   time.Duration
	Transition time.Duration
}

func (cfg *GracePeriodsConfig) MarshalJSON() ([]byte, error) {
	var tmp struct {
		ShutdownSeconds   string `json:"shutdownSeconds,omitempty"`
		TransitionSeconds string `json:"transitionSeconds,omitempty"`
	}

	if d := cfg.Shutdown; d != 0 {
		tmp.ShutdownSeconds = d.String()
	}

	if d := cfg.Transition; d != 0 {
		tmp.TransitionSeconds = d.String()
	}

	return json.Marshal(&tmp)
}

func (cfg *GracePeriodsConfig) UnmarshalJSON(data []byte) (err error) {
	var tmp struct {
		Shutdown   string `json:"shutdownSeconds,omitempty"`
		Transition string `json:"transitionSeconds,omitempty"`
	}

	if err = json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	if tmp.Shutdown != "" {
		cfg.Shutdown, err = time.ParseDuration(tmp.Shutdown)
		if err != nil {
			return err
		}
	}

	if tmp.Transition != "" {
		cfg.Transition, err = time.ParseDuration(tmp.Transition)
		if err != nil {
			return err
		}
	}

	return nil
}

// ReplicationTrackerConfig contains the config for the replication tracker.
type ReplicationTrackerConfig struct {
	// Mode can be disable, polling or heartbeat. Default is disable.
	Mode              string `json:"mode,omitempty"`
	HeartbeatInterval time.Duration
	HeartbeatOnDemand time.Duration
}

func (cfg *ReplicationTrackerConfig) MarshalJSON() ([]byte, error) {
	tmp := struct {
		Mode                     string `json:"mode,omitempty"`
		HeartbeatIntervalSeconds string `json:"heartbeatIntervalSeconds,omitempty"`
		HeartbeatOnDemandSeconds string `json:"heartbeatOnDemandSeconds,omitempty"`
	}{
		Mode: cfg.Mode,
	}

	if d := cfg.HeartbeatInterval; d != 0 {
		tmp.HeartbeatIntervalSeconds = d.String()
	}

	if d := cfg.HeartbeatOnDemand; d != 0 {
		tmp.HeartbeatOnDemandSeconds = d.String()
	}

	return json.Marshal(&tmp)
}

func (cfg *ReplicationTrackerConfig) UnmarshalJSON(data []byte) (err error) {
	var tmp struct {
		Mode              string `json:"mode,omitempty"`
		HeartbeatInterval string `json:"heartbeatIntervalSeconds,omitempty"`
		HeartbeatOnDemand string `json:"heartbeatOnDemandSeconds,omitempty"`
	}

	if err = json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	if tmp.HeartbeatInterval != "" {
		cfg.HeartbeatInterval, err = time.ParseDuration(tmp.HeartbeatInterval)
		if err != nil {
			return err
		}
	}

	if tmp.HeartbeatOnDemand != "" {
		cfg.HeartbeatOnDemand, err = time.ParseDuration(tmp.HeartbeatOnDemand)
		if err != nil {
			return err
		}
	}

	cfg.Mode = tmp.Mode

	return nil
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

// RowStreamerConfig contains configuration parameters for a vstreamer (source) that is
// copying the contents of a table to a target
type RowStreamerConfig struct {
	MaxInnoDBTrxHistLen int64 `json:"maxInnoDBTrxHistLen,omitempty"`
	MaxMySQLReplLagSecs int64 `json:"maxMySQLReplLagSecs,omitempty"`
}

// NewCurrentConfig returns a copy of the current config.
func NewCurrentConfig() *TabletConfig {
	return currentConfig.Clone()
}

// NewDefaultConfig returns a new TabletConfig with pre-initialized defaults.
func NewDefaultConfig() *TabletConfig {
	return defaultConfig.Clone()
}

// Clone creates a clone of TabletConfig.
func (c *TabletConfig) Clone() *TabletConfig {
	tc := *c
	if tc.DB != nil {
		tc.DB = c.DB.Clone()
	}
	return &tc
}

// SetTxTimeoutForWorkload updates workload transaction timeouts. Used in tests only.
func (c *TabletConfig) SetTxTimeoutForWorkload(val time.Duration, workload querypb.ExecuteOptions_Workload) {
	switch workload {
	case querypb.ExecuteOptions_OLAP:
		c.Olap.TxTimeout = val
	case querypb.ExecuteOptions_OLTP:
		c.Oltp.TxTimeout = val
	default:
		panic(fmt.Sprintf("unsupported workload type: %v", workload))
	}
}

// TxTimeoutForWorkload returns the transaction timeout for the given workload
// type. Defaults to returning OLTP timeout.
func (c *TabletConfig) TxTimeoutForWorkload(workload querypb.ExecuteOptions_Workload) time.Duration {
	switch workload {
	case querypb.ExecuteOptions_DBA:
		return 0
	case querypb.ExecuteOptions_OLAP:
		return c.Olap.TxTimeout
	default:
		return c.Oltp.TxTimeout
	}
}

// Verify checks for contradicting flags.
func (c *TabletConfig) Verify() error {
	if err := c.verifyTransactionLimitConfig(); err != nil {
		return err
	}
	if err := c.verifyTxThrottlerConfig(); err != nil {
		return err
	}
	if v := c.HotRowProtection.MaxQueueSize; v <= 0 {
		return fmt.Errorf("--hot_row_protection_max_queue_size must be > 0 (specified value: %v)", v)
	}
	if v := c.HotRowProtection.MaxGlobalQueueSize; v <= 0 {
		return fmt.Errorf("--hot_row_protection_max_global_queue_size must be > 0 (specified value: %v)", v)
	}
	if globalSize, size := c.HotRowProtection.MaxGlobalQueueSize, c.HotRowProtection.MaxQueueSize; globalSize < size {
		return fmt.Errorf("global queue size must be >= per row (range) queue size: -hot_row_protection_max_global_queue_size < hot_row_protection_max_queue_size (%v < %v)", globalSize, size)
	}
	if v := c.HotRowProtection.MaxConcurrency; v <= 0 {
		return fmt.Errorf("--hot_row_protection_concurrent_transactions must be > 0 (specified value: %v)", v)
	}
	return nil
}

// verifyTransactionLimitConfig checks TransactionLimitConfig for sanity
func (c *TabletConfig) verifyTransactionLimitConfig() error {
	actual, dryRun := c.EnableTransactionLimit, c.EnableTransactionLimitDryRun
	if actual && dryRun {
		return errors.New("only one of two flags allowed: --enable_transaction_limit or --enable_transaction_limit_dry_run")
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
		return errors.New("no user discriminating fields selected for transaction limiter, everyone would share single chunk of transaction pool. Override with at least one of --transaction_limit_by flags set to true")
	}
	if v := c.TransactionLimitPerUser; v <= 0 || v >= 1 {
		return fmt.Errorf("--transaction_limit_per_user should be a fraction within range (0, 1) (specified value: %v)", v)
	}
	if limit := int(c.TransactionLimitPerUser * float64(c.TxPool.Size)); limit == 0 {
		return fmt.Errorf("effective transaction limit per user is 0 due to rounding, increase --transaction_limit_per_user")
	}
	return nil
}

// verifyTxThrottlerConfig checks the TxThrottler related config for sanity.
func (c *TabletConfig) verifyTxThrottlerConfig() error {
	if !c.EnableTxThrottler {
		return nil
	}

	err := throttler.MaxReplicationLagModuleConfig{Configuration: c.TxThrottlerConfig.Get()}.Verify()
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "failed to parse throttlerdatapb.Configuration config: %v", err)
	}

	if v := c.TxThrottlerDefaultPriority; v > sqlparser.MaxPriorityValue || v < 0 {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "--tx-throttler-default-priority must be > 0 and < 100 (specified value: %d)", v)
	}

	if c.TxThrottlerTabletTypes == nil || len(*c.TxThrottlerTabletTypes) == 0 {
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "--tx-throttler-tablet-types must be defined when transaction throttler is enabled")
	}
	for _, tabletType := range *c.TxThrottlerTabletTypes {
		switch tabletType {
		case topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY:
			continue
		default:
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported tablet type %q", tabletType)
		}
	}

	return nil
}

// Some of these values are for documentation purposes.
// They actually get overwritten during Init.
var defaultConfig = TabletConfig{
	OltpReadPool: ConnPoolConfig{
		Size:        16,
		IdleTimeout: 30 * time.Minute,
	},
	OlapReadPool: ConnPoolConfig{
		Size:        200,
		IdleTimeout: 30 * time.Minute,
	},
	TxPool: ConnPoolConfig{
		Size:        20,
		Timeout:     time.Second,
		IdleTimeout: 30 * time.Minute,
	},
	Olap: OlapConfig{
		TxTimeout: 30 * time.Second,
	},
	Oltp: OltpConfig{
		QueryTimeout: 30 * time.Second,
		TxTimeout:    30 * time.Second,
		MaxRows:      10000,
	},
	Healthcheck: HealthcheckConfig{
		Interval:           20 * time.Second,
		DegradedThreshold:  30 * time.Second,
		UnhealthyThreshold: 2 * time.Hour,
	},
	ReplicationTracker: ReplicationTrackerConfig{
		Mode:              Disable,
		HeartbeatInterval: 250 * time.Millisecond,
	},
	GracePeriods: GracePeriodsConfig{
		Shutdown: 3 * time.Second,
	},
	HotRowProtection: HotRowProtectionConfig{
		Mode: Disable,
		// Default value is the same as TxPool.Size.
		MaxQueueSize:       20,
		MaxGlobalQueueSize: 1000,
		// Allow more than 1 transaction for the same hot row through to have enough
		// of them ready in MySQL and profit from a pipelining effect.
		MaxConcurrency: 5,
	},
	Consolidator:                Enable,
	ConsolidatorStreamTotalSize: 128 * 1024 * 1024,
	ConsolidatorStreamQuerySize: 2 * 1024 * 1024,
	// The value for StreamBufferSize was chosen after trying out a few of
	// them. Too small buffers force too many packets to be sent. Too big
	// buffers force the clients to read them in multiple chunks and make
	// memory copies.  so with the encoding overhead, this seems to work
	// great (the overhead makes the final packets on the wire about twice
	// bigger than this).
	StreamBufferSize: 32 * 1024,
	QueryCacheMemory: 32 * 1024 * 1024, // 32 mb for our query cache
	// The doorkeeper for the plan cache is disabled by default in endtoend tests to ensure
	// results are consistent between runs.
	QueryCacheDoorkeeper: !servenv.TestingEndtoend,
	SchemaReloadInterval: 30 * time.Minute,
	// SchemaChangeReloadTimeout is used for the signal reload operation where we have to query mysqld.
	// The queries during the signal reload operation are typically expected to have low load,
	// but in busy systems with many tables, some queries may take longer than anticipated.
	// Therefore, the default value should be generous to ensure completion.
	SchemaChangeReloadTimeout:  30 * time.Second,
	MessagePostponeParallelism: 4,
	SignalWhenSchemaChange:     true,

	EnableTxThrottler:              false,
	TxThrottlerConfig:              defaultTxThrottlerConfig(),
	TxThrottlerHealthCheckCells:    []string{},
	TxThrottlerDefaultPriority:     sqlparser.MaxPriorityValue, // This leads to all queries being candidates to throttle
	TxThrottlerTabletTypes:         &topoproto.TabletTypeListFlag{topodatapb.TabletType_REPLICA},
	TxThrottlerDryRun:              false,
	TxThrottlerTopoRefreshInterval: time.Minute * 5,

	TransactionLimitConfig: defaultTransactionLimitConfig(),

	EnforceStrictTransTables: true,
	EnableOnlineDDL:          true,
	EnableTableGC:            true,

	RowStreamer: RowStreamerConfig{
		MaxInnoDBTrxHistLen: 1000000,
		MaxMySQLReplLagSecs: 43200,
	},

	EnablePerWorkloadTableMetrics: false,
	EnableSettingsPool:            true,
}

// defaultTxThrottlerConfig returns the default TxThrottlerConfigFlag object based on
// a throttler.DefaultMaxReplicationLagModuleConfig().Configuration and overrides some of
// its fields. It panics on error.
func defaultTxThrottlerConfig() *TxThrottlerConfigFlag {
	// Take throttler.DefaultMaxReplicationLagModuleConfig and override some fields.
	config := throttler.DefaultMaxReplicationLagModuleConfig().Configuration
	// TODO(erez): Make DefaultMaxReplicationLagModuleConfig() return a MaxReplicationLagSec of 10
	// and remove this line.
	config.MaxReplicationLagSec = 10
	return &TxThrottlerConfigFlag{config}
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
