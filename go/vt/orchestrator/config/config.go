/*
   Copyright 2014 Outbrain Inc.

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

package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"

	"gopkg.in/gcfg.v1"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
)

var (
	envVariableRegexp = regexp.MustCompile("[$][{](.*)[}]")
)

const (
	LostInRecoveryDowntimeSeconds int = 60 * 60 * 24 * 365
	DefaultStatusAPIEndpoint          = "/api/status"
)

var configurationLoaded chan bool = make(chan bool)

const (
	HealthPollSeconds                            = 1
	RaftHealthPollSeconds                        = 10
	RecoveryPollSeconds                          = 1
	ActiveNodeExpireSeconds                      = 5
	BinlogFileHistoryDays                        = 1
	MaintenanceOwner                             = "orchestrator"
	AuditPageSize                                = 20
	MaintenancePurgeDays                         = 7
	MySQLTopologyMaxPoolConnections              = 3
	MaintenanceExpireMinutes                     = 10
	AgentHttpTimeoutSeconds                      = 60
	PseudoGTIDCoordinatesHistoryHeuristicMinutes = 2
	DebugMetricsIntervalSeconds                  = 10
	PseudoGTIDSchema                             = "_pseudo_gtid_"
	PseudoGTIDIntervalSeconds                    = 5
	PseudoGTIDExpireMinutes                      = 60
	StaleInstanceCoordinatesExpireSeconds        = 60
	CheckAutoPseudoGTIDGrantsIntervalSeconds     = 60
	SelectTrueQuery                              = "select 1"
)

// Configuration makes for orchestrator configuration input, which can be provided by user via JSON formatted file.
// Some of the parameteres have reasonable default values, and some (like database credentials) are
// strictly expected from user.
// TODO(sougou): change this to yaml parsing, and possible merge with tabletenv.
type Configuration struct {
	Debug                                      bool   // set debug mode (similar to --debug option)
	EnableSyslog                               bool   // Should logs be directed (in addition) to syslog daemon?
	ListenAddress                              string // Where orchestrator HTTP should listen for TCP
	ListenSocket                               string // Where orchestrator HTTP should listen for unix socket (default: empty; when given, TCP is disabled)
	HTTPAdvertise                              string // optional, for raft setups, what is the HTTP address this node will advertise to its peers (potentially use where behind NAT or when rerouting ports; example: "http://11.22.33.44:3030")
	AgentsServerPort                           string // port orchestrator agents talk back to
	Durability                                 string // The type of durability to enforce. Default is "semi_sync". Other values are dictated by registered plugins
	MySQLTopologyUser                          string
	MySQLTopologyPassword                      string
	MySQLReplicaUser                           string // If set, use this credential instead of discovering from mysql. TODO(sougou): deprecate this in favor of fetching from vttablet
	MySQLReplicaPassword                       string
	MySQLTopologyCredentialsConfigFile         string // my.cnf style configuration file from where to pick credentials. Expecting `user`, `password` under `[client]` section
	MySQLTopologySSLPrivateKeyFile             string // Private key file used to authenticate with a Topology mysql instance with TLS
	MySQLTopologySSLCertFile                   string // Certificate PEM file used to authenticate with a Topology mysql instance with TLS
	MySQLTopologySSLCAFile                     string // Certificate Authority PEM file used to authenticate with a Topology mysql instance with TLS
	MySQLTopologySSLSkipVerify                 bool   // If true, do not strictly validate mutual TLS certs for Topology mysql instances
	MySQLTopologyUseMutualTLS                  bool   // Turn on TLS authentication with the Topology MySQL instances
	MySQLTopologyUseMixedTLS                   bool   // Mixed TLS and non-TLS authentication with the Topology MySQL instances
	TLSCacheTTLFactor                          uint   // Factor of InstancePollSeconds that we set as TLS info cache expiry
	BackendDB                                  string // EXPERIMENTAL: type of backend db; either "mysql" or "sqlite3"
	SQLite3DataFile                            string // when BackendDB == "sqlite3", full path to sqlite3 datafile
	SkipOrchestratorDatabaseUpdate             bool   // When true, do not check backend database schema nor attempt to update it. Useful when you may be running multiple versions of orchestrator, and you only wish certain boxes to dictate the db structure (or else any time a different orchestrator version runs it will rebuild database schema)
	PanicIfDifferentDatabaseDeploy             bool   // When true, and this process finds the orchestrator backend DB was provisioned by a different version, panic
	RaftEnabled                                bool   // When true, setup orchestrator in a raft consensus layout. When false (default) all Raft* variables are ignored
	RaftBind                                   string
	RaftAdvertise                              string
	RaftDataDir                                string
	DefaultRaftPort                            int      // if a RaftNodes entry does not specify port, use this one
	RaftNodes                                  []string // Raft nodes to make initial connection with
	ExpectFailureAnalysisConcensus             bool
	MySQLOrchestratorHost                      string
	MySQLOrchestratorMaxPoolConnections        int // The maximum size of the connection pool to the Orchestrator backend.
	MySQLOrchestratorPort                      uint
	MySQLOrchestratorDatabase                  string
	MySQLOrchestratorUser                      string
	MySQLOrchestratorPassword                  string
	MySQLOrchestratorCredentialsConfigFile     string   // my.cnf style configuration file from where to pick credentials. Expecting `user`, `password` under `[client]` section
	MySQLOrchestratorSSLPrivateKeyFile         string   // Private key file used to authenticate with the Orchestrator mysql instance with TLS
	MySQLOrchestratorSSLCertFile               string   // Certificate PEM file used to authenticate with the Orchestrator mysql instance with TLS
	MySQLOrchestratorSSLCAFile                 string   // Certificate Authority PEM file used to authenticate with the Orchestrator mysql instance with TLS
	MySQLOrchestratorSSLSkipVerify             bool     // If true, do not strictly validate mutual TLS certs for the Orchestrator mysql instances
	MySQLOrchestratorUseMutualTLS              bool     // Turn on TLS authentication with the Orchestrator MySQL instance
	MySQLOrchestratorReadTimeoutSeconds        int      // Number of seconds before backend mysql read operation is aborted (driver-side)
	MySQLOrchestratorRejectReadOnly            bool     // Reject read only connections https://github.com/go-sql-driver/mysql#rejectreadonly
	MySQLConnectTimeoutSeconds                 int      // Number of seconds before connection is aborted (driver-side)
	MySQLDiscoveryReadTimeoutSeconds           int      // Number of seconds before topology mysql read operation is aborted (driver-side). Used for discovery queries.
	MySQLTopologyReadTimeoutSeconds            int      // Number of seconds before topology mysql read operation is aborted (driver-side). Used for all but discovery queries.
	MySQLConnectionLifetimeSeconds             int      // Number of seconds the mysql driver will keep database connection alive before recycling it
	DefaultInstancePort                        int      // In case port was not specified on command line
	SlaveLagQuery                              string   // Synonym to ReplicationLagQuery
	ReplicationLagQuery                        string   // custom query to check on replica lg (e.g. heartbeat table). Must return a single row with a single numeric column, which is the lag.
	ReplicationCredentialsQuery                string   // custom query to get replication credentials. Must return a single row, with two text columns: 1st is username, 2nd is password. This is optional, and can be used by orchestrator to configure replication after master takeover or setup of co-masters. You need to ensure the orchestrator user has the privileges to run this query
	DiscoverByShowSlaveHosts                   bool     // Attempt SHOW SLAVE HOSTS before PROCESSLIST
	UseSuperReadOnly                           bool     // Should orchestrator super_read_only any time it sets read_only
	InstancePollSeconds                        uint     // Number of seconds between instance reads
	InstanceWriteBufferSize                    int      // Instance write buffer size (max number of instances to flush in one INSERT ODKU)
	BufferInstanceWrites                       bool     // Set to 'true' for write-optimization on backend table (compromise: writes can be stale and overwrite non stale data)
	InstanceFlushIntervalMilliseconds          int      // Max interval between instance write buffer flushes
	SkipMaxScaleCheck                          bool     // If you don't ever have MaxScale BinlogServer in your topology (and most people don't), set this to 'true' to save some pointless queries
	UnseenInstanceForgetHours                  uint     // Number of hours after which an unseen instance is forgotten
	SnapshotTopologiesIntervalHours            uint     // Interval in hour between snapshot-topologies invocation. Default: 0 (disabled)
	DiscoveryMaxConcurrency                    uint     // Number of goroutines doing hosts discovery
	DiscoveryQueueCapacity                     uint     // Buffer size of the discovery queue. Should be greater than the number of DB instances being discovered
	DiscoveryQueueMaxStatisticsSize            int      // The maximum number of individual secondly statistics taken of the discovery queue
	DiscoveryCollectionRetentionSeconds        uint     // Number of seconds to retain the discovery collection information
	DiscoverySeeds                             []string // Hard coded array of hostname:port, ensuring orchestrator discovers these hosts upon startup, assuming not already known to orchestrator
	InstanceBulkOperationsWaitTimeoutSeconds   uint     // Time to wait on a single instance when doing bulk (many instances) operation
	HostnameResolveMethod                      string   // Method by which to "normalize" hostname ("none"/"default"/"cname")
	MySQLHostnameResolveMethod                 string   // Method by which to "normalize" hostname via MySQL server. ("none"/"@@hostname"/"@@report_host"; default "@@hostname")
	SkipBinlogServerUnresolveCheck             bool     // Skip the double-check that an unresolved hostname resolves back to same hostname for binlog servers
	ExpiryHostnameResolvesMinutes              int      // Number of minutes after which to expire hostname-resolves
	RejectHostnameResolvePattern               string   // Regexp pattern for resolved hostname that will not be accepted (not cached, not written to db). This is done to avoid storing wrong resolves due to network glitches.
	ReasonableReplicationLagSeconds            int      // Above this value is considered a problem
	ProblemIgnoreHostnameFilters               []string // Will minimize problem visualization for hostnames matching given regexp filters
	VerifyReplicationFilters                   bool     // Include replication filters check before approving topology refactoring
	ReasonableMaintenanceReplicationLagSeconds int      // Above this value move-up and move-below are blocked
	CandidateInstanceExpireMinutes             uint     // Minutes after which a suggestion to use an instance as a candidate replica (to be preferably promoted on master failover) is expired.
	AuditLogFile                               string   // Name of log file for audit operations. Disabled when empty.
	AuditToSyslog                              bool     // If true, audit messages are written to syslog
	AuditToBackendDB                           bool     // If true, audit messages are written to the backend DB's `audit` table (default: true)
	AuditPurgeDays                             uint     // Days after which audit entries are purged from the database
	RemoveTextFromHostnameDisplay              string   // Text to strip off the hostname on cluster/clusters pages
	ReadOnly                                   bool
	AuthenticationMethod                       string // Type of autherntication to use, if any. "" for none, "basic" for BasicAuth, "multi" for advanced BasicAuth, "proxy" for forwarded credentials via reverse proxy, "token" for token based access
	OAuthClientId                              string
	OAuthClientSecret                          string
	OAuthScopes                                []string
	HTTPAuthUser                               string            // Username for HTTP Basic authentication (blank disables authentication)
	HTTPAuthPassword                           string            // Password for HTTP Basic authentication
	AuthUserHeader                             string            // HTTP header indicating auth user, when AuthenticationMethod is "proxy"
	PowerAuthUsers                             []string          // On AuthenticationMethod == "proxy", list of users that can make changes. All others are read-only.
	PowerAuthGroups                            []string          // list of unix groups the authenticated user must be a member of to make changes.
	AccessTokenUseExpirySeconds                uint              // Time by which an issued token must be used
	AccessTokenExpiryMinutes                   uint              // Time after which HTTP access token expires
	ClusterNameToAlias                         map[string]string // map between regex matching cluster name to a human friendly alias
	DetectClusterAliasQuery                    string            // Optional query (executed on topology instance) that returns the alias of a cluster. Query will only be executed on cluster master (though until the topology's master is resovled it may execute on other/all replicas). If provided, must return one row, one column
	DetectClusterDomainQuery                   string            // Optional query (executed on topology instance) that returns the VIP/CNAME/Alias/whatever domain name for the master of this cluster. Query will only be executed on cluster master (though until the topology's master is resovled it may execute on other/all replicas). If provided, must return one row, one column
	DetectInstanceAliasQuery                   string            // Optional query (executed on topology instance) that returns the alias of an instance. If provided, must return one row, one column
	DetectPromotionRuleQuery                   string            // Optional query (executed on topology instance) that returns the promotion rule of an instance. If provided, must return one row, one column.
	DataCenterPattern                          string            // Regexp pattern with one group, extracting the datacenter name from the hostname
	RegionPattern                              string            // Regexp pattern with one group, extracting the region name from the hostname
	PhysicalEnvironmentPattern                 string            // Regexp pattern with one group, extracting physical environment info from hostname (e.g. combination of datacenter & prod/dev env)
	DetectDataCenterQuery                      string            // Optional query (executed on topology instance) that returns the data center of an instance. If provided, must return one row, one column. Overrides DataCenterPattern and useful for installments where DC cannot be inferred by hostname
	DetectRegionQuery                          string            // Optional query (executed on topology instance) that returns the region of an instance. If provided, must return one row, one column. Overrides RegionPattern and useful for installments where Region cannot be inferred by hostname
	DetectPhysicalEnvironmentQuery             string            // Optional query (executed on topology instance) that returns the physical environment of an instance. If provided, must return one row, one column. Overrides PhysicalEnvironmentPattern and useful for installments where env cannot be inferred by hostname
	DetectSemiSyncEnforcedQuery                string            // Optional query (executed on topology instance) to determine whether semi-sync is fully enforced for master writes (async fallback is not allowed under any circumstance). If provided, must return one row, one column, value 0 or 1.
	SupportFuzzyPoolHostnames                  bool              // Should "submit-pool-instances" command be able to pass list of fuzzy instances (fuzzy means non-fqdn, but unique enough to recognize). Defaults 'true', implies more queries on backend db
	InstancePoolExpiryMinutes                  uint              // Time after which entries in database_instance_pool are expired (resubmit via `submit-pool-instances`)
	PromotionIgnoreHostnameFilters             []string          // Orchestrator will not promote replicas with hostname matching pattern (via -c recovery; for example, avoid promoting dev-dedicated machines)
	ServeAgentsHttp                            bool              // Spawn another HTTP interface dedicated for orchestrator-agent
	AgentsUseSSL                               bool              // When "true" orchestrator will listen on agents port with SSL as well as connect to agents via SSL
	AgentsUseMutualTLS                         bool              // When "true" Use mutual TLS for the server to agent communication
	AgentSSLSkipVerify                         bool              // When using SSL for the Agent, should we ignore SSL certification error
	AgentSSLPrivateKeyFile                     string            // Name of Agent SSL private key file, applies only when AgentsUseSSL = true
	AgentSSLCertFile                           string            // Name of Agent SSL certification file, applies only when AgentsUseSSL = true
	AgentSSLCAFile                             string            // Name of the Agent Certificate Authority file, applies only when AgentsUseSSL = true
	AgentSSLValidOUs                           []string          // Valid organizational units when using mutual TLS to communicate with the agents
	UseSSL                                     bool              // Use SSL on the server web port
	UseMutualTLS                               bool              // When "true" Use mutual TLS for the server's web and API connections
	SSLSkipVerify                              bool              // When using SSL, should we ignore SSL certification error
	SSLPrivateKeyFile                          string            // Name of SSL private key file, applies only when UseSSL = true
	SSLCertFile                                string            // Name of SSL certification file, applies only when UseSSL = true
	SSLCAFile                                  string            // Name of the Certificate Authority file, applies only when UseSSL = true
	SSLValidOUs                                []string          // Valid organizational units when using mutual TLS
	StatusEndpoint                             string            // Override the status endpoint.  Defaults to '/api/status'
	StatusOUVerify                             bool              // If true, try to verify OUs when Mutual TLS is on.  Defaults to false
	AgentPollMinutes                           uint              // Minutes between agent polling
	UnseenAgentForgetHours                     uint              // Number of hours after which an unseen agent is forgotten
	StaleSeedFailMinutes                       uint              // Number of minutes after which a stale (no progress) seed is considered failed.
	SeedAcceptableBytesDiff                    int64             // Difference in bytes between seed source & target data size that is still considered as successful copy
	SeedWaitSecondsBeforeSend                  int64             // Number of seconds for waiting before start send data command on agent
	AutoPseudoGTID                             bool              // Should orchestrator automatically inject Pseudo-GTID entries to the masters
	PseudoGTIDPattern                          string            // Pattern to look for in binary logs that makes for a unique entry (pseudo GTID). When empty, Pseudo-GTID based refactoring is disabled.
	PseudoGTIDPatternIsFixedSubstring          bool              // If true, then PseudoGTIDPattern is not treated as regular expression but as fixed substring, and can boost search time
	PseudoGTIDMonotonicHint                    string            // subtring in Pseudo-GTID entry which indicates Pseudo-GTID entries are expected to be monotonically increasing
	DetectPseudoGTIDQuery                      string            // Optional query which is used to authoritatively decide whether pseudo gtid is enabled on instance
	BinlogEventsChunkSize                      int               // Chunk size (X) for SHOW BINLOG|RELAYLOG EVENTS LIMIT ?,X statements. Smaller means less locking and mroe work to be done
	SkipBinlogEventsContaining                 []string          // When scanning/comparing binlogs for Pseudo-GTID, skip entries containing given texts. These are NOT regular expressions (would consume too much CPU while scanning binlogs), just substrings to find.
	ReduceReplicationAnalysisCount             bool              // When true, replication analysis will only report instances where possibility of handled problems is possible in the first place (e.g. will not report most leaf nodes, that are mostly uninteresting). When false, provides an entry for every known instance
	FailureDetectionPeriodBlockMinutes         int               // The time for which an instance's failure discovery is kept "active", so as to avoid concurrent "discoveries" of the instance's failure; this preceeds any recovery process, if any.
	RecoveryPeriodBlockMinutes                 int               // (supported for backwards compatibility but please use newer `RecoveryPeriodBlockSeconds` instead) The time for which an instance's recovery is kept "active", so as to avoid concurrent recoveries on smae instance as well as flapping
	RecoveryPeriodBlockSeconds                 int               // (overrides `RecoveryPeriodBlockMinutes`) The time for which an instance's recovery is kept "active", so as to avoid concurrent recoveries on smae instance as well as flapping
	RecoveryIgnoreHostnameFilters              []string          // Recovery analysis will completely ignore hosts matching given patterns
	RecoverMasterClusterFilters                []string          // Only do master recovery on clusters matching these regexp patterns (of course the ".*" pattern matches everything)
	RecoverIntermediateMasterClusterFilters    []string          // Only do IM recovery on clusters matching these regexp patterns (of course the ".*" pattern matches everything)
	ProcessesShellCommand                      string            // Shell that executes command scripts
	OnFailureDetectionProcesses                []string          // Processes to execute when detecting a failover scenario (before making a decision whether to failover or not). May and should use some of these placeholders: {failureType}, {instanceType}, {isMaster}, {isCoMaster}, {failureDescription}, {command}, {failedHost}, {failureCluster}, {failureClusterAlias}, {failureClusterDomain}, {failedPort}, {successorHost}, {successorPort}, {successorAlias}, {countReplicas}, {replicaHosts}, {isDowntimed}, {autoMasterRecovery}, {autoIntermediateMasterRecovery}
	PreGracefulTakeoverProcesses               []string          // Processes to execute before doing a failover (aborting operation should any once of them exits with non-zero code; order of execution undefined). May and should use some of these placeholders: {failureType}, {instanceType}, {isMaster}, {isCoMaster}, {failureDescription}, {command}, {failedHost}, {failureCluster}, {failureClusterAlias}, {failureClusterDomain}, {failedPort}, {successorHost}, {successorPort}, {countReplicas}, {replicaHosts}, {isDowntimed}
	PreFailoverProcesses                       []string          // Processes to execute before doing a failover (aborting operation should any once of them exits with non-zero code; order of execution undefined). May and should use some of these placeholders: {failureType}, {instanceType}, {isMaster}, {isCoMaster}, {failureDescription}, {command}, {failedHost}, {failureCluster}, {failureClusterAlias}, {failureClusterDomain}, {failedPort}, {countReplicas}, {replicaHosts}, {isDowntimed}
	PostFailoverProcesses                      []string          // Processes to execute after doing a failover (order of execution undefined). May and should use some of these placeholders: {failureType}, {instanceType}, {isMaster}, {isCoMaster}, {failureDescription}, {command}, {failedHost}, {failureCluster}, {failureClusterAlias}, {failureClusterDomain}, {failedPort}, {successorHost}, {successorPort}, {successorAlias}, {countReplicas}, {replicaHosts}, {isDowntimed}, {isSuccessful}, {lostReplicas}, {countLostReplicas}
	PostUnsuccessfulFailoverProcesses          []string          // Processes to execute after a not-completely-successful failover (order of execution undefined). May and should use some of these placeholders: {failureType}, {instanceType}, {isMaster}, {isCoMaster}, {failureDescription}, {command}, {failedHost}, {failureCluster}, {failureClusterAlias}, {failureClusterDomain}, {failedPort}, {successorHost}, {successorPort}, {successorAlias}, {countReplicas}, {replicaHosts}, {isDowntimed}, {isSuccessful}, {lostReplicas}, {countLostReplicas}
	PostMasterFailoverProcesses                []string          // Processes to execute after doing a master failover (order of execution undefined). Uses same placeholders as PostFailoverProcesses
	PostIntermediateMasterFailoverProcesses    []string          // Processes to execute after doing a master failover (order of execution undefined). Uses same placeholders as PostFailoverProcesses
	PostGracefulTakeoverProcesses              []string          // Processes to execute after runnign a graceful master takeover. Uses same placeholders as PostFailoverProcesses
	PostTakeMasterProcesses                    []string          // Processes to execute after a successful Take-Master event has taken place
	CoMasterRecoveryMustPromoteOtherCoMaster   bool              // When 'false', anything can get promoted (and candidates are prefered over others). When 'true', orchestrator will promote the other co-master or else fail
	DetachLostSlavesAfterMasterFailover        bool              // synonym to DetachLostReplicasAfterMasterFailover
	DetachLostReplicasAfterMasterFailover      bool              // Should replicas that are not to be lost in master recovery (i.e. were more up-to-date than promoted replica) be forcibly detached
	ApplyMySQLPromotionAfterMasterFailover     bool              // Should orchestrator take upon itself to apply MySQL master promotion: set read_only=0, detach replication, etc.
	PreventCrossDataCenterMasterFailover       bool              // When true (default: false), cross-DC master failover are not allowed, orchestrator will do all it can to only fail over within same DC, or else not fail over at all.
	PreventCrossRegionMasterFailover           bool              // When true (default: false), cross-region master failover are not allowed, orchestrator will do all it can to only fail over within same region, or else not fail over at all.
	MasterFailoverLostInstancesDowntimeMinutes uint              // Number of minutes to downtime any server that was lost after a master failover (including failed master & lost replicas). 0 to disable
	MasterFailoverDetachSlaveMasterHost        bool              // synonym to MasterFailoverDetachReplicaMasterHost
	MasterFailoverDetachReplicaMasterHost      bool              // Should orchestrator issue a detach-replica-master-host on newly promoted master (this makes sure the new master will not attempt to replicate old master if that comes back to life). Defaults 'false'. Meaningless if ApplyMySQLPromotionAfterMasterFailover is 'true'.
	FailMasterPromotionOnLagMinutes            uint              // when > 0, fail a master promotion if the candidate replica is lagging >= configured number of minutes.
	FailMasterPromotionIfSQLThreadNotUpToDate  bool              // when true, and a master failover takes place, if candidate master has not consumed all relay logs, promotion is aborted with error
	DelayMasterPromotionIfSQLThreadNotUpToDate bool              // when true, and a master failover takes place, if candidate master has not consumed all relay logs, delay promotion until the sql thread has caught up
	PostponeSlaveRecoveryOnLagMinutes          uint              // Synonym to PostponeReplicaRecoveryOnLagMinutes
	PostponeReplicaRecoveryOnLagMinutes        uint              // On crash recovery, replicas that are lagging more than given minutes are only resurrected late in the recovery process, after master/IM has been elected and processes executed. Value of 0 disables this feature
	OSCIgnoreHostnameFilters                   []string          // OSC replicas recommendation will ignore replica hostnames matching given patterns
	GraphiteAddr                               string            // Optional; address of graphite port. If supplied, metrics will be written here
	GraphitePath                               string            // Prefix for graphite path. May include {hostname} magic placeholder
	GraphiteConvertHostnameDotsToUnderscores   bool              // If true, then hostname's dots are converted to underscores before being used in graphite path
	GraphitePollSeconds                        int               // Graphite writes interval. 0 disables.
	URLPrefix                                  string            // URL prefix to run orchestrator on non-root web path, e.g. /orchestrator to put it behind nginx.
	DiscoveryIgnoreReplicaHostnameFilters      []string          // Regexp filters to apply to prevent auto-discovering new replicas. Usage: unreachable servers due to firewalls, applications which trigger binlog dumps
	DiscoveryIgnoreMasterHostnameFilters       []string          // Regexp filters to apply to prevent auto-discovering a master. Usage: pointing your master temporarily to replicate seom data from external host
	DiscoveryIgnoreHostnameFilters             []string          // Regexp filters to apply to prevent discovering instances of any kind
	ConsulAddress                              string            // Address where Consul HTTP api is found. Example: 127.0.0.1:8500
	ConsulScheme                               string            // Scheme (http or https) for Consul
	ConsulAclToken                             string            // ACL token used to write to Consul KV
	ConsulCrossDataCenterDistribution          bool              // should orchestrator automatically auto-deduce all consul DCs and write KVs in all DCs
	ZkAddress                                  string            // UNSUPPERTED YET. Address where (single or multiple) ZooKeeper servers are found, in `srv1[:port1][,srv2[:port2]...]` format. Default port is 2181. Example: srv-a,srv-b:12181,srv-c
	KVClusterMasterPrefix                      string            // Prefix to use for clusters' masters entries in KV stores (internal, consul, ZK), default: "mysql/master"
	WebMessage                                 string            // If provided, will be shown on all web pages below the title bar
	MaxConcurrentReplicaOperations             int               // Maximum number of concurrent operations on replicas
	InstanceDBExecContextTimeoutSeconds        int               // Timeout on context used while calling ExecContext on instance database
	LockShardTimeoutSeconds                    int               // Timeout on context used to lock shard. Should be a small value because we should fail-fast
}

// ToJSONString will marshal this configuration as JSON
func (this *Configuration) ToJSONString() string {
	b, _ := json.Marshal(this)
	return string(b)
}

// Config is *the* configuration instance, used globally to get configuration data
var Config = newConfiguration()
var readFileNames []string

func newConfiguration() *Configuration {
	return &Configuration{
		Debug:                                      false,
		EnableSyslog:                               false,
		ListenAddress:                              ":3000",
		ListenSocket:                               "",
		HTTPAdvertise:                              "",
		AgentsServerPort:                           ":3001",
		Durability:                                 "none",
		StatusEndpoint:                             DefaultStatusAPIEndpoint,
		StatusOUVerify:                             false,
		BackendDB:                                  "sqlite",
		SQLite3DataFile:                            "file::memory:?mode=memory&cache=shared",
		SkipOrchestratorDatabaseUpdate:             false,
		PanicIfDifferentDatabaseDeploy:             false,
		RaftBind:                                   "127.0.0.1:10008",
		RaftAdvertise:                              "",
		RaftDataDir:                                "",
		DefaultRaftPort:                            10008,
		RaftNodes:                                  []string{},
		ExpectFailureAnalysisConcensus:             true,
		MySQLOrchestratorMaxPoolConnections:        128, // limit concurrent conns to backend DB
		MySQLOrchestratorPort:                      3306,
		MySQLTopologyUseMutualTLS:                  false,
		MySQLTopologyUseMixedTLS:                   true,
		MySQLOrchestratorUseMutualTLS:              false,
		MySQLConnectTimeoutSeconds:                 2,
		MySQLOrchestratorReadTimeoutSeconds:        30,
		MySQLOrchestratorRejectReadOnly:            false,
		MySQLDiscoveryReadTimeoutSeconds:           10,
		MySQLTopologyReadTimeoutSeconds:            600,
		MySQLConnectionLifetimeSeconds:             0,
		DefaultInstancePort:                        3306,
		TLSCacheTTLFactor:                          100,
		InstancePollSeconds:                        5,
		InstanceWriteBufferSize:                    100,
		BufferInstanceWrites:                       false,
		InstanceFlushIntervalMilliseconds:          100,
		SkipMaxScaleCheck:                          true,
		UnseenInstanceForgetHours:                  240,
		SnapshotTopologiesIntervalHours:            0,
		DiscoverByShowSlaveHosts:                   false,
		UseSuperReadOnly:                           false,
		DiscoveryMaxConcurrency:                    300,
		DiscoveryQueueCapacity:                     100000,
		DiscoveryQueueMaxStatisticsSize:            120,
		DiscoveryCollectionRetentionSeconds:        120,
		DiscoverySeeds:                             []string{},
		InstanceBulkOperationsWaitTimeoutSeconds:   10,
		HostnameResolveMethod:                      "default",
		MySQLHostnameResolveMethod:                 "none",
		SkipBinlogServerUnresolveCheck:             true,
		ExpiryHostnameResolvesMinutes:              60,
		RejectHostnameResolvePattern:               "",
		ReasonableReplicationLagSeconds:            10,
		ProblemIgnoreHostnameFilters:               []string{},
		VerifyReplicationFilters:                   false,
		ReasonableMaintenanceReplicationLagSeconds: 20,
		CandidateInstanceExpireMinutes:             60,
		AuditLogFile:                               "",
		AuditToSyslog:                              false,
		AuditToBackendDB:                           false,
		AuditPurgeDays:                             7,
		RemoveTextFromHostnameDisplay:              "",
		ReadOnly:                                   false,
		AuthenticationMethod:                       "",
		HTTPAuthUser:                               "",
		HTTPAuthPassword:                           "",
		AuthUserHeader:                             "X-Forwarded-User",
		PowerAuthUsers:                             []string{"*"},
		PowerAuthGroups:                            []string{},
		AccessTokenUseExpirySeconds:                60,
		AccessTokenExpiryMinutes:                   1440,
		ClusterNameToAlias:                         make(map[string]string),
		DetectClusterAliasQuery:                    "",
		DetectClusterDomainQuery:                   "",
		DetectInstanceAliasQuery:                   "",
		DetectPromotionRuleQuery:                   "",
		DataCenterPattern:                          "",
		PhysicalEnvironmentPattern:                 "",
		DetectDataCenterQuery:                      "",
		DetectPhysicalEnvironmentQuery:             "",
		DetectSemiSyncEnforcedQuery:                "",
		SupportFuzzyPoolHostnames:                  true,
		InstancePoolExpiryMinutes:                  60,
		PromotionIgnoreHostnameFilters:             []string{},
		ServeAgentsHttp:                            false,
		AgentsUseSSL:                               false,
		AgentsUseMutualTLS:                         false,
		AgentSSLValidOUs:                           []string{},
		AgentSSLSkipVerify:                         false,
		AgentSSLPrivateKeyFile:                     "",
		AgentSSLCertFile:                           "",
		AgentSSLCAFile:                             "",
		UseSSL:                                     false,
		UseMutualTLS:                               false,
		SSLValidOUs:                                []string{},
		SSLSkipVerify:                              false,
		SSLPrivateKeyFile:                          "",
		SSLCertFile:                                "",
		SSLCAFile:                                  "",
		AgentPollMinutes:                           60,
		UnseenAgentForgetHours:                     6,
		StaleSeedFailMinutes:                       60,
		SeedAcceptableBytesDiff:                    8192,
		SeedWaitSecondsBeforeSend:                  2,
		AutoPseudoGTID:                             false,
		PseudoGTIDPattern:                          "",
		PseudoGTIDPatternIsFixedSubstring:          false,
		PseudoGTIDMonotonicHint:                    "",
		DetectPseudoGTIDQuery:                      "",
		BinlogEventsChunkSize:                      10000,
		SkipBinlogEventsContaining:                 []string{},
		ReduceReplicationAnalysisCount:             true,
		FailureDetectionPeriodBlockMinutes:         60,
		RecoveryPeriodBlockMinutes:                 60,
		RecoveryPeriodBlockSeconds:                 3600,
		RecoveryIgnoreHostnameFilters:              []string{},
		RecoverMasterClusterFilters:                []string{"*"},
		RecoverIntermediateMasterClusterFilters:    []string{},
		ProcessesShellCommand:                      "bash",
		OnFailureDetectionProcesses:                []string{},
		PreGracefulTakeoverProcesses:               []string{},
		PreFailoverProcesses:                       []string{},
		PostMasterFailoverProcesses:                []string{},
		PostIntermediateMasterFailoverProcesses:    []string{},
		PostFailoverProcesses:                      []string{},
		PostUnsuccessfulFailoverProcesses:          []string{},
		PostGracefulTakeoverProcesses:              []string{},
		PostTakeMasterProcesses:                    []string{},
		CoMasterRecoveryMustPromoteOtherCoMaster:   true,
		DetachLostSlavesAfterMasterFailover:        true,
		ApplyMySQLPromotionAfterMasterFailover:     true,
		PreventCrossDataCenterMasterFailover:       false,
		PreventCrossRegionMasterFailover:           false,
		MasterFailoverLostInstancesDowntimeMinutes: 0,
		MasterFailoverDetachSlaveMasterHost:        false,
		FailMasterPromotionOnLagMinutes:            0,
		FailMasterPromotionIfSQLThreadNotUpToDate:  false,
		DelayMasterPromotionIfSQLThreadNotUpToDate: true,
		PostponeSlaveRecoveryOnLagMinutes:          0,
		OSCIgnoreHostnameFilters:                   []string{},
		GraphiteAddr:                               "",
		GraphitePath:                               "",
		GraphiteConvertHostnameDotsToUnderscores:   true,
		GraphitePollSeconds:                        60,
		URLPrefix:                                  "",
		DiscoveryIgnoreReplicaHostnameFilters:      []string{},
		ConsulAddress:                              "",
		ConsulScheme:                               "http",
		ConsulAclToken:                             "",
		ConsulCrossDataCenterDistribution:          false,
		ZkAddress:                                  "",
		KVClusterMasterPrefix:                      "mysql/master",
		WebMessage:                                 "",
		MaxConcurrentReplicaOperations:             5,
		InstanceDBExecContextTimeoutSeconds:        30,
		LockShardTimeoutSeconds:                    1,
	}
}

func (this *Configuration) postReadAdjustments() error {
	if this.MySQLOrchestratorCredentialsConfigFile != "" {
		mySQLConfig := struct {
			Client struct {
				User     string
				Password string
			}
		}{}
		err := gcfg.ReadFileInto(&mySQLConfig, this.MySQLOrchestratorCredentialsConfigFile)
		if err != nil {
			log.Fatalf("Failed to parse gcfg data from file: %+v", err)
		} else {
			log.Debugf("Parsed orchestrator credentials from %s", this.MySQLOrchestratorCredentialsConfigFile)
			this.MySQLOrchestratorUser = mySQLConfig.Client.User
			this.MySQLOrchestratorPassword = mySQLConfig.Client.Password
		}
	}
	{
		// We accept password in the form "${SOME_ENV_VARIABLE}" in which case we pull
		// the given variable from os env
		submatch := envVariableRegexp.FindStringSubmatch(this.MySQLOrchestratorPassword)
		if len(submatch) > 1 {
			this.MySQLOrchestratorPassword = os.Getenv(submatch[1])
		}
	}
	if this.MySQLTopologyCredentialsConfigFile != "" {
		mySQLConfig := struct {
			Client struct {
				User     string
				Password string
			}
		}{}
		err := gcfg.ReadFileInto(&mySQLConfig, this.MySQLTopologyCredentialsConfigFile)
		if err != nil {
			log.Fatalf("Failed to parse gcfg data from file: %+v", err)
		} else {
			log.Debugf("Parsed topology credentials from %s", this.MySQLTopologyCredentialsConfigFile)
			this.MySQLTopologyUser = mySQLConfig.Client.User
			this.MySQLTopologyPassword = mySQLConfig.Client.Password
		}
	}
	{
		// We accept password in the form "${SOME_ENV_VARIABLE}" in which case we pull
		// the given variable from os env
		submatch := envVariableRegexp.FindStringSubmatch(this.MySQLTopologyPassword)
		if len(submatch) > 1 {
			this.MySQLTopologyPassword = os.Getenv(submatch[1])
		}
	}

	if this.RecoveryPeriodBlockSeconds == 0 && this.RecoveryPeriodBlockMinutes > 0 {
		// RecoveryPeriodBlockSeconds is a newer addition that overrides RecoveryPeriodBlockMinutes
		// The code does not consider RecoveryPeriodBlockMinutes anymore, but RecoveryPeriodBlockMinutes
		// still supported in config file for backwards compatibility
		this.RecoveryPeriodBlockSeconds = this.RecoveryPeriodBlockMinutes * 60
	}

	{
		if this.ReplicationLagQuery != "" && this.SlaveLagQuery != "" && this.ReplicationLagQuery != this.SlaveLagQuery {
			return fmt.Errorf("config's ReplicationLagQuery and SlaveLagQuery are synonyms and cannot both be defined")
		}
		// ReplicationLagQuery is the replacement param to SlaveLagQuery
		if this.ReplicationLagQuery == "" {
			this.ReplicationLagQuery = this.SlaveLagQuery
		}
		// We reset SlaveLagQuery because we want to support multiple config file loading;
		// One of the next config files may indicate a new value for ReplicationLagQuery.
		// If we do not reset SlaveLagQuery, then the two will have a conflict.
		this.SlaveLagQuery = ""
	}

	{
		if this.DetachLostSlavesAfterMasterFailover {
			this.DetachLostReplicasAfterMasterFailover = true
		}
	}

	{
		if this.MasterFailoverDetachSlaveMasterHost {
			this.MasterFailoverDetachReplicaMasterHost = true
		}
	}
	if this.FailMasterPromotionIfSQLThreadNotUpToDate && this.DelayMasterPromotionIfSQLThreadNotUpToDate {
		return fmt.Errorf("Cannot have both FailMasterPromotionIfSQLThreadNotUpToDate and DelayMasterPromotionIfSQLThreadNotUpToDate enabled")
	}
	if this.FailMasterPromotionOnLagMinutes > 0 && this.ReplicationLagQuery == "" {
		return fmt.Errorf("nonzero FailMasterPromotionOnLagMinutes requires ReplicationLagQuery to be set")
	}
	{
		if this.PostponeReplicaRecoveryOnLagMinutes != 0 && this.PostponeSlaveRecoveryOnLagMinutes != 0 &&
			this.PostponeReplicaRecoveryOnLagMinutes != this.PostponeSlaveRecoveryOnLagMinutes {
			return fmt.Errorf("config's PostponeReplicaRecoveryOnLagMinutes and PostponeSlaveRecoveryOnLagMinutes are synonyms and cannot both be defined")
		}
		if this.PostponeSlaveRecoveryOnLagMinutes != 0 {
			this.PostponeReplicaRecoveryOnLagMinutes = this.PostponeSlaveRecoveryOnLagMinutes
		}
	}

	if this.URLPrefix != "" {
		// Ensure the prefix starts with "/" and has no trailing one.
		this.URLPrefix = strings.TrimLeft(this.URLPrefix, "/")
		this.URLPrefix = strings.TrimRight(this.URLPrefix, "/")
		this.URLPrefix = "/" + this.URLPrefix
	}

	if this.IsSQLite() && this.SQLite3DataFile == "" {
		return fmt.Errorf("SQLite3DataFile must be set when BackendDB is sqlite3")
	}
	if this.RaftEnabled && this.RaftDataDir == "" {
		return fmt.Errorf("RaftDataDir must be defined since raft is enabled (RaftEnabled)")
	}
	if this.RaftEnabled && this.RaftBind == "" {
		return fmt.Errorf("RaftBind must be defined since raft is enabled (RaftEnabled)")
	}
	if this.RaftAdvertise == "" {
		this.RaftAdvertise = this.RaftBind
	}
	if this.KVClusterMasterPrefix != "/" {
		// "/" remains "/"
		// "prefix" turns to "prefix/"
		// "some/prefix///" turns to "some/prefix/"
		this.KVClusterMasterPrefix = strings.TrimRight(this.KVClusterMasterPrefix, "/")
		this.KVClusterMasterPrefix = fmt.Sprintf("%s/", this.KVClusterMasterPrefix)
	}
	if this.AutoPseudoGTID {
		this.PseudoGTIDPattern = "drop view if exists `_pseudo_gtid_`"
		this.PseudoGTIDPatternIsFixedSubstring = true
		this.PseudoGTIDMonotonicHint = "asc:"
		this.DetectPseudoGTIDQuery = SelectTrueQuery
	}
	if this.HTTPAdvertise != "" {
		u, err := url.Parse(this.HTTPAdvertise)
		if err != nil {
			return fmt.Errorf("Failed parsing HTTPAdvertise %s: %s", this.HTTPAdvertise, err.Error())
		}
		if u.Scheme == "" {
			return fmt.Errorf("If specified, HTTPAdvertise must include scheme (http:// or https://)")
		}
		if u.Hostname() == "" {
			return fmt.Errorf("If specified, HTTPAdvertise must include host name")
		}
		if u.Port() == "" {
			return fmt.Errorf("If specified, HTTPAdvertise must include port number")
		}
		if u.Path != "" {
			return fmt.Errorf("If specified, HTTPAdvertise must not specify a path")
		}
		if this.InstanceWriteBufferSize <= 0 {
			this.BufferInstanceWrites = false
		}
	}
	return nil
}

func (this *Configuration) IsSQLite() bool {
	return strings.Contains(this.BackendDB, "sqlite")
}

func (this *Configuration) IsMySQL() bool {
	return this.BackendDB == "mysql" || this.BackendDB == ""
}

// read reads configuration from given file, or silently skips if the file does not exist.
// If the file does exist, then it is expected to be in valid JSON format or the function bails out.
func read(fileName string) (*Configuration, error) {
	if fileName == "" {
		return Config, fmt.Errorf("Empty file name")
	}
	file, err := os.Open(fileName)
	if err != nil {
		return Config, err
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(Config)
	if err == nil {
		log.Infof("Read config: %s", fileName)
	} else {
		log.Fatal("Cannot read config file:", fileName, err)
	}
	if err := Config.postReadAdjustments(); err != nil {
		log.Fatale(err)
	}
	return Config, err
}

// Read reads configuration from zero, either, some or all given files, in order of input.
// A file can override configuration provided in previous file.
func Read(fileNames ...string) *Configuration {
	for _, fileName := range fileNames {
		read(fileName)
	}
	readFileNames = fileNames
	return Config
}

// ForceRead reads configuration from given file name or bails out if it fails
func ForceRead(fileName string) *Configuration {
	_, err := read(fileName)
	if err != nil {
		log.Fatal("Cannot read config file:", fileName, err)
	}
	readFileNames = []string{fileName}
	return Config
}

// Reload re-reads configuration from last used files
func Reload(extraFileNames ...string) *Configuration {
	for _, fileName := range readFileNames {
		read(fileName)
	}
	for _, fileName := range extraFileNames {
		read(fileName)
	}
	return Config
}

// MarkConfigurationLoaded is called once configuration has first been loaded.
// Listeners on ConfigurationLoaded will get a notification
func MarkConfigurationLoaded() {
	go func() {
		for {
			configurationLoaded <- true
		}
	}()
	// wait for it
	<-configurationLoaded
}

// WaitForConfigurationToBeLoaded does just that. It will return after
// the configuration file has been read off disk.
func WaitForConfigurationToBeLoaded() {
	<-configurationLoaded
}
