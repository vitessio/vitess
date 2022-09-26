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

	"vitess.io/vitess/go/vt/log"

	"gopkg.in/gcfg.v1"
)

var (
	envVariableRegexp = regexp.MustCompile("[$][{](.*)[}]")
)

const (
	LostInRecoveryDowntimeSeconds int = 60 * 60 * 24 * 365
	DefaultStatusAPIEndpoint          = "/api/status"
)

var configurationLoaded = make(chan bool)

const (
	HealthPollSeconds                     = 1
	ActiveNodeExpireSeconds               = 5
	MaintenanceOwner                      = "vtorc"
	AuditPageSize                         = 20
	MaintenancePurgeDays                  = 7
	MySQLTopologyMaxPoolConnections       = 3
	MaintenanceExpireMinutes              = 10
	DebugMetricsIntervalSeconds           = 10
	StaleInstanceCoordinatesExpireSeconds = 60
)

// Configuration makes for vtorc configuration input, which can be provided by user via JSON formatted file.
// Some of the parameteres have reasonable default values, and some (like database credentials) are
// strictly expected from user.
// TODO(sougou): change this to yaml parsing, and possible merge with tabletenv.
type Configuration struct {
	Debug                                       bool   // set debug mode (similar to --debug option)
	EnableSyslog                                bool   // Should logs be directed (in addition) to syslog daemon?
	ListenAddress                               string // Where vtorc HTTP should listen for TCP
	ListenSocket                                string // Where vtorc HTTP should listen for unix socket (default: empty; when given, TCP is disabled)
	HTTPAdvertise                               string // optional, for raft setups, what is the HTTP address this node will advertise to its peers (potentially use where behind NAT or when rerouting ports; example: "http://11.22.33.44:3030")
	AgentsServerPort                            string // port vtorc agents talk back to
	MySQLTopologyUser                           string // The user VTOrc will use to connect to MySQL instances
	MySQLTopologyPassword                       string // The password VTOrc will use to connect to MySQL instances
	MySQLReplicaUser                            string // User to set on replica MySQL instances while configuring replication settings on them. If set, use this credential instead of discovering from mysql. TODO(sougou): deprecate this in favor of fetching from vttablet
	MySQLReplicaPassword                        string // Password to set on replica MySQL instances while configuring replication settings on them.
	MySQLTopologyCredentialsConfigFile          string // my.cnf style configuration file from where to pick credentials. Expecting `user`, `password` under `[client]` section
	MySQLTopologySSLPrivateKeyFile              string // Private key file used to authenticate with a Topology mysql instance with TLS
	MySQLTopologySSLCertFile                    string // Certificate PEM file used to authenticate with a Topology mysql instance with TLS
	MySQLTopologySSLCAFile                      string // Certificate Authority PEM file used to authenticate with a Topology mysql instance with TLS
	MySQLTopologySSLSkipVerify                  bool   // If true, do not strictly validate mutual TLS certs for Topology mysql instances
	MySQLTopologyUseMutualTLS                   bool   // Turn on TLS authentication with the Topology MySQL instances
	MySQLTopologyUseMixedTLS                    bool   // Mixed TLS and non-TLS authentication with the Topology MySQL instances
	TLSCacheTTLFactor                           uint   // Factor of InstancePollSeconds that we set as TLS info cache expiry
	BackendDB                                   string // EXPERIMENTAL: type of backend db; either "mysql" or "sqlite3"
	SQLite3DataFile                             string // when BackendDB == "sqlite3", full path to sqlite3 datafile
	SkipOrchestratorDatabaseUpdate              bool   // When true, do not check backend database schema nor attempt to update it. Useful when you may be running multiple versions of vtorc, and you only wish certain boxes to dictate the db structure (or else any time a different vtorc version runs it will rebuild database schema)
	PanicIfDifferentDatabaseDeploy              bool   // When true, and this process finds the vtorc backend DB was provisioned by a different version, panic
	RaftEnabled                                 bool   // When true, setup vtorc in a raft consensus layout. When false (default) all Raft* variables are ignored
	RaftBind                                    string
	RaftAdvertise                               string
	RaftDataDir                                 string
	DefaultRaftPort                             int      // if a RaftNodes entry does not specify port, use this one
	RaftNodes                                   []string // Raft nodes to make initial connection with
	ExpectFailureAnalysisConcensus              bool
	MySQLVTOrcHost                              string
	MySQLVTOrcMaxPoolConnections                int // The maximum size of the connection pool to the VTOrc backend.
	MySQLVTOrcPort                              uint
	MySQLVTOrcDatabase                          string
	MySQLVTOrcUser                              string
	MySQLVTOrcPassword                          string
	MySQLVTOrcCredentialsConfigFile             string   // my.cnf style configuration file from where to pick credentials. Expecting `user`, `password` under `[client]` section
	MySQLVTOrcSSLPrivateKeyFile                 string   // Private key file used to authenticate with the VTOrc mysql instance with TLS
	MySQLVTOrcSSLCertFile                       string   // Certificate PEM file used to authenticate with the VTOrc mysql instance with TLS
	MySQLVTOrcSSLCAFile                         string   // Certificate Authority PEM file used to authenticate with the VTOrc mysql instance with TLS
	MySQLVTOrcSSLSkipVerify                     bool     // If true, do not strictly validate mutual TLS certs for the VTOrc mysql instances
	MySQLVTOrcUseMutualTLS                      bool     // Turn on TLS authentication with the VTOrc MySQL instance
	MySQLVTOrcReadTimeoutSeconds                int      // Number of seconds before backend mysql read operation is aborted (driver-side)
	MySQLVTOrcRejectReadOnly                    bool     // Reject read only connections https://github.com/go-sql-driver/mysql#rejectreadonly
	MySQLConnectTimeoutSeconds                  int      // Number of seconds before connection is aborted (driver-side)
	MySQLDiscoveryReadTimeoutSeconds            int      // Number of seconds before topology mysql read operation is aborted (driver-side). Used for discovery queries.
	MySQLTopologyReadTimeoutSeconds             int      // Number of seconds before topology mysql read operation is aborted (driver-side). Used for all but discovery queries.
	MySQLConnectionLifetimeSeconds              int      // Number of seconds the mysql driver will keep database connection alive before recycling it
	DefaultInstancePort                         int      // In case port was not specified on command line
	ReplicationLagQuery                         string   // custom query to check on replica lg (e.g. heartbeat table). Must return a single row with a single numeric column, which is the lag.
	ReplicationCredentialsQuery                 string   // custom query to get replication credentials. Must return a single row, with two text columns: 1st is username, 2nd is password. This is optional, and can be used by vtorc to configure replication after primary takeover or setup of co-primary. You need to ensure the vtorc user has the privileges to run this query
	DiscoverByShowSlaveHosts                    bool     // Attempt SHOW SLAVE HOSTS before PROCESSLIST
	UseSuperReadOnly                            bool     // Should vtorc super_read_only any time it sets read_only
	InstancePollSeconds                         uint     // Number of seconds between instance reads
	InstanceWriteBufferSize                     int      // Instance write buffer size (max number of instances to flush in one INSERT ODKU)
	BufferInstanceWrites                        bool     // Set to 'true' for write-optimization on backend table (compromise: writes can be stale and overwrite non stale data)
	InstanceFlushIntervalMilliseconds           int      // Max interval between instance write buffer flushes
	UnseenInstanceForgetHours                   uint     // Number of hours after which an unseen instance is forgotten
	SnapshotTopologiesIntervalHours             uint     // Interval in hour between snapshot-topologies invocation. Default: 0 (disabled)
	DiscoveryMaxConcurrency                     uint     // Number of goroutines doing hosts discovery
	DiscoveryQueueCapacity                      uint     // Buffer size of the discovery queue. Should be greater than the number of DB instances being discovered
	DiscoveryQueueMaxStatisticsSize             int      // The maximum number of individual secondly statistics taken of the discovery queue
	DiscoveryCollectionRetentionSeconds         uint     // Number of seconds to retain the discovery collection information
	DiscoverySeeds                              []string // Hard coded array of hostname:port, ensuring vtorc discovers these hosts upon startup, assuming not already known to vtorc
	InstanceBulkOperationsWaitTimeoutSeconds    uint     // Time to wait on a single instance when doing bulk (many instances) operation
	HostnameResolveMethod                       string   // Method by which to "normalize" hostname ("none"/"default"/"cname")
	MySQLHostnameResolveMethod                  string   // Method by which to "normalize" hostname via MySQL server. ("none"/"@@hostname"/"@@report_host"; default "@@hostname")
	SkipBinlogServerUnresolveCheck              bool     // Skip the double-check that an unresolved hostname resolves back to same hostname for binlog servers
	ExpiryHostnameResolvesMinutes               int      // Number of minutes after which to expire hostname-resolves
	RejectHostnameResolvePattern                string   // Regexp pattern for resolved hostname that will not be accepted (not cached, not written to db). This is done to avoid storing wrong resolves due to network glitches.
	ReasonableReplicationLagSeconds             int      // Above this value is considered a problem
	ProblemIgnoreHostnameFilters                []string // Will minimize problem visualization for hostnames matching given regexp filters
	VerifyReplicationFilters                    bool     // Include replication filters check before approving topology refactoring
	ReasonableMaintenanceReplicationLagSeconds  int      // Above this value move-up and move-below are blocked
	CandidateInstanceExpireMinutes              uint     // Minutes after which a suggestion to use an instance as a candidate replica (to be preferably promoted on primary failover) is expired.
	AuditLogFile                                string   // Name of log file for audit operations. Disabled when empty.
	AuditToSyslog                               bool     // If true, audit messages are written to syslog
	AuditToBackendDB                            bool     // If true, audit messages are written to the backend DB's `audit` table (default: true)
	AuditPurgeDays                              uint     // Days after which audit entries are purged from the database
	RemoveTextFromHostnameDisplay               string   // Text to strip off the hostname on cluster/clusters pages
	ReadOnly                                    bool
	AuthenticationMethod                        string // Type of autherntication to use, if any. "" for none, "basic" for BasicAuth, "multi" for advanced BasicAuth, "proxy" for forwarded credentials via reverse proxy, "token" for token based access
	OAuthClientID                               string
	OAuthClientSecret                           string
	OAuthScopes                                 []string
	HTTPAuthUser                                string            // Username for HTTP Basic authentication (blank disables authentication)
	HTTPAuthPassword                            string            // Password for HTTP Basic authentication
	AuthUserHeader                              string            // HTTP header indicating auth user, when AuthenticationMethod is "proxy"
	PowerAuthUsers                              []string          // On AuthenticationMethod == "proxy", list of users that can make changes. All others are read-only.
	PowerAuthGroups                             []string          // list of unix groups the authenticated user must be a member of to make changes.
	AccessTokenUseExpirySeconds                 uint              // Time by which an issued token must be used
	AccessTokenExpiryMinutes                    uint              // Time after which HTTP access token expires
	ClusterNameToAlias                          map[string]string // map between regex matching cluster name to a human friendly alias
	DetectClusterAliasQuery                     string            // Optional query (executed on topology instance) that returns the alias of a cluster. Query will only be executed on cluster primary (though until the topology's primary is resovled it may execute on other/all replicas). If provided, must return one row, one column
	DetectClusterDomainQuery                    string            // Optional query (executed on topology instance) that returns the VIP/CNAME/Alias/whatever domain name for the primary of this cluster. Query will only be executed on cluster primary (though until the topology's primary is resovled it may execute on other/all replicas). If provided, must return one row, one column
	DetectInstanceAliasQuery                    string            // Optional query (executed on topology instance) that returns the alias of an instance. If provided, must return one row, one column
	DetectPromotionRuleQuery                    string            // Optional query (executed on topology instance) that returns the promotion rule of an instance. If provided, must return one row, one column.
	DataCenterPattern                           string            // Regexp pattern with one group, extracting the datacenter name from the hostname
	RegionPattern                               string            // Regexp pattern with one group, extracting the region name from the hostname
	PhysicalEnvironmentPattern                  string            // Regexp pattern with one group, extracting physical environment info from hostname (e.g. combination of datacenter & prod/dev env)
	DetectDataCenterQuery                       string            // Optional query (executed on topology instance) that returns the data center of an instance. If provided, must return one row, one column. Overrides DataCenterPattern and useful for installments where DC cannot be inferred by hostname
	DetectRegionQuery                           string            // Optional query (executed on topology instance) that returns the region of an instance. If provided, must return one row, one column. Overrides RegionPattern and useful for installments where Region cannot be inferred by hostname
	DetectPhysicalEnvironmentQuery              string            // Optional query (executed on topology instance) that returns the physical environment of an instance. If provided, must return one row, one column. Overrides PhysicalEnvironmentPattern and useful for installments where env cannot be inferred by hostname
	DetectSemiSyncEnforcedQuery                 string            // Optional query (executed on topology instance) to determine whether semi-sync is fully enforced for primary writes (async fallback is not allowed under any circumstance). If provided, must return one row, one column, value 0 or 1.
	SupportFuzzyPoolHostnames                   bool              // Should "submit-pool-instances" command be able to pass list of fuzzy instances (fuzzy means non-fqdn, but unique enough to recognize). Defaults 'true', implies more queries on backend db
	InstancePoolExpiryMinutes                   uint              // Time after which entries in database_instance_pool are expired (resubmit via `submit-pool-instances`)
	PromotionIgnoreHostnameFilters              []string          // VTOrc will not promote replicas with hostname matching pattern (via -c recovery; for example, avoid promoting dev-dedicated machines)
	ServeAgentsHTTP                             bool              // Spawn another HTTP interface dedicated for vtorc-agent
	AgentsUseSSL                                bool              // When "true" vtorc will listen on agents port with SSL as well as connect to agents via SSL
	AgentsUseMutualTLS                          bool              // When "true" Use mutual TLS for the server to agent communication
	AgentSSLSkipVerify                          bool              // When using SSL for the Agent, should we ignore SSL certification error
	AgentSSLPrivateKeyFile                      string            // Name of Agent SSL private key file, applies only when AgentsUseSSL = true
	AgentSSLCertFile                            string            // Name of Agent SSL certification file, applies only when AgentsUseSSL = true
	AgentSSLCAFile                              string            // Name of the Agent Certificate Authority file, applies only when AgentsUseSSL = true
	AgentSSLValidOUs                            []string          // Valid organizational units when using mutual TLS to communicate with the agents
	UseSSL                                      bool              // Use SSL on the server web port
	UseMutualTLS                                bool              // When "true" Use mutual TLS for the server's web and API connections
	SSLSkipVerify                               bool              // When using SSL, should we ignore SSL certification error
	SSLPrivateKeyFile                           string            // Name of SSL private key file, applies only when UseSSL = true
	SSLCertFile                                 string            // Name of SSL certification file, applies only when UseSSL = true
	SSLCAFile                                   string            // Name of the Certificate Authority file, applies only when UseSSL = true
	SSLValidOUs                                 []string          // Valid organizational units when using mutual TLS
	StatusEndpoint                              string            // Override the status endpoint.  Defaults to '/api/status'
	StatusOUVerify                              bool              // If true, try to verify OUs when Mutual TLS is on.  Defaults to false
	AgentPollMinutes                            uint              // Minutes between agent polling
	UnseenAgentForgetHours                      uint              // Number of hours after which an unseen agent is forgotten
	StaleSeedFailMinutes                        uint              // Number of minutes after which a stale (no progress) seed is considered failed.
	SeedAcceptableBytesDiff                     int64             // Difference in bytes between seed source & target data size that is still considered as successful copy
	SeedWaitSecondsBeforeSend                   int64             // Number of seconds for waiting before start send data command on agent
	BinlogEventsChunkSize                       int               // Chunk size (X) for SHOW BINLOG|RELAYLOG EVENTS LIMIT ?,X statements. Smaller means less locking and mroe work to be done
	ReduceReplicationAnalysisCount              bool              // When true, replication analysis will only report instances where possibility of handled problems is possible in the first place (e.g. will not report most leaf nodes, that are mostly uninteresting). When false, provides an entry for every known instance
	FailureDetectionPeriodBlockMinutes          int               // The time for which an instance's failure discovery is kept "active", so as to avoid concurrent "discoveries" of the instance's failure; this preceeds any recovery process, if any.
	RecoveryPeriodBlockMinutes                  int               // (supported for backwards compatibility but please use newer `RecoveryPeriodBlockSeconds` instead) The time for which an instance's recovery is kept "active", so as to avoid concurrent recoveries on smae instance as well as flapping
	RecoveryPeriodBlockSeconds                  int               // (overrides `RecoveryPeriodBlockMinutes`) The time for which an instance's recovery is kept "active", so as to avoid concurrent recoveries on smae instance as well as flapping
	RecoveryIgnoreHostnameFilters               []string          // Recovery analysis will completely ignore hosts matching given patterns
	RecoverPrimaryClusterFilters                []string          // Only do primary recovery on clusters matching these regexp patterns (of course the ".*" pattern matches everything)
	RecoverIntermediatePrimaryClusterFilters    []string          // Only do IM recovery on clusters matching these regexp patterns (of course the ".*" pattern matches everything)
	ProcessesShellCommand                       string            // Shell that executes command scripts
	OnFailureDetectionProcesses                 []string          // Processes to execute when detecting a failover scenario (before making a decision whether to failover or not). May and should use some of these placeholders: {failureType}, {instanceType}, {isPrimary}, {isCoPrimary}, {failureDescription}, {command}, {failedHost}, {failureCluster}, {failureClusterDomain}, {failedPort}, {successorHost}, {successorPort}, {successorAlias}, {countReplicas}, {replicaHosts}, {isDowntimed}, {autoPrimaryRecovery}, {autoIntermediatePrimaryRecovery}
	PreFailoverProcesses                        []string          // Processes to execute before doing a failover (aborting operation should any once of them exits with non-zero code; order of execution undefined). May and should use some of these placeholders: {failureType}, {instanceType}, {isPrimary}, {isCoPrimary}, {failureDescription}, {command}, {failedHost}, {failureCluster}, {failureClusterDomain}, {failedPort}, {countReplicas}, {replicaHosts}, {isDowntimed}
	PostFailoverProcesses                       []string          // Processes to execute after doing a failover (order of execution undefined). May and should use some of these placeholders: {failureType}, {instanceType}, {isPrimary}, {isCoPrimary}, {failureDescription}, {command}, {failedHost}, {failureCluster}, {failureClusterDomain}, {failedPort}, {successorHost}, {successorPort}, {successorAlias}, {countReplicas}, {replicaHosts}, {isDowntimed}, {isSuccessful}, {lostReplicas}, {countLostReplicas}
	PostUnsuccessfulFailoverProcesses           []string          // Processes to execute after a not-completely-successful failover (order of execution undefined). May and should use some of these placeholders: {failureType}, {instanceType}, {isPrimary}, {isCoPrimary}, {failureDescription}, {command}, {failedHost}, {failureCluster}, {failureClusterDomain}, {failedPort}, {successorHost}, {successorPort}, {successorAlias}, {countReplicas}, {replicaHosts}, {isDowntimed}, {isSuccessful}, {lostReplicas}, {countLostReplicas}
	PostPrimaryFailoverProcesses                []string          // Processes to execute after doing a primary failover (order of execution undefined). Uses same placeholders as PostFailoverProcesses
	PostIntermediatePrimaryFailoverProcesses    []string          // Processes to execute after doing a primary failover (order of execution undefined). Uses same placeholders as PostFailoverProcesses
	PostTakePrimaryProcesses                    []string          // Processes to execute after a successful Take-Primary event has taken place
	CoPrimaryRecoveryMustPromoteOtherCoPrimary  bool              // When 'false', anything can get promoted (and candidates are prefered over others). When 'true', vtorc will promote the other co-primary or else fail
	DetachLostReplicasAfterPrimaryFailover      bool              // Should replicas that are not to be lost in primary recovery (i.e. were more up-to-date than promoted replica) be forcibly detached
	ApplyMySQLPromotionAfterPrimaryFailover     bool              // Should vtorc take upon itself to apply MySQL primary promotion: set read_only=0, detach replication, etc.
	PreventCrossDataCenterPrimaryFailover       bool              // When true (default: false), cross-DC primary failover are not allowed, vtorc will do all it can to only fail over within same DC, or else not fail over at all.
	PreventCrossRegionPrimaryFailover           bool              // When true (default: false), cross-region primary failover are not allowed, vtorc will do all it can to only fail over within same region, or else not fail over at all.
	PrimaryFailoverLostInstancesDowntimeMinutes uint              // Number of minutes to downtime any server that was lost after a primary failover (including failed primary & lost replicas). 0 to disable
	PrimaryFailoverDetachReplicaPrimaryHost     bool              // Should vtorc issue a detach-replica-primary-host on newly promoted primary (this makes sure the new primary will not attempt to replicate old primary if that comes back to life). Defaults 'false'. Meaningless if ApplyMySQLPromotionAfterPrimaryFailover is 'true'.
	FailPrimaryPromotionOnLagMinutes            uint              // when > 0, fail a primary promotion if the candidate replica is lagging >= configured number of minutes.
	FailPrimaryPromotionIfSQLThreadNotUpToDate  bool              // when true, and a primary failover takes place, if candidate primary has not consumed all relay logs, promotion is aborted with error
	DelayPrimaryPromotionIfSQLThreadNotUpToDate bool              // when true, and a primary failover takes place, if candidate primary has not consumed all relay logs, delay promotion until the sql thread has caught up
	PostponeReplicaRecoveryOnLagMinutes         uint              // On crash recovery, replicas that are lagging more than given minutes are only resurrected late in the recovery process, after primary/IM has been elected and processes executed. Value of 0 disables this feature
	OSCIgnoreHostnameFilters                    []string          // OSC replicas recommendation will ignore replica hostnames matching given patterns
	URLPrefix                                   string            // URL prefix to run vtorc on non-root web path, e.g. /vtorc to put it behind nginx.
	DiscoveryIgnoreReplicaHostnameFilters       []string          // Regexp filters to apply to prevent auto-discovering new replicas. Usage: unreachable servers due to firewalls, applications which trigger binlog dumps
	DiscoveryIgnorePrimaryHostnameFilters       []string          // Regexp filters to apply to prevent auto-discovering a primary. Usage: pointing your primary temporarily to replicate seom data from external host
	DiscoveryIgnoreHostnameFilters              []string          // Regexp filters to apply to prevent discovering instances of any kind
	WebMessage                                  string            // If provided, will be shown on all web pages below the title bar
	MaxConcurrentReplicaOperations              int               // Maximum number of concurrent operations on replicas
	InstanceDBExecContextTimeoutSeconds         int               // Timeout on context used while calling ExecContext on instance database
	LockShardTimeoutSeconds                     int               // Timeout on context used to lock shard. Should be a small value because we should fail-fast
	WaitReplicasTimeoutSeconds                  int               // Timeout on amount of time to wait for the replicas in case of ERS. Should be a small value because we should fail-fast. Should not be larger than LockShardTimeoutSeconds since that is the total time we use for an ERS.
	TopoInformationRefreshSeconds               int               // Timer duration on which VTOrc refreshes the keyspace and vttablet records from the topo-server.
	RecoveryPollSeconds                         int               // Timer duration on which VTOrc recovery analysis runs
}

// ToJSONString will marshal this configuration as JSON
func (config *Configuration) ToJSONString() string {
	b, _ := json.Marshal(config)
	return string(b)
}

// Config is *the* configuration instance, used globally to get configuration data
var Config = newConfiguration()
var readFileNames []string

func newConfiguration() *Configuration {
	return &Configuration{
		Debug:                                       false,
		EnableSyslog:                                false,
		ListenAddress:                               ":3000",
		ListenSocket:                                "",
		HTTPAdvertise:                               "",
		AgentsServerPort:                            ":3001",
		StatusEndpoint:                              DefaultStatusAPIEndpoint,
		StatusOUVerify:                              false,
		BackendDB:                                   "sqlite",
		SQLite3DataFile:                             "file::memory:?mode=memory&cache=shared",
		SkipOrchestratorDatabaseUpdate:              false,
		PanicIfDifferentDatabaseDeploy:              false,
		RaftBind:                                    "127.0.0.1:10008",
		RaftAdvertise:                               "",
		RaftDataDir:                                 "",
		DefaultRaftPort:                             10008,
		RaftNodes:                                   []string{},
		ExpectFailureAnalysisConcensus:              true,
		MySQLVTOrcMaxPoolConnections:                128, // limit concurrent conns to backend DB
		MySQLVTOrcPort:                              3306,
		MySQLTopologyUseMutualTLS:                   false,
		MySQLTopologyUseMixedTLS:                    true,
		MySQLVTOrcUseMutualTLS:                      false,
		MySQLConnectTimeoutSeconds:                  2,
		MySQLVTOrcReadTimeoutSeconds:                30,
		MySQLVTOrcRejectReadOnly:                    false,
		MySQLDiscoveryReadTimeoutSeconds:            10,
		MySQLTopologyReadTimeoutSeconds:             600,
		MySQLConnectionLifetimeSeconds:              0,
		DefaultInstancePort:                         3306,
		TLSCacheTTLFactor:                           100,
		InstancePollSeconds:                         5,
		InstanceWriteBufferSize:                     100,
		BufferInstanceWrites:                        false,
		InstanceFlushIntervalMilliseconds:           100,
		UnseenInstanceForgetHours:                   240,
		SnapshotTopologiesIntervalHours:             0,
		DiscoverByShowSlaveHosts:                    false,
		UseSuperReadOnly:                            false,
		DiscoveryMaxConcurrency:                     300,
		DiscoveryQueueCapacity:                      100000,
		DiscoveryQueueMaxStatisticsSize:             120,
		DiscoveryCollectionRetentionSeconds:         120,
		DiscoverySeeds:                              []string{},
		InstanceBulkOperationsWaitTimeoutSeconds:    10,
		HostnameResolveMethod:                       "default",
		MySQLHostnameResolveMethod:                  "none",
		SkipBinlogServerUnresolveCheck:              true,
		ExpiryHostnameResolvesMinutes:               60,
		RejectHostnameResolvePattern:                "",
		ReasonableReplicationLagSeconds:             10,
		ProblemIgnoreHostnameFilters:                []string{},
		VerifyReplicationFilters:                    false,
		ReasonableMaintenanceReplicationLagSeconds:  20,
		CandidateInstanceExpireMinutes:              60,
		AuditLogFile:                                "",
		AuditToSyslog:                               false,
		AuditToBackendDB:                            false,
		AuditPurgeDays:                              7,
		RemoveTextFromHostnameDisplay:               "",
		ReadOnly:                                    false,
		AuthenticationMethod:                        "",
		HTTPAuthUser:                                "",
		HTTPAuthPassword:                            "",
		AuthUserHeader:                              "X-Forwarded-User",
		PowerAuthUsers:                              []string{"*"},
		PowerAuthGroups:                             []string{},
		AccessTokenUseExpirySeconds:                 60,
		AccessTokenExpiryMinutes:                    1440,
		ClusterNameToAlias:                          make(map[string]string),
		DetectClusterAliasQuery:                     "",
		DetectClusterDomainQuery:                    "",
		DetectInstanceAliasQuery:                    "",
		DetectPromotionRuleQuery:                    "",
		DataCenterPattern:                           "",
		PhysicalEnvironmentPattern:                  "",
		DetectDataCenterQuery:                       "",
		DetectPhysicalEnvironmentQuery:              "",
		DetectSemiSyncEnforcedQuery:                 "",
		SupportFuzzyPoolHostnames:                   true,
		InstancePoolExpiryMinutes:                   60,
		PromotionIgnoreHostnameFilters:              []string{},
		ServeAgentsHTTP:                             false,
		AgentsUseSSL:                                false,
		AgentsUseMutualTLS:                          false,
		AgentSSLValidOUs:                            []string{},
		AgentSSLSkipVerify:                          false,
		AgentSSLPrivateKeyFile:                      "",
		AgentSSLCertFile:                            "",
		AgentSSLCAFile:                              "",
		UseSSL:                                      false,
		UseMutualTLS:                                false,
		SSLValidOUs:                                 []string{},
		SSLSkipVerify:                               false,
		SSLPrivateKeyFile:                           "",
		SSLCertFile:                                 "",
		SSLCAFile:                                   "",
		AgentPollMinutes:                            60,
		UnseenAgentForgetHours:                      6,
		StaleSeedFailMinutes:                        60,
		SeedAcceptableBytesDiff:                     8192,
		SeedWaitSecondsBeforeSend:                   2,
		BinlogEventsChunkSize:                       10000,
		ReduceReplicationAnalysisCount:              true,
		FailureDetectionPeriodBlockMinutes:          60,
		RecoveryPeriodBlockMinutes:                  60,
		RecoveryPeriodBlockSeconds:                  3600,
		RecoveryIgnoreHostnameFilters:               []string{},
		RecoverPrimaryClusterFilters:                []string{"*"},
		RecoverIntermediatePrimaryClusterFilters:    []string{},
		ProcessesShellCommand:                       "bash",
		OnFailureDetectionProcesses:                 []string{},
		PreFailoverProcesses:                        []string{},
		PostPrimaryFailoverProcesses:                []string{},
		PostIntermediatePrimaryFailoverProcesses:    []string{},
		PostFailoverProcesses:                       []string{},
		PostUnsuccessfulFailoverProcesses:           []string{},
		PostTakePrimaryProcesses:                    []string{},
		CoPrimaryRecoveryMustPromoteOtherCoPrimary:  true,
		DetachLostReplicasAfterPrimaryFailover:      true,
		ApplyMySQLPromotionAfterPrimaryFailover:     true,
		PreventCrossDataCenterPrimaryFailover:       false,
		PreventCrossRegionPrimaryFailover:           false,
		PrimaryFailoverLostInstancesDowntimeMinutes: 0,
		PrimaryFailoverDetachReplicaPrimaryHost:     false,
		FailPrimaryPromotionOnLagMinutes:            0,
		FailPrimaryPromotionIfSQLThreadNotUpToDate:  false,
		DelayPrimaryPromotionIfSQLThreadNotUpToDate: true,
		PostponeReplicaRecoveryOnLagMinutes:         0,
		OSCIgnoreHostnameFilters:                    []string{},
		URLPrefix:                                   "",
		DiscoveryIgnoreReplicaHostnameFilters:       []string{},
		WebMessage:                                  "",
		MaxConcurrentReplicaOperations:              5,
		InstanceDBExecContextTimeoutSeconds:         30,
		LockShardTimeoutSeconds:                     30,
		WaitReplicasTimeoutSeconds:                  30,
		TopoInformationRefreshSeconds:               15,
		RecoveryPollSeconds:                         1,
	}
}

func (config *Configuration) postReadAdjustments() error {
	if config.MySQLVTOrcCredentialsConfigFile != "" {
		mySQLConfig := struct {
			Client struct {
				User     string
				Password string
			}
		}{}
		err := gcfg.ReadFileInto(&mySQLConfig, config.MySQLVTOrcCredentialsConfigFile)
		if err != nil {
			log.Fatalf("Failed to parse gcfg data from file: %+v", err)
		} else {
			log.Infof("Parsed vtorc credentials from %s", config.MySQLVTOrcCredentialsConfigFile)
			config.MySQLVTOrcUser = mySQLConfig.Client.User
			config.MySQLVTOrcPassword = mySQLConfig.Client.Password
		}
	}
	{
		// We accept password in the form "${SOME_ENV_VARIABLE}" in which case we pull
		// the given variable from os env
		submatch := envVariableRegexp.FindStringSubmatch(config.MySQLVTOrcPassword)
		if len(submatch) > 1 {
			config.MySQLVTOrcPassword = os.Getenv(submatch[1])
		}
	}
	if config.MySQLTopologyCredentialsConfigFile != "" {
		mySQLConfig := struct {
			Client struct {
				User     string
				Password string
			}
		}{}
		err := gcfg.ReadFileInto(&mySQLConfig, config.MySQLTopologyCredentialsConfigFile)
		if err != nil {
			log.Fatalf("Failed to parse gcfg data from file: %+v", err)
		} else {
			log.Infof("Parsed topology credentials from %s", config.MySQLTopologyCredentialsConfigFile)
			config.MySQLTopologyUser = mySQLConfig.Client.User
			config.MySQLTopologyPassword = mySQLConfig.Client.Password
		}
	}
	{
		// We accept password in the form "${SOME_ENV_VARIABLE}" in which case we pull
		// the given variable from os env
		submatch := envVariableRegexp.FindStringSubmatch(config.MySQLTopologyPassword)
		if len(submatch) > 1 {
			config.MySQLTopologyPassword = os.Getenv(submatch[1])
		}
	}

	if config.RecoveryPeriodBlockSeconds == 0 && config.RecoveryPeriodBlockMinutes > 0 {
		// RecoveryPeriodBlockSeconds is a newer addition that overrides RecoveryPeriodBlockMinutes
		// The code does not consider RecoveryPeriodBlockMinutes anymore, but RecoveryPeriodBlockMinutes
		// still supported in config file for backwards compatibility
		config.RecoveryPeriodBlockSeconds = config.RecoveryPeriodBlockMinutes * 60
	}

	if config.FailPrimaryPromotionIfSQLThreadNotUpToDate && config.DelayPrimaryPromotionIfSQLThreadNotUpToDate {
		return fmt.Errorf("Cannot have both FailPrimaryPromotionIfSQLThreadNotUpToDate and DelayPrimaryPromotionIfSQLThreadNotUpToDate enabled")
	}
	if config.FailPrimaryPromotionOnLagMinutes > 0 && config.ReplicationLagQuery == "" {
		return fmt.Errorf("nonzero FailPrimaryPromotionOnLagMinutes requires ReplicationLagQuery to be set")
	}

	if config.URLPrefix != "" {
		// Ensure the prefix starts with "/" and has no trailing one.
		config.URLPrefix = strings.TrimLeft(config.URLPrefix, "/")
		config.URLPrefix = strings.TrimRight(config.URLPrefix, "/")
		config.URLPrefix = "/" + config.URLPrefix
	}

	if config.IsSQLite() && config.SQLite3DataFile == "" {
		return fmt.Errorf("SQLite3DataFile must be set when BackendDB is sqlite3")
	}
	if config.RaftEnabled && config.RaftDataDir == "" {
		return fmt.Errorf("RaftDataDir must be defined since raft is enabled (RaftEnabled)")
	}
	if config.RaftEnabled && config.RaftBind == "" {
		return fmt.Errorf("RaftBind must be defined since raft is enabled (RaftEnabled)")
	}
	if config.RaftAdvertise == "" {
		config.RaftAdvertise = config.RaftBind
	}
	if config.HTTPAdvertise != "" {
		u, err := url.Parse(config.HTTPAdvertise)
		if err != nil {
			return fmt.Errorf("Failed parsing HTTPAdvertise %s: %s", config.HTTPAdvertise, err.Error())
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
		if config.InstanceWriteBufferSize <= 0 {
			config.BufferInstanceWrites = false
		}
	}
	return nil
}

func (config *Configuration) IsSQLite() bool {
	return strings.Contains(config.BackendDB, "sqlite")
}

func (config *Configuration) IsMySQL() bool {
	return config.BackendDB == "mysql" || config.BackendDB == ""
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
		log.Fatal(err)
	}
	return Config, err
}

// Read reads configuration from zero, either, some or all given files, in order of input.
// A file can override configuration provided in previous file.
func Read(fileNames ...string) *Configuration {
	for _, fileName := range fileNames {
		_, _ = read(fileName)
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
		_, _ = read(fileName)
	}
	for _, fileName := range extraFileNames {
		_, _ = read(fileName)
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
