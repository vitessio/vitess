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
	MySQLTopologyUser                          string // The user VTOrc will use to connect to MySQL instances
	MySQLTopologyPassword                      string // The password VTOrc will use to connect to MySQL instances
	MySQLReplicaUser                           string // User to set on replica MySQL instances while configuring replication settings on them. If set, use this credential instead of discovering from mysql. TODO(sougou): deprecate this in favor of fetching from vttablet
	MySQLReplicaPassword                       string // Password to set on replica MySQL instances while configuring replication settings on them.
	MySQLTopologySSLPrivateKeyFile             string // Private key file used to authenticate with a Topology mysql instance with TLS
	MySQLTopologySSLCertFile                   string // Certificate PEM file used to authenticate with a Topology mysql instance with TLS
	MySQLTopologySSLCAFile                     string // Certificate Authority PEM file used to authenticate with a Topology mysql instance with TLS
	MySQLTopologySSLSkipVerify                 bool   // If true, do not strictly validate mutual TLS certs for Topology mysql instances
	MySQLTopologyUseMutualTLS                  bool   // Turn on TLS authentication with the Topology MySQL instances
	MySQLTopologyUseMixedTLS                   bool   // Mixed TLS and non-TLS authentication with the Topology MySQL instances
	TLSCacheTTLFactor                          uint   // Factor of InstancePollSeconds that we set as TLS info cache expiry
	SQLite3DataFile                            string // full path to sqlite3 datafile
	MySQLVTOrcHost                             string
	MySQLVTOrcMaxPoolConnections               int // The maximum size of the connection pool to the VTOrc backend.
	MySQLVTOrcPort                             uint
	MySQLVTOrcDatabase                         string
	MySQLVTOrcUser                             string
	MySQLVTOrcPassword                         string
	MySQLVTOrcCredentialsConfigFile            string   // my.cnf style configuration file from where to pick credentials. Expecting `user`, `password` under `[client]` section
	MySQLVTOrcSSLPrivateKeyFile                string   // Private key file used to authenticate with the VTOrc mysql instance with TLS
	MySQLVTOrcSSLCertFile                      string   // Certificate PEM file used to authenticate with the VTOrc mysql instance with TLS
	MySQLVTOrcSSLCAFile                        string   // Certificate Authority PEM file used to authenticate with the VTOrc mysql instance with TLS
	MySQLVTOrcSSLSkipVerify                    bool     // If true, do not strictly validate mutual TLS certs for the VTOrc mysql instances
	MySQLVTOrcUseMutualTLS                     bool     // Turn on TLS authentication with the VTOrc MySQL instance
	MySQLVTOrcReadTimeoutSeconds               int      // Number of seconds before backend mysql read operation is aborted (driver-side)
	MySQLVTOrcRejectReadOnly                   bool     // Reject read only connections https://github.com/go-sql-driver/mysql#rejectreadonly
	MySQLConnectTimeoutSeconds                 int      // Number of seconds before connection is aborted (driver-side)
	MySQLDiscoveryReadTimeoutSeconds           int      // Number of seconds before topology mysql read operation is aborted (driver-side). Used for discovery queries.
	MySQLTopologyReadTimeoutSeconds            int      // Number of seconds before topology mysql read operation is aborted (driver-side). Used for all but discovery queries.
	MySQLConnectionLifetimeSeconds             int      // Number of seconds the mysql driver will keep database connection alive before recycling it
	DefaultInstancePort                        int      // In case port was not specified on command line
	DiscoverByShowSlaveHosts                   bool     // Attempt SHOW SLAVE HOSTS before PROCESSLIST
	UseSuperReadOnly                           bool     // Should vtorc super_read_only any time it sets read_only
	InstancePollSeconds                        uint     // Number of seconds between instance reads
	InstanceWriteBufferSize                    int      // Instance write buffer size (max number of instances to flush in one INSERT ODKU)
	BufferInstanceWrites                       bool     // Set to 'true' for write-optimization on backend table (compromise: writes can be stale and overwrite non stale data)
	InstanceFlushIntervalMilliseconds          int      // Max interval between instance write buffer flushes
	UnseenInstanceForgetHours                  uint     // Number of hours after which an unseen instance is forgotten
	SnapshotTopologiesIntervalHours            uint     // Interval in hour between snapshot-topologies invocation. Default: 0 (disabled)
	DiscoveryMaxConcurrency                    uint     // Number of goroutines doing hosts discovery
	DiscoveryQueueCapacity                     uint     // Buffer size of the discovery queue. Should be greater than the number of DB instances being discovered
	DiscoveryQueueMaxStatisticsSize            int      // The maximum number of individual secondly statistics taken of the discovery queue
	DiscoveryCollectionRetentionSeconds        uint     // Number of seconds to retain the discovery collection information
	DiscoverySeeds                             []string // Hard coded array of hostname:port, ensuring vtorc discovers these hosts upon startup, assuming not already known to vtorc
	InstanceBulkOperationsWaitTimeoutSeconds   uint     // Time to wait on a single instance when doing bulk (many instances) operation
	HostnameResolveMethod                      string   // Method by which to "normalize" hostname ("none"/"default"/"cname")
	SkipBinlogServerUnresolveCheck             bool     // Skip the double-check that an unresolved hostname resolves back to same hostname for binlog servers
	ExpiryHostnameResolvesMinutes              int      // Number of minutes after which to expire hostname-resolves
	RejectHostnameResolvePattern               string   // Regexp pattern for resolved hostname that will not be accepted (not cached, not written to db). This is done to avoid storing wrong resolves due to network glitches.
	ReasonableReplicationLagSeconds            int      // Above this value is considered a problem
	ProblemIgnoreHostnameFilters               []string // Will minimize problem visualization for hostnames matching given regexp filters
	VerifyReplicationFilters                   bool     // Include replication filters check before approving topology refactoring
	ReasonableMaintenanceReplicationLagSeconds int      // Above this value move-up and move-below are blocked
	CandidateInstanceExpireMinutes             uint     // Minutes after which a suggestion to use an instance as a candidate replica (to be preferably promoted on primary failover) is expired.
	AuditLogFile                               string   // Name of log file for audit operations. Disabled when empty.
	AuditToSyslog                              bool     // If true, audit messages are written to syslog
	AuditToBackendDB                           bool     // If true, audit messages are written to the backend DB's `audit` table (default: true)
	AuditPurgeDays                             uint     // Days after which audit entries are purged from the database
	RemoveTextFromHostnameDisplay              string   // Text to strip off the hostname on cluster/clusters pages
	ReadOnly                                   bool
	AccessTokenUseExpirySeconds                uint              // Time by which an issued token must be used
	AccessTokenExpiryMinutes                   uint              // Time after which HTTP access token expires
	ClusterNameToAlias                         map[string]string // map between regex matching cluster name to a human friendly alias
	DetectClusterAliasQuery                    string            // Optional query (executed on topology instance) that returns the alias of a cluster. Query will only be executed on cluster primary (though until the topology's primary is resovled it may execute on other/all replicas). If provided, must return one row, one column
	DetectClusterDomainQuery                   string            // Optional query (executed on topology instance) that returns the VIP/CNAME/Alias/whatever domain name for the primary of this cluster. Query will only be executed on cluster primary (though until the topology's primary is resovled it may execute on other/all replicas). If provided, must return one row, one column
	DetectInstanceAliasQuery                   string            // Optional query (executed on topology instance) that returns the alias of an instance. If provided, must return one row, one column
	DetectPromotionRuleQuery                   string            // Optional query (executed on topology instance) that returns the promotion rule of an instance. If provided, must return one row, one column.
	DataCenterPattern                          string            // Regexp pattern with one group, extracting the datacenter name from the hostname
	RegionPattern                              string            // Regexp pattern with one group, extracting the region name from the hostname
	PhysicalEnvironmentPattern                 string            // Regexp pattern with one group, extracting physical environment info from hostname (e.g. combination of datacenter & prod/dev env)
	DetectDataCenterQuery                      string            // Optional query (executed on topology instance) that returns the data center of an instance. If provided, must return one row, one column. Overrides DataCenterPattern and useful for installments where DC cannot be inferred by hostname
	DetectRegionQuery                          string            // Optional query (executed on topology instance) that returns the region of an instance. If provided, must return one row, one column. Overrides RegionPattern and useful for installments where Region cannot be inferred by hostname
	DetectPhysicalEnvironmentQuery             string            // Optional query (executed on topology instance) that returns the physical environment of an instance. If provided, must return one row, one column. Overrides PhysicalEnvironmentPattern and useful for installments where env cannot be inferred by hostname
	DetectSemiSyncEnforcedQuery                string            // Optional query (executed on topology instance) to determine whether semi-sync is fully enforced for primary writes (async fallback is not allowed under any circumstance). If provided, must return one row, one column, value 0 or 1.
	SupportFuzzyPoolHostnames                  bool              // Should "submit-pool-instances" command be able to pass list of fuzzy instances (fuzzy means non-fqdn, but unique enough to recognize). Defaults 'true', implies more queries on backend db
	InstancePoolExpiryMinutes                  uint              // Time after which entries in database_instance_pool are expired (resubmit via `submit-pool-instances`)
	PromotionIgnoreHostnameFilters             []string          // VTOrc will not promote replicas with hostname matching pattern (via -c recovery; for example, avoid promoting dev-dedicated machines)
	UseSSL                                     bool              // Use SSL on the server web port
	UseMutualTLS                               bool              // When "true" Use mutual TLS for the server's web and API connections
	SSLSkipVerify                              bool              // When using SSL, should we ignore SSL certification error
	SSLPrivateKeyFile                          string            // Name of SSL private key file, applies only when UseSSL = true
	SSLCertFile                                string            // Name of SSL certification file, applies only when UseSSL = true
	SSLCAFile                                  string            // Name of the Certificate Authority file, applies only when UseSSL = true
	SSLValidOUs                                []string          // Valid organizational units when using mutual TLS
	StatusEndpoint                             string            // Override the status endpoint.  Defaults to '/api/status'
	StatusOUVerify                             bool              // If true, try to verify OUs when Mutual TLS is on.  Defaults to false
	FailureDetectionPeriodBlockMinutes         int               // The time for which an instance's failure discovery is kept "active", so as to avoid concurrent "discoveries" of the instance's failure; this preceeds any recovery process, if any.
	RecoveryPeriodBlockMinutes                 int               // (supported for backwards compatibility but please use newer `RecoveryPeriodBlockSeconds` instead) The time for which an instance's recovery is kept "active", so as to avoid concurrent recoveries on smae instance as well as flapping
	RecoveryPeriodBlockSeconds                 int               // (overrides `RecoveryPeriodBlockMinutes`) The time for which an instance's recovery is kept "active", so as to avoid concurrent recoveries on smae instance as well as flapping
	RecoveryIgnoreHostnameFilters              []string          // Recovery analysis will completely ignore hosts matching given patterns
	RecoverPrimaryClusterFilters               []string          // Only do primary recovery on clusters matching these regexp patterns (of course the ".*" pattern matches everything)
	RecoverIntermediatePrimaryClusterFilters   []string          // Only do IM recovery on clusters matching these regexp patterns (of course the ".*" pattern matches everything)
	PreventCrossDataCenterPrimaryFailover      bool              // When true (default: false), cross-DC primary failover are not allowed, vtorc will do all it can to only fail over within same DC, or else not fail over at all.
	OSCIgnoreHostnameFilters                   []string          // OSC replicas recommendation will ignore replica hostnames matching given patterns
	URLPrefix                                  string            // URL prefix to run vtorc on non-root web path, e.g. /vtorc to put it behind nginx.
	DiscoveryIgnoreReplicaHostnameFilters      []string          // Regexp filters to apply to prevent auto-discovering new replicas. Usage: unreachable servers due to firewalls, applications which trigger binlog dumps
	DiscoveryIgnorePrimaryHostnameFilters      []string          // Regexp filters to apply to prevent auto-discovering a primary. Usage: pointing your primary temporarily to replicate seom data from external host
	DiscoveryIgnoreHostnameFilters             []string          // Regexp filters to apply to prevent discovering instances of any kind
	WebMessage                                 string            // If provided, will be shown on all web pages below the title bar
	MaxConcurrentReplicaOperations             int               // Maximum number of concurrent operations on replicas
	LockShardTimeoutSeconds                    int               // Timeout on context used to lock shard. Should be a small value because we should fail-fast
	WaitReplicasTimeoutSeconds                 int               // Timeout on amount of time to wait for the replicas in case of ERS. Should be a small value because we should fail-fast. Should not be larger than LockShardTimeoutSeconds since that is the total time we use for an ERS.
	TopoInformationRefreshSeconds              int               // Timer duration on which VTOrc refreshes the keyspace and vttablet records from the topo-server.
	RecoveryPollSeconds                        int               // Timer duration on which VTOrc recovery analysis runs
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
		StatusEndpoint:                             DefaultStatusAPIEndpoint,
		StatusOUVerify:                             false,
		SQLite3DataFile:                            "file::memory:?mode=memory&cache=shared",
		MySQLVTOrcMaxPoolConnections:               128, // limit concurrent conns to backend DB
		MySQLVTOrcPort:                             3306,
		MySQLTopologyUseMutualTLS:                  false,
		MySQLTopologyUseMixedTLS:                   true,
		MySQLVTOrcUseMutualTLS:                     false,
		MySQLConnectTimeoutSeconds:                 2,
		MySQLVTOrcReadTimeoutSeconds:               30,
		MySQLVTOrcRejectReadOnly:                   false,
		MySQLDiscoveryReadTimeoutSeconds:           10,
		MySQLTopologyReadTimeoutSeconds:            600,
		MySQLConnectionLifetimeSeconds:             0,
		DefaultInstancePort:                        3306,
		TLSCacheTTLFactor:                          100,
		InstancePollSeconds:                        5,
		InstanceWriteBufferSize:                    100,
		BufferInstanceWrites:                       false,
		InstanceFlushIntervalMilliseconds:          100,
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
		UseSSL:                                     false,
		UseMutualTLS:                               false,
		SSLValidOUs:                                []string{},
		SSLSkipVerify:                              false,
		SSLPrivateKeyFile:                          "",
		SSLCertFile:                                "",
		SSLCAFile:                                  "",
		FailureDetectionPeriodBlockMinutes:         60,
		RecoveryPeriodBlockMinutes:                 60,
		RecoveryPeriodBlockSeconds:                 3600,
		RecoveryIgnoreHostnameFilters:              []string{},
		RecoverPrimaryClusterFilters:               []string{"*"},
		RecoverIntermediatePrimaryClusterFilters:   []string{},
		PreventCrossDataCenterPrimaryFailover:      false,
		OSCIgnoreHostnameFilters:                   []string{},
		URLPrefix:                                  "",
		DiscoveryIgnoreReplicaHostnameFilters:      []string{},
		WebMessage:                                 "",
		MaxConcurrentReplicaOperations:             5,
		LockShardTimeoutSeconds:                    30,
		WaitReplicasTimeoutSeconds:                 30,
		TopoInformationRefreshSeconds:              15,
		RecoveryPollSeconds:                        1,
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

	if config.URLPrefix != "" {
		// Ensure the prefix starts with "/" and has no trailing one.
		config.URLPrefix = strings.TrimLeft(config.URLPrefix, "/")
		config.URLPrefix = strings.TrimRight(config.URLPrefix, "/")
		config.URLPrefix = "/" + config.URLPrefix
	}

	if config.IsSQLite() && config.SQLite3DataFile == "" {
		return fmt.Errorf("SQLite3DataFile must be set when BackendDB is sqlite3")
	}

	if config.InstanceWriteBufferSize <= 0 {
		config.BufferInstanceWrites = false
	}
	return nil
}

// TODO: Simplify the callers and delete this function
func (config *Configuration) IsSQLite() bool {
	return true
}

// TODO: Simplify the callers and delete this function
func (config *Configuration) IsMySQL() bool {
	return false
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
