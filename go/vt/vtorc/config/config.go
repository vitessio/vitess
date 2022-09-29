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

	"vitess.io/vitess/go/vt/log"
)

const (
	LostInRecoveryDowntimeSeconds int = 60 * 60 * 24 * 365
)

var configurationLoaded = make(chan bool)

const (
	HealthPollSeconds                     = 1
	ActiveNodeExpireSeconds               = 5
	MaintenanceOwner                      = "vtorc"
	AuditPageSize                         = 20
	MaintenancePurgeDays                  = 7
	MaintenanceExpireMinutes              = 10
	DebugMetricsIntervalSeconds           = 10
	StaleInstanceCoordinatesExpireSeconds = 60
)

// Configuration makes for vtorc configuration input, which can be provided by user via JSON formatted file.
// Some of the parameteres have reasonable default values, and some (like database credentials) are
// strictly expected from user.
// TODO(sougou): change this to yaml parsing, and possible merge with tabletenv.
type Configuration struct {
	SQLite3DataFile                          string   // full path to sqlite3 datafile
	InstancePollSeconds                      uint     // Number of seconds between instance reads
	UnseenInstanceForgetHours                uint     // Number of hours after which an unseen instance is forgotten
	SnapshotTopologiesIntervalHours          uint     // Interval in hour between snapshot-topologies invocation. Default: 0 (disabled)
	DiscoveryMaxConcurrency                  uint     // Number of goroutines doing hosts discovery
	DiscoveryQueueCapacity                   uint     // Buffer size of the discovery queue. Should be greater than the number of DB instances being discovered
	DiscoveryQueueMaxStatisticsSize          int      // The maximum number of individual secondly statistics taken of the discovery queue
	DiscoveryCollectionRetentionSeconds      uint     // Number of seconds to retain the discovery collection information
	DiscoverySeeds                           []string // Hard coded array of hostname:port, ensuring vtorc discovers these hosts upon startup, assuming not already known to vtorc
	InstanceBulkOperationsWaitTimeoutSeconds uint     // Time to wait on a single instance when doing bulk (many instances) operation
	HostnameResolveMethod                    string   // Method by which to "normalize" hostname ("none"/"default"/"cname")
	SkipBinlogServerUnresolveCheck           bool     // Skip the double-check that an unresolved hostname resolves back to same hostname for binlog servers
	ExpiryHostnameResolvesMinutes            int      // Number of minutes after which to expire hostname-resolves
	RejectHostnameResolvePattern             string   // Regexp pattern for resolved hostname that will not be accepted (not cached, not written to db). This is done to avoid storing wrong resolves due to network glitches.
	ReasonableReplicationLagSeconds          int      // Above this value is considered a problem
	ProblemIgnoreHostnameFilters             []string // Will minimize problem visualization for hostnames matching given regexp filters
	CandidateInstanceExpireMinutes           uint     // Minutes after which a suggestion to use an instance as a candidate replica (to be preferably promoted on primary failover) is expired.
	AuditLogFile                             string   // Name of log file for audit operations. Disabled when empty.
	AuditToSyslog                            bool     // If true, audit messages are written to syslog
	AuditToBackendDB                         bool     // If true, audit messages are written to the backend DB's `audit` table (default: true)
	AuditPurgeDays                           uint     // Days after which audit entries are purged from the database
	InstancePoolExpiryMinutes                uint     // Time after which entries in database_instance_pool are expired (resubmit via `submit-pool-instances`)
	FailureDetectionPeriodBlockMinutes       int      // The time for which an instance's failure discovery is kept "active", so as to avoid concurrent "discoveries" of the instance's failure; this preceeds any recovery process, if any.
	RecoveryPeriodBlockSeconds               int      // (overrides `RecoveryPeriodBlockMinutes`) The time for which an instance's recovery is kept "active", so as to avoid concurrent recoveries on smae instance as well as flapping
	PreventCrossDataCenterPrimaryFailover    bool     // When true (default: false), cross-DC primary failover are not allowed, vtorc will do all it can to only fail over within same DC, or else not fail over at all.
	LockShardTimeoutSeconds                  int      // Timeout on context used to lock shard. Should be a small value because we should fail-fast
	WaitReplicasTimeoutSeconds               int      // Timeout on amount of time to wait for the replicas in case of ERS. Should be a small value because we should fail-fast. Should not be larger than LockShardTimeoutSeconds since that is the total time we use for an ERS.
	TopoInformationRefreshSeconds            int      // Timer duration on which VTOrc refreshes the keyspace and vttablet records from the topo-server.
	RecoveryPollSeconds                      int      // Timer duration on which VTOrc recovery analysis runs
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
		SQLite3DataFile:                          "file::memory:?mode=memory&cache=shared",
		InstancePollSeconds:                      5,
		UnseenInstanceForgetHours:                240,
		SnapshotTopologiesIntervalHours:          0,
		DiscoveryMaxConcurrency:                  300,
		DiscoveryQueueCapacity:                   100000,
		DiscoveryQueueMaxStatisticsSize:          120,
		DiscoveryCollectionRetentionSeconds:      120,
		DiscoverySeeds:                           []string{},
		InstanceBulkOperationsWaitTimeoutSeconds: 10,
		HostnameResolveMethod:                    "default",
		SkipBinlogServerUnresolveCheck:           true,
		ExpiryHostnameResolvesMinutes:            60,
		RejectHostnameResolvePattern:             "",
		ReasonableReplicationLagSeconds:          10,
		ProblemIgnoreHostnameFilters:             []string{},
		CandidateInstanceExpireMinutes:           60,
		AuditLogFile:                             "",
		AuditToSyslog:                            false,
		AuditToBackendDB:                         false,
		AuditPurgeDays:                           7,
		InstancePoolExpiryMinutes:                60,
		FailureDetectionPeriodBlockMinutes:       60,
		RecoveryPeriodBlockSeconds:               3600,
		PreventCrossDataCenterPrimaryFailover:    false,
		LockShardTimeoutSeconds:                  30,
		WaitReplicasTimeoutSeconds:               30,
		TopoInformationRefreshSeconds:            15,
		RecoveryPollSeconds:                      1,
	}
}

func (config *Configuration) postReadAdjustments() error {
	if config.IsSQLite() && config.SQLite3DataFile == "" {
		return fmt.Errorf("SQLite3DataFile must be set")
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
