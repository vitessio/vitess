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
	"time"

	"github.com/spf13/pflag"

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
	DiscoveryMaxConcurrency               = 300 // Number of goroutines doing hosts discovery
	DiscoveryQueueCapacity                = 100000
	DiscoveryQueueMaxStatisticsSize       = 120
	DiscoveryCollectionRetentionSeconds   = 120
	UnseenInstanceForgetHours             = 240 // Number of hours after which an unseen instance is forgotten
	CandidateInstanceExpireMinutes        = 60  // Minutes after which a suggestion to use an instance as a candidate replica (to be preferably promoted on primary failover) is expired.
	FailureDetectionPeriodBlockMinutes    = 60  // The time for which an instance's failure discovery is kept "active", so as to avoid concurrent "discoveries" of the instance's failure; this preceeds any recovery process, if any.
)

var (
	sqliteDataFile                 = "file::memory:?mode=memory&cache=shared"
	instancePollTime               = 5 * time.Second
	snapshotTopologyInterval       = 0 * time.Hour
	reasonableReplicationLag       = 10 * time.Second
	auditFileLocation              = ""
	auditToBackend                 = false
	auditToSyslog                  = false
	auditPurgeDuration             = 7 * 24 * time.Hour // Equivalent of 7 days
	recoveryPeriodBlockDuration    = 30 * time.Second
	preventCrossCellFailover       = false
	waitReplicasTimeout            = 30 * time.Second
	topoInformationRefreshDuration = 15 * time.Second
	recoveryPollDuration           = 1 * time.Second
	ersEnabled                     = true
)

// RegisterFlags registers the flags required by VTOrc
func RegisterFlags(fs *pflag.FlagSet) {
	fs.StringVar(&sqliteDataFile, "sqlite-data-file", sqliteDataFile, "SQLite Datafile to use as VTOrc's database")
	fs.DurationVar(&instancePollTime, "instance-poll-time", instancePollTime, "Timer duration on which VTOrc refreshes MySQL information")
	fs.DurationVar(&snapshotTopologyInterval, "snapshot-topology-interval", snapshotTopologyInterval, "Timer duration on which VTOrc takes a snapshot of the current MySQL information it has in the database. Should be in multiple of hours")
	fs.DurationVar(&reasonableReplicationLag, "reasonable-replication-lag", reasonableReplicationLag, "Maximum replication lag on replicas which is deemed to be acceptable")
	fs.StringVar(&auditFileLocation, "audit-file-location", auditFileLocation, "File location where the audit logs are to be stored")
	fs.BoolVar(&auditToBackend, "audit-to-backend", auditToBackend, "Whether to store the audit log in the VTOrc database")
	fs.BoolVar(&auditToSyslog, "audit-to-syslog", auditToSyslog, "Whether to store the audit log in the syslog")
	fs.DurationVar(&auditPurgeDuration, "audit-purge-duration", auditPurgeDuration, "Duration for which audit logs are held before being purged. Should be in multiples of days")
	fs.DurationVar(&recoveryPeriodBlockDuration, "recovery-period-block-duration", recoveryPeriodBlockDuration, "Duration for which a new recovery is blocked on an instance after running a recovery")
	fs.BoolVar(&preventCrossCellFailover, "prevent-cross-cell-failover", preventCrossCellFailover, "Prevent VTOrc from promoting a primary in a different cell than the current primary in case of a failover")
	fs.Duration("lock-shard-timeout", 30*time.Second, "Duration for which a shard lock is held when running a recovery")
	_ = fs.MarkDeprecated("lock-shard-timeout", "Please use lock-timeout instead.")
	fs.DurationVar(&waitReplicasTimeout, "wait-replicas-timeout", waitReplicasTimeout, "Duration for which to wait for replica's to respond when issuing RPCs")
	fs.DurationVar(&topoInformationRefreshDuration, "topo-information-refresh-duration", topoInformationRefreshDuration, "Timer duration on which VTOrc refreshes the keyspace and vttablet records from the topology server")
	fs.DurationVar(&recoveryPollDuration, "recovery-poll-duration", recoveryPollDuration, "Timer duration on which VTOrc polls its database to run a recovery")
	fs.BoolVar(&ersEnabled, "allow-emergency-reparent", ersEnabled, "Whether VTOrc should be allowed to run emergency reparent operation when it detects a dead primary")
}

// Configuration makes for vtorc configuration input, which can be provided by user via JSON formatted file.
// Some of the parameteres have reasonable default values, and some (like database credentials) are
// strictly expected from user.
// TODO(sougou): change this to yaml parsing, and possible merge with tabletenv.
type Configuration struct {
	SQLite3DataFile                       string // full path to sqlite3 datafile
	InstancePollSeconds                   uint   // Number of seconds between instance reads
	SnapshotTopologiesIntervalHours       uint   // Interval in hour between snapshot-topologies invocation. Default: 0 (disabled)
	ReasonableReplicationLagSeconds       int    // Above this value is considered a problem
	AuditLogFile                          string // Name of log file for audit operations. Disabled when empty.
	AuditToSyslog                         bool   // If true, audit messages are written to syslog
	AuditToBackendDB                      bool   // If true, audit messages are written to the backend DB's `audit` table (default: true)
	AuditPurgeDays                        uint   // Days after which audit entries are purged from the database
	RecoveryPeriodBlockSeconds            int    // (overrides `RecoveryPeriodBlockMinutes`) The time for which an instance's recovery is kept "active", so as to avoid concurrent recoveries on smae instance as well as flapping
	PreventCrossDataCenterPrimaryFailover bool   // When true (default: false), cross-DC primary failover are not allowed, vtorc will do all it can to only fail over within same DC, or else not fail over at all.
	WaitReplicasTimeoutSeconds            int    // Timeout on amount of time to wait for the replicas in case of ERS. Should be a small value because we should fail-fast. Should not be larger than LockTimeout since that is the total time we use for an ERS.
	TopoInformationRefreshSeconds         int    // Timer duration on which VTOrc refreshes the keyspace and vttablet records from the topo-server.
	RecoveryPollSeconds                   int    // Timer duration on which VTOrc recovery analysis runs
}

// ToJSONString will marshal this configuration as JSON
func (config *Configuration) ToJSONString() string {
	b, _ := json.Marshal(config)
	return string(b)
}

// Config is *the* configuration instance, used globally to get configuration data
var Config = newConfiguration()
var readFileNames []string

// UpdateConfigValuesFromFlags is used to update the config values from the flags defined.
// This is done before we read any configuration files from the user. So the config files take precedence.
func UpdateConfigValuesFromFlags() {
	Config.SQLite3DataFile = sqliteDataFile
	Config.InstancePollSeconds = uint(instancePollTime / time.Second)
	Config.InstancePollSeconds = uint(instancePollTime / time.Second)
	Config.SnapshotTopologiesIntervalHours = uint(snapshotTopologyInterval / time.Hour)
	Config.ReasonableReplicationLagSeconds = int(reasonableReplicationLag / time.Second)
	Config.AuditLogFile = auditFileLocation
	Config.AuditToBackendDB = auditToBackend
	Config.AuditToSyslog = auditToSyslog
	Config.AuditPurgeDays = uint(auditPurgeDuration / (time.Hour * 24))
	Config.RecoveryPeriodBlockSeconds = int(recoveryPeriodBlockDuration / time.Second)
	Config.PreventCrossDataCenterPrimaryFailover = preventCrossCellFailover
	Config.WaitReplicasTimeoutSeconds = int(waitReplicasTimeout / time.Second)
	Config.TopoInformationRefreshSeconds = int(topoInformationRefreshDuration / time.Second)
	Config.RecoveryPollSeconds = int(recoveryPollDuration / time.Second)
}

// ERSEnabled reports whether VTOrc is allowed to run ERS or not.
func ERSEnabled() bool {
	return ersEnabled
}

// SetERSEnabled sets the value for the ersEnabled variable. This should only be used from tests.
func SetERSEnabled(val bool) {
	ersEnabled = val
}

// LogConfigValues is used to log the config values.
func LogConfigValues() {
	b, _ := json.MarshalIndent(Config, "", "\t")
	log.Infof("Running with Configuration - %v", string(b))
}

func newConfiguration() *Configuration {
	return &Configuration{
		SQLite3DataFile:                       "file::memory:?mode=memory&cache=shared",
		InstancePollSeconds:                   5,
		SnapshotTopologiesIntervalHours:       0,
		ReasonableReplicationLagSeconds:       10,
		AuditLogFile:                          "",
		AuditToSyslog:                         false,
		AuditToBackendDB:                      false,
		AuditPurgeDays:                        7,
		RecoveryPeriodBlockSeconds:            30,
		PreventCrossDataCenterPrimaryFailover: false,
		WaitReplicasTimeoutSeconds:            30,
		TopoInformationRefreshSeconds:         15,
		RecoveryPollSeconds:                   1,
	}
}

func (config *Configuration) postReadAdjustments() error {
	if config.SQLite3DataFile == "" {
		return fmt.Errorf("SQLite3DataFile must be set")
	}

	return nil
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
