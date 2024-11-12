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

	"vitess.io/vitess/go/viperutil"
	"vitess.io/vitess/go/viperutil/debug"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
)

var configurationLoaded = make(chan bool)

const (
	HealthPollSeconds                     = 1
	AuditPageSize                         = 20
	DebugMetricsIntervalSeconds           = 10
	StaleInstanceCoordinatesExpireSeconds = 60
	DiscoveryMaxConcurrency               = 300 // Number of goroutines doing hosts discovery
	DiscoveryQueueCapacity                = 100000
	DiscoveryQueueMaxStatisticsSize       = 120
	DiscoveryCollectionRetentionSeconds   = 120
	UnseenInstanceForgetHours             = 240 // Number of hours after which an unseen instance is forgotten
)

var (
	instancePollTime = viperutil.Configure(
		"instance-pPollTime",
		viperutil.Options[time.Duration]{
			FlagName: "instance-poll-time",
			Default:  5 * time.Second,
			Dynamic:  true,
		},
	)

	preventCrossCellFailover = viperutil.Configure(
		"PreventCrossCellFailover",
		viperutil.Options[bool]{
			FlagName: "prevent-cross-cell-failover",
			Default:  false,
			Dynamic:  true,
		},
	)

	sqliteDataFile = viperutil.Configure(
		"SQLiteDataFile",
		viperutil.Options[string]{
			FlagName: "sqlite-data-file",
			Default:  "file::memory:?mode=memory&cache=shared",
			Dynamic:  false,
		},
	)

	snapshotTopologyInterval = viperutil.Configure(
		"snapshotTopologyInterval",
		viperutil.Options[time.Duration]{
			FlagName: "snapshot-topology-interval",
			Default:  0 * time.Hour,
			Dynamic:  true,
		},
	)

	reasonableReplicationLag = viperutil.Configure(
		"reasonableReplicationLag",
		viperutil.Options[time.Duration]{
			FlagName: "reasonable-replication-lag",
			Default:  10 * time.Second,
			Dynamic:  true,
		},
	)

	auditFileLocation = viperutil.Configure(
		"AuditFileLocation",
		viperutil.Options[string]{
			FlagName: "audit-file-location",
			Default:  "",
			Dynamic:  false,
		},
	)
)

var (
	auditToBackend                 = false
	auditToSyslog                  = false
	auditPurgeDuration             = 7 * 24 * time.Hour // Equivalent of 7 days
	waitReplicasTimeout            = 30 * time.Second
	tolerableReplicationLag        = 0 * time.Second
	topoInformationRefreshDuration = 15 * time.Second
	recoveryPollDuration           = 1 * time.Second
	ersEnabled                     = true
	convertTabletsWithErrantGTIDs  = false
)

func init() {
	servenv.OnParseFor("vtorc", registerFlags)
}

// registerFlags registers the flags required by VTOrc
func registerFlags(fs *pflag.FlagSet) {
	fs.String("sqlite-data-file", sqliteDataFile.Default(), "SQLite Datafile to use as VTOrc's database")
	fs.Duration("instance-poll-time", instancePollTime.Default(), "Timer duration on which VTOrc refreshes MySQL information")
	fs.Duration("snapshot-topology-interval", snapshotTopologyInterval.Default(), "Timer duration on which VTOrc takes a snapshot of the current MySQL information it has in the database. Should be in multiple of hours")
	fs.Duration("reasonable-replication-lag", reasonableReplicationLag.Default(), "Maximum replication lag on replicas which is deemed to be acceptable")
	fs.String("audit-file-location", auditFileLocation.Default(), "File location where the audit logs are to be stored")
	fs.BoolVar(&auditToBackend, "audit-to-backend", auditToBackend, "Whether to store the audit log in the VTOrc database")
	fs.BoolVar(&auditToSyslog, "audit-to-syslog", auditToSyslog, "Whether to store the audit log in the syslog")
	fs.DurationVar(&auditPurgeDuration, "audit-purge-duration", auditPurgeDuration, "Duration for which audit logs are held before being purged. Should be in multiples of days")
	fs.Bool("prevent-cross-cell-failover", preventCrossCellFailover.Default(), "Prevent VTOrc from promoting a primary in a different cell than the current primary in case of a failover")
	fs.DurationVar(&waitReplicasTimeout, "wait-replicas-timeout", waitReplicasTimeout, "Duration for which to wait for replica's to respond when issuing RPCs")
	fs.DurationVar(&tolerableReplicationLag, "tolerable-replication-lag", tolerableReplicationLag, "Amount of replication lag that is considered acceptable for a tablet to be eligible for promotion when Vitess makes the choice of a new primary in PRS")
	fs.DurationVar(&topoInformationRefreshDuration, "topo-information-refresh-duration", topoInformationRefreshDuration, "Timer duration on which VTOrc refreshes the keyspace and vttablet records from the topology server")
	fs.DurationVar(&recoveryPollDuration, "recovery-poll-duration", recoveryPollDuration, "Timer duration on which VTOrc polls its database to run a recovery")
	fs.BoolVar(&ersEnabled, "allow-emergency-reparent", ersEnabled, "Whether VTOrc should be allowed to run emergency reparent operation when it detects a dead primary")
	fs.BoolVar(&convertTabletsWithErrantGTIDs, "change-tablets-with-errant-gtid-to-drained", convertTabletsWithErrantGTIDs, "Whether VTOrc should be changing the type of tablets with errant GTIDs to DRAINED")

	viperutil.BindFlags(fs,
		instancePollTime,
		preventCrossCellFailover,
		sqliteDataFile,
		snapshotTopologyInterval,
		reasonableReplicationLag,
		auditFileLocation,
	)
}

// Configuration makes for vtorc configuration input, which can be provided by user via JSON formatted file.
// Some of the parameters have reasonable default values, and some (like database credentials) are
// strictly expected from user.
// TODO(sougou): change this to yaml parsing, and possible merge with tabletenv.
type Configuration struct {
	AuditLogFile                   string // Name of log file for audit operations. Disabled when empty.
	AuditToSyslog                  bool   // If true, audit messages are written to syslog
	AuditToBackendDB               bool   // If true, audit messages are written to the backend DB's `audit` table (default: true)
	AuditPurgeDays                 uint   // Days after which audit entries are purged from the database
	WaitReplicasTimeoutSeconds     int    // Timeout on amount of time to wait for the replicas in case of ERS. Should be a small value because we should fail-fast. Should not be larger than LockTimeout since that is the total time we use for an ERS.
	TolerableReplicationLagSeconds int    // Amount of replication lag that is considered acceptable for a tablet to be eligible for promotion when Vitess makes the choice of a new primary in PRS.
	TopoInformationRefreshSeconds  int    // Timer duration on which VTOrc refreshes the keyspace and vttablet records from the topo-server.
	RecoveryPollSeconds            int    // Timer duration on which VTOrc recovery analysis runs
}

// ToJSONString will marshal this configuration as JSON
func (config *Configuration) ToJSONString() string {
	b, _ := json.Marshal(config)
	return string(b)
}

// Config is *the* configuration instance, used globally to get configuration data
var Config = newConfiguration()

// GetInstancePollTime is a getter function.
func GetInstancePollTime() time.Duration {
	return instancePollTime.Get()
}

// SetInstancePollTime is a setter function.
func SetInstancePollTime(v time.Duration) {
	instancePollTime.Set(v)
}

// GetInstancePollSeconds gets the instance poll time but in seconds.
func GetInstancePollSeconds() uint {
	return uint(instancePollTime.Get() / time.Second)
}

// GetPreventCrossCellFailover is a getter function.
func GetPreventCrossCellFailover() bool {
	return preventCrossCellFailover.Get()
}

// GetSQLiteDataFile is a getter function.
func GetSQLiteDataFile() string {
	return sqliteDataFile.Get()
}

// GetReasonableReplicationLagSeconds gets the reasonable replication lag but in seconds.
func GetReasonableReplicationLagSeconds() int64 {
	return int64(reasonableReplicationLag.Get() / time.Second)
}

// GetSnapshotTopologyInterval is a getter function.
func GetSnapshotTopologyInterval() time.Duration {
	return snapshotTopologyInterval.Get()
}

// GetAuditFileLocation is a getter function.
func GetAuditFileLocation() string {
	return auditFileLocation.Get()
}

// SetAuditFileLocation is a setter function.
func SetAuditFileLocation(v string) {
	auditFileLocation.Set(v)
}

// UpdateConfigValuesFromFlags is used to update the config values from the flags defined.
// This is done before we read any configuration files from the user. So the config files take precedence.
func UpdateConfigValuesFromFlags() {
	Config.AuditToBackendDB = auditToBackend
	Config.AuditToSyslog = auditToSyslog
	Config.AuditPurgeDays = uint(auditPurgeDuration / (time.Hour * 24))
	Config.WaitReplicasTimeoutSeconds = int(waitReplicasTimeout / time.Second)
	Config.TolerableReplicationLagSeconds = int(tolerableReplicationLag / time.Second)
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

// ConvertTabletWithErrantGTIDs reports whether VTOrc is allowed to change the tablet type of tablets with errant GTIDs to DRAINED.
func ConvertTabletWithErrantGTIDs() bool {
	return convertTabletsWithErrantGTIDs
}

// SetConvertTabletWithErrantGTIDs sets the value for the convertTabletWithErrantGTIDs variable. This should only be used from tests.
func SetConvertTabletWithErrantGTIDs(val bool) {
	convertTabletsWithErrantGTIDs = val
}

// LogConfigValues is used to log the config values.
func LogConfigValues() {
	log.Infof("Running with Configuration - %v", debug.AllSettings())
}

func newConfiguration() *Configuration {
	return &Configuration{
		AuditToSyslog:                 false,
		AuditToBackendDB:              false,
		AuditPurgeDays:                7,
		WaitReplicasTimeoutSeconds:    30,
		TopoInformationRefreshSeconds: 15,
		RecoveryPollSeconds:           1,
	}
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
	return Config, err
}

// Reload re-reads configuration from last used files
func Reload(extraFileNames ...string) *Configuration {
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
