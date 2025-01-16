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
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/viperutil"
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
		"instance-poll-time",
		viperutil.Options[time.Duration]{
			FlagName: "instance-poll-time",
			Default:  5 * time.Second,
			Dynamic:  true,
		},
	)

	preventCrossCellFailover = viperutil.Configure(
		"prevent-cross-cell-failover",
		viperutil.Options[bool]{
			FlagName: "prevent-cross-cell-failover",
			Default:  false,
			Dynamic:  true,
		},
	)

	sqliteDataFile = viperutil.Configure(
		"sqlite-data-file",
		viperutil.Options[string]{
			FlagName: "sqlite-data-file",
			Default:  "file::memory:?mode=memory&cache=shared",
			Dynamic:  false,
		},
	)

	snapshotTopologyInterval = viperutil.Configure(
		"snapshot-topology-interval",
		viperutil.Options[time.Duration]{
			FlagName: "snapshot-topology-interval",
			Default:  0 * time.Hour,
			Dynamic:  true,
		},
	)

	reasonableReplicationLag = viperutil.Configure(
		"reasonable-replication-lag",
		viperutil.Options[time.Duration]{
			FlagName: "reasonable-replication-lag",
			Default:  10 * time.Second,
			Dynamic:  true,
		},
	)

	auditFileLocation = viperutil.Configure(
		"audit-file-location",
		viperutil.Options[string]{
			FlagName: "audit-file-location",
			Default:  "",
			Dynamic:  false,
		},
	)

	auditToBackend = viperutil.Configure(
		"audit-to-backend",
		viperutil.Options[bool]{
			FlagName: "audit-to-backend",
			Default:  false,
			Dynamic:  true,
		},
	)

	auditToSyslog = viperutil.Configure(
		"audit-to-syslog",
		viperutil.Options[bool]{
			FlagName: "audit-to-syslog",
			Default:  false,
			Dynamic:  true,
		},
	)

	auditPurgeDuration = viperutil.Configure(
		"audit-purge-duration",
		viperutil.Options[time.Duration]{
			FlagName: "audit-purge-duration",
			Default:  7 * 24 * time.Hour,
			Dynamic:  true,
		},
	)

	waitReplicasTimeout = viperutil.Configure(
		"wait-replicas-timeout",
		viperutil.Options[time.Duration]{
			FlagName: "wait-replicas-timeout",
			Default:  30 * time.Second,
			Dynamic:  true,
		},
	)

	tolerableReplicationLag = viperutil.Configure(
		"tolerable-replication-lag",
		viperutil.Options[time.Duration]{
			FlagName: "tolerable-replication-lag",
			Default:  0 * time.Second,
			Dynamic:  true,
		},
	)

	topoInformationRefreshDuration = viperutil.Configure(
		"topo-information-refresh-duration",
		viperutil.Options[time.Duration]{
			FlagName: "topo-information-refresh-duration",
			Default:  15 * time.Second,
			Dynamic:  true,
		},
	)

	recoveryPollDuration = viperutil.Configure(
		"recovery-poll-duration",
		viperutil.Options[time.Duration]{
			FlagName: "recovery-poll-duration",
			Default:  1 * time.Second,
			Dynamic:  true,
		},
	)

	ersEnabled = viperutil.Configure(
		"allow-emergency-reparent",
		viperutil.Options[bool]{
			FlagName: "allow-emergency-reparent",
			Default:  true,
			Dynamic:  true,
		},
	)

	convertTabletsWithErrantGTIDs = viperutil.Configure(
		"change-tablets-with-errant-gtid-to-drained",
		viperutil.Options[bool]{
			FlagName: "change-tablets-with-errant-gtid-to-drained",
			Default:  false,
			Dynamic:  true,
		},
	)

	enablePrimaryDiskStalledRecovery = viperutil.Configure(
		"enable-primary-disk-stalled-recovery",
		viperutil.Options[bool]{
			FlagName: "enable-primary-disk-stalled-recovery",
			Default:  false,
			Dynamic:  true,
		},
	)
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
	fs.Bool("audit-to-backend", auditToBackend.Default(), "Whether to store the audit log in the VTOrc database")
	fs.Bool("audit-to-syslog", auditToSyslog.Default(), "Whether to store the audit log in the syslog")
	fs.Duration("audit-purge-duration", auditPurgeDuration.Default(), "Duration for which audit logs are held before being purged. Should be in multiples of days")
	fs.Bool("prevent-cross-cell-failover", preventCrossCellFailover.Default(), "Prevent VTOrc from promoting a primary in a different cell than the current primary in case of a failover")
	fs.Duration("wait-replicas-timeout", waitReplicasTimeout.Default(), "Duration for which to wait for replica's to respond when issuing RPCs")
	fs.Duration("tolerable-replication-lag", tolerableReplicationLag.Default(), "Amount of replication lag that is considered acceptable for a tablet to be eligible for promotion when Vitess makes the choice of a new primary in PRS")
	fs.Duration("topo-information-refresh-duration", topoInformationRefreshDuration.Default(), "Timer duration on which VTOrc refreshes the keyspace and vttablet records from the topology server")
	fs.Duration("recovery-poll-duration", recoveryPollDuration.Default(), "Timer duration on which VTOrc polls its database to run a recovery")
	fs.Bool("allow-emergency-reparent", ersEnabled.Default(), "Whether VTOrc should be allowed to run emergency reparent operation when it detects a dead primary")
	fs.Bool("change-tablets-with-errant-gtid-to-drained", convertTabletsWithErrantGTIDs.Default(), "Whether VTOrc should be changing the type of tablets with errant GTIDs to DRAINED")
	fs.Bool("enable-primary-disk-stalled-recovery", enablePrimaryDiskStalledRecovery.Default(), "Whether VTOrc should detect a stalled disk on the primary and failover")

	viperutil.BindFlags(fs,
		instancePollTime,
		preventCrossCellFailover,
		sqliteDataFile,
		snapshotTopologyInterval,
		reasonableReplicationLag,
		auditFileLocation,
		auditToBackend,
		auditToSyslog,
		auditPurgeDuration,
		waitReplicasTimeout,
		tolerableReplicationLag,
		topoInformationRefreshDuration,
		recoveryPollDuration,
		ersEnabled,
		convertTabletsWithErrantGTIDs,
		enablePrimaryDiskStalledRecovery,
	)
}

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

// GetAuditToSyslog is a getter function.
func GetAuditToSyslog() bool {
	return auditToSyslog.Get()
}

// SetAuditToSyslog is a setter function.
func SetAuditToSyslog(v bool) {
	auditToSyslog.Set(v)
}

// GetAuditToBackend is a getter function.
func GetAuditToBackend() bool {
	return auditToBackend.Get()
}

// SetAuditToBackend is a setter function.
func SetAuditToBackend(v bool) {
	auditToBackend.Set(v)
}

// GetAuditPurgeDays gets the audit purge duration but in days.
func GetAuditPurgeDays() int64 {
	return int64(auditPurgeDuration.Get() / (24 * time.Hour))
}

// SetAuditPurgeDays sets the audit purge duration.
func SetAuditPurgeDays(days int64) {
	auditPurgeDuration.Set(time.Duration(days) * 24 * time.Hour)
}

// GetWaitReplicasTimeout is a getter function.
func GetWaitReplicasTimeout() time.Duration {
	return waitReplicasTimeout.Get()
}

// GetTolerableReplicationLag is a getter function.
func GetTolerableReplicationLag() time.Duration {
	return tolerableReplicationLag.Get()
}

// GetTopoInformationRefreshDuration is a getter function.
func GetTopoInformationRefreshDuration() time.Duration {
	return topoInformationRefreshDuration.Get()
}

// GetRecoveryPollDuration is a getter function.
func GetRecoveryPollDuration() time.Duration {
	return recoveryPollDuration.Get()
}

// ERSEnabled reports whether VTOrc is allowed to run ERS or not.
func ERSEnabled() bool {
	return ersEnabled.Get()
}

// SetERSEnabled sets the value for the ersEnabled variable. This should only be used from tests.
func SetERSEnabled(val bool) {
	ersEnabled.Set(val)
}

// ConvertTabletWithErrantGTIDs reports whether VTOrc is allowed to change the tablet type of tablets with errant GTIDs to DRAINED.
func ConvertTabletWithErrantGTIDs() bool {
	return convertTabletsWithErrantGTIDs.Get()
}

// SetConvertTabletWithErrantGTIDs sets the value for the convertTabletWithErrantGTIDs variable. This should only be used from tests.
func SetConvertTabletWithErrantGTIDs(val bool) {
	convertTabletsWithErrantGTIDs.Set(val)
}

// GetStalledDiskPrimaryRecovery reports whether VTOrc is allowed to check for and recovery stalled disk problems.
func GetStalledDiskPrimaryRecovery() bool {
	return enablePrimaryDiskStalledRecovery.Get()
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
