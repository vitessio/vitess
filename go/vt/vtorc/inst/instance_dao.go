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

package inst

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sjmudd/stopwatch"
	"golang.org/x/sync/semaphore"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/util"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const maxBackendOpTime = time.Second * 5

var (
	instanceReadSem  = semaphore.NewWeighted(config.GetBackendReadConcurrency())
	instanceWriteSem = semaphore.NewWeighted(config.GetBackendWriteConcurrency())
)

var forgetAliases *cache.Cache

var (
	readTopologyInstanceCounter = stats.NewCounter("InstanceReadTopology", "Number of times an instance was read from the topology")
	readInstanceCounter         = stats.NewCounter("InstanceRead", "Number of times an instance was read")
	currentErrantGTIDCount      = stats.NewGaugesWithSingleLabel("CurrentErrantGTIDCount", "Number of errant GTIDs a vttablet currently has", "TabletAlias")
)

var (
	emptyQuotesRegexp            = regexp.MustCompile(`^""$`)
	cacheInitializationCompleted atomic.Bool
)

func init() {
	go initializeInstanceDao()
}

func initializeInstanceDao() {
	config.WaitForConfigurationToBeLoaded()
	forgetAliases = cache.New(config.GetInstancePollTime()*3, time.Second)
	cacheInitializationCompleted.Store(true)
}

// ExecDBWriteFunc chooses how to execute a write onto the database: whether synchronously or not
func ExecDBWriteFunc(f func() error) error {
	ctx, cancel := context.WithTimeout(context.Background(), maxBackendOpTime)
	defer cancel()

	if err := instanceWriteSem.Acquire(ctx, 1); err != nil {
		return err
	}

	// catch the exec time and error if there is one
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
		}
		instanceWriteSem.Release(1)
	}()
	res := f()
	return res
}

func ExpireTableData(tableName string, timestampColumn string) error {
	writeFunc := func() error {
		query := fmt.Sprintf(`DELETE
			FROM %s
			WHERE
				%s < DATETIME('now', PRINTF('-%%d DAY', ?))
			`,
			tableName,
			timestampColumn,
		)
		_, err := db.ExecVTOrc(query, config.GetAuditPurgeDays())
		return err
	}
	return ExecDBWriteFunc(writeFunc)
}

// logReadTopologyInstanceError logs an error, if applicable, for a ReadTopologyInstance operation,
// providing context and hint as for the source of the error. If there's no hint just provide the
// original error.
func logReadTopologyInstanceError(tabletAlias string, hint string, err error) error {
	if err == nil {
		return nil
	}
	if !util.ClearToLog("ReadTopologyInstance", tabletAlias) {
		return err
	}
	var msg string
	if hint == "" {
		msg = fmt.Sprintf("ReadTopologyInstance(%+v): %+v", tabletAlias, err)
	} else {
		msg = fmt.Sprintf("ReadTopologyInstance(%+v) %+v: %+v",
			tabletAlias,
			strings.ReplaceAll(hint, "%", "%%"), // escape %
			err)
	}
	log.Error(msg)
	return errors.New(msg)
}

// RegisterStats registers stats from the inst package
func RegisterStats() {
	stats.NewGaugeFunc("ErrantGtidTabletCount", "Number of tablets with errant GTIDs", func() int64 {
		instances, _ := ReadInstancesWithErrantGTIds("", "")
		return int64(len(instances))
	})
}

// ReadTopologyInstanceBufferable connects to a topology MySQL instance
// and collects information on the server and its replication state.
// It writes the information retrieved into vtorc's backend.
// - writes are optionally buffered.
// - timing information can be collected for the stages performed.
func ReadTopologyInstanceBufferable(tabletAlias string, latency *stopwatch.NamedStopwatch) (inst *Instance, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = logReadTopologyInstanceError(tabletAlias, "Unexpected, aborting", tb.Errorf("%+v", r))
		}
	}()

	var waitGroup sync.WaitGroup
	var tablet *topodatapb.Tablet
	var fs *replicationdatapb.FullStatus
	readingStartTime := time.Now()
	stalledDisk := false
	instance := NewInstance()
	instanceFound := false
	partialSuccess := false
	errorChan := make(chan error, 32)

	if tabletAlias == "" {
		return instance, fmt.Errorf("ReadTopologyInstance will not act on empty tablet alias")
	}

	lastAttemptedCheckTimer := time.AfterFunc(time.Second, func() {
		go func() {
			_ = UpdateInstanceLastAttemptedCheck(tabletAlias)
		}()
	})

	latency.Start("instance")

	tablet, err = ReadTablet(tabletAlias)
	if err != nil {
		goto Cleanup
	}
	if tablet == nil {
		// This can happen because Orc rediscovers instances by alt hostnames,
		// lit localhost, ip, etc.
		// TODO(sougou): disable this ability.
		goto Cleanup
	}

	fs, err = fullStatus(tablet)
	if err != nil {
		goto Cleanup
	}
	if config.GetStalledDiskPrimaryRecovery() && fs.DiskStalled {
		stalledDisk = true
		goto Cleanup
	}
	partialSuccess = true // We at least managed to read something from the server.

	instance.Hostname = tablet.MysqlHostname
	instance.Port = int(tablet.MysqlPort)
	{
		// We begin with a few operations we can run concurrently, and which do not depend on anything
		instance.ServerID = uint(fs.ServerId)
		instance.TabletType = fs.TabletType
		instance.Version = fs.Version
		instance.ReadOnly = fs.ReadOnly
		instance.LogBinEnabled = fs.LogBinEnabled
		instance.BinlogFormat = fs.BinlogFormat
		instance.LogReplicationUpdatesEnabled = fs.LogReplicaUpdates
		instance.VersionComment = fs.VersionComment

		if instance.LogBinEnabled && fs.PrimaryStatus != nil {
			binlogPos, err := getBinlogCoordinatesFromPositionString(fs.PrimaryStatus.FilePosition)
			instance.SelfBinlogCoordinates = binlogPos
			errorChan <- err
		}

		instance.SemiSyncPrimaryEnabled = fs.SemiSyncPrimaryEnabled
		instance.SemiSyncReplicaEnabled = fs.SemiSyncReplicaEnabled
		instance.SemiSyncPrimaryWaitForReplicaCount = uint(fs.SemiSyncWaitForReplicaCount)
		instance.SemiSyncPrimaryTimeout = fs.SemiSyncPrimaryTimeout

		instance.SemiSyncPrimaryClients = uint(fs.SemiSyncPrimaryClients)
		instance.SemiSyncPrimaryStatus = fs.SemiSyncPrimaryStatus
		instance.SemiSyncReplicaStatus = fs.SemiSyncReplicaStatus
		instance.SemiSyncBlocked = fs.SemiSyncBlocked

		if instance.IsOracleMySQL() || instance.IsPercona() {
			// Stuff only supported on Oracle / Percona MySQL
			// ...
			// @@gtid_mode only available in Oracle / Percona MySQL >= 5.6
			instance.GTIDMode = fs.GtidMode
			instance.ServerUUID = fs.ServerUuid
			if fs.PrimaryStatus != nil {
				GtidExecutedPos, err := replication.DecodePosition(fs.PrimaryStatus.Position)
				errorChan <- err
				if err == nil && GtidExecutedPos.GTIDSet != nil {
					instance.ExecutedGtidSet = GtidExecutedPos.GTIDSet.String()
				}
			}
			GtidPurgedPos, err := replication.DecodePosition(fs.GtidPurged)
			errorChan <- err
			if err == nil && GtidPurgedPos.GTIDSet != nil {
				instance.GtidPurged = GtidPurgedPos.GTIDSet.String()
			}
			instance.BinlogRowImage = fs.BinlogRowImage

			if instance.GTIDMode != "" && instance.GTIDMode != "OFF" {
				instance.SupportsOracleGTID = true
			}
		}
	}

	instance.ReplicationIOThreadState = ReplicationThreadStateNoThread
	instance.ReplicationSQLThreadState = ReplicationThreadStateNoThread
	if fs.ReplicationStatus != nil {
		instance.HasReplicationCredentials = fs.ReplicationStatus.SourceUser != ""

		instance.ReplicationIOThreadState = ReplicationThreadStateFromReplicationState(replication.ReplicationState(fs.ReplicationStatus.IoState))
		instance.ReplicationSQLThreadState = ReplicationThreadStateFromReplicationState(replication.ReplicationState(fs.ReplicationStatus.SqlState))
		instance.ReplicationIOThreadRuning = instance.ReplicationIOThreadState.IsRunning()
		instance.ReplicationSQLThreadRuning = instance.ReplicationSQLThreadState.IsRunning()

		binlogPos, err := getBinlogCoordinatesFromPositionString(fs.ReplicationStatus.RelayLogSourceBinlogEquivalentPosition)
		instance.ReadBinlogCoordinates = binlogPos
		errorChan <- err

		binlogPos, err = getBinlogCoordinatesFromPositionString(fs.ReplicationStatus.FilePosition)
		instance.ExecBinlogCoordinates = binlogPos
		errorChan <- err
		instance.IsDetached, _ = instance.ExecBinlogCoordinates.ExtractDetachedCoordinates()

		binlogPos, err = getBinlogCoordinatesFromPositionString(fs.ReplicationStatus.RelayLogFilePosition)
		instance.RelaylogCoordinates = binlogPos
		instance.RelaylogCoordinates.Type = RelayLog
		errorChan <- err

		instance.LastSQLError = emptyQuotesRegexp.ReplaceAllString(strconv.QuoteToASCII(fs.ReplicationStatus.LastSqlError), "")
		instance.LastIOError = emptyQuotesRegexp.ReplaceAllString(strconv.QuoteToASCII(fs.ReplicationStatus.LastIoError), "")

		instance.SQLDelay = fs.ReplicationStatus.SqlDelay
		instance.UsingOracleGTID = fs.ReplicationStatus.AutoPosition
		instance.SourceUUID = fs.ReplicationStatus.SourceUuid
		instance.HasReplicationFilters = fs.ReplicationStatus.HasReplicationFilters

		instance.SourceHost = fs.ReplicationStatus.SourceHost
		instance.SourcePort = int(fs.ReplicationStatus.SourcePort)

		if fs.ReplicationStatus.ReplicationLagUnknown {
			instance.SecondsBehindPrimary.Valid = false
		} else {
			instance.SecondsBehindPrimary.Valid = true
			instance.SecondsBehindPrimary.Int64 = int64(fs.ReplicationStatus.ReplicationLagSeconds)
		}
		if instance.SecondsBehindPrimary.Valid && instance.SecondsBehindPrimary.Int64 < 0 {
			log.Warningf("Alias: %+v, instance.SecondsBehindPrimary < 0 [%+v], correcting to 0", tabletAlias, instance.SecondsBehindPrimary.Int64)
			instance.SecondsBehindPrimary.Int64 = 0
		}
		// And until told otherwise:
		instance.ReplicationLagSeconds = instance.SecondsBehindPrimary

		instance.AllowTLS = fs.ReplicationStatus.SslAllowed
	}

	if fs.ReplicationConfiguration != nil {
		instance.ReplicaNetTimeout = fs.ReplicationConfiguration.ReplicaNetTimeout
		instance.HeartbeatInterval = fs.ReplicationConfiguration.HeartbeatInterval
	}

	instanceFound = true

	// -------------------------------------------------------------------------
	// Anything after this point does not affect the fact the instance is found.
	// No `goto Cleanup` after this point.
	// -------------------------------------------------------------------------

	instance.Cell = tablet.Alias.Cell
	instance.InstanceAlias = topoproto.TabletAliasString(tablet.Alias)

	{
		latency.Start("backend")
		err = ReadInstanceClusterAttributes(instance)
		latency.Stop("backend")
		_ = logReadTopologyInstanceError(tabletAlias, "ReadInstanceClusterAttributes", err)
	}

Cleanup:
	waitGroup.Wait()
	close(errorChan)
	err = func() error {
		if err != nil {
			return err
		}

		for err := range errorChan {
			if err != nil {
				return err
			}
		}
		return nil
	}()

	if instanceFound {
		if instance.IsCoPrimary {
			// Take co-primary into account, and avoid infinite loop
			instance.AncestryUUID = fmt.Sprintf("%s,%s", instance.SourceUUID, instance.ServerUUID)
		} else {
			instance.AncestryUUID = fmt.Sprintf("%s,%s", instance.AncestryUUID, instance.ServerUUID)
		}
		// Add replication group ancestry UUID as well. Otherwise, VTOrc thinks there are errant GTIDs in group
		// members and its replicas, even though they are not.
		instance.AncestryUUID = strings.Trim(instance.AncestryUUID, ",")
		err = detectErrantGTIDs(instance, tablet)
	}

	latency.Stop("instance")
	readTopologyInstanceCounter.Add(1)

	if instanceFound {
		instance.LastDiscoveryLatency = time.Since(readingStartTime)
		instance.IsLastCheckValid = true
		instance.IsRecentlyChecked = true
		instance.IsUpToDate = true
		latency.Start("backend")
		_ = WriteInstance(instance, instanceFound, err)
		lastAttemptedCheckTimer.Stop()
		latency.Stop("backend")
		return instance, nil
	}

	// Something is wrong, could be network-wise. Record that we
	// tried to check the instance. last_attempted_check is also
	// updated on success by writeInstance. If the reason is a
	// stalled disk, we can record that as well.
	latency.Start("backend")
	_ = UpdateInstanceLastChecked(tabletAlias, partialSuccess, stalledDisk)
	latency.Stop("backend")
	return nil, err
}

// detectErrantGTIDs detects the errant GTIDs on an instance.
func detectErrantGTIDs(instance *Instance, tablet *topodatapb.Tablet) (err error) {
	// If the tablet is not replicating from anyone, then it could be the previous primary.
	// We should check for errant GTIDs by finding the difference with the shard's current primary.
	if instance.primaryExecutedGtidSet == "" && instance.SourceHost == "" {
		var primaryInstance *Instance
		primaryAlias, _, _ := ReadShardPrimaryInformation(tablet.Keyspace, tablet.Shard)
		if primaryAlias != "" {
			// Check if the current tablet is the primary.
			// If it is, then we don't need to run errant gtid detection on it.
			if primaryAlias == instance.InstanceAlias {
				return nil
			}
			primaryInstance, _, _ = ReadInstance(primaryAlias)
		}
		// Only run errant GTID detection, if we are sure that the data read of the current primary
		// is up-to-date enough to reflect that it has been promoted. This is needed to prevent
		// flagging incorrect errant GTIDs. If we were to use old data, we could have some GTIDs
		// accepted by the old primary (this tablet) that don't show in the new primary's set.
		if primaryInstance != nil {
			if primaryInstance.SourceHost == "" {
				instance.primaryExecutedGtidSet = primaryInstance.ExecutedGtidSet
			}
		}
	}
	if instance.ExecutedGtidSet != "" && instance.primaryExecutedGtidSet != "" {
		// Compare primary & replica GTID sets, but ignore the sets that present the primary's UUID.
		// This is because vtorc may pool primary and replica at an inconvenient timing,
		// such that the replica may _seems_ to have more entries than the primary, when in fact
		// it's just that the primary's probing is stale.
		redactedExecutedGtidSet, _ := replication.ParseMysql56GTIDSet(instance.ExecutedGtidSet)
		for _, uuid := range strings.Split(instance.AncestryUUID, ",") {
			uuidSID, err := replication.ParseSID(uuid)
			if err != nil {
				continue
			}
			if uuid != instance.ServerUUID {
				redactedExecutedGtidSet = redactedExecutedGtidSet.RemoveUUID(uuidSID)
			}
			if instance.IsCoPrimary && uuid == instance.ServerUUID {
				// If this is a co-primary, then this server is likely to show its own generated GTIDs as errant,
				// because its co-primary has not applied them yet
				redactedExecutedGtidSet = redactedExecutedGtidSet.RemoveUUID(uuidSID)
			}
		}
		if !redactedExecutedGtidSet.Empty() {
			redactedPrimaryExecutedGtidSet, _ := replication.ParseMysql56GTIDSet(instance.primaryExecutedGtidSet)
			if sourceSID, err := replication.ParseSID(instance.SourceUUID); err == nil {
				redactedPrimaryExecutedGtidSet = redactedPrimaryExecutedGtidSet.RemoveUUID(sourceSID)
			}

			// find errant gtid positions
			errantGtidSet := redactedExecutedGtidSet.Difference(redactedPrimaryExecutedGtidSet)
			if !errantGtidSet.Empty() {
				instance.GtidErrant = errantGtidSet.String()
				currentErrantGTIDCount.Set(instance.InstanceAlias, errantGtidSet.Count())
			}
		}
	}
	return err
}

// getKeyspaceShardName returns a single string having both the keyspace and shard
func getKeyspaceShardName(keyspace, shard string) string {
	return fmt.Sprintf("%v:%v", keyspace, shard)
}

func getBinlogCoordinatesFromPositionString(position string) (BinlogCoordinates, error) {
	pos, err := replication.DecodePosition(position)
	if err != nil || pos.GTIDSet == nil {
		return BinlogCoordinates{}, err
	}
	binLogCoordinates, err := ParseBinlogCoordinates(pos.String())
	if err != nil {
		return BinlogCoordinates{}, err
	}
	return *binLogCoordinates, nil
}

// ReadInstanceClusterAttributes will return the cluster name for a given instance by looking at its primary
// and getting it from there.
// It is a non-recursive function and so-called-recursion is performed upon periodic reading of
// instances.
func ReadInstanceClusterAttributes(instance *Instance) (err error) {
	var primaryReplicationDepth uint
	var ancestryUUID string
	var primaryExecutedGtidSet string
	primaryDataFound := false

	query := `SELECT
		replication_depth,
		source_host,
		source_port,
		ancestry_uuid,
		executed_gtid_set
	FROM database_instance
	WHERE
		hostname = ?
		AND port = ?`
	primaryHostname := instance.SourceHost
	primaryPort := instance.SourcePort
	args := sqlutils.Args(primaryHostname, primaryPort)
	err = db.QueryVTOrc(query, args, func(m sqlutils.RowMap) error {
		primaryReplicationDepth = m.GetUint("replication_depth")
		primaryHostname = m.GetString("source_host")
		primaryPort = m.GetInt("source_port")
		ancestryUUID = m.GetString("ancestry_uuid")
		primaryExecutedGtidSet = m.GetString("executed_gtid_set")
		primaryDataFound = true
		return nil
	})
	if err != nil {
		log.Error(err)
		return err
	}

	var replicationDepth uint
	if primaryDataFound {
		replicationDepth = primaryReplicationDepth + 1
	}
	isCoPrimary := primaryHostname == instance.Hostname && primaryPort == instance.Port

	instance.ReplicationDepth = replicationDepth
	instance.IsCoPrimary = isCoPrimary
	instance.AncestryUUID = ancestryUUID
	instance.primaryExecutedGtidSet = primaryExecutedGtidSet
	return nil
}

// readInstanceRow reads a single instance row from the vtorc backend database.
func readInstanceRow(m sqlutils.RowMap) *Instance {
	instance := NewInstance()

	instance.Hostname = m.GetString("hostname")
	instance.Port = m.GetInt("port")
	instance.TabletType = topodatapb.TabletType(m.GetInt("tablet_type"))
	instance.Cell = m.GetString("cell")
	instance.ServerID = m.GetUint("server_id")
	instance.ServerUUID = m.GetString("server_uuid")
	instance.Version = m.GetString("version")
	instance.VersionComment = m.GetString("version_comment")
	instance.ReadOnly = m.GetBool("read_only")
	instance.BinlogFormat = m.GetString("binlog_format")
	instance.BinlogRowImage = m.GetString("binlog_row_image")
	instance.LogBinEnabled = m.GetBool("log_bin")
	instance.LogReplicationUpdatesEnabled = m.GetBool("log_replica_updates")
	instance.SourceHost = m.GetString("source_host")
	instance.SourcePort = m.GetInt("source_port")
	instance.ReplicaNetTimeout = m.GetInt32("replica_net_timeout")
	instance.HeartbeatInterval = m.GetFloat64("heartbeat_interval")
	instance.ReplicationSQLThreadRuning = m.GetBool("replica_sql_running")
	instance.ReplicationIOThreadRuning = m.GetBool("replica_io_running")
	instance.ReplicationSQLThreadState = ReplicationThreadState(m.GetInt("replication_sql_thread_state"))
	instance.ReplicationIOThreadState = ReplicationThreadState(m.GetInt("replication_io_thread_state"))
	instance.HasReplicationFilters = m.GetBool("has_replication_filters")
	instance.SupportsOracleGTID = m.GetBool("supports_oracle_gtid")
	instance.UsingOracleGTID = m.GetBool("oracle_gtid")
	instance.SourceUUID = m.GetString("source_uuid")
	instance.AncestryUUID = m.GetString("ancestry_uuid")
	instance.ExecutedGtidSet = m.GetString("executed_gtid_set")
	instance.GTIDMode = m.GetString("gtid_mode")
	instance.GtidPurged = m.GetString("gtid_purged")
	instance.GtidErrant = m.GetString("gtid_errant")
	instance.SelfBinlogCoordinates.LogFile = m.GetString("binary_log_file")
	instance.SelfBinlogCoordinates.LogPos = m.GetUint64("binary_log_pos")
	instance.ReadBinlogCoordinates.LogFile = m.GetString("source_log_file")
	instance.ReadBinlogCoordinates.LogPos = m.GetUint64("read_source_log_pos")
	instance.ExecBinlogCoordinates.LogFile = m.GetString("relay_source_log_file")
	instance.ExecBinlogCoordinates.LogPos = m.GetUint64("exec_source_log_pos")
	instance.IsDetached, _ = instance.ExecBinlogCoordinates.ExtractDetachedCoordinates()
	instance.RelaylogCoordinates.LogFile = m.GetString("relay_log_file")
	instance.RelaylogCoordinates.LogPos = m.GetUint64("relay_log_pos")
	instance.RelaylogCoordinates.Type = RelayLog
	instance.LastSQLError = m.GetString("last_sql_error")
	instance.LastIOError = m.GetString("last_io_error")
	instance.SecondsBehindPrimary = m.GetNullInt64("replication_lag_seconds")
	instance.ReplicationLagSeconds = m.GetNullInt64("replica_lag_seconds")
	instance.SQLDelay = m.GetUint32("sql_delay")
	instance.SemiSyncEnforced = m.GetBool("semi_sync_enforced")
	instance.SemiSyncPrimaryEnabled = m.GetBool("semi_sync_primary_enabled")
	instance.SemiSyncPrimaryTimeout = m.GetUint64("semi_sync_primary_timeout")
	instance.SemiSyncPrimaryWaitForReplicaCount = m.GetUint("semi_sync_primary_wait_for_replica_count")
	instance.SemiSyncReplicaEnabled = m.GetBool("semi_sync_replica_enabled")
	instance.SemiSyncPrimaryStatus = m.GetBool("semi_sync_primary_status")
	instance.SemiSyncPrimaryClients = m.GetUint("semi_sync_primary_clients")
	instance.SemiSyncReplicaStatus = m.GetBool("semi_sync_replica_status")
	instance.SemiSyncBlocked = m.GetBool("semi_sync_blocked")
	instance.ReplicationDepth = m.GetUint("replication_depth")
	instance.IsCoPrimary = m.GetBool("is_co_primary")
	instance.HasReplicationCredentials = m.GetBool("has_replication_credentials")
	instance.IsUpToDate = m.GetUint("seconds_since_last_checked") <= config.GetInstancePollSeconds()
	instance.IsRecentlyChecked = m.GetUint("seconds_since_last_checked") <= config.GetInstancePollSeconds()*5
	instance.LastSeenTimestamp = m.GetString("last_seen")
	instance.IsLastCheckValid = m.GetBool("is_last_check_valid")
	instance.SecondsSinceLastSeen = m.GetNullInt64("seconds_since_last_seen")
	instance.AllowTLS = m.GetBool("allow_tls")
	instance.InstanceAlias = m.GetString("alias")
	instance.LastDiscoveryLatency = time.Duration(m.GetInt64("last_discovery_latency")) * time.Nanosecond

	instance.applyFlavorName()

	// problems
	if !instance.IsLastCheckValid {
		instance.Problems = append(instance.Problems, "last_check_invalid")
	} else if !instance.IsRecentlyChecked {
		instance.Problems = append(instance.Problems, "not_recently_checked")
	} else if instance.ReplicationThreadsExist() && !instance.ReplicaRunning() {
		instance.Problems = append(instance.Problems, "not_replicating")
	} else if instance.ReplicationLagSeconds.Valid && util.AbsInt64(instance.ReplicationLagSeconds.Int64-int64(instance.SQLDelay)) > int64(config.GetReasonableReplicationLagSeconds()) {
		instance.Problems = append(instance.Problems, "replication_lag")
	}
	if instance.GtidErrant != "" {
		instance.Problems = append(instance.Problems, "errant_gtid")
	}

	return instance
}

// readInstancesByCondition is a generic function to read instances from the backend database
func readInstancesByCondition(condition string, args []any, sort string) ([](*Instance), error) {
	readFunc := func() ([]*Instance, error) {
		var instances []*Instance

		if sort == "" {
			sort = `alias`
		}
		query := fmt.Sprintf(`SELECT
				*,
				STRFTIME('%%s', 'now') - STRFTIME('%%s', last_checked) AS seconds_since_last_checked,
				IFNULL(last_checked <= last_seen, 0) AS is_last_check_valid,
				STRFTIME('%%s', 'now') - STRFTIME('%%s', last_seen) AS seconds_since_last_seen
			FROM
				vitess_tablet
				LEFT JOIN database_instance USING (alias, hostname, port)
			WHERE
				%s
			ORDER BY
				%s
			`,
			condition,
			sort,
		)

		err := db.QueryVTOrc(query, args, func(m sqlutils.RowMap) error {
			instance := readInstanceRow(m)
			instances = append(instances, instance)
			return nil
		})
		if err != nil {
			log.Error(err)
			return instances, err
		}
		return instances, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), maxBackendOpTime)
	defer cancel()

	if err := instanceReadSem.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer instanceReadSem.Release(1)

	return readFunc()
}

// ReadInstance reads an instance from the vtorc backend database
func ReadInstance(tabletAlias string) (*Instance, bool, error) {
	condition := `alias = ?`
	instances, err := readInstancesByCondition(condition, sqlutils.Args(tabletAlias), "")
	// We know there will be at most one (alias is the PK).
	// And we expect to find one.
	readInstanceCounter.Add(1)
	if len(instances) == 0 {
		return nil, false, err
	}
	if err != nil {
		return instances[0], false, err
	}
	return instances[0], true, nil
}

// ReadProblemInstances reads all instances with problems
func ReadProblemInstances(keyspace string, shard string) ([](*Instance), error) {
	condition := `
		keyspace LIKE (CASE WHEN ? = '' THEN '%' ELSE ? END)
		AND shard LIKE (CASE WHEN ? = '' THEN '%' ELSE ? END)
		AND (
			(last_seen < last_checked)
			OR (STRFTIME('%%s', 'now') - STRFTIME('%%s', last_checked) > ?)
			OR (replication_sql_thread_state NOT IN (-1 ,1))
			OR (replication_io_thread_state NOT IN (-1 ,1))
			OR (ABS(CAST(replication_lag_seconds AS integer) - CAST(sql_delay AS integer)) > ?)
			OR (ABS(CAST(replica_lag_seconds AS integer) - CAST(sql_delay AS integer)) > ?)
			OR (gtid_errant != '')
		)`

	args := sqlutils.Args(keyspace, keyspace, shard, shard, config.GetInstancePollSeconds()*5, config.GetReasonableReplicationLagSeconds(), config.GetReasonableReplicationLagSeconds())
	return readInstancesByCondition(condition, args, "")
}

// ReadInstancesWithErrantGTIds reads all instances with errant GTIDs
func ReadInstancesWithErrantGTIds(keyspace string, shard string) ([]*Instance, error) {
	condition := `
		keyspace LIKE (CASE WHEN ? = '' THEN '%' ELSE ? END)
		AND shard LIKE (CASE WHEN ? = '' THEN '%' ELSE ? END)
		AND gtid_errant != ''`

	args := sqlutils.Args(keyspace, keyspace, shard, shard)
	return readInstancesByCondition(condition, args, "")
}

// GetKeyspaceShardName gets the keyspace shard name for the given instance key
func GetKeyspaceShardName(tabletAlias string) (keyspace string, shard string, err error) {
	query := `SELECT
		keyspace,
		shard
	FROM
		vitess_tablet
	WHERE
		alias = ?
	`
	err = db.QueryVTOrc(query, sqlutils.Args(tabletAlias), func(m sqlutils.RowMap) error {
		keyspace = m.GetString("keyspace")
		shard = m.GetString("shard")
		return nil
	})
	if err != nil {
		log.Error(err)
	}
	return keyspace, shard, err
}

// ReadOutdatedInstanceKeys reads and returns keys for all instances that are not up to date (i.e.
// pre-configured time has passed since they were last checked) or the ones whose tablet information was read
// but not the mysql information. This could happen if the durability policy of the keyspace wasn't
// available at the time it was discovered. This would lead to not having the record of the tablet in the
// database_instance table.
// We also check for the case where an attempt at instance checking has been made, that hasn't
// resulted in an actual check! This can happen when TCP/IP connections are hung, in which case the "check"
// never returns. In such case we multiply interval by a factor, so as not to open too many connections on
// the instance.
func ReadOutdatedInstanceKeys() ([]string, error) {
	var res []string
	query := `SELECT
		alias
	FROM
		database_instance
	WHERE
		CASE
			WHEN last_attempted_check <= last_checked
			THEN last_checked < DATETIME('now', PRINTF('-%d SECOND', ?))
			ELSE last_checked < DATETIME('now', PRINTF('-%d SECOND', ?))
		END
	UNION
	SELECT
		vitess_tablet.alias
	FROM
		vitess_tablet LEFT JOIN database_instance ON (
		vitess_tablet.alias = database_instance.alias
	)
	WHERE
		database_instance.alias IS NULL
	`
	args := sqlutils.Args(config.GetInstancePollSeconds(), 2*config.GetInstancePollSeconds())

	err := db.QueryVTOrc(query, args, func(m sqlutils.RowMap) error {
		tabletAlias := m.GetString("alias")
		if !InstanceIsForgotten(tabletAlias) {
			// only if not in "forget" cache
			res = append(res, tabletAlias)
		}
		// We don;t return an error because we want to keep filling the outdated instances list.
		return nil
	})
	if err != nil {
		log.Error(err)
	}
	return res, err
}

func mkInsert(table string, columns []string, values []string, nrRows int, insertIgnore bool) (string, error) {
	if len(columns) == 0 {
		return "", errors.New("Column list cannot be empty")
	}
	if nrRows < 1 {
		return "", errors.New("nrRows must be a positive number")
	}
	if len(columns) != len(values) {
		return "", errors.New("number of values must be equal to number of columns")
	}

	var q strings.Builder
	insertStr := "REPLACE INTO"
	if insertIgnore {
		insertStr = "INSERT OR IGNORE INTO"
	}
	valRow := fmt.Sprintf("(%s)", strings.Join(values, ", "))
	var val strings.Builder
	val.WriteString(valRow)
	for i := 1; i < nrRows; i++ {
		val.WriteString(",\n                ") // indent VALUES, see below
		val.WriteString(valRow)
	}

	col := strings.Join(columns, ", ")
	query := fmt.Sprintf(`%s %s
			(%s)
		VALUES
			%s
		`,
		insertStr,
		table,
		col,
		val.String(),
	)
	q.WriteString(query)

	return q.String(), nil
}

func mkInsertForInstances(instances []*Instance, instanceWasActuallyFound bool, updateLastSeen bool) (string, []any, error) {
	if len(instances) == 0 {
		return "", nil, nil
	}

	insertIgnore := !instanceWasActuallyFound

	columns := []string{
		"alias",
		"hostname",
		"port",
		"cell",
		"last_checked",
		"last_attempted_check",
		"last_check_partial_success",
		"tablet_type",
		"server_id",
		"server_uuid",
		"version",
		"major_version",
		"version_comment",
		"binlog_server",
		"read_only",
		"binlog_format",
		"binlog_row_image",
		"log_bin",
		"log_replica_updates",
		"binary_log_file",
		"binary_log_pos",
		"source_host",
		"source_port",
		"replica_net_timeout",
		"heartbeat_interval",
		"replica_sql_running",
		"replica_io_running",
		"replication_sql_thread_state",
		"replication_io_thread_state",
		"has_replication_filters",
		"supports_oracle_gtid",
		"oracle_gtid",
		"source_uuid",
		"ancestry_uuid",
		"executed_gtid_set",
		"gtid_mode",
		"gtid_purged",
		"gtid_errant",
		"source_log_file",
		"read_source_log_pos",
		"relay_source_log_file",
		"exec_source_log_pos",
		"relay_log_file",
		"relay_log_pos",
		"last_sql_error",
		"last_io_error",
		"replication_lag_seconds",
		"replica_lag_seconds",
		"sql_delay",
		"replication_depth",
		"is_co_primary",
		"has_replication_credentials",
		"allow_tls",
		"semi_sync_enforced",
		"semi_sync_primary_enabled",
		"semi_sync_primary_timeout",
		"semi_sync_primary_wait_for_replica_count",
		"semi_sync_replica_enabled",
		"semi_sync_primary_status",
		"semi_sync_primary_clients",
		"semi_sync_replica_status",
		"semi_sync_blocked",
		"last_discovery_latency",
		"is_disk_stalled",
	}

	values := make([]string, len(columns))
	for i := range columns {
		values[i] = "?"
	}
	values[4] = "DATETIME('now')" // last_checked
	values[5] = "DATETIME('now')" // last_attempted_check
	values[6] = "1"               // last_check_partial_success

	if updateLastSeen {
		columns = append(columns, "last_seen")
		values = append(values, "DATETIME('now')")
	}

	var args []any
	for _, instance := range instances {
		// number of columns minus 2 as last_checked and last_attempted_check
		// updated with NOW()
		args = append(args, instance.InstanceAlias)
		args = append(args, instance.Hostname)
		args = append(args, instance.Port)
		args = append(args, instance.Cell)
		args = append(args, int(instance.TabletType))
		args = append(args, instance.ServerID)
		args = append(args, instance.ServerUUID)
		args = append(args, instance.Version)
		args = append(args, instance.MajorVersionString())
		args = append(args, instance.VersionComment)
		args = append(args, instance.IsBinlogServer())
		args = append(args, instance.ReadOnly)
		args = append(args, instance.BinlogFormat)
		args = append(args, instance.BinlogRowImage)
		args = append(args, instance.LogBinEnabled)
		args = append(args, instance.LogReplicationUpdatesEnabled)
		args = append(args, instance.SelfBinlogCoordinates.LogFile)
		args = append(args, instance.SelfBinlogCoordinates.LogPos)
		args = append(args, instance.SourceHost)
		args = append(args, instance.SourcePort)
		args = append(args, instance.ReplicaNetTimeout)
		args = append(args, instance.HeartbeatInterval)
		args = append(args, instance.ReplicationSQLThreadRuning)
		args = append(args, instance.ReplicationIOThreadRuning)
		args = append(args, instance.ReplicationSQLThreadState)
		args = append(args, instance.ReplicationIOThreadState)
		args = append(args, instance.HasReplicationFilters)
		args = append(args, instance.SupportsOracleGTID)
		args = append(args, instance.UsingOracleGTID)
		args = append(args, instance.SourceUUID)
		args = append(args, instance.AncestryUUID)
		args = append(args, instance.ExecutedGtidSet)
		args = append(args, instance.GTIDMode)
		args = append(args, instance.GtidPurged)
		args = append(args, instance.GtidErrant)
		args = append(args, instance.ReadBinlogCoordinates.LogFile)
		args = append(args, instance.ReadBinlogCoordinates.LogPos)
		args = append(args, instance.ExecBinlogCoordinates.LogFile)
		args = append(args, instance.ExecBinlogCoordinates.LogPos)
		args = append(args, instance.RelaylogCoordinates.LogFile)
		args = append(args, instance.RelaylogCoordinates.LogPos)
		args = append(args, instance.LastSQLError)
		args = append(args, instance.LastIOError)
		args = append(args, instance.SecondsBehindPrimary)
		args = append(args, instance.ReplicationLagSeconds)
		args = append(args, instance.SQLDelay)
		args = append(args, instance.ReplicationDepth)
		args = append(args, instance.IsCoPrimary)
		args = append(args, instance.HasReplicationCredentials)
		args = append(args, instance.AllowTLS)
		args = append(args, instance.SemiSyncEnforced)
		args = append(args, instance.SemiSyncPrimaryEnabled)
		args = append(args, instance.SemiSyncPrimaryTimeout)
		args = append(args, instance.SemiSyncPrimaryWaitForReplicaCount)
		args = append(args, instance.SemiSyncReplicaEnabled)
		args = append(args, instance.SemiSyncPrimaryStatus)
		args = append(args, instance.SemiSyncPrimaryClients)
		args = append(args, instance.SemiSyncReplicaStatus)
		args = append(args, instance.SemiSyncBlocked)
		args = append(args, instance.LastDiscoveryLatency.Nanoseconds())
		args = append(args, instance.StalledDisk)
	}

	sql, err := mkInsert("database_instance", columns, values, len(instances), insertIgnore)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to build query: %v", err)
		log.Errorf(errMsg)
		return sql, args, errors.New(errMsg)
	}

	return sql, args, nil
}

// writeManyInstances stores instances in the vtorc backend
func writeManyInstances(instances []*Instance, instanceWasActuallyFound bool, updateLastSeen bool) error {
	writeInstances := [](*Instance){}
	for _, instance := range instances {
		if InstanceIsForgotten(instance.InstanceAlias) {
			continue
		}
		writeInstances = append(writeInstances, instance)
	}
	if len(writeInstances) == 0 {
		return nil // nothing to write
	}
	sql, args, err := mkInsertForInstances(writeInstances, instanceWasActuallyFound, updateLastSeen)
	if err != nil {
		return err
	}
	if _, err := db.ExecVTOrc(sql, args...); err != nil {
		return err
	}
	return nil
}

// WriteInstance stores an instance in the vtorc backend
func WriteInstance(instance *Instance, instanceWasActuallyFound bool, lastError error) error {
	if lastError != nil {
		log.Infof("writeInstance: will not update database_instance due to error: %+v", lastError)
		return nil
	}
	return writeManyInstances([]*Instance{instance}, instanceWasActuallyFound, true)
}

// UpdateInstanceLastChecked updates the last_check timestamp in the vtorc backed database
// for a given instance
func UpdateInstanceLastChecked(tabletAlias string, partialSuccess bool, stalledDisk bool) error {
	writeFunc := func() error {
		_, err := db.ExecVTOrc(`UPDATE database_instance
			SET
				last_checked = DATETIME('now'),
				last_check_partial_success = ?,
				is_disk_stalled = ?
			WHERE
				alias = ?
			`,
			partialSuccess,
			stalledDisk,
			tabletAlias,
		)
		if err != nil {
			log.Error(err)
		}
		return err
	}
	return ExecDBWriteFunc(writeFunc)
}

// UpdateInstanceLastAttemptedCheck updates the last_attempted_check timestamp in the vtorc backed database
// for a given instance.
// This is used as a failsafe mechanism in case access to the instance gets hung (it happens), in which case
// the entire ReadTopology gets stuck (and no, connection timeout nor driver timeouts don't help. Don't look at me,
// the world is a harsh place to live in).
// And so we make sure to note down *before* we even attempt to access the instance; and this raises a red flag when we
// wish to access the instance again: if last_attempted_check is *newer* than last_checked, that's bad news and means
// we have a "hanging" issue.
func UpdateInstanceLastAttemptedCheck(tabletAlias string) error {
	writeFunc := func() error {
		_, err := db.ExecVTOrc(`UPDATE database_instance
			SET
				last_attempted_check = DATETIME('now')
			WHERE
				alias = ?
			`,
			tabletAlias,
		)
		if err != nil {
			log.Error(err)
		}
		return err
	}
	return ExecDBWriteFunc(writeFunc)
}

func InstanceIsForgotten(tabletAlias string) bool {
	_, found := forgetAliases.Get(tabletAlias)
	return found
}

// ForgetInstance removes an instance entry from the vtorc backed database.
// It may be auto-rediscovered through topology or requested for discovery by multiple means.
func ForgetInstance(tabletAlias string) error {
	if tabletAlias == "" {
		errMsg := "ForgetInstance(): empty tabletAlias"
		log.Errorf(errMsg)
		return errors.New(errMsg)
	}
	forgetAliases.Set(tabletAlias, true, cache.DefaultExpiration)
	log.Infof("Forgetting: %v", tabletAlias)

	// Remove this tablet from errant GTID count metric.
	currentErrantGTIDCount.Reset(tabletAlias)

	// Delete from the 'vitess_tablet' table.
	_, err := db.ExecVTOrc(`DELETE
		FROM vitess_tablet
		WHERE
			alias = ?
		`,
		tabletAlias,
	)
	if err != nil {
		log.Error(err)
		return err
	}

	// Also delete from the 'database_instance' table.
	sqlResult, err := db.ExecVTOrc(`DELETE
		FROM database_instance
		WHERE
			alias = ?
		`,
		tabletAlias,
	)
	if err != nil {
		log.Error(err)
		return err
	}
	// Get the number of rows affected. If they are zero, then we tried to forget an instance that doesn't exist.
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		log.Error(err)
		return err
	}
	if rows == 0 {
		errMsg := fmt.Sprintf("ForgetInstance(): tablet %+v not found", tabletAlias)
		log.Error(errMsg)
		return errors.New(errMsg)
	}
	_ = AuditOperation("forget", tabletAlias, "")
	return nil
}

// ForgetLongUnseenInstances will remove entries of all instances that have long since been last seen.
func ForgetLongUnseenInstances() error {
	sqlResult, err := db.ExecVTOrc(`DELETE
		FROM database_instance
		WHERE
			last_seen < DATETIME('now', PRINTF('-%d HOUR', ?))
		`,
		config.UnseenInstanceForgetHours,
	)
	if err != nil {
		log.Error(err)
		return err
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		log.Error(err)
		return err
	}
	if rows > 0 {
		_ = AuditOperation("forget-unseen", "", fmt.Sprintf("Forgotten instances: %d", rows))
	}
	return err
}

// SnapshotTopologies records topology graph for all existing topologies
func SnapshotTopologies() error {
	writeFunc := func() error {
		_, err := db.ExecVTOrc(`INSERT OR IGNORE
			INTO database_instance_topology_history (
				snapshot_unix_timestamp,
				alias,
				hostname,
				port,
				source_host,
				source_port,
				keyspace,
				shard,
				version
			)
			SELECT
				STRFTIME('%s', 'now'),
				vitess_tablet.alias, vitess_tablet.hostname, vitess_tablet.port,
				database_instance.source_host, database_instance.source_port,
				vitess_tablet.keyspace, vitess_tablet.shard, database_instance.version
			FROM
				vitess_tablet LEFT JOIN database_instance USING (alias, hostname, port)
			`,
		)
		if err != nil {
			log.Error(err)
			return err
		}

		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

func ExpireStaleInstanceBinlogCoordinates() error {
	expireSeconds := config.GetReasonableReplicationLagSeconds() * 2
	if expireSeconds < config.StaleInstanceCoordinatesExpireSeconds {
		expireSeconds = config.StaleInstanceCoordinatesExpireSeconds
	}
	writeFunc := func() error {
		_, err := db.ExecVTOrc(`DELETE
			FROM database_instance_stale_binlog_coordinates
			WHERE
				first_seen < DATETIME('now', PRINTF('-%d SECOND', ?))
			`,
			expireSeconds,
		)
		if err != nil {
			log.Error(err)
		}
		return err
	}
	return ExecDBWriteFunc(writeFunc)
}

// GetDatabaseState takes the snapshot of the database and returns it.
func GetDatabaseState() (string, error) {
	type tableState struct {
		TableName string
		Rows      []sqlutils.RowMap
	}

	var dbState []tableState
	for _, tableName := range db.TableNames {
		ts := tableState{
			TableName: tableName,
		}
		err := db.QueryVTOrc("SELECT * FROM "+tableName, nil, func(rowMap sqlutils.RowMap) error {
			ts.Rows = append(ts.Rows, rowMap)
			return nil
		})
		if err != nil {
			return "", err
		}
		dbState = append(dbState, ts)
	}
	jsonData, err := json.MarshalIndent(dbState, "", "\t")
	if err != nil {
		return "", err
	}

	return string(jsonData), nil
}
