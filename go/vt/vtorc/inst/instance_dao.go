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
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/rcrowley/go-metrics"
	"github.com/sjmudd/stopwatch"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"

	vitessmysql "vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/vt/log"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
	"vitess.io/vitess/go/vt/vtorc/collection"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/metrics/query"
	"vitess.io/vitess/go/vt/vtorc/util"
	math "vitess.io/vitess/go/vt/vtorc/util"
)

const (
	backendDBConcurrency = 20
)

var instanceReadChan = make(chan bool, backendDBConcurrency)
var instanceWriteChan = make(chan bool, backendDBConcurrency)

// Constant strings for Group Replication information
// See https://dev.mysql.com/doc/refman/8.0/en/replication-group-members-table.html for additional information.
const (
	// Group member roles
	GroupReplicationMemberRolePrimary   = "PRIMARY"
	GroupReplicationMemberRoleSecondary = "SECONDARY"
	// Group member states
	GroupReplicationMemberStateOnline      = "ONLINE"
	GroupReplicationMemberStateRecovering  = "RECOVERING"
	GroupReplicationMemberStateUnreachable = "UNREACHABLE"
	GroupReplicationMemberStateOffline     = "OFFLINE"
	GroupReplicationMemberStateError       = "ERROR"
)

var forgetInstanceKeys *cache.Cache

var accessDeniedCounter = metrics.NewCounter()
var readTopologyInstanceCounter = metrics.NewCounter()
var readInstanceCounter = metrics.NewCounter()
var writeInstanceCounter = metrics.NewCounter()
var backendWrites = collection.CreateOrReturnCollection("BACKEND_WRITES")
var writeBufferLatency = stopwatch.NewNamedStopwatch()

var emptyQuotesRegexp = regexp.MustCompile(`^""$`)

func init() {
	_ = metrics.Register("instance.access_denied", accessDeniedCounter)
	_ = metrics.Register("instance.read_topology", readTopologyInstanceCounter)
	_ = metrics.Register("instance.read", readInstanceCounter)
	_ = metrics.Register("instance.write", writeInstanceCounter)
	_ = writeBufferLatency.AddMany([]string{"wait", "write"})
	writeBufferLatency.Start("wait")

	go initializeInstanceDao()
}

func initializeInstanceDao() {
	config.WaitForConfigurationToBeLoaded()
	forgetInstanceKeys = cache.New(time.Duration(config.Config.InstancePollSeconds*3)*time.Second, time.Second)
}

// ExecDBWriteFunc chooses how to execute a write onto the database: whether synchronuously or not
func ExecDBWriteFunc(f func() error) error {
	m := query.NewMetric()

	instanceWriteChan <- true
	m.WaitLatency = time.Since(m.Timestamp)

	// catch the exec time and error if there is one
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}

			if s, ok := r.(string); ok {
				m.Err = errors.New(s)
			} else {
				m.Err = r.(error)
			}
		}
		m.ExecuteLatency = time.Since(m.Timestamp.Add(m.WaitLatency))
		_ = backendWrites.Append(m)
		<-instanceWriteChan // assume this takes no time
	}()
	res := f()
	return res
}

func ExpireTableData(tableName string, timestampColumn string) error {
	query := fmt.Sprintf("delete from %s where %s < NOW() - INTERVAL ? DAY", tableName, timestampColumn)
	writeFunc := func() error {
		_, err := db.ExecVTOrc(query, config.Config.AuditPurgeDays)
		return err
	}
	return ExecDBWriteFunc(writeFunc)
}

// logReadTopologyInstanceError logs an error, if applicable, for a ReadTopologyInstance operation,
// providing context and hint as for the source of the error. If there's no hint just provide the
// original error.
func logReadTopologyInstanceError(instanceKey *InstanceKey, hint string, err error) error {
	if err == nil {
		return nil
	}
	if !util.ClearToLog("ReadTopologyInstance", instanceKey.StringCode()) {
		return err
	}
	var msg string
	if hint == "" {
		msg = fmt.Sprintf("ReadTopologyInstance(%+v): %+v", *instanceKey, err)
	} else {
		msg = fmt.Sprintf("ReadTopologyInstance(%+v) %+v: %+v",
			*instanceKey,
			strings.Replace(hint, "%", "%%", -1), // escape %
			err)
	}
	log.Errorf(msg)
	return fmt.Errorf(msg)
}

// ReadTopologyInstance collects information on the state of a MySQL
// server and writes the result synchronously to the vtorc
// backend.
func ReadTopologyInstance(instanceKey *InstanceKey) (*Instance, error) {
	return ReadTopologyInstanceBufferable(instanceKey, nil)
}

// ReadTopologyInstanceBufferable connects to a topology MySQL instance
// and collects information on the server and its replication state.
// It writes the information retrieved into vtorc's backend.
// - writes are optionally buffered.
// - timing information can be collected for the stages performed.
func ReadTopologyInstanceBufferable(instanceKey *InstanceKey, latency *stopwatch.NamedStopwatch) (inst *Instance, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = logReadTopologyInstanceError(instanceKey, "Unexpected, aborting", tb.Errorf("%+v", r))
		}
	}()

	var waitGroup sync.WaitGroup
	var tablet *topodatapb.Tablet
	var durability reparentutil.Durabler
	var fullStatus *replicationdatapb.FullStatus
	readingStartTime := time.Now()
	instance := NewInstance()
	instanceFound := false
	partialSuccess := false
	resolvedHostname := ""
	errorChan := make(chan error, 32)
	var resolveErr error

	if !instanceKey.IsValid() {
		latency.Start("backend")
		if err := UpdateInstanceLastAttemptedCheck(instanceKey); err != nil {
			log.Errorf("ReadTopologyInstanceBufferable: %+v: %v", instanceKey, err)
		}
		latency.Stop("backend")
		return instance, fmt.Errorf("ReadTopologyInstance will not act on invalid instance key: %+v", *instanceKey)
	}

	lastAttemptedCheckTimer := time.AfterFunc(time.Second, func() {
		go func() {
			_ = UpdateInstanceLastAttemptedCheck(instanceKey)
		}()
	})

	latency.Start("instance")

	tablet, err = ReadTablet(*instanceKey)
	if err != nil {
		goto Cleanup
	}
	if tablet == nil {
		// This can happen because Orc rediscovers instances by alt hostnames,
		// lit localhost, ip, etc.
		// TODO(sougou): disable this ability.
		goto Cleanup
	}

	durability, err = GetDurabilityPolicy(tablet)
	if err != nil {
		goto Cleanup
	}

	fullStatus, err = FullStatus(*instanceKey)
	if err != nil {
		goto Cleanup
	}
	partialSuccess = true // We at least managed to read something from the server.

	instance.Key = *instanceKey
	{
		// We begin with a few operations we can run concurrently, and which do not depend on anything
		instance.ServerID = uint(fullStatus.ServerId)
		instance.Version = fullStatus.Version
		instance.ReadOnly = fullStatus.ReadOnly
		instance.LogBinEnabled = fullStatus.LogBinEnabled
		instance.BinlogFormat = fullStatus.BinlogFormat
		instance.LogReplicationUpdatesEnabled = fullStatus.LogReplicaUpdates
		instance.VersionComment = fullStatus.VersionComment
		resolvedHostname = instance.Key.Hostname

		if instance.LogBinEnabled && fullStatus.PrimaryStatus != nil {
			binlogPos, err := getBinlogCoordinatesFromPositionString(fullStatus.PrimaryStatus.FilePosition)
			instance.SelfBinlogCoordinates = binlogPos
			errorChan <- err
		}

		instance.SemiSyncPrimaryEnabled = fullStatus.SemiSyncPrimaryEnabled
		instance.SemiSyncReplicaEnabled = fullStatus.SemiSyncReplicaEnabled
		instance.SemiSyncPrimaryWaitForReplicaCount = uint(fullStatus.SemiSyncWaitForReplicaCount)
		instance.SemiSyncPrimaryTimeout = fullStatus.SemiSyncPrimaryTimeout

		instance.SemiSyncPrimaryClients = uint(fullStatus.SemiSyncPrimaryClients)
		instance.SemiSyncPrimaryStatus = fullStatus.SemiSyncPrimaryStatus
		instance.SemiSyncReplicaStatus = fullStatus.SemiSyncReplicaStatus

		if (instance.IsOracleMySQL() || instance.IsPercona()) && !instance.IsSmallerMajorVersionByString("5.6") {
			// Stuff only supported on Oracle MySQL >= 5.6
			// ...
			// @@gtid_mode only available in Orcale MySQL >= 5.6
			instance.GTIDMode = fullStatus.GtidMode
			instance.ServerUUID = fullStatus.ServerUuid
			if fullStatus.PrimaryStatus != nil {
				GtidExecutedPos, err := vitessmysql.DecodePosition(fullStatus.PrimaryStatus.Position)
				errorChan <- err
				if err == nil && GtidExecutedPos.GTIDSet != nil {
					instance.ExecutedGtidSet = GtidExecutedPos.GTIDSet.String()
				}
			}
			GtidPurgedPos, err := vitessmysql.DecodePosition(fullStatus.GtidPurged)
			errorChan <- err
			if err == nil && GtidPurgedPos.GTIDSet != nil {
				instance.GtidPurged = GtidPurgedPos.GTIDSet.String()
			}
			instance.BinlogRowImage = fullStatus.BinlogRowImage

			if instance.GTIDMode != "" && instance.GTIDMode != "OFF" {
				instance.SupportsOracleGTID = true
			}
		}
	}
	if resolvedHostname != instance.Key.Hostname {
		latency.Start("backend")
		UpdateResolvedHostname(instance.Key.Hostname, resolvedHostname)
		latency.Stop("backend")
		instance.Key.Hostname = resolvedHostname
	}
	if instance.Key.Hostname == "" {
		err = fmt.Errorf("ReadTopologyInstance: empty hostname (%+v). Bailing out", *instanceKey)
		goto Cleanup
	}
	go func() {
		_ = ResolveHostnameIPs(instance.Key.Hostname)
	}()

	instance.ReplicationIOThreadState = ReplicationThreadStateNoThread
	instance.ReplicationSQLThreadState = ReplicationThreadStateNoThread
	if fullStatus.ReplicationStatus != nil {
		instance.HasReplicationCredentials = fullStatus.ReplicationStatus.SourceUser != ""

		instance.ReplicationIOThreadState = ReplicationThreadStateFromReplicationState(vitessmysql.ReplicationState(fullStatus.ReplicationStatus.IoState))
		instance.ReplicationSQLThreadState = ReplicationThreadStateFromReplicationState(vitessmysql.ReplicationState(fullStatus.ReplicationStatus.SqlState))
		instance.ReplicationIOThreadRuning = instance.ReplicationIOThreadState.IsRunning()
		instance.ReplicationSQLThreadRuning = instance.ReplicationSQLThreadState.IsRunning()

		binlogPos, err := getBinlogCoordinatesFromPositionString(fullStatus.ReplicationStatus.RelayLogSourceBinlogEquivalentPosition)
		instance.ReadBinlogCoordinates = binlogPos
		errorChan <- err

		binlogPos, err = getBinlogCoordinatesFromPositionString(fullStatus.ReplicationStatus.FilePosition)
		instance.ExecBinlogCoordinates = binlogPos
		errorChan <- err
		instance.IsDetached, _ = instance.ExecBinlogCoordinates.ExtractDetachedCoordinates()

		binlogPos, err = getBinlogCoordinatesFromPositionString(fullStatus.ReplicationStatus.RelayLogFilePosition)
		instance.RelaylogCoordinates = binlogPos
		instance.RelaylogCoordinates.Type = RelayLog
		errorChan <- err

		instance.LastSQLError = emptyQuotesRegexp.ReplaceAllString(strconv.QuoteToASCII(fullStatus.ReplicationStatus.LastSqlError), "")
		instance.LastIOError = emptyQuotesRegexp.ReplaceAllString(strconv.QuoteToASCII(fullStatus.ReplicationStatus.LastIoError), "")

		instance.SQLDelay = uint(fullStatus.ReplicationStatus.SqlDelay)
		instance.UsingOracleGTID = fullStatus.ReplicationStatus.AutoPosition
		instance.UsingMariaDBGTID = fullStatus.ReplicationStatus.UsingGtid
		instance.SourceUUID = fullStatus.ReplicationStatus.SourceUuid
		instance.HasReplicationFilters = fullStatus.ReplicationStatus.HasReplicationFilters

		primaryHostname := fullStatus.ReplicationStatus.SourceHost
		primaryKey, err := NewResolveInstanceKey(primaryHostname, int(fullStatus.ReplicationStatus.SourcePort))
		if err != nil {
			_ = logReadTopologyInstanceError(instanceKey, "NewResolveInstanceKey", err)
		}
		primaryKey.Hostname, resolveErr = ResolveHostname(primaryKey.Hostname)
		if resolveErr != nil {
			_ = logReadTopologyInstanceError(instanceKey, fmt.Sprintf("ResolveHostname(%q)", primaryKey.Hostname), resolveErr)
		}
		instance.SourceKey = *primaryKey
		instance.IsDetachedPrimary = instance.SourceKey.IsDetached()

		if fullStatus.ReplicationStatus.ReplicationLagUnknown {
			instance.SecondsBehindPrimary.Valid = false
		} else {
			instance.SecondsBehindPrimary.Valid = true
			instance.SecondsBehindPrimary.Int64 = int64(fullStatus.ReplicationStatus.ReplicationLagSeconds)
		}
		if instance.SecondsBehindPrimary.Valid && instance.SecondsBehindPrimary.Int64 < 0 {
			log.Warningf("Host: %+v, instance.ReplicationLagSeconds < 0 [%+v], correcting to 0", instanceKey, instance.SecondsBehindPrimary.Int64)
			instance.SecondsBehindPrimary.Int64 = 0
		}
		// And until told otherwise:
		instance.ReplicationLagSeconds = instance.SecondsBehindPrimary

		instance.AllowTLS = fullStatus.ReplicationStatus.SslAllowed
	}

	instanceFound = true

	// -------------------------------------------------------------------------
	// Anything after this point does not affect the fact the instance is found.
	// No `goto Cleanup` after this point.
	// -------------------------------------------------------------------------

	instance.DataCenter = tablet.Alias.Cell
	instance.InstanceAlias = topoproto.TabletAliasString(tablet.Alias)

	{
		latency.Start("backend")
		err = ReadInstanceClusterAttributes(instance)
		latency.Stop("backend")
		_ = logReadTopologyInstanceError(instanceKey, "ReadInstanceClusterAttributes", err)
	}

	// We need to update candidate_database_instance.
	// We register the rule even if it hasn't changed,
	// to bump the last_suggested time.
	instance.PromotionRule = PromotionRule(durability, tablet)
	err = RegisterCandidateInstance(NewCandidateDatabaseInstance(instanceKey, instance.PromotionRule).WithCurrentTime())
	_ = logReadTopologyInstanceError(instanceKey, "RegisterCandidateInstance", err)

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
		instance.AncestryUUID = fmt.Sprintf("%s,%s", instance.AncestryUUID, instance.ReplicationGroupName)
		instance.AncestryUUID = strings.Trim(instance.AncestryUUID, ",")
		if instance.ExecutedGtidSet != "" && instance.primaryExecutedGtidSet != "" {
			// Compare primary & replica GTID sets, but ignore the sets that present the primary's UUID.
			// This is because vtorc may pool primary and replica at an inconvenient timing,
			// such that the replica may _seems_ to have more entries than the primary, when in fact
			// it's just that the primary's probing is stale.
			redactedExecutedGtidSet, _ := NewOracleGtidSet(instance.ExecutedGtidSet)
			for _, uuid := range strings.Split(instance.AncestryUUID, ",") {
				if uuid != instance.ServerUUID {
					redactedExecutedGtidSet.RemoveUUID(uuid)
				}
				if instance.IsCoPrimary && uuid == instance.ServerUUID {
					// If this is a co-primary, then this server is likely to show its own generated GTIDs as errant,
					// because its co-primary has not applied them yet
					redactedExecutedGtidSet.RemoveUUID(uuid)
				}
			}
			// Avoid querying the database if there's no point:
			if !redactedExecutedGtidSet.IsEmpty() {
				redactedPrimaryExecutedGtidSet, _ := NewOracleGtidSet(instance.primaryExecutedGtidSet)
				redactedPrimaryExecutedGtidSet.RemoveUUID(instance.SourceUUID)

				instance.GtidErrant, err = vitessmysql.Subtract(redactedExecutedGtidSet.String(), redactedPrimaryExecutedGtidSet.String())
			}
		}
	}

	latency.Stop("instance")
	readTopologyInstanceCounter.Inc(1)

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
	// updated on success by writeInstance.
	latency.Start("backend")
	_ = UpdateInstanceLastChecked(instanceKey, partialSuccess)
	latency.Stop("backend")
	return nil, err
}

// getKeyspaceShardName returns a single string having both the keyspace and shard
func getKeyspaceShardName(keyspace, shard string) string {
	return fmt.Sprintf("%v:%v", keyspace, shard)
}

func getBinlogCoordinatesFromPositionString(position string) (BinlogCoordinates, error) {
	pos, err := vitessmysql.DecodePosition(position)
	if err != nil || pos.GTIDSet == nil {
		return BinlogCoordinates{}, err
	}
	binLogCoordinates, err := ParseBinlogCoordinates(pos.String())
	if err != nil {
		return BinlogCoordinates{}, err
	}
	return *binLogCoordinates, nil
}

func ReadReplicationGroupPrimary(instance *Instance) (err error) {
	query := `
	SELECT
		replication_group_primary_host,
		replication_group_primary_port
	FROM
		database_instance
	WHERE
		replication_group_name = ?
		AND replication_group_member_role = 'PRIMARY'
`
	queryArgs := sqlutils.Args(instance.ReplicationGroupName)
	err = db.QueryVTOrc(query, queryArgs, func(row sqlutils.RowMap) error {
		groupPrimaryHost := row.GetString("replication_group_primary_host")
		groupPrimaryPort := row.GetInt("replication_group_primary_port")
		resolvedGroupPrimary, err := NewResolveInstanceKey(groupPrimaryHost, groupPrimaryPort)
		if err != nil {
			return err
		}
		instance.ReplicationGroupPrimaryInstanceKey = *resolvedGroupPrimary
		return nil
	})
	return err
}

// ReadInstanceClusterAttributes will return the cluster name for a given instance by looking at its primary
// and getting it from there.
// It is a non-recursive function and so-called-recursion is performed upon periodic reading of
// instances.
func ReadInstanceClusterAttributes(instance *Instance) (err error) {
	var primaryOrGroupPrimaryInstanceKey InstanceKey
	var primaryOrGroupPrimaryReplicationDepth uint
	var ancestryUUID string
	var primaryOrGroupPrimaryExecutedGtidSet string
	primaryOrGroupPrimaryDataFound := false

	query := `
			select
					replication_depth,
					source_host,
					source_port,
					ancestry_uuid,
					executed_gtid_set
				from database_instance
				where hostname=? and port=?
	`
	// For instances that are part of a replication group, if the host is not the group's primary, we use the
	// information from the group primary. If it is the group primary, we use the information of its primary
	// (if it has any). If it is not a group member, we use the information from the host's primary.
	if instance.IsReplicationGroupSecondary() {
		primaryOrGroupPrimaryInstanceKey = instance.ReplicationGroupPrimaryInstanceKey
	} else {
		primaryOrGroupPrimaryInstanceKey = instance.SourceKey
	}
	args := sqlutils.Args(primaryOrGroupPrimaryInstanceKey.Hostname, primaryOrGroupPrimaryInstanceKey.Port)
	err = db.QueryVTOrc(query, args, func(m sqlutils.RowMap) error {
		primaryOrGroupPrimaryReplicationDepth = m.GetUint("replication_depth")
		primaryOrGroupPrimaryInstanceKey.Hostname = m.GetString("source_host")
		primaryOrGroupPrimaryInstanceKey.Port = m.GetInt("source_port")
		ancestryUUID = m.GetString("ancestry_uuid")
		primaryOrGroupPrimaryExecutedGtidSet = m.GetString("executed_gtid_set")
		primaryOrGroupPrimaryDataFound = true
		return nil
	})
	if err != nil {
		log.Error(err)
		return err
	}

	var replicationDepth uint
	if primaryOrGroupPrimaryDataFound {
		replicationDepth = primaryOrGroupPrimaryReplicationDepth + 1
	}
	isCoPrimary := false
	if primaryOrGroupPrimaryInstanceKey.Equals(&instance.Key) {
		// co-primary calls for special case, in fear of the infinite loop
		isCoPrimary = true
	}
	instance.ReplicationDepth = replicationDepth
	instance.IsCoPrimary = isCoPrimary
	instance.AncestryUUID = ancestryUUID
	instance.primaryExecutedGtidSet = primaryOrGroupPrimaryExecutedGtidSet
	return nil
}

// readInstanceRow reads a single instance row from the vtorc backend database.
func readInstanceRow(m sqlutils.RowMap) *Instance {
	instance := NewInstance()

	instance.Key.Hostname = m.GetString("hostname")
	instance.Key.Port = m.GetInt("port")
	instance.ServerID = m.GetUint("server_id")
	instance.ServerUUID = m.GetString("server_uuid")
	instance.Version = m.GetString("version")
	instance.VersionComment = m.GetString("version_comment")
	instance.ReadOnly = m.GetBool("read_only")
	instance.BinlogFormat = m.GetString("binlog_format")
	instance.BinlogRowImage = m.GetString("binlog_row_image")
	instance.LogBinEnabled = m.GetBool("log_bin")
	instance.LogReplicationUpdatesEnabled = m.GetBool("log_replica_updates")
	instance.SourceKey.Hostname = m.GetString("source_host")
	instance.SourceKey.Port = m.GetInt("source_port")
	instance.IsDetachedPrimary = instance.SourceKey.IsDetached()
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
	instance.UsingMariaDBGTID = m.GetBool("mariadb_gtid")
	instance.SelfBinlogCoordinates.LogFile = m.GetString("binary_log_file")
	instance.SelfBinlogCoordinates.LogPos = m.GetUint32("binary_log_pos")
	instance.ReadBinlogCoordinates.LogFile = m.GetString("source_log_file")
	instance.ReadBinlogCoordinates.LogPos = m.GetUint32("read_source_log_pos")
	instance.ExecBinlogCoordinates.LogFile = m.GetString("relay_source_log_file")
	instance.ExecBinlogCoordinates.LogPos = m.GetUint32("exec_source_log_pos")
	instance.IsDetached, _ = instance.ExecBinlogCoordinates.ExtractDetachedCoordinates()
	instance.RelaylogCoordinates.LogFile = m.GetString("relay_log_file")
	instance.RelaylogCoordinates.LogPos = m.GetUint32("relay_log_pos")
	instance.RelaylogCoordinates.Type = RelayLog
	instance.LastSQLError = m.GetString("last_sql_error")
	instance.LastIOError = m.GetString("last_io_error")
	instance.SecondsBehindPrimary = m.GetNullInt64("replication_lag_seconds")
	instance.ReplicationLagSeconds = m.GetNullInt64("replica_lag_seconds")
	instance.SQLDelay = m.GetUint("sql_delay")
	instance.DataCenter = m.GetString("data_center")
	instance.Region = m.GetString("region")
	instance.PhysicalEnvironment = m.GetString("physical_environment")
	instance.SemiSyncEnforced = m.GetBool("semi_sync_enforced")
	instance.SemiSyncPrimaryEnabled = m.GetBool("semi_sync_primary_enabled")
	instance.SemiSyncPrimaryTimeout = m.GetUint64("semi_sync_primary_timeout")
	instance.SemiSyncPrimaryWaitForReplicaCount = m.GetUint("semi_sync_primary_wait_for_replica_count")
	instance.SemiSyncReplicaEnabled = m.GetBool("semi_sync_replica_enabled")
	instance.SemiSyncPrimaryStatus = m.GetBool("semi_sync_primary_status")
	instance.SemiSyncPrimaryClients = m.GetUint("semi_sync_primary_clients")
	instance.SemiSyncReplicaStatus = m.GetBool("semi_sync_replica_status")
	instance.ReplicationDepth = m.GetUint("replication_depth")
	instance.IsCoPrimary = m.GetBool("is_co_primary")
	instance.HasReplicationCredentials = m.GetBool("has_replication_credentials")
	instance.IsUpToDate = (m.GetUint("seconds_since_last_checked") <= config.Config.InstancePollSeconds)
	instance.IsRecentlyChecked = (m.GetUint("seconds_since_last_checked") <= config.Config.InstancePollSeconds*5)
	instance.LastSeenTimestamp = m.GetString("last_seen")
	instance.IsLastCheckValid = m.GetBool("is_last_check_valid")
	instance.SecondsSinceLastSeen = m.GetNullInt64("seconds_since_last_seen")
	instance.IsCandidate = m.GetBool("is_candidate")
	instance.PromotionRule = promotionrule.CandidatePromotionRule(m.GetString("promotion_rule"))
	instance.IsDowntimed = m.GetBool("is_downtimed")
	instance.DowntimeReason = m.GetString("downtime_reason")
	instance.DowntimeOwner = m.GetString("downtime_owner")
	instance.DowntimeEndTimestamp = m.GetString("downtime_end_timestamp")
	instance.ElapsedDowntime = time.Second * time.Duration(m.GetInt("elapsed_downtime_seconds"))
	instance.UnresolvedHostname = m.GetString("unresolved_hostname")
	instance.AllowTLS = m.GetBool("allow_tls")
	instance.InstanceAlias = m.GetString("instance_alias")
	instance.LastDiscoveryLatency = time.Duration(m.GetInt64("last_discovery_latency")) * time.Nanosecond

	instance.applyFlavorName()

	/* Read Group Replication variables below */
	instance.ReplicationGroupName = m.GetString("replication_group_name")
	instance.ReplicationGroupIsSinglePrimary = m.GetBool("replication_group_is_single_primary_mode")
	instance.ReplicationGroupMemberState = m.GetString("replication_group_member_state")
	instance.ReplicationGroupMemberRole = m.GetString("replication_group_member_role")
	instance.ReplicationGroupPrimaryInstanceKey = InstanceKey{Hostname: m.GetString("replication_group_primary_host"),
		Port: m.GetInt("replication_group_primary_port")}
	_ = instance.ReplicationGroupMembers.ReadJSON(m.GetString("replication_group_members"))
	//instance.ReplicationGroup = m.GetString("replication_group_")

	// problems
	if !instance.IsLastCheckValid {
		instance.Problems = append(instance.Problems, "last_check_invalid")
	} else if !instance.IsRecentlyChecked {
		instance.Problems = append(instance.Problems, "not_recently_checked")
	} else if instance.ReplicationThreadsExist() && !instance.ReplicaRunning() {
		instance.Problems = append(instance.Problems, "not_replicating")
	} else if instance.ReplicationLagSeconds.Valid && math.AbsInt64(instance.ReplicationLagSeconds.Int64-int64(instance.SQLDelay)) > int64(config.Config.ReasonableReplicationLagSeconds) {
		instance.Problems = append(instance.Problems, "replication_lag")
	}
	if instance.GtidErrant != "" {
		instance.Problems = append(instance.Problems, "errant_gtid")
	}
	// Group replication problems
	if instance.ReplicationGroupName != "" && instance.ReplicationGroupMemberState != GroupReplicationMemberStateOnline {
		instance.Problems = append(instance.Problems, "group_replication_member_not_online")
	}

	return instance
}

// readInstancesByCondition is a generic function to read instances from the backend database
func readInstancesByCondition(condition string, args []any, sort string) ([](*Instance), error) {
	readFunc := func() ([](*Instance), error) {
		instances := [](*Instance){}

		if sort == "" {
			sort = `hostname, port`
		}
		query := fmt.Sprintf(`
		select
			*,
			unix_timestamp() - unix_timestamp(last_checked) as seconds_since_last_checked,
			ifnull(last_checked <= last_seen, 0) as is_last_check_valid,
			unix_timestamp() - unix_timestamp(last_seen) as seconds_since_last_seen,
			candidate_database_instance.last_suggested is not null
				 and candidate_database_instance.promotion_rule in ('must', 'prefer') as is_candidate,
			ifnull(nullif(candidate_database_instance.promotion_rule, ''), 'neutral') as promotion_rule,
			ifnull(unresolved_hostname, '') as unresolved_hostname,
			(database_instance_downtime.downtime_active is not null and ifnull(database_instance_downtime.end_timestamp, now()) > now()) as is_downtimed,
    	ifnull(database_instance_downtime.reason, '') as downtime_reason,
			ifnull(database_instance_downtime.owner, '') as downtime_owner,
			ifnull(unix_timestamp() - unix_timestamp(begin_timestamp), 0) as elapsed_downtime_seconds,
    	ifnull(database_instance_downtime.end_timestamp, '') as downtime_end_timestamp
		from
			database_instance
			left join vitess_tablet using (hostname, port)
			left join candidate_database_instance using (hostname, port)
			left join hostname_unresolve using (hostname)
			left join database_instance_downtime using (hostname, port)
		where
			%s
		order by
			%s
			`, condition, sort)

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
	instanceReadChan <- true
	instances, err := readFunc()
	<-instanceReadChan
	return instances, err
}

func readInstancesByExactKey(instanceKey *InstanceKey) ([](*Instance), error) {
	condition := `
			hostname = ?
			and port = ?
		`
	return readInstancesByCondition(condition, sqlutils.Args(instanceKey.Hostname, instanceKey.Port), "")
}

// ReadInstance reads an instance from the vtorc backend database
func ReadInstance(instanceKey *InstanceKey) (*Instance, bool, error) {
	instances, err := readInstancesByExactKey(instanceKey)
	// We know there will be at most one (hostname & port are PK)
	// And we expect to find one
	readInstanceCounter.Inc(1)
	if len(instances) == 0 {
		return nil, false, err
	}
	if err != nil {
		return instances[0], false, err
	}
	return instances[0], true, nil
}

// ReadReplicaInstances reads replicas of a given primary
func ReadReplicaInstances(primaryKey *InstanceKey) ([](*Instance), error) {
	condition := `
			source_host = ?
			and source_port = ?
		`
	return readInstancesByCondition(condition, sqlutils.Args(primaryKey.Hostname, primaryKey.Port), "")
}

// ReadReplicaInstancesIncludingBinlogServerSubReplicas returns a list of direct slves including any replicas
// of a binlog server replica
func ReadReplicaInstancesIncludingBinlogServerSubReplicas(primaryKey *InstanceKey) ([](*Instance), error) {
	replicas, err := ReadReplicaInstances(primaryKey)
	if err != nil {
		return replicas, err
	}
	for _, replica := range replicas {
		replica := replica
		if replica.IsBinlogServer() {
			binlogServerReplicas, err := ReadReplicaInstancesIncludingBinlogServerSubReplicas(&replica.Key)
			if err != nil {
				return replicas, err
			}
			replicas = append(replicas, binlogServerReplicas...)
		}
	}
	return replicas, err
}

// ReadProblemInstances reads all instances with problems
func ReadProblemInstances(keyspace string, shard string) ([](*Instance), error) {
	condition := `
			keyspace LIKE (CASE WHEN ? = '' THEN '%' ELSE ? END)
			and shard LIKE (CASE WHEN ? = '' THEN '%' ELSE ? END)
			and (
				(last_seen < last_checked)
				or (unix_timestamp() - unix_timestamp(last_checked) > ?)
				or (replication_sql_thread_state not in (-1 ,1))
				or (replication_io_thread_state not in (-1 ,1))
				or (abs(cast(replication_lag_seconds as signed) - cast(sql_delay as signed)) > ?)
				or (abs(cast(replica_lag_seconds as signed) - cast(sql_delay as signed)) > ?)
				or (gtid_errant != '')
				or (replication_group_name != '' and replication_group_member_state != 'ONLINE')
			)
		`

	args := sqlutils.Args(keyspace, keyspace, shard, shard, config.Config.InstancePollSeconds*5, config.Config.ReasonableReplicationLagSeconds, config.Config.ReasonableReplicationLagSeconds)
	instances, err := readInstancesByCondition(condition, args, "")
	if err != nil {
		return instances, err
	}
	var reportedInstances [](*Instance)
	for _, instance := range instances {
		skip := false
		if instance.IsDowntimed {
			skip = true
		}
		if !skip {
			reportedInstances = append(reportedInstances, instance)
		}
	}
	return reportedInstances, nil
}

// ReadLostInRecoveryInstances returns all instances (potentially filtered by cluster)
// which are currently indicated as downtimed due to being lost during a topology recovery.
func ReadLostInRecoveryInstances(keyspace string, shard string) ([](*Instance), error) {
	condition := `
		ifnull(
			database_instance_downtime.downtime_active = 1
			and database_instance_downtime.end_timestamp > now()
			and database_instance_downtime.reason = ?, 0)
		and ? IN ('', keyspace)
		and ? IN ('', shard)
	`
	return readInstancesByCondition(condition, sqlutils.Args(DowntimeLostInRecoveryMessage, keyspace, shard), "keyspace asc, shard asc, replication_depth asc")
}

// readUnseenPrimaryKeys will read list of primaries that have never been seen, and yet whose replicas
// seem to be replicating.
func readUnseenPrimaryKeys() ([]InstanceKey, error) {
	res := []InstanceKey{}

	err := db.QueryVTOrcRowsMap(`
			SELECT DISTINCT
			    replica_instance.source_host, replica_instance.source_port
			FROM
			    database_instance replica_instance
			        LEFT JOIN
			    hostname_resolve ON (replica_instance.source_host = hostname_resolve.hostname)
			        LEFT JOIN
			    database_instance primary_instance ON (
			    	COALESCE(hostname_resolve.resolved_hostname, replica_instance.source_host) = primary_instance.hostname
			    	and replica_instance.source_port = primary_instance.port)
			WHERE
			    primary_instance.last_checked IS NULL
			    and replica_instance.source_host != ''
			    and replica_instance.source_host != '_'
			    and replica_instance.source_port > 0
			    and replica_instance.replica_io_running = 1
			`, func(m sqlutils.RowMap) error {
		instanceKey, _ := NewResolveInstanceKey(m.GetString("source_host"), m.GetInt("source_port"))
		// we ignore the error. It can be expected that we are unable to resolve the hostname.
		// Maybe that's how we got here in the first place!
		res = append(res, *instanceKey)

		return nil
	})
	if err != nil {
		log.Error(err)
		return res, err
	}

	return res, nil
}

// ForgetUnseenInstancesDifferentlyResolved will purge instances which are invalid, and whose hostname
// appears on the hostname_resolved table; this means some time in the past their hostname was unresovled, and now
// resovled to a different value; the old hostname is never accessed anymore and the old entry should be removed.
func ForgetUnseenInstancesDifferentlyResolved() error {
	query := `
			select
				database_instance.hostname, database_instance.port
			from
					hostname_resolve
					JOIN database_instance ON (hostname_resolve.hostname = database_instance.hostname)
			where
					hostname_resolve.hostname != hostname_resolve.resolved_hostname
					AND ifnull(last_checked <= last_seen, 0) = 0
	`
	keys := NewInstanceKeyMap()
	err := db.QueryVTOrc(query, nil, func(m sqlutils.RowMap) error {
		key := InstanceKey{
			Hostname: m.GetString("hostname"),
			Port:     m.GetInt("port"),
		}
		keys.AddKey(key)
		return nil
	})
	var rowsAffected int64
	for _, key := range keys.GetInstanceKeys() {
		sqlResult, err := db.ExecVTOrc(`
			delete from
				database_instance
			where
		    hostname = ? and port = ?
			`, key.Hostname, key.Port,
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
		rowsAffected = rowsAffected + rows
	}
	_ = AuditOperation("forget-unseen-differently-resolved", nil, fmt.Sprintf("Forgotten instances: %d", rowsAffected))
	return err
}

// readUnknownPrimaryHostnameResolves will figure out the resolved hostnames of primary-hosts which cannot be found.
// It uses the hostname_resolve_history table to heuristically guess the correct hostname (based on "this was the
// last time we saw this hostname and it resolves into THAT")
func readUnknownPrimaryHostnameResolves() (map[string]string, error) {
	res := make(map[string]string)
	err := db.QueryVTOrcRowsMap(`
			SELECT DISTINCT
			    replica_instance.source_host, hostname_resolve_history.resolved_hostname
			FROM
			    database_instance replica_instance
			LEFT JOIN hostname_resolve ON (replica_instance.source_host = hostname_resolve.hostname)
			LEFT JOIN database_instance primary_instance ON (
			    COALESCE(hostname_resolve.resolved_hostname, replica_instance.source_host) = primary_instance.hostname
			    and replica_instance.source_port = primary_instance.port
			) LEFT JOIN hostname_resolve_history ON (replica_instance.source_host = hostname_resolve_history.hostname)
			WHERE
			    primary_instance.last_checked IS NULL
			    and replica_instance.source_host != ''
			    and replica_instance.source_host != '_'
			    and replica_instance.source_port > 0
			`, func(m sqlutils.RowMap) error {
		res[m.GetString("source_host")] = m.GetString("resolved_hostname")
		return nil
	})
	if err != nil {
		log.Error(err)
		return res, err
	}

	return res, nil
}

// ResolveUnknownPrimaryHostnameResolves fixes missing hostname resolves based on hostname_resolve_history
// The use case is replicas replicating from some unknown-hostname which cannot be otherwise found. This could
// happen due to an expire unresolve together with clearing up of hostname cache.
func ResolveUnknownPrimaryHostnameResolves() error {

	hostnameResolves, err := readUnknownPrimaryHostnameResolves()
	if err != nil {
		return err
	}
	for hostname, resolvedHostname := range hostnameResolves {
		UpdateResolvedHostname(hostname, resolvedHostname)
	}

	_ = AuditOperation("resolve-unknown-primaries", nil, fmt.Sprintf("Num resolved hostnames: %d", len(hostnameResolves)))
	return err
}

// GetKeyspaceShardName gets the keyspace shard name for the given instance key
func GetKeyspaceShardName(instanceKey *InstanceKey) (keyspace string, shard string, err error) {
	query := `
		select
			keyspace,
			shard
		from
			vitess_tablet
		where
			hostname = ?
			and port = ?
			`
	err = db.QueryVTOrc(query, sqlutils.Args(instanceKey.Hostname, instanceKey.Port), func(m sqlutils.RowMap) error {
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
func ReadOutdatedInstanceKeys() ([]InstanceKey, error) {
	res := []InstanceKey{}
	query := `
		SELECT
			hostname, port
		FROM
			database_instance
		WHERE
			CASE
				WHEN last_attempted_check <= last_checked
				THEN last_checked < now() - interval ? second
				ELSE last_checked < now() - interval ? second
			END
		UNION
		SELECT
			vitess_tablet.hostname, vitess_tablet.port
		FROM
			vitess_tablet LEFT JOIN database_instance ON (
			vitess_tablet.hostname = database_instance.hostname
			AND vitess_tablet.port = database_instance.port
		)
		WHERE
			database_instance.hostname IS NULL
			`
	args := sqlutils.Args(config.Config.InstancePollSeconds, 2*config.Config.InstancePollSeconds)

	err := db.QueryVTOrc(query, args, func(m sqlutils.RowMap) error {
		instanceKey, merr := NewResolveInstanceKey(m.GetString("hostname"), m.GetInt("port"))
		if merr != nil {
			log.Error(merr)
		} else if !InstanceIsForgotten(instanceKey) {
			// only if not in "forget" cache
			res = append(res, *instanceKey)
		}
		// We don;t return an error because we want to keep filling the outdated instances list.
		return nil
	})

	if err != nil {
		log.Error(err)
	}
	return res, err

}

func mkInsertOdku(table string, columns []string, values []string, nrRows int, insertIgnore bool) (string, error) {
	if len(columns) == 0 {
		return "", errors.New("Column list cannot be empty")
	}
	if nrRows < 1 {
		return "", errors.New("nrRows must be a positive number")
	}
	if len(columns) != len(values) {
		return "", errors.New("number of values must be equal to number of columns")
	}

	var q bytes.Buffer
	var ignore string
	if insertIgnore {
		ignore = "ignore"
	}
	var valRow = fmt.Sprintf("(%s)", strings.Join(values, ", "))
	var val bytes.Buffer
	val.WriteString(valRow)
	for i := 1; i < nrRows; i++ {
		val.WriteString(",\n                ") // indent VALUES, see below
		val.WriteString(valRow)
	}

	var col = strings.Join(columns, ", ")
	var odku bytes.Buffer
	odku.WriteString(fmt.Sprintf("%s=VALUES(%s)", columns[0], columns[0]))
	for _, c := range columns[1:] {
		odku.WriteString(", ")
		odku.WriteString(fmt.Sprintf("%s=VALUES(%s)", c, c))
	}

	q.WriteString(fmt.Sprintf(`INSERT %s INTO %s
                (%s)
        VALUES
                %s
        ON DUPLICATE KEY UPDATE
                %s
        `,
		ignore, table, col, val.String(), odku.String()))

	return q.String(), nil
}

func mkInsertOdkuForInstances(instances []*Instance, instanceWasActuallyFound bool, updateLastSeen bool) (string, []any, error) {
	if len(instances) == 0 {
		return "", nil, nil
	}

	insertIgnore := false
	if !instanceWasActuallyFound {
		insertIgnore = true
	}
	var columns = []string{
		"hostname",
		"port",
		"last_checked",
		"last_attempted_check",
		"last_check_partial_success",
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
		"mariadb_gtid",
		"pseudo_gtid",
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
		"data_center",
		"region",
		"physical_environment",
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
		"instance_alias",
		"last_discovery_latency",
		"replication_group_name",
		"replication_group_is_single_primary_mode",
		"replication_group_member_state",
		"replication_group_member_role",
		"replication_group_members",
		"replication_group_primary_host",
		"replication_group_primary_port",
	}

	var values = make([]string, len(columns))
	for i := range columns {
		values[i] = "?"
	}
	values[2] = "NOW()" // last_checked
	values[3] = "NOW()" // last_attempted_check
	values[4] = "1"     // last_check_partial_success

	if updateLastSeen {
		columns = append(columns, "last_seen")
		values = append(values, "NOW()")
	}

	var args []any
	for _, instance := range instances {
		// number of columns minus 2 as last_checked and last_attempted_check
		// updated with NOW()
		args = append(args, instance.Key.Hostname)
		args = append(args, instance.Key.Port)
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
		args = append(args, instance.SourceKey.Hostname)
		args = append(args, instance.SourceKey.Port)
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
		args = append(args, instance.UsingMariaDBGTID)
		args = append(args, instance.UsingPseudoGTID)
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
		args = append(args, instance.DataCenter)
		args = append(args, instance.Region)
		args = append(args, instance.PhysicalEnvironment)
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
		args = append(args, instance.InstanceAlias)
		args = append(args, instance.LastDiscoveryLatency.Nanoseconds())
		args = append(args, instance.ReplicationGroupName)
		args = append(args, instance.ReplicationGroupIsSinglePrimary)
		args = append(args, instance.ReplicationGroupMemberState)
		args = append(args, instance.ReplicationGroupMemberRole)
		args = append(args, instance.ReplicationGroupMembers.ToJSONString())
		args = append(args, instance.ReplicationGroupPrimaryInstanceKey.Hostname)
		args = append(args, instance.ReplicationGroupPrimaryInstanceKey.Port)
	}

	sql, err := mkInsertOdku("database_instance", columns, values, len(instances), insertIgnore)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to build query: %v", err)
		log.Errorf(errMsg)
		return sql, args, fmt.Errorf(errMsg)
	}

	return sql, args, nil
}

// writeManyInstances stores instances in the vtorc backend
func writeManyInstances(instances []*Instance, instanceWasActuallyFound bool, updateLastSeen bool) error {
	writeInstances := [](*Instance){}
	for _, instance := range instances {
		if InstanceIsForgotten(&instance.Key) && !instance.IsSeed() {
			continue
		}
		writeInstances = append(writeInstances, instance)
	}
	if len(writeInstances) == 0 {
		return nil // nothing to write
	}
	sql, args, err := mkInsertOdkuForInstances(writeInstances, instanceWasActuallyFound, updateLastSeen)
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
func UpdateInstanceLastChecked(instanceKey *InstanceKey, partialSuccess bool) error {
	writeFunc := func() error {
		_, err := db.ExecVTOrc(`
        	update
        		database_instance
        	set
						last_checked = NOW(),
						last_check_partial_success = ?
			where
				hostname = ?
				and port = ?`,
			partialSuccess,
			instanceKey.Hostname,
			instanceKey.Port,
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
func UpdateInstanceLastAttemptedCheck(instanceKey *InstanceKey) error {
	writeFunc := func() error {
		_, err := db.ExecVTOrc(`
    	update
    		database_instance
    	set
    		last_attempted_check = NOW()
			where
				hostname = ?
				and port = ?`,
			instanceKey.Hostname,
			instanceKey.Port,
		)
		if err != nil {
			log.Error(err)
		}
		return err
	}
	return ExecDBWriteFunc(writeFunc)
}

func InstanceIsForgotten(instanceKey *InstanceKey) bool {
	_, found := forgetInstanceKeys.Get(instanceKey.StringCode())
	return found
}

// ForgetInstance removes an instance entry from the vtorc backed database.
// It may be auto-rediscovered through topology or requested for discovery by multiple means.
func ForgetInstance(instanceKey *InstanceKey) error {
	if instanceKey == nil {
		errMsg := "ForgetInstance(): nil instanceKey"
		log.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	forgetInstanceKeys.Set(instanceKey.StringCode(), true, cache.DefaultExpiration)
	sqlResult, err := db.ExecVTOrc(`
			delete
				from database_instance
			where
				hostname = ? and port = ?`,
		instanceKey.Hostname,
		instanceKey.Port,
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
	if rows == 0 {
		errMsg := fmt.Sprintf("ForgetInstance(): instance %+v not found", *instanceKey)
		log.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	_ = AuditOperation("forget", instanceKey, "")
	return nil
}

// ForgetLongUnseenInstances will remove entries of all instacnes that have long since been last seen.
func ForgetLongUnseenInstances() error {
	sqlResult, err := db.ExecVTOrc(`
			delete
				from database_instance
			where
				last_seen < NOW() - interval ? hour`,
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
	_ = AuditOperation("forget-unseen", nil, fmt.Sprintf("Forgotten instances: %d", rows))
	return err
}

// SnapshotTopologies records topology graph for all existing topologies
func SnapshotTopologies() error {
	writeFunc := func() error {
		_, err := db.ExecVTOrc(`
        	insert ignore into
        		database_instance_topology_history (snapshot_unix_timestamp,
        			hostname, port, source_host, source_port, version)
        	select
        		UNIX_TIMESTAMP(NOW()),
        		hostname, port, source_host, source_port, version
			from
				database_instance
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

// RecordStaleInstanceBinlogCoordinates snapshots the binlog coordinates of instances
func RecordStaleInstanceBinlogCoordinates(instanceKey *InstanceKey, binlogCoordinates *BinlogCoordinates) error {
	args := sqlutils.Args(
		instanceKey.Hostname, instanceKey.Port,
		binlogCoordinates.LogFile, binlogCoordinates.LogPos,
	)
	_, err := db.ExecVTOrc(`
			delete from
				database_instance_stale_binlog_coordinates
			where
				hostname=? and port=?
				and (
					binary_log_file != ?
					or binary_log_pos != ?
				)
				`,
		args...,
	)
	if err != nil {
		log.Error(err)
		return err
	}
	_, err = db.ExecVTOrc(`
			insert ignore into
				database_instance_stale_binlog_coordinates (
					hostname, port,	binary_log_file, binary_log_pos, first_seen
				)
				values (
					?, ?, ?, ?, NOW()
				)`,
		args...)
	if err != nil {
		log.Error(err)
	}
	return err
}

func ExpireStaleInstanceBinlogCoordinates() error {
	expireSeconds := config.Config.ReasonableReplicationLagSeconds * 2
	if expireSeconds < config.StaleInstanceCoordinatesExpireSeconds {
		expireSeconds = config.StaleInstanceCoordinatesExpireSeconds
	}
	writeFunc := func() error {
		_, err := db.ExecVTOrc(`
					delete from database_instance_stale_binlog_coordinates
					where first_seen < NOW() - INTERVAL ? SECOND
					`, expireSeconds,
		)
		if err != nil {
			log.Error(err)
		}
		return err
	}
	return ExecDBWriteFunc(writeFunc)
}
