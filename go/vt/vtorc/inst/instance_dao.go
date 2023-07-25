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
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/rcrowley/go-metrics"
	"github.com/sjmudd/stopwatch"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/external/golib/sqlutils"

	vitessmysql "vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/vt/log"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
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

var (
	// Mutex to protect the access of the following variable
	errantGtidMapMu = sync.Mutex{}
	errantGtidMap   = make(map[string]string)
)

var forgetAliases *cache.Cache

var accessDeniedCounter = metrics.NewCounter()
var readTopologyInstanceCounter = metrics.NewCounter()
var readInstanceCounter = metrics.NewCounter()
var writeInstanceCounter = metrics.NewCounter()
var backendWrites = collection.CreateOrReturnCollection("BACKEND_WRITES")
var writeBufferLatency = stopwatch.NewNamedStopwatch()

var emptyQuotesRegexp = regexp.MustCompile(`^""$`)
var cacheInitializationCompleted atomic.Bool

func init() {
	_ = metrics.Register("instance.access_denied", accessDeniedCounter)
	_ = metrics.Register("instance.read_topology", readTopologyInstanceCounter)
	_ = metrics.Register("instance.read", readInstanceCounter)
	_ = metrics.Register("instance.write", writeInstanceCounter)
	_ = writeBufferLatency.AddMany([]string{"wait", "write"})
	writeBufferLatency.Start("wait")
	stats.NewStringMapFuncWithMultiLabels("ErrantGtidMap", "Metric to track the errant GTIDs detected by VTOrc", []string{"TabletAlias"}, "ErrantGtid", func() map[string]string {
		errantGtidMapMu.Lock()
		defer errantGtidMapMu.Unlock()
		return errantGtidMap
	})

	go initializeInstanceDao()
}

func initializeInstanceDao() {
	config.WaitForConfigurationToBeLoaded()
	forgetAliases = cache.New(time.Duration(config.Config.InstancePollSeconds*3)*time.Second, time.Second)
	cacheInitializationCompleted.Store(true)
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
			strings.Replace(hint, "%", "%%", -1), // escape %
			err)
	}
	log.Errorf(msg)
	return fmt.Errorf(msg)
}

// ReadTopologyInstance collects information on the state of a MySQL
// server and writes the result synchronously to the vtorc
// backend.
func ReadTopologyInstance(tabletAlias string) (*Instance, error) {
	return ReadTopologyInstanceBufferable(tabletAlias, nil)
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
	var fullStatus *replicationdatapb.FullStatus
	readingStartTime := time.Now()
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

	fullStatus, err = FullStatus(tabletAlias)
	if err != nil {
		goto Cleanup
	}
	partialSuccess = true // We at least managed to read something from the server.

	instance.Hostname = tablet.MysqlHostname
	instance.Port = int(tablet.MysqlPort)
	{
		// We begin with a few operations we can run concurrently, and which do not depend on anything
		instance.ServerID = uint(fullStatus.ServerId)
		instance.Version = fullStatus.Version
		instance.ReadOnly = fullStatus.ReadOnly
		instance.LogBinEnabled = fullStatus.LogBinEnabled
		instance.BinlogFormat = fullStatus.BinlogFormat
		instance.LogReplicationUpdatesEnabled = fullStatus.LogReplicaUpdates
		instance.VersionComment = fullStatus.VersionComment

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

		if instance.IsOracleMySQL() || instance.IsPercona() {
			// Stuff only supported on Oracle / Percona MySQL
			// ...
			// @@gtid_mode only available in Oracle / Percona MySQL >= 5.6
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

		instance.SourceHost = fullStatus.ReplicationStatus.SourceHost
		instance.SourcePort = int(fullStatus.ReplicationStatus.SourcePort)

		if fullStatus.ReplicationStatus.ReplicationLagUnknown {
			instance.SecondsBehindPrimary.Valid = false
		} else {
			instance.SecondsBehindPrimary.Valid = true
			instance.SecondsBehindPrimary.Int64 = int64(fullStatus.ReplicationStatus.ReplicationLagSeconds)
		}
		if instance.SecondsBehindPrimary.Valid && instance.SecondsBehindPrimary.Int64 < 0 {
			log.Warningf("Alias: %+v, instance.SecondsBehindPrimary < 0 [%+v], correcting to 0", tabletAlias, instance.SecondsBehindPrimary.Int64)
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
		// update the errant gtid map
		go func() {
			errantGtidMapMu.Lock()
			defer errantGtidMapMu.Unlock()
			errantGtidMap[topoproto.TabletAliasString(tablet.Alias)] = instance.GtidErrant
		}()
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
	_ = UpdateInstanceLastChecked(tabletAlias, partialSuccess)
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

// ReadInstanceClusterAttributes will return the cluster name for a given instance by looking at its primary
// and getting it from there.
// It is a non-recursive function and so-called-recursion is performed upon periodic reading of
// instances.
func ReadInstanceClusterAttributes(instance *Instance) (err error) {
	var primaryReplicationDepth uint
	var ancestryUUID string
	var primaryExecutedGtidSet string
	primaryDataFound := false

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
	isCoPrimary := false
	if primaryHostname == instance.Hostname && primaryPort == instance.Port {
		// co-primary calls for special case, in fear of the infinite loop
		isCoPrimary = true
	}
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
	} else if instance.ReplicationLagSeconds.Valid && math.AbsInt64(instance.ReplicationLagSeconds.Int64-int64(instance.SQLDelay)) > int64(config.Config.ReasonableReplicationLagSeconds) {
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
		query := fmt.Sprintf(`
		select
			*,
			unix_timestamp() - unix_timestamp(last_checked) as seconds_since_last_checked,
			ifnull(last_checked <= last_seen, 0) as is_last_check_valid,
			unix_timestamp() - unix_timestamp(last_seen) as seconds_since_last_seen
		from
			vitess_tablet
			left join database_instance using (alias, hostname, port)
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

// ReadInstance reads an instance from the vtorc backend database
func ReadInstance(tabletAlias string) (*Instance, bool, error) {
	condition := `
			alias = ?
		`
	instances, err := readInstancesByCondition(condition, sqlutils.Args(tabletAlias), "")
	// We know there will be at most one (alias is the PK).
	// And we expect to find one.
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
func ReadReplicaInstances(primaryHost string, primaryPort int) ([](*Instance), error) {
	condition := `
			source_host = ?
			and source_port = ?
		`
	return readInstancesByCondition(condition, sqlutils.Args(primaryHost, primaryPort), "")
}

// ReadReplicaInstancesIncludingBinlogServerSubReplicas returns a list of direct slves including any replicas
// of a binlog server replica
func ReadReplicaInstancesIncludingBinlogServerSubReplicas(primaryHost string, primaryPort int) ([](*Instance), error) {
	replicas, err := ReadReplicaInstances(primaryHost, primaryPort)
	if err != nil {
		return replicas, err
	}
	for _, replica := range replicas {
		replica := replica
		if replica.IsBinlogServer() {
			binlogServerReplicas, err := ReadReplicaInstancesIncludingBinlogServerSubReplicas(replica.Hostname, replica.Port)
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
			)
		`

	args := sqlutils.Args(keyspace, keyspace, shard, shard, config.Config.InstancePollSeconds*5, config.Config.ReasonableReplicationLagSeconds, config.Config.ReasonableReplicationLagSeconds)
	return readInstancesByCondition(condition, args, "")
}

// GetKeyspaceShardName gets the keyspace shard name for the given instance key
func GetKeyspaceShardName(tabletAlias string) (keyspace string, shard string, err error) {
	query := `
		select
			keyspace,
			shard
		from
			vitess_tablet
		where
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
	query := `
		SELECT
			alias
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
			vitess_tablet.alias
		FROM
			vitess_tablet LEFT JOIN database_instance ON (
			vitess_tablet.alias = database_instance.alias
		)
		WHERE
			database_instance.alias IS NULL
			`
	args := sqlutils.Args(config.Config.InstancePollSeconds, 2*config.Config.InstancePollSeconds)

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
		"alias",
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
		"last_discovery_latency",
	}

	var values = make([]string, len(columns))
	for i := range columns {
		values[i] = "?"
	}
	values[3] = "NOW()" // last_checked
	values[4] = "NOW()" // last_attempted_check
	values[5] = "1"     // last_check_partial_success

	if updateLastSeen {
		columns = append(columns, "last_seen")
		values = append(values, "NOW()")
	}

	var args []any
	for _, instance := range instances {
		// number of columns minus 2 as last_checked and last_attempted_check
		// updated with NOW()
		args = append(args, instance.InstanceAlias)
		args = append(args, instance.Hostname)
		args = append(args, instance.Port)
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
		args = append(args, instance.LastDiscoveryLatency.Nanoseconds())
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
		if InstanceIsForgotten(instance.InstanceAlias) {
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
func UpdateInstanceLastChecked(tabletAlias string, partialSuccess bool) error {
	writeFunc := func() error {
		_, err := db.ExecVTOrc(`
        	update
        		database_instance
        	set
						last_checked = NOW(),
						last_check_partial_success = ?
			where
				alias = ?`,
			partialSuccess,
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
		_, err := db.ExecVTOrc(`
    	update
    		database_instance
    	set
    		last_attempted_check = NOW()
			where
				alias = ?`,
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
		return fmt.Errorf(errMsg)
	}
	forgetAliases.Set(tabletAlias, true, cache.DefaultExpiration)
	log.Infof("Forgetting: %v", tabletAlias)

	// Delete from the 'vitess_tablet' table.
	_, err := db.ExecVTOrc(`
					delete
						from vitess_tablet
					where
						alias = ?`,
		tabletAlias,
	)
	if err != nil {
		log.Error(err)
		return err
	}

	// Also delete from the 'database_instance' table.
	sqlResult, err := db.ExecVTOrc(`
			delete
				from database_instance
			where
				alias = ?`,
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
		log.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	_ = AuditOperation("forget", tabletAlias, "")
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
	_ = AuditOperation("forget-unseen", "", fmt.Sprintf("Forgotten instances: %d", rows))
	return err
}

// SnapshotTopologies records topology graph for all existing topologies
func SnapshotTopologies() error {
	writeFunc := func() error {
		_, err := db.ExecVTOrc(`
        	insert ignore into
        		database_instance_topology_history (snapshot_unix_timestamp,
        			alias, hostname, port, source_host, source_port, keyspace, shard, version)
        	select
        		UNIX_TIMESTAMP(NOW()),
				vitess_tablet.alias, vitess_tablet.hostname, vitess_tablet.port, 
				database_instance.source_host, database_instance.source_port, 
				vitess_tablet.keyspace, vitess_tablet.shard, database_instance.version
			from
				vitess_tablet left join database_instance using (alias, hostname, port)
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
func RecordStaleInstanceBinlogCoordinates(tabletAlias string, binlogCoordinates *BinlogCoordinates) error {
	args := sqlutils.Args(
		tabletAlias,
		binlogCoordinates.LogFile, binlogCoordinates.LogPos,
	)
	_, err := db.ExecVTOrc(`
			delete from
				database_instance_stale_binlog_coordinates
			where
				alias = ?
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
					alias,	binary_log_file, binary_log_pos, first_seen
				)
				values (
					?, ?, ?, NOW()
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
