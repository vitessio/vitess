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
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/patrickmn/go-cache"
	"github.com/rcrowley/go-metrics"
	"github.com/sjmudd/stopwatch"

	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/math"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"

	"vitess.io/vitess/go/vt/orchestrator/attributes"
	"vitess.io/vitess/go/vt/orchestrator/collection"
	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/db"
	"vitess.io/vitess/go/vt/orchestrator/kv"
	"vitess.io/vitess/go/vt/orchestrator/metrics/query"
	"vitess.io/vitess/go/vt/orchestrator/util"
)

const (
	backendDBConcurrency       = 20
	retryInstanceFunctionCount = 5
	retryInterval              = 500 * time.Millisecond
	error1045AccessDenied      = "Error 1045: Access denied for user"
	errorConnectionRefused     = "getsockopt: connection refused"
	errorNoSuchHost            = "no such host"
	errorIOTimeout             = "i/o timeout"
)

var instanceReadChan = make(chan bool, backendDBConcurrency)
var instanceWriteChan = make(chan bool, backendDBConcurrency)

// InstancesByCountReplicas is a sortable type for Instance
type InstancesByCountReplicas [](*Instance)

func (this InstancesByCountReplicas) Len() int      { return len(this) }
func (this InstancesByCountReplicas) Swap(i, j int) { this[i], this[j] = this[j], this[i] }
func (this InstancesByCountReplicas) Less(i, j int) bool {
	return len(this[i].Replicas) < len(this[j].Replicas)
}

// Constant strings for Group Replication information
// See https://dev.mysql.com/doc/refman/8.0/en/replication-group-members-table.html for additional information.
const (
	// Group member roles
	GroupReplicationMemberRolePrimary   = "PRIMARY"
	GroupReplicationMemberRoleSecondary = "SECONDARY"
	// Group member states
	GroupReplicationMemberStateOnline     = "ONLINE"
	GroupReplicationMemberStateRecovering = "RECOVERING"
	GroupReplicationMemberStateOffline    = "OFFLINE"
	GroupReplicationMemberStateError      = "ERROR"
)

// We use this map to identify whether the query failed because the server does not support group replication or due
// to a different reason.
var GroupReplicationNotSupportedErrors = map[uint16]bool{
	// If either the group replication global variables are not known or the
	// performance_schema.replication_group_members table does not exist, the host does not support group
	// replication, at least in the form supported here.
	1193: true, // ERROR: 1193 (HY000): Unknown system variable 'group_replication_group_name'
	1146: true, // ERROR: 1146 (42S02): Table 'performance_schema.replication_group_members' doesn't exist
}

// instanceKeyInformativeClusterName is a non-authoritative cache; used for auditing or general purpose.
var instanceKeyInformativeClusterName *cache.Cache
var forgetInstanceKeys *cache.Cache
var clusterInjectedPseudoGTIDCache *cache.Cache

var accessDeniedCounter = metrics.NewCounter()
var readTopologyInstanceCounter = metrics.NewCounter()
var readInstanceCounter = metrics.NewCounter()
var writeInstanceCounter = metrics.NewCounter()
var backendWrites = collection.CreateOrReturnCollection("BACKEND_WRITES")
var writeBufferMetrics = collection.CreateOrReturnCollection("WRITE_BUFFER")
var writeBufferLatency = stopwatch.NewNamedStopwatch()

var emptyQuotesRegexp = regexp.MustCompile(`^""$`)

func init() {
	metrics.Register("instance.access_denied", accessDeniedCounter)
	metrics.Register("instance.read_topology", readTopologyInstanceCounter)
	metrics.Register("instance.read", readInstanceCounter)
	metrics.Register("instance.write", writeInstanceCounter)
	writeBufferLatency.AddMany([]string{"wait", "write"})
	writeBufferLatency.Start("wait")

	go initializeInstanceDao()
}

func initializeInstanceDao() {
	config.WaitForConfigurationToBeLoaded()
	instanceWriteBuffer = make(chan instanceUpdateObject, config.Config.InstanceWriteBufferSize)
	instanceKeyInformativeClusterName = cache.New(time.Duration(config.Config.InstancePollSeconds/2)*time.Second, time.Second)
	forgetInstanceKeys = cache.New(time.Duration(config.Config.InstancePollSeconds*3)*time.Second, time.Second)
	clusterInjectedPseudoGTIDCache = cache.New(time.Minute, time.Second)
	// spin off instance write buffer flushing
	go func() {
		flushTick := time.Tick(time.Duration(config.Config.InstanceFlushIntervalMilliseconds) * time.Millisecond) //nolint SA1015: using time.Tick leaks the underlying ticker
		for {
			// it is time to flush
			select {
			case <-flushTick:
				flushInstanceWriteBuffer()
			case <-forceFlushInstanceWriteBuffer:
				flushInstanceWriteBuffer()
			}
		}
	}()
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
		backendWrites.Append(m)
		<-instanceWriteChan // assume this takes no time
	}()
	res := f()
	return res
}

func ExpireTableData(tableName string, timestampColumn string) error {
	query := fmt.Sprintf("delete from %s where %s < NOW() - INTERVAL ? DAY", tableName, timestampColumn)
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(query, config.Config.AuditPurgeDays)
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
	return log.Errorf(msg)
}

// ReadTopologyInstance collects information on the state of a MySQL
// server and writes the result synchronously to the orchestrator
// backend.
func ReadTopologyInstance(instanceKey *InstanceKey) (*Instance, error) {
	return ReadTopologyInstanceBufferable(instanceKey, false, nil)
}

func RetryInstanceFunction(f func() (*Instance, error)) (instance *Instance, err error) {
	for i := 0; i < retryInstanceFunctionCount; i++ {
		if instance, err = f(); err == nil {
			return instance, nil
		}
	}
	return instance, err
}

// Is this an error which means that we shouldn't try going more queries for this discovery attempt?
func unrecoverableError(err error) bool {
	contains := []string{
		error1045AccessDenied,
		errorConnectionRefused,
		errorIOTimeout,
		errorNoSuchHost,
	}
	for _, k := range contains {
		if strings.Contains(err.Error(), k) {
			return true
		}
	}
	return false
}

// Check if the instance is a MaxScale binlog server (a proxy not a real
// MySQL server) and also update the resolved hostname
func (instance *Instance) checkMaxScale(db *sql.DB, latency *stopwatch.NamedStopwatch) (isMaxScale bool, resolvedHostname string, err error) {
	if config.Config.SkipMaxScaleCheck {
		return isMaxScale, resolvedHostname, err
	}

	latency.Start("instance")
	err = sqlutils.QueryRowsMap(db, "show variables like 'maxscale%'", func(m sqlutils.RowMap) error {
		if m.GetString("Variable_name") == "MAXSCALE_VERSION" {
			originalVersion := m.GetString("Value")
			if originalVersion == "" {
				originalVersion = m.GetString("value")
			}
			if originalVersion == "" {
				originalVersion = "0.0.0"
			}
			instance.Version = originalVersion + "-maxscale"
			instance.ServerID = 0
			instance.ServerUUID = ""
			instance.Uptime = 0
			instance.Binlog_format = "INHERIT"
			instance.ReadOnly = true
			instance.LogBinEnabled = true
			instance.LogReplicationUpdatesEnabled = true
			resolvedHostname = instance.Key.Hostname
			latency.Start("backend")
			UpdateResolvedHostname(resolvedHostname, resolvedHostname)
			latency.Stop("backend")
			isMaxScale = true
		}
		return nil
	})
	latency.Stop("instance")

	// Detect failed connection attempts and don't report the command
	// we are executing as that might be confusing.
	if err != nil {
		if strings.Contains(err.Error(), error1045AccessDenied) {
			accessDeniedCounter.Inc(1)
		}
		if unrecoverableError(err) {
			logReadTopologyInstanceError(&instance.Key, "", err)
		} else {
			logReadTopologyInstanceError(&instance.Key, "show variables like 'maxscale%'", err)
		}
	}

	return isMaxScale, resolvedHostname, err
}

// expectReplicationThreadsState expects both replication threads to be running, or both to be not running.
// Specifically, it looks for both to be "Yes" or for both to be "No".
func expectReplicationThreadsState(instanceKey *InstanceKey, expectedState ReplicationThreadState) (expectationMet bool, err error) {
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return false, err
	}
	err = sqlutils.QueryRowsMap(db, "show slave status", func(m sqlutils.RowMap) error {
		ioThreadState := ReplicationThreadStateFromStatus(m.GetString("Slave_IO_Running"))
		sqlThreadState := ReplicationThreadStateFromStatus(m.GetString("Slave_SQL_Running"))

		if ioThreadState == expectedState && sqlThreadState == expectedState {
			expectationMet = true
		}
		return nil
	})
	return expectationMet, err
}

// ReadTopologyInstanceBufferable connects to a topology MySQL instance
// and collects information on the server and its replication state.
// It writes the information retrieved into orchestrator's backend.
// - writes are optionally buffered.
// - timing information can be collected for the stages performed.
func ReadTopologyInstanceBufferable(instanceKey *InstanceKey, bufferWrites bool, latency *stopwatch.NamedStopwatch) (inst *Instance, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = logReadTopologyInstanceError(instanceKey, "Unexpected, aborting", tb.Errorf("%+v", r))
		}
	}()

	var waitGroup sync.WaitGroup
	var serverUuidWaitGroup sync.WaitGroup
	var tablet *topodatapb.Tablet
	readingStartTime := time.Now()
	instance := NewInstance()
	instanceFound := false
	partialSuccess := false
	foundByShowSlaveHosts := false
	resolvedHostname := ""
	maxScaleMasterHostname := ""
	isMaxScale := false
	isMaxScale110 := false
	slaveStatusFound := false
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
		go UpdateInstanceLastAttemptedCheck(instanceKey)
	})

	latency.Start("instance")
	db, err := db.OpenDiscovery(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		goto Cleanup
	}

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

	instance.Key = *instanceKey

	if isMaxScale, resolvedHostname, err = instance.checkMaxScale(db, latency); err != nil {
		// We do not "goto Cleanup" here, although it should be the correct flow.
		// Reason is 5.7's new security feature that requires GRANTs on performance_schema.session_variables.
		// There is a wrong decision making in this design and the migration path to 5.7 will be difficult.
		// I don't want orchestrator to put even more burden on this.
		// If the statement errors, then we are unable to determine that this is maxscale, hence assume it is not.
		// In which case there would be other queries sent to the server that are not affected by 5.7 behavior, and that will fail.

		// Certain errors are not recoverable (for this discovery process) so it's fine to go to Cleanup
		if unrecoverableError(err) {
			goto Cleanup
		}
	}

	if isMaxScale {
		if strings.Contains(instance.Version, "1.1.0") {
			isMaxScale110 = true

			// Buggy buggy maxscale 1.1.0. Reported Master_Host can be corrupted.
			// Therefore we (currently) take @@hostname (which is masquerading as master host anyhow)
			err = db.QueryRow("select @@hostname").Scan(&maxScaleMasterHostname)
			if err != nil {
				goto Cleanup
			}
		}
		if isMaxScale110 {
			// Only this is supported:
			db.QueryRow("select @@server_id").Scan(&instance.ServerID)
		} else {
			db.QueryRow("select @@global.server_id").Scan(&instance.ServerID)
			db.QueryRow("select @@global.server_uuid").Scan(&instance.ServerUUID)
		}
	} else {
		// NOT MaxScale

		// We begin with a few operations we can run concurrently, and which do not depend on anything
		{
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				var dummy string
				// show global status works just as well with 5.6 & 5.7 (5.7 moves variables to performance_schema)
				err := db.QueryRow("show global status like 'Uptime'").Scan(&dummy, &instance.Uptime)

				if err != nil {
					logReadTopologyInstanceError(instanceKey, "show global status like 'Uptime'", err)

					// We do not "goto Cleanup" here, although it should be the correct flow.
					// Reason is 5.7's new security feature that requires GRANTs on performance_schema.global_variables.
					// There is a wrong decisionmaking in this design and the migration path to 5.7 will be difficult.
					// I don't want orchestrator to put even more burden on this. The 'Uptime' variable is not that important
					// so as to completely fail reading a 5.7 instance.
					// This is supposed to be fixed in 5.7.9
				}
				errorChan <- err
			}()
		}

		var mysqlHostname, mysqlReportHost string
		err = db.QueryRow("select @@global.hostname, ifnull(@@global.report_host, ''), @@global.server_id, @@global.version, @@global.version_comment, @@global.read_only, @@global.binlog_format, @@global.log_bin, @@global.log_slave_updates").Scan(
			&mysqlHostname, &mysqlReportHost, &instance.ServerID, &instance.Version, &instance.VersionComment, &instance.ReadOnly, &instance.Binlog_format, &instance.LogBinEnabled, &instance.LogReplicationUpdatesEnabled)
		if err != nil {
			goto Cleanup
		}
		partialSuccess = true // We at least managed to read something from the server.
		switch strings.ToLower(config.Config.MySQLHostnameResolveMethod) {
		case "none":
			resolvedHostname = instance.Key.Hostname
		case "default", "hostname", "@@hostname":
			resolvedHostname = mysqlHostname
		case "report_host", "@@report_host":
			if mysqlReportHost == "" {
				err = fmt.Errorf("MySQLHostnameResolveMethod configured to use @@report_host but %+v has NULL/empty @@report_host", instanceKey)
				goto Cleanup
			}
			resolvedHostname = mysqlReportHost
		default:
			resolvedHostname = instance.Key.Hostname
		}

		if instance.LogBinEnabled {
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				err := sqlutils.QueryRowsMap(db, "show master status", func(m sqlutils.RowMap) error {
					var err error
					instance.SelfBinlogCoordinates.LogFile = m.GetString("File")
					instance.SelfBinlogCoordinates.LogPos = m.GetInt64("Position")
					return err
				})
				errorChan <- err
			}()
		}

		{
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				semiSyncMasterPluginLoaded := false
				semiSyncReplicaPluginLoaded := false
				err := sqlutils.QueryRowsMap(db, "show global variables like 'rpl_semi_sync_%'", func(m sqlutils.RowMap) error {
					if m.GetString("Variable_name") == "rpl_semi_sync_master_enabled" {
						instance.SemiSyncMasterEnabled = (m.GetString("Value") == "ON")
						semiSyncMasterPluginLoaded = true
					} else if m.GetString("Variable_name") == "rpl_semi_sync_master_timeout" {
						instance.SemiSyncMasterTimeout = m.GetUint64("Value")
					} else if m.GetString("Variable_name") == "rpl_semi_sync_master_wait_for_slave_count" {
						instance.SemiSyncMasterWaitForReplicaCount = m.GetUint("Value")
					} else if m.GetString("Variable_name") == "rpl_semi_sync_slave_enabled" {
						instance.SemiSyncReplicaEnabled = (m.GetString("Value") == "ON")
						semiSyncReplicaPluginLoaded = true
					}
					return nil
				})
				instance.SemiSyncAvailable = (semiSyncMasterPluginLoaded && semiSyncReplicaPluginLoaded)
				errorChan <- err
			}()
		}
		{
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				err := sqlutils.QueryRowsMap(db, "show global status like 'rpl_semi_sync_%'", func(m sqlutils.RowMap) error {
					if m.GetString("Variable_name") == "Rpl_semi_sync_master_status" {
						instance.SemiSyncMasterStatus = (m.GetString("Value") == "ON")
					} else if m.GetString("Variable_name") == "Rpl_semi_sync_master_clients" {
						instance.SemiSyncMasterClients = m.GetUint("Value")
					} else if m.GetString("Variable_name") == "Rpl_semi_sync_slave_status" {
						instance.SemiSyncReplicaStatus = (m.GetString("Value") == "ON")
					}

					return nil
				})
				errorChan <- err
			}()
		}
		if (instance.IsOracleMySQL() || instance.IsPercona()) && !instance.IsSmallerMajorVersionByString("5.6") {
			waitGroup.Add(1)
			serverUuidWaitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				defer serverUuidWaitGroup.Done()
				var masterInfoRepositoryOnTable bool
				// Stuff only supported on Oracle MySQL >= 5.6
				// ...
				// @@gtid_mode only available in Orcale MySQL >= 5.6
				// Previous version just issued this query brute-force, but I don't like errors being issued where they shouldn't.
				_ = db.QueryRow("select @@global.gtid_mode, @@global.server_uuid, @@global.gtid_executed, @@global.gtid_purged, @@global.master_info_repository = 'TABLE', @@global.binlog_row_image").Scan(&instance.GTIDMode, &instance.ServerUUID, &instance.ExecutedGtidSet, &instance.GtidPurged, &masterInfoRepositoryOnTable, &instance.BinlogRowImage)
				if instance.GTIDMode != "" && instance.GTIDMode != "OFF" {
					instance.SupportsOracleGTID = true
				}
				if config.Config.ReplicationCredentialsQuery != "" {
					instance.ReplicationCredentialsAvailable = true
				} else if masterInfoRepositoryOnTable {
					_ = db.QueryRow("select count(*) > 0 and MAX(User_name) != '' from mysql.slave_master_info").Scan(&instance.ReplicationCredentialsAvailable)
				}
			}()
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
	go ResolveHostnameIPs(instance.Key.Hostname)

	// TODO(sougou) delete DataCenterPattern
	if config.Config.DataCenterPattern != "" {
		if pattern, err := regexp.Compile(config.Config.DataCenterPattern); err == nil {
			match := pattern.FindStringSubmatch(instance.Key.Hostname)
			if len(match) != 0 {
				instance.DataCenter = match[1]
			}
		}
		// This can be overriden by later invocation of DetectDataCenterQuery
	}
	if config.Config.RegionPattern != "" {
		if pattern, err := regexp.Compile(config.Config.RegionPattern); err == nil {
			match := pattern.FindStringSubmatch(instance.Key.Hostname)
			if len(match) != 0 {
				instance.Region = match[1]
			}
		}
		// This can be overriden by later invocation of DetectRegionQuery
	}
	if config.Config.PhysicalEnvironmentPattern != "" {
		if pattern, err := regexp.Compile(config.Config.PhysicalEnvironmentPattern); err == nil {
			match := pattern.FindStringSubmatch(instance.Key.Hostname)
			if len(match) != 0 {
				instance.PhysicalEnvironment = match[1]
			}
		}
		// This can be overriden by later invocation of DetectPhysicalEnvironmentQuery
	}

	instance.ReplicationIOThreadState = ReplicationThreadStateNoThread
	instance.ReplicationSQLThreadState = ReplicationThreadStateNoThread
	err = sqlutils.QueryRowsMap(db, "show slave status", func(m sqlutils.RowMap) error {
		instance.HasReplicationCredentials = (m.GetString("Master_User") != "")
		instance.ReplicationIOThreadState = ReplicationThreadStateFromStatus(m.GetString("Slave_IO_Running"))
		instance.ReplicationSQLThreadState = ReplicationThreadStateFromStatus(m.GetString("Slave_SQL_Running"))
		instance.ReplicationIOThreadRuning = instance.ReplicationIOThreadState.IsRunning()
		if isMaxScale110 {
			// Covering buggy MaxScale 1.1.0
			instance.ReplicationIOThreadRuning = instance.ReplicationIOThreadRuning && (m.GetString("Slave_IO_State") == "Binlog Dump")
		}
		instance.ReplicationSQLThreadRuning = instance.ReplicationSQLThreadState.IsRunning()
		instance.ReadBinlogCoordinates.LogFile = m.GetString("Master_Log_File")
		instance.ReadBinlogCoordinates.LogPos = m.GetInt64("Read_Master_Log_Pos")
		instance.ExecBinlogCoordinates.LogFile = m.GetString("Relay_Master_Log_File")
		instance.ExecBinlogCoordinates.LogPos = m.GetInt64("Exec_Master_Log_Pos")
		instance.IsDetached, _ = instance.ExecBinlogCoordinates.ExtractDetachedCoordinates()
		instance.RelaylogCoordinates.LogFile = m.GetString("Relay_Log_File")
		instance.RelaylogCoordinates.LogPos = m.GetInt64("Relay_Log_Pos")
		instance.RelaylogCoordinates.Type = RelayLog
		instance.LastSQLError = emptyQuotesRegexp.ReplaceAllString(strconv.QuoteToASCII(m.GetString("Last_SQL_Error")), "")
		instance.LastIOError = emptyQuotesRegexp.ReplaceAllString(strconv.QuoteToASCII(m.GetString("Last_IO_Error")), "")
		instance.SQLDelay = m.GetUintD("SQL_Delay", 0)
		instance.UsingOracleGTID = (m.GetIntD("Auto_Position", 0) == 1)
		instance.UsingMariaDBGTID = (m.GetStringD("Using_Gtid", "No") != "No")
		instance.MasterUUID = m.GetStringD("Master_UUID", "No")
		instance.HasReplicationFilters = ((m.GetStringD("Replicate_Do_DB", "") != "") || (m.GetStringD("Replicate_Ignore_DB", "") != "") || (m.GetStringD("Replicate_Do_Table", "") != "") || (m.GetStringD("Replicate_Ignore_Table", "") != "") || (m.GetStringD("Replicate_Wild_Do_Table", "") != "") || (m.GetStringD("Replicate_Wild_Ignore_Table", "") != ""))

		masterHostname := m.GetString("Master_Host")
		if isMaxScale110 {
			// Buggy buggy maxscale 1.1.0. Reported Master_Host can be corrupted.
			// Therefore we (currently) take @@hostname (which is masquarading as master host anyhow)
			masterHostname = maxScaleMasterHostname
		}
		masterKey, err := NewResolveInstanceKey(masterHostname, m.GetInt("Master_Port"))
		if err != nil {
			logReadTopologyInstanceError(instanceKey, "NewResolveInstanceKey", err)
		}
		masterKey.Hostname, resolveErr = ResolveHostname(masterKey.Hostname)
		if resolveErr != nil {
			logReadTopologyInstanceError(instanceKey, fmt.Sprintf("ResolveHostname(%q)", masterKey.Hostname), resolveErr)
		}
		instance.MasterKey = *masterKey
		instance.IsDetachedMaster = instance.MasterKey.IsDetached()
		instance.SecondsBehindMaster = m.GetNullInt64("Seconds_Behind_Master")
		if instance.SecondsBehindMaster.Valid && instance.SecondsBehindMaster.Int64 < 0 {
			log.Warningf("Host: %+v, instance.SecondsBehindMaster < 0 [%+v], correcting to 0", instanceKey, instance.SecondsBehindMaster.Int64)
			instance.SecondsBehindMaster.Int64 = 0
		}
		// And until told otherwise:
		instance.ReplicationLagSeconds = instance.SecondsBehindMaster

		instance.AllowTLS = (m.GetString("Master_SSL_Allowed") == "Yes")
		// Not breaking the flow even on error
		slaveStatusFound = true
		return nil
	})
	if err != nil {
		goto Cleanup
	}
	// Populate GR information for the instance in Oracle MySQL 8.0+. To do this we need to wait for the Server UUID to
	// be populated to be able to find this instance's information in performance_schema.replication_group_members by
	// comparing UUIDs. We could instead resolve the MEMBER_HOST and MEMBER_PORT columns into an InstanceKey and compare
	// those instead, but this could require external calls for name resolving, whereas comparing UUIDs does not.
	serverUuidWaitGroup.Wait()
	if instance.IsOracleMySQL() && !instance.IsSmallerMajorVersionByString("8.0") {
		err := PopulateGroupReplicationInformation(instance, db)
		if err != nil {
			goto Cleanup
		}
	}
	if isMaxScale && !slaveStatusFound {
		err = fmt.Errorf("No 'SHOW SLAVE STATUS' output found for a MaxScale instance: %+v", instanceKey)
		goto Cleanup
	}

	if config.Config.ReplicationLagQuery != "" && !isMaxScale {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			if err := db.QueryRow(config.Config.ReplicationLagQuery).Scan(&instance.ReplicationLagSeconds); err == nil {
				if instance.ReplicationLagSeconds.Valid && instance.ReplicationLagSeconds.Int64 < 0 {
					log.Warningf("Host: %+v, instance.SlaveLagSeconds < 0 [%+v], correcting to 0", instanceKey, instance.ReplicationLagSeconds.Int64)
					instance.ReplicationLagSeconds.Int64 = 0
				}
			} else {
				instance.ReplicationLagSeconds = instance.SecondsBehindMaster
				logReadTopologyInstanceError(instanceKey, "ReplicationLagQuery", err)
			}
		}()
	}

	instanceFound = true

	// -------------------------------------------------------------------------
	// Anything after this point does not affect the fact the instance is found.
	// No `goto Cleanup` after this point.
	// -------------------------------------------------------------------------

	// Get replicas, either by SHOW SLAVE HOSTS or via PROCESSLIST
	// MaxScale does not support PROCESSLIST, so SHOW SLAVE HOSTS is the only option
	if config.Config.DiscoverByShowSlaveHosts || isMaxScale {
		err := sqlutils.QueryRowsMap(db, `show slave hosts`,
			func(m sqlutils.RowMap) error {
				// MaxScale 1.1 may trigger an error with this command, but
				// also we may see issues if anything on the MySQL server locks up.
				// Consequently it's important to validate the values received look
				// good prior to calling ResolveHostname()
				host := m.GetString("Host")
				port := m.GetIntD("Port", 0)
				if host == "" || port == 0 {
					if isMaxScale && host == "" && port == 0 {
						// MaxScale reports a bad response sometimes so ignore it.
						// - seen in 1.1.0 and 1.4.3.4
						return nil
					}
					// otherwise report the error to the caller
					return fmt.Errorf("ReadTopologyInstance(%+v) 'show slave hosts' returned row with <host,port>: <%v,%v>", instanceKey, host, port)
				}

				replicaKey, err := NewResolveInstanceKey(host, port)
				if err == nil && replicaKey.IsValid() {
					if !RegexpMatchPatterns(replicaKey.StringCode(), config.Config.DiscoveryIgnoreReplicaHostnameFilters) {
						instance.AddReplicaKey(replicaKey)
					}
					foundByShowSlaveHosts = true
				}
				return err
			})

		logReadTopologyInstanceError(instanceKey, "show slave hosts", err)
	}
	if !foundByShowSlaveHosts && !isMaxScale {
		// Either not configured to read SHOW SLAVE HOSTS or nothing was there.
		// Discover by information_schema.processlist
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			err := sqlutils.QueryRowsMap(db, `
      	select
      		substring_index(host, ':', 1) as slave_hostname
      	from
      		information_schema.processlist
      	where
          command IN ('Binlog Dump', 'Binlog Dump GTID')
  		`,
				func(m sqlutils.RowMap) error {
					cname, resolveErr := ResolveHostname(m.GetString("slave_hostname"))
					if resolveErr != nil {
						logReadTopologyInstanceError(instanceKey, "ResolveHostname: processlist", resolveErr)
					}
					replicaKey := InstanceKey{Hostname: cname, Port: instance.Key.Port}
					if !RegexpMatchPatterns(replicaKey.StringCode(), config.Config.DiscoveryIgnoreReplicaHostnameFilters) {
						instance.AddReplicaKey(&replicaKey)
					}
					return err
				})

			logReadTopologyInstanceError(instanceKey, "processlist", err)
		}()
	}

	if instance.IsNDB() {
		// Discover by ndbinfo about MySQL Cluster SQL nodes
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			err := sqlutils.QueryRowsMap(db, `
      	select
      		substring(service_URI,9) mysql_host
      	from
      		ndbinfo.processes
      	where
          process_name='mysqld'
  		`,
				func(m sqlutils.RowMap) error {
					cname, resolveErr := ResolveHostname(m.GetString("mysql_host"))
					if resolveErr != nil {
						logReadTopologyInstanceError(instanceKey, "ResolveHostname: ndbinfo", resolveErr)
					}
					replicaKey := InstanceKey{Hostname: cname, Port: instance.Key.Port}
					instance.AddReplicaKey(&replicaKey)
					return err
				})

			logReadTopologyInstanceError(instanceKey, "ndbinfo", err)
		}()
	}

	// TODO(sougou): delete DetectDataCenterQuery
	if config.Config.DetectDataCenterQuery != "" && !isMaxScale {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			err := db.QueryRow(config.Config.DetectDataCenterQuery).Scan(&instance.DataCenter)
			logReadTopologyInstanceError(instanceKey, "DetectDataCenterQuery", err)
		}()
	}
	instance.DataCenter = tablet.Alias.Cell

	// TODO(sougou): use cell alias to identify regions.
	if config.Config.DetectRegionQuery != "" && !isMaxScale {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			err := db.QueryRow(config.Config.DetectRegionQuery).Scan(&instance.Region)
			logReadTopologyInstanceError(instanceKey, "DetectRegionQuery", err)
		}()
	}

	if config.Config.DetectPhysicalEnvironmentQuery != "" && !isMaxScale {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			err := db.QueryRow(config.Config.DetectPhysicalEnvironmentQuery).Scan(&instance.PhysicalEnvironment)
			logReadTopologyInstanceError(instanceKey, "DetectPhysicalEnvironmentQuery", err)
		}()
	}

	// TODO(sougou): delete DetectInstanceAliasQuery
	if config.Config.DetectInstanceAliasQuery != "" && !isMaxScale {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			err := db.QueryRow(config.Config.DetectInstanceAliasQuery).Scan(&instance.InstanceAlias)
			logReadTopologyInstanceError(instanceKey, "DetectInstanceAliasQuery", err)
		}()
	}
	instance.InstanceAlias = topoproto.TabletAliasString(tablet.Alias)

	// TODO(sougou): come up with a strategy for semi-sync
	if config.Config.DetectSemiSyncEnforcedQuery != "" && !isMaxScale {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			err := db.QueryRow(config.Config.DetectSemiSyncEnforcedQuery).Scan(&instance.SemiSyncEnforced)
			logReadTopologyInstanceError(instanceKey, "DetectSemiSyncEnforcedQuery", err)
		}()
	}

	{
		latency.Start("backend")
		err = ReadInstanceClusterAttributes(instance)
		latency.Stop("backend")
		logReadTopologyInstanceError(instanceKey, "ReadInstanceClusterAttributes", err)
	}

	{
		// Pseudo GTID
		// Depends on ReadInstanceClusterAttributes above
		instance.UsingPseudoGTID = false
		if config.Config.AutoPseudoGTID {
			var err error
			instance.UsingPseudoGTID, err = isInjectedPseudoGTID(instance.ClusterName)
			log.Errore(err)
		} else if config.Config.DetectPseudoGTIDQuery != "" {
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				if resultData, err := sqlutils.QueryResultData(db, config.Config.DetectPseudoGTIDQuery); err == nil {
					if len(resultData) > 0 {
						if len(resultData[0]) > 0 {
							if resultData[0][0].Valid && resultData[0][0].String == "1" {
								instance.UsingPseudoGTID = true
							}
						}
					}
				} else {
					logReadTopologyInstanceError(instanceKey, "DetectPseudoGTIDQuery", err)
				}
			}()
		}
	}

	// We need to update candidate_database_instance.
	// We register the rule even if it hasn't changed,
	// to bump the last_suggested time.
	instance.PromotionRule = PromotionRule(tablet)
	err = RegisterCandidateInstance(NewCandidateDatabaseInstance(instanceKey, instance.PromotionRule).WithCurrentTime())
	logReadTopologyInstanceError(instanceKey, "RegisterCandidateInstance", err)

	// TODO(sougou): delete cluster_alias_override metadata
	instance.SuggestedClusterAlias = fmt.Sprintf("%v:%v", tablet.Keyspace, tablet.Shard)

	if instance.ReplicationDepth == 0 && config.Config.DetectClusterDomainQuery != "" && !isMaxScale {
		// Only need to do on masters
		domainName := ""
		if err := db.QueryRow(config.Config.DetectClusterDomainQuery).Scan(&domainName); err != nil {
			domainName = ""
			logReadTopologyInstanceError(instanceKey, "DetectClusterDomainQuery", err)
		}
		if domainName != "" {
			latency.Start("backend")
			err := WriteClusterDomainName(instance.ClusterName, domainName)
			latency.Stop("backend")
			logReadTopologyInstanceError(instanceKey, "WriteClusterDomainName", err)
		}
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
		if instance.IsCoMaster {
			// Take co-master into account, and avoid infinite loop
			instance.AncestryUUID = fmt.Sprintf("%s,%s", instance.MasterUUID, instance.ServerUUID)
		} else {
			instance.AncestryUUID = fmt.Sprintf("%s,%s", instance.AncestryUUID, instance.ServerUUID)
		}
		// Add replication group ancestry UUID as well. Otherwise, Orchestrator thinks there are errant GTIDs in group
		// members and its slaves, even though they are not.
		instance.AncestryUUID = fmt.Sprintf("%s,%s", instance.AncestryUUID, instance.ReplicationGroupName)
		instance.AncestryUUID = strings.Trim(instance.AncestryUUID, ",")
		if instance.ExecutedGtidSet != "" && instance.masterExecutedGtidSet != "" {
			// Compare master & replica GTID sets, but ignore the sets that present the master's UUID.
			// This is because orchestrator may pool master and replica at an inconvenient timing,
			// such that the replica may _seems_ to have more entries than the master, when in fact
			// it's just that the master's probing is stale.
			redactedExecutedGtidSet, _ := NewOracleGtidSet(instance.ExecutedGtidSet)
			for _, uuid := range strings.Split(instance.AncestryUUID, ",") {
				if uuid != instance.ServerUUID {
					redactedExecutedGtidSet.RemoveUUID(uuid)
				}
				if instance.IsCoMaster && uuid == instance.ServerUUID {
					// If this is a co-master, then this server is likely to show its own generated GTIDs as errant,
					// because its co-master has not applied them yet
					redactedExecutedGtidSet.RemoveUUID(uuid)
				}
			}
			// Avoid querying the database if there's no point:
			if !redactedExecutedGtidSet.IsEmpty() {
				redactedMasterExecutedGtidSet, _ := NewOracleGtidSet(instance.masterExecutedGtidSet)
				redactedMasterExecutedGtidSet.RemoveUUID(instance.MasterUUID)

				db.QueryRow("select gtid_subtract(?, ?)", redactedExecutedGtidSet.String(), redactedMasterExecutedGtidSet.String()).Scan(&instance.GtidErrant)
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
		if bufferWrites {
			enqueueInstanceWrite(instance, instanceFound, err)
		} else {
			WriteInstance(instance, instanceFound, err)
		}
		lastAttemptedCheckTimer.Stop()
		latency.Stop("backend")
		return instance, nil
	}

	// Something is wrong, could be network-wise. Record that we
	// tried to check the instance. last_attempted_check is also
	// updated on success by writeInstance.
	latency.Start("backend")
	_ = UpdateInstanceLastChecked(&instance.Key, partialSuccess)
	latency.Stop("backend")
	return nil, err
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
	err = db.QueryOrchestrator(query, queryArgs, func(row sqlutils.RowMap) error {
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

// ReadInstanceClusterAttributes will return the cluster name for a given instance by looking at its master
// and getting it from there.
// It is a non-recursive function and so-called-recursion is performed upon periodic reading of
// instances.
func ReadInstanceClusterAttributes(instance *Instance) (err error) {
	var masterOrGroupPrimaryInstanceKey InstanceKey
	var masterOrGroupPrimaryClusterName string
	var masterOrGroupPrimaryReplicationDepth uint
	var ancestryUUID string
	var masterOrGroupPrimaryExecutedGtidSet string
	masterOrGroupPrimaryDataFound := false

	// Read the cluster_name of the _master_ or _group_primary_ of our instance, derive it from there.
	query := `
			select
					cluster_name,
					suggested_cluster_alias,
					replication_depth,
					master_host,
					master_port,
					ancestry_uuid,
					executed_gtid_set
				from database_instance
				where hostname=? and port=?
	`
	// For instances that are part of a replication group, if the host is not the group's primary, we use the
	// information from the group primary. If it is the group primary, we use the information of its master
	// (if it has any). If it is not a group member, we use the information from the host's master.
	if instance.IsReplicationGroupSecondary() {
		masterOrGroupPrimaryInstanceKey = instance.ReplicationGroupPrimaryInstanceKey
	} else {
		masterOrGroupPrimaryInstanceKey = instance.MasterKey
	}
	args := sqlutils.Args(masterOrGroupPrimaryInstanceKey.Hostname, masterOrGroupPrimaryInstanceKey.Port)
	err = db.QueryOrchestrator(query, args, func(m sqlutils.RowMap) error {
		masterOrGroupPrimaryClusterName = m.GetString("cluster_name")
		masterOrGroupPrimaryReplicationDepth = m.GetUint("replication_depth")
		masterOrGroupPrimaryInstanceKey.Hostname = m.GetString("master_host")
		masterOrGroupPrimaryInstanceKey.Port = m.GetInt("master_port")
		ancestryUUID = m.GetString("ancestry_uuid")
		masterOrGroupPrimaryExecutedGtidSet = m.GetString("executed_gtid_set")
		masterOrGroupPrimaryDataFound = true
		return nil
	})
	if err != nil {
		return log.Errore(err)
	}

	var replicationDepth uint = 0
	var clusterName string
	if masterOrGroupPrimaryDataFound {
		replicationDepth = masterOrGroupPrimaryReplicationDepth + 1
		clusterName = masterOrGroupPrimaryClusterName
	}
	clusterNameByInstanceKey := instance.Key.StringCode()
	if clusterName == "" {
		// Nothing from master; we set it to be named after the instance itself
		clusterName = clusterNameByInstanceKey
	}

	isCoMaster := false
	if masterOrGroupPrimaryInstanceKey.Equals(&instance.Key) {
		// co-master calls for special case, in fear of the infinite loop
		isCoMaster = true
		clusterNameByCoMasterKey := instance.MasterKey.StringCode()
		if clusterName != clusterNameByInstanceKey && clusterName != clusterNameByCoMasterKey {
			// Can be caused by a co-master topology failover
			log.Errorf("ReadInstanceClusterAttributes: in co-master topology %s is not in (%s, %s). Forcing it to become one of them", clusterName, clusterNameByInstanceKey, clusterNameByCoMasterKey)
			clusterName = math.TernaryString(instance.Key.SmallerThan(&instance.MasterKey), clusterNameByInstanceKey, clusterNameByCoMasterKey)
		}
		if clusterName == clusterNameByInstanceKey {
			// circular replication. Avoid infinite ++ on replicationDepth
			replicationDepth = 0
			ancestryUUID = ""
		} // While the other stays "1"
	}
	instance.ClusterName = clusterName
	instance.ReplicationDepth = replicationDepth
	instance.IsCoMaster = isCoMaster
	instance.AncestryUUID = ancestryUUID
	instance.masterExecutedGtidSet = masterOrGroupPrimaryExecutedGtidSet
	return nil
}

type byNamePort [](*InstanceKey)

func (this byNamePort) Len() int      { return len(this) }
func (this byNamePort) Swap(i, j int) { this[i], this[j] = this[j], this[i] }
func (this byNamePort) Less(i, j int) bool {
	return (this[i].Hostname < this[j].Hostname) ||
		(this[i].Hostname == this[j].Hostname && this[i].Port < this[j].Port)
}

// BulkReadInstance returns a list of all instances from the database
// - I only need the Hostname and Port fields.
// - I must use readInstancesByCondition to ensure all column
//   settings are correct.
func BulkReadInstance() ([](*InstanceKey), error) {
	// no condition (I want all rows) and no sorting (but this is done by Hostname, Port anyway)
	const (
		condition = "1=1"
		orderBy   = ""
	)
	var instanceKeys [](*InstanceKey)

	instances, err := readInstancesByCondition(condition, nil, orderBy)
	if err != nil {
		return nil, fmt.Errorf("BulkReadInstance: %+v", err)
	}

	// update counters if we picked anything up
	if len(instances) > 0 {
		readInstanceCounter.Inc(int64(len(instances)))

		for _, instance := range instances {
			instanceKeys = append(instanceKeys, &instance.Key)
		}
		// sort on orchestrator and not the backend (should be redundant)
		sort.Sort(byNamePort(instanceKeys))
	}

	return instanceKeys, nil
}

func ReadInstancePromotionRule(instance *Instance) (err error) {
	var promotionRule CandidatePromotionRule = NeutralPromoteRule
	query := `
			select
				ifnull(nullif(promotion_rule, ''), 'neutral') as promotion_rule
				from candidate_database_instance
				where hostname=? and port=?
	`
	args := sqlutils.Args(instance.Key.Hostname, instance.Key.Port)

	err = db.QueryOrchestrator(query, args, func(m sqlutils.RowMap) error {
		promotionRule = CandidatePromotionRule(m.GetString("promotion_rule"))
		return nil
	})
	instance.PromotionRule = promotionRule
	return log.Errore(err)
}

// readInstanceRow reads a single instance row from the orchestrator backend database.
func readInstanceRow(m sqlutils.RowMap) *Instance {
	instance := NewInstance()

	instance.Key.Hostname = m.GetString("hostname")
	instance.Key.Port = m.GetInt("port")
	instance.Uptime = m.GetUint("uptime")
	instance.ServerID = m.GetUint("server_id")
	instance.ServerUUID = m.GetString("server_uuid")
	instance.Version = m.GetString("version")
	instance.VersionComment = m.GetString("version_comment")
	instance.ReadOnly = m.GetBool("read_only")
	instance.Binlog_format = m.GetString("binlog_format")
	instance.BinlogRowImage = m.GetString("binlog_row_image")
	instance.LogBinEnabled = m.GetBool("log_bin")
	instance.LogReplicationUpdatesEnabled = m.GetBool("log_slave_updates")
	instance.MasterKey.Hostname = m.GetString("master_host")
	instance.MasterKey.Port = m.GetInt("master_port")
	instance.IsDetachedMaster = instance.MasterKey.IsDetached()
	instance.ReplicationSQLThreadRuning = m.GetBool("slave_sql_running")
	instance.ReplicationIOThreadRuning = m.GetBool("slave_io_running")
	instance.ReplicationSQLThreadState = ReplicationThreadState(m.GetInt("replication_sql_thread_state"))
	instance.ReplicationIOThreadState = ReplicationThreadState(m.GetInt("replication_io_thread_state"))
	instance.HasReplicationFilters = m.GetBool("has_replication_filters")
	instance.SupportsOracleGTID = m.GetBool("supports_oracle_gtid")
	instance.UsingOracleGTID = m.GetBool("oracle_gtid")
	instance.MasterUUID = m.GetString("master_uuid")
	instance.AncestryUUID = m.GetString("ancestry_uuid")
	instance.ExecutedGtidSet = m.GetString("executed_gtid_set")
	instance.GTIDMode = m.GetString("gtid_mode")
	instance.GtidPurged = m.GetString("gtid_purged")
	instance.GtidErrant = m.GetString("gtid_errant")
	instance.UsingMariaDBGTID = m.GetBool("mariadb_gtid")
	instance.UsingPseudoGTID = m.GetBool("pseudo_gtid")
	instance.SelfBinlogCoordinates.LogFile = m.GetString("binary_log_file")
	instance.SelfBinlogCoordinates.LogPos = m.GetInt64("binary_log_pos")
	instance.ReadBinlogCoordinates.LogFile = m.GetString("master_log_file")
	instance.ReadBinlogCoordinates.LogPos = m.GetInt64("read_master_log_pos")
	instance.ExecBinlogCoordinates.LogFile = m.GetString("relay_master_log_file")
	instance.ExecBinlogCoordinates.LogPos = m.GetInt64("exec_master_log_pos")
	instance.IsDetached, _ = instance.ExecBinlogCoordinates.ExtractDetachedCoordinates()
	instance.RelaylogCoordinates.LogFile = m.GetString("relay_log_file")
	instance.RelaylogCoordinates.LogPos = m.GetInt64("relay_log_pos")
	instance.RelaylogCoordinates.Type = RelayLog
	instance.LastSQLError = m.GetString("last_sql_error")
	instance.LastIOError = m.GetString("last_io_error")
	instance.SecondsBehindMaster = m.GetNullInt64("seconds_behind_master")
	instance.ReplicationLagSeconds = m.GetNullInt64("slave_lag_seconds")
	instance.SQLDelay = m.GetUint("sql_delay")
	replicasJSON := m.GetString("slave_hosts")
	instance.ClusterName = m.GetString("cluster_name")
	instance.SuggestedClusterAlias = m.GetString("suggested_cluster_alias")
	instance.DataCenter = m.GetString("data_center")
	instance.Region = m.GetString("region")
	instance.PhysicalEnvironment = m.GetString("physical_environment")
	instance.SemiSyncEnforced = m.GetBool("semi_sync_enforced")
	instance.SemiSyncAvailable = m.GetBool("semi_sync_available")
	instance.SemiSyncMasterEnabled = m.GetBool("semi_sync_master_enabled")
	instance.SemiSyncMasterTimeout = m.GetUint64("semi_sync_master_timeout")
	instance.SemiSyncMasterWaitForReplicaCount = m.GetUint("semi_sync_master_wait_for_slave_count")
	instance.SemiSyncReplicaEnabled = m.GetBool("semi_sync_replica_enabled")
	instance.SemiSyncMasterStatus = m.GetBool("semi_sync_master_status")
	instance.SemiSyncMasterClients = m.GetUint("semi_sync_master_clients")
	instance.SemiSyncReplicaStatus = m.GetBool("semi_sync_replica_status")
	instance.ReplicationDepth = m.GetUint("replication_depth")
	instance.IsCoMaster = m.GetBool("is_co_master")
	instance.ReplicationCredentialsAvailable = m.GetBool("replication_credentials_available")
	instance.HasReplicationCredentials = m.GetBool("has_replication_credentials")
	instance.IsUpToDate = (m.GetUint("seconds_since_last_checked") <= config.Config.InstancePollSeconds)
	instance.IsRecentlyChecked = (m.GetUint("seconds_since_last_checked") <= config.Config.InstancePollSeconds*5)
	instance.LastSeenTimestamp = m.GetString("last_seen")
	instance.IsLastCheckValid = m.GetBool("is_last_check_valid")
	instance.SecondsSinceLastSeen = m.GetNullInt64("seconds_since_last_seen")
	instance.IsCandidate = m.GetBool("is_candidate")
	instance.PromotionRule = CandidatePromotionRule(m.GetString("promotion_rule"))
	instance.IsDowntimed = m.GetBool("is_downtimed")
	instance.DowntimeReason = m.GetString("downtime_reason")
	instance.DowntimeOwner = m.GetString("downtime_owner")
	instance.DowntimeEndTimestamp = m.GetString("downtime_end_timestamp")
	instance.ElapsedDowntime = time.Second * time.Duration(m.GetInt("elapsed_downtime_seconds"))
	instance.UnresolvedHostname = m.GetString("unresolved_hostname")
	instance.AllowTLS = m.GetBool("allow_tls")
	instance.InstanceAlias = m.GetString("instance_alias")
	instance.LastDiscoveryLatency = time.Duration(m.GetInt64("last_discovery_latency")) * time.Nanosecond

	instance.Replicas.ReadJson(replicasJSON)
	instance.applyFlavorName()

	/* Read Group Replication variables below */
	instance.ReplicationGroupName = m.GetString("replication_group_name")
	instance.ReplicationGroupIsSinglePrimary = m.GetBool("replication_group_is_single_primary_mode")
	instance.ReplicationGroupMemberState = m.GetString("replication_group_member_state")
	instance.ReplicationGroupMemberRole = m.GetString("replication_group_member_role")
	instance.ReplicationGroupPrimaryInstanceKey = InstanceKey{Hostname: m.GetString("replication_group_primary_host"),
		Port: m.GetInt("replication_group_primary_port")}
	instance.ReplicationGroupMembers.ReadJson(m.GetString("replication_group_members"))
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
func readInstancesByCondition(condition string, args []interface{}, sort string) ([](*Instance), error) {
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
			left join candidate_database_instance using (hostname, port)
			left join hostname_unresolve using (hostname)
			left join database_instance_downtime using (hostname, port)
		where
			%s
		order by
			%s
			`, condition, sort)

		err := db.QueryOrchestrator(query, args, func(m sqlutils.RowMap) error {
			instance := readInstanceRow(m)
			instances = append(instances, instance)
			return nil
		})
		if err != nil {
			return instances, log.Errore(err)
		}
		err = PopulateInstancesAgents(instances)
		if err != nil {
			return instances, log.Errore(err)
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

// ReadInstance reads an instance from the orchestrator backend database
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

// ReadClusterInstances reads all instances of a given cluster
func ReadClusterInstances(clusterName string) ([](*Instance), error) {
	if strings.Contains(clusterName, "'") {
		return [](*Instance){}, log.Errorf("Invalid cluster name: %s", clusterName)
	}
	condition := `cluster_name = ?`
	return readInstancesByCondition(condition, sqlutils.Args(clusterName), "")
}

// ReadClusterWriteableMaster returns the/a writeable master of this cluster
// Typically, the cluster name indicates the master of the cluster. However, in circular
// master-master replication one master can assume the name of the cluster, and it is
// not guaranteed that it is the writeable one.
func ReadClusterWriteableMaster(clusterName string) ([](*Instance), error) {
	condition := `
		cluster_name = ?
		and read_only = 0
		and (replication_depth = 0 or is_co_master)
	`
	return readInstancesByCondition(condition, sqlutils.Args(clusterName), "replication_depth asc")
}

// ReadClusterMaster returns the master of this cluster.
// - if the cluster has co-masters, the/a writable one is returned
// - if the cluster has a single master, that master is retuened whether it is read-only or writable.
func ReadClusterMaster(clusterName string) ([](*Instance), error) {
	condition := `
		cluster_name = ?
		and (replication_depth = 0 or is_co_master)
	`
	return readInstancesByCondition(condition, sqlutils.Args(clusterName), "read_only asc, replication_depth asc")
}

// ReadWriteableClustersMasters returns writeable masters of all clusters, but only one
// per cluster, in similar logic to ReadClusterWriteableMaster
func ReadWriteableClustersMasters() (instances [](*Instance), err error) {
	condition := `
		read_only = 0
		and (replication_depth = 0 or is_co_master)
	`
	allMasters, err := readInstancesByCondition(condition, sqlutils.Args(), "cluster_name asc, replication_depth asc")
	if err != nil {
		return instances, err
	}
	visitedClusters := make(map[string]bool)
	for _, instance := range allMasters {
		if !visitedClusters[instance.ClusterName] {
			visitedClusters[instance.ClusterName] = true
			instances = append(instances, instance)
		}
	}
	return instances, err
}

// ReadClusterAliasInstances reads all instances of a cluster alias
func ReadClusterAliasInstances(clusterAlias string) ([](*Instance), error) {
	condition := `suggested_cluster_alias = ? `
	return readInstancesByCondition(condition, sqlutils.Args(clusterAlias), "")
}

// ReadReplicaInstances reads replicas of a given master
func ReadReplicaInstances(masterKey *InstanceKey) ([](*Instance), error) {
	condition := `
			master_host = ?
			and master_port = ?
		`
	return readInstancesByCondition(condition, sqlutils.Args(masterKey.Hostname, masterKey.Port), "")
}

// ReadReplicaInstancesIncludingBinlogServerSubReplicas returns a list of direct slves including any replicas
// of a binlog server replica
func ReadReplicaInstancesIncludingBinlogServerSubReplicas(masterKey *InstanceKey) ([](*Instance), error) {
	replicas, err := ReadReplicaInstances(masterKey)
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

// ReadBinlogServerReplicaInstances reads direct replicas of a given master that are binlog servers
func ReadBinlogServerReplicaInstances(masterKey *InstanceKey) ([](*Instance), error) {
	condition := `
			master_host = ?
			and master_port = ?
			and binlog_server = 1
		`
	return readInstancesByCondition(condition, sqlutils.Args(masterKey.Hostname, masterKey.Port), "")
}

// ReadUnseenInstances reads all instances which were not recently seen
func ReadUnseenInstances() ([](*Instance), error) {
	condition := `last_seen < last_checked`
	return readInstancesByCondition(condition, sqlutils.Args(), "")
}

// ReadProblemInstances reads all instances with problems
func ReadProblemInstances(clusterName string) ([](*Instance), error) {
	condition := `
			cluster_name LIKE (CASE WHEN ? = '' THEN '%' ELSE ? END)
			and (
				(last_seen < last_checked)
				or (unix_timestamp() - unix_timestamp(last_checked) > ?)
				or (replication_sql_thread_state not in (-1 ,1))
				or (replication_io_thread_state not in (-1 ,1))
				or (abs(cast(seconds_behind_master as signed) - cast(sql_delay as signed)) > ?)
				or (abs(cast(slave_lag_seconds as signed) - cast(sql_delay as signed)) > ?)
				or (gtid_errant != '')
				or (replication_group_name != '' and replication_group_member_state != 'ONLINE')
			)
		`

	args := sqlutils.Args(clusterName, clusterName, config.Config.InstancePollSeconds*5, config.Config.ReasonableReplicationLagSeconds, config.Config.ReasonableReplicationLagSeconds)
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
		if RegexpMatchPatterns(instance.Key.StringCode(), config.Config.ProblemIgnoreHostnameFilters) {
			skip = true
		}
		if !skip {
			reportedInstances = append(reportedInstances, instance)
		}
	}
	return reportedInstances, nil
}

// SearchInstances reads all instances qualifying for some searchString
func SearchInstances(searchString string) ([](*Instance), error) {
	searchString = strings.TrimSpace(searchString)
	condition := `
			instr(hostname, ?) > 0
			or instr(cluster_name, ?) > 0
			or instr(version, ?) > 0
			or instr(version_comment, ?) > 0
			or instr(concat(hostname, ':', port), ?) > 0
			or instr(suggested_cluster_alias, ?) > 0
			or concat(server_id, '') = ?
			or concat(port, '') = ?
		`
	args := sqlutils.Args(searchString, searchString, searchString, searchString, searchString, searchString, searchString, searchString)
	return readInstancesByCondition(condition, args, `replication_depth asc, num_slave_hosts desc, cluster_name, hostname, port`)
}

// FindInstances reads all instances whose name matches given pattern
func FindInstances(regexpPattern string) (result [](*Instance), err error) {
	result = [](*Instance){}
	r, err := regexp.Compile(regexpPattern)
	if err != nil {
		return result, err
	}
	condition := `1=1`
	unfiltered, err := readInstancesByCondition(condition, sqlutils.Args(), `replication_depth asc, num_slave_hosts desc, cluster_name, hostname, port`)
	if err != nil {
		return unfiltered, err
	}
	for _, instance := range unfiltered {
		if r.MatchString(instance.Key.DisplayString()) {
			result = append(result, instance)
		}
	}
	return result, nil
}

// findFuzzyInstances return instances whose names are like the one given (host & port substrings)
// For example, the given `mydb-3:3306` might find `myhosts-mydb301-production.mycompany.com:3306`
func findFuzzyInstances(fuzzyInstanceKey *InstanceKey) ([](*Instance), error) {
	condition := `
		hostname like concat('%%', ?, '%%')
		and port = ?
	`
	return readInstancesByCondition(condition, sqlutils.Args(fuzzyInstanceKey.Hostname, fuzzyInstanceKey.Port), `replication_depth asc, num_slave_hosts desc, cluster_name, hostname, port`)
}

// ReadFuzzyInstanceKey accepts a fuzzy instance key and expects to return a single, fully qualified,
// known instance key.
func ReadFuzzyInstanceKey(fuzzyInstanceKey *InstanceKey) *InstanceKey {
	if fuzzyInstanceKey == nil {
		return nil
	}
	if fuzzyInstanceKey.IsIPv4() {
		// avoid fuzziness. When looking for 10.0.0.1 we don't want to match 10.0.0.15!
		return nil
	}
	if fuzzyInstanceKey.Hostname != "" {
		// Fuzzy instance search
		if fuzzyInstances, _ := findFuzzyInstances(fuzzyInstanceKey); len(fuzzyInstances) == 1 {
			return &(fuzzyInstances[0].Key)
		}
	}
	return nil
}

// ReadFuzzyInstanceKeyIfPossible accepts a fuzzy instance key and hopes to return a single, fully qualified,
// known instance key, or else the original given key
func ReadFuzzyInstanceKeyIfPossible(fuzzyInstanceKey *InstanceKey) *InstanceKey {
	if instanceKey := ReadFuzzyInstanceKey(fuzzyInstanceKey); instanceKey != nil {
		return instanceKey
	}
	return fuzzyInstanceKey
}

// ReadFuzzyInstance accepts a fuzzy instance key and expects to return a single instance.
// Multiple instances matching the fuzzy keys are not allowed.
func ReadFuzzyInstance(fuzzyInstanceKey *InstanceKey) (*Instance, error) {
	if fuzzyInstanceKey == nil {
		return nil, log.Errorf("ReadFuzzyInstance received nil input")
	}
	if fuzzyInstanceKey.IsIPv4() {
		// avoid fuzziness. When looking for 10.0.0.1 we don't want to match 10.0.0.15!
		instance, _, err := ReadInstance(fuzzyInstanceKey)
		return instance, err
	}
	if fuzzyInstanceKey.Hostname != "" {
		// Fuzzy instance search
		if fuzzyInstances, _ := findFuzzyInstances(fuzzyInstanceKey); len(fuzzyInstances) == 1 {
			return fuzzyInstances[0], nil
		}
	}
	return nil, log.Errorf("Cannot determine fuzzy instance %+v", *fuzzyInstanceKey)
}

// ReadLostInRecoveryInstances returns all instances (potentially filtered by cluster)
// which are currently indicated as downtimed due to being lost during a topology recovery.
func ReadLostInRecoveryInstances(clusterName string) ([](*Instance), error) {
	condition := `
		ifnull(
			database_instance_downtime.downtime_active = 1
			and database_instance_downtime.end_timestamp > now()
			and database_instance_downtime.reason = ?, 0)
		and ? IN ('', cluster_name)
	`
	return readInstancesByCondition(condition, sqlutils.Args(DowntimeLostInRecoveryMessage, clusterName), "cluster_name asc, replication_depth asc")
}

// ReadDowntimedInstances returns all instances currently downtimed, potentially filtered by cluster
func ReadDowntimedInstances(clusterName string) ([](*Instance), error) {
	condition := `
		ifnull(
			database_instance_downtime.downtime_active = 1
			and database_instance_downtime.end_timestamp > now()
			, 0)
		and ? IN ('', cluster_name)
	`
	return readInstancesByCondition(condition, sqlutils.Args(clusterName), "cluster_name asc, replication_depth asc")
}

// ReadClusterCandidateInstances reads cluster instances which are also marked as candidates
func ReadClusterCandidateInstances(clusterName string) ([](*Instance), error) {
	condition := `
			cluster_name = ?
			and concat(hostname, ':', port) in (
				select concat(hostname, ':', port)
					from candidate_database_instance
					where promotion_rule in ('must', 'prefer')
			)
			`
	return readInstancesByCondition(condition, sqlutils.Args(clusterName), "")
}

// ReadClusterNeutralPromotionRuleInstances reads cluster instances whose promotion-rule is marked as 'neutral'
func ReadClusterNeutralPromotionRuleInstances(clusterName string) (neutralInstances [](*Instance), err error) {
	instances, err := ReadClusterInstances(clusterName)
	if err != nil {
		return neutralInstances, err
	}
	for _, instance := range instances {
		if instance.PromotionRule == NeutralPromoteRule {
			neutralInstances = append(neutralInstances, instance)
		}
	}
	return neutralInstances, nil
}

// filterOSCInstances will filter the given list such that only replicas fit for OSC control remain.
func filterOSCInstances(instances [](*Instance)) [](*Instance) {
	result := [](*Instance){}
	for _, instance := range instances {
		if RegexpMatchPatterns(instance.Key.StringCode(), config.Config.OSCIgnoreHostnameFilters) {
			continue
		}
		if instance.IsBinlogServer() {
			continue
		}
		if !instance.IsLastCheckValid {
			continue
		}
		result = append(result, instance)
	}
	return result
}

// GetClusterOSCReplicas returns a heuristic list of replicas which are fit as controll replicas for an OSC operation.
// These would be intermediate masters
func GetClusterOSCReplicas(clusterName string) ([](*Instance), error) {
	var intermediateMasters [](*Instance)
	result := [](*Instance){}
	var err error
	if strings.Contains(clusterName, "'") {
		return [](*Instance){}, log.Errorf("Invalid cluster name: %s", clusterName)
	}
	{
		// Pick up to two busiest IMs
		condition := `
			replication_depth = 1
			and num_slave_hosts > 0
			and cluster_name = ?
		`
		intermediateMasters, err = readInstancesByCondition(condition, sqlutils.Args(clusterName), "")
		if err != nil {
			return result, err
		}
		sort.Sort(sort.Reverse(InstancesByCountReplicas(intermediateMasters)))
		intermediateMasters = filterOSCInstances(intermediateMasters)
		intermediateMasters = intermediateMasters[0:math.MinInt(2, len(intermediateMasters))]
		result = append(result, intermediateMasters...)
	}
	{
		// Get 2 replicas of found IMs, if possible
		if len(intermediateMasters) == 1 {
			// Pick 2 replicas for this IM
			replicas, err := ReadReplicaInstances(&(intermediateMasters[0].Key))
			if err != nil {
				return result, err
			}
			sort.Sort(sort.Reverse(InstancesByCountReplicas(replicas)))
			replicas = filterOSCInstances(replicas)
			replicas = replicas[0:math.MinInt(2, len(replicas))]
			result = append(result, replicas...)

		}
		if len(intermediateMasters) == 2 {
			// Pick one replica from each IM (should be possible)
			for _, im := range intermediateMasters {
				replicas, err := ReadReplicaInstances(&im.Key)
				if err != nil {
					return result, err
				}
				sort.Sort(sort.Reverse(InstancesByCountReplicas(replicas)))
				replicas = filterOSCInstances(replicas)
				if len(replicas) > 0 {
					result = append(result, replicas[0])
				}
			}
		}
	}
	{
		// Get 2 3rd tier replicas, if possible
		condition := `
			replication_depth = 3
			and cluster_name = ?
		`
		replicas, err := readInstancesByCondition(condition, sqlutils.Args(clusterName), "")
		if err != nil {
			return result, err
		}
		sort.Sort(sort.Reverse(InstancesByCountReplicas(replicas)))
		replicas = filterOSCInstances(replicas)
		replicas = replicas[0:math.MinInt(2, len(replicas))]
		result = append(result, replicas...)
	}
	{
		// Get 2 1st tier leaf replicas, if possible
		condition := `
			replication_depth = 1
			and num_slave_hosts = 0
			and cluster_name = ?
		`
		replicas, err := readInstancesByCondition(condition, sqlutils.Args(clusterName), "")
		if err != nil {
			return result, err
		}
		replicas = filterOSCInstances(replicas)
		replicas = replicas[0:math.MinInt(2, len(replicas))]
		result = append(result, replicas...)
	}

	return result, nil
}

// GetClusterGhostReplicas returns a list of replicas that can serve as the connected servers
// for a [gh-ost](https://github.com/github/gh-ost) operation. A gh-ost operation prefers to talk
// to a RBR replica that has no children.
func GetClusterGhostReplicas(clusterName string) (result [](*Instance), err error) {
	condition := `
			replication_depth > 0
			and binlog_format = 'ROW'
			and cluster_name = ?
		`
	instances, err := readInstancesByCondition(condition, sqlutils.Args(clusterName), "num_slave_hosts asc")
	if err != nil {
		return result, err
	}

	for _, instance := range instances {
		skipThisHost := false
		if instance.IsBinlogServer() {
			skipThisHost = true
		}
		if !instance.IsLastCheckValid {
			skipThisHost = true
		}
		if !instance.LogBinEnabled {
			skipThisHost = true
		}
		if !instance.LogReplicationUpdatesEnabled {
			skipThisHost = true
		}
		if !skipThisHost {
			result = append(result, instance)
		}
	}

	return result, err
}

// GetInstancesMaxLag returns the maximum lag in a set of instances
func GetInstancesMaxLag(instances [](*Instance)) (maxLag int64, err error) {
	if len(instances) == 0 {
		return 0, log.Errorf("No instances found in GetInstancesMaxLag")
	}
	for _, clusterInstance := range instances {
		if clusterInstance.ReplicationLagSeconds.Valid && clusterInstance.ReplicationLagSeconds.Int64 > maxLag {
			maxLag = clusterInstance.ReplicationLagSeconds.Int64
		}
	}
	return maxLag, nil
}

// GetClusterHeuristicLag returns a heuristic lag for a cluster, based on its OSC replicas
func GetClusterHeuristicLag(clusterName string) (int64, error) {
	instances, err := GetClusterOSCReplicas(clusterName)
	if err != nil {
		return 0, err
	}
	return GetInstancesMaxLag(instances)
}

// GetHeuristicClusterPoolInstances returns instances of a cluster which are also pooled. If `pool` argument
// is empty, all pools are considered, otherwise, only instances of given pool are considered.
func GetHeuristicClusterPoolInstances(clusterName string, pool string) (result [](*Instance), err error) {
	result = [](*Instance){}
	instances, err := ReadClusterInstances(clusterName)
	if err != nil {
		return result, err
	}

	pooledInstanceKeys := NewInstanceKeyMap()
	clusterPoolInstances, err := ReadClusterPoolInstances(clusterName, pool)
	if err != nil {
		return result, err
	}
	for _, clusterPoolInstance := range clusterPoolInstances {
		pooledInstanceKeys.AddKey(InstanceKey{Hostname: clusterPoolInstance.Hostname, Port: clusterPoolInstance.Port})
	}

	for _, instance := range instances {
		skipThisHost := false
		if instance.IsBinlogServer() {
			skipThisHost = true
		}
		if !instance.IsLastCheckValid {
			skipThisHost = true
		}
		if !pooledInstanceKeys.HasKey(instance.Key) {
			skipThisHost = true
		}
		if !skipThisHost {
			result = append(result, instance)
		}
	}

	return result, err
}

// GetHeuristicClusterPoolInstancesLag returns a heuristic lag for the instances participating
// in a cluster pool (or all the cluster's pools)
func GetHeuristicClusterPoolInstancesLag(clusterName string, pool string) (int64, error) {
	instances, err := GetHeuristicClusterPoolInstances(clusterName, pool)
	if err != nil {
		return 0, err
	}
	return GetInstancesMaxLag(instances)
}

// updateInstanceClusterName
func updateInstanceClusterName(instance *Instance) error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
			update
				database_instance
			set
				cluster_name=?
			where
				hostname=? and port=?
        	`, instance.ClusterName, instance.Key.Hostname, instance.Key.Port,
		)
		if err != nil {
			return log.Errore(err)
		}
		AuditOperation("update-cluster-name", &instance.Key, fmt.Sprintf("set to %s", instance.ClusterName))
		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

// ReplaceClusterName replaces all occurances of oldClusterName with newClusterName
// It is called after a master failover
func ReplaceClusterName(oldClusterName string, newClusterName string) error {
	if oldClusterName == "" {
		return log.Errorf("replaceClusterName: skipping empty oldClusterName")
	}
	if newClusterName == "" {
		return log.Errorf("replaceClusterName: skipping empty newClusterName")
	}
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
			update
				database_instance
			set
				cluster_name=?
			where
				cluster_name=?
				`, newClusterName, oldClusterName,
		)
		if err != nil {
			return log.Errore(err)
		}
		AuditOperation("replace-cluster-name", nil, fmt.Sprintf("replaxced %s with %s", oldClusterName, newClusterName))
		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

// ReviewUnseenInstances reviews instances that have not been seen (suposedly dead) and updates some of their data
func ReviewUnseenInstances() error {
	instances, err := ReadUnseenInstances()
	if err != nil {
		return log.Errore(err)
	}
	operations := 0
	for _, instance := range instances {
		instance := instance

		masterHostname, err := ResolveHostname(instance.MasterKey.Hostname)
		if err != nil {
			log.Errore(err)
			continue
		}
		instance.MasterKey.Hostname = masterHostname
		savedClusterName := instance.ClusterName

		if err := ReadInstanceClusterAttributes(instance); err != nil {
			log.Errore(err)
		} else if instance.ClusterName != savedClusterName {
			updateInstanceClusterName(instance)
			operations++
		}
	}

	AuditOperation("review-unseen-instances", nil, fmt.Sprintf("Operations: %d", operations))
	return err
}

// readUnseenMasterKeys will read list of masters that have never been seen, and yet whose replicas
// seem to be replicating.
func readUnseenMasterKeys() ([]InstanceKey, error) {
	res := []InstanceKey{}

	err := db.QueryOrchestratorRowsMap(`
			SELECT DISTINCT
			    slave_instance.master_host, slave_instance.master_port
			FROM
			    database_instance slave_instance
			        LEFT JOIN
			    hostname_resolve ON (slave_instance.master_host = hostname_resolve.hostname)
			        LEFT JOIN
			    database_instance master_instance ON (
			    	COALESCE(hostname_resolve.resolved_hostname, slave_instance.master_host) = master_instance.hostname
			    	and slave_instance.master_port = master_instance.port)
			WHERE
			    master_instance.last_checked IS NULL
			    and slave_instance.master_host != ''
			    and slave_instance.master_host != '_'
			    and slave_instance.master_port > 0
			    and slave_instance.slave_io_running = 1
			`, func(m sqlutils.RowMap) error {
		instanceKey, _ := NewResolveInstanceKey(m.GetString("master_host"), m.GetInt("master_port"))
		// we ignore the error. It can be expected that we are unable to resolve the hostname.
		// Maybe that's how we got here in the first place!
		res = append(res, *instanceKey)

		return nil
	})
	if err != nil {
		return res, log.Errore(err)
	}

	return res, nil
}

// InjectSeed: intented to be used to inject an instance upon startup, assuming it's not already known to orchestrator.
func InjectSeed(instanceKey *InstanceKey) error {
	if instanceKey == nil {
		return fmt.Errorf("InjectSeed: nil instanceKey")
	}
	clusterName := instanceKey.StringCode()
	// minimal details:
	instance := &Instance{Key: *instanceKey, Version: "Unknown", ClusterName: clusterName}
	instance.SetSeed()
	err := WriteInstance(instance, false, nil)
	log.Debugf("InjectSeed: %+v, %+v", *instanceKey, err)
	AuditOperation("inject-seed", instanceKey, "injected")
	return err
}

// InjectUnseenMasters will review masters of instances that are known to be replicating, yet which are not listed
// in database_instance. Since their replicas are listed as replicating, we can assume that such masters actually do
// exist: we shall therefore inject them with minimal details into the database_instance table.
func InjectUnseenMasters() error {

	unseenMasterKeys, err := readUnseenMasterKeys()
	if err != nil {
		return err
	}

	operations := 0
	for _, masterKey := range unseenMasterKeys {
		masterKey := masterKey

		if RegexpMatchPatterns(masterKey.StringCode(), config.Config.DiscoveryIgnoreMasterHostnameFilters) {
			log.Debugf("InjectUnseenMasters: skipping discovery of %+v because it matches DiscoveryIgnoreMasterHostnameFilters", masterKey)
			continue
		}
		if RegexpMatchPatterns(masterKey.StringCode(), config.Config.DiscoveryIgnoreHostnameFilters) {
			log.Debugf("InjectUnseenMasters: skipping discovery of %+v because it matches DiscoveryIgnoreHostnameFilters", masterKey)
			continue
		}

		clusterName := masterKey.StringCode()
		// minimal details:
		instance := Instance{Key: masterKey, Version: "Unknown", ClusterName: clusterName}
		if err := WriteInstance(&instance, false, nil); err == nil {
			operations++
		}
	}

	AuditOperation("inject-unseen-masters", nil, fmt.Sprintf("Operations: %d", operations))
	return err
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
	err := db.QueryOrchestrator(query, nil, func(m sqlutils.RowMap) error {
		key := InstanceKey{
			Hostname: m.GetString("hostname"),
			Port:     m.GetInt("port"),
		}
		keys.AddKey(key)
		return nil
	})
	var rowsAffected int64 = 0
	for _, key := range keys.GetInstanceKeys() {
		sqlResult, err := db.ExecOrchestrator(`
			delete from
				database_instance
			where
		    hostname = ? and port = ?
			`, key.Hostname, key.Port,
		)
		if err != nil {
			return log.Errore(err)
		}
		rows, err := sqlResult.RowsAffected()
		if err != nil {
			return log.Errore(err)
		}
		rowsAffected = rowsAffected + rows
	}
	AuditOperation("forget-unseen-differently-resolved", nil, fmt.Sprintf("Forgotten instances: %d", rowsAffected))
	return err
}

// readUnknownMasterHostnameResolves will figure out the resolved hostnames of master-hosts which cannot be found.
// It uses the hostname_resolve_history table to heuristically guess the correct hostname (based on "this was the
// last time we saw this hostname and it resolves into THAT")
func readUnknownMasterHostnameResolves() (map[string]string, error) {
	res := make(map[string]string)
	err := db.QueryOrchestratorRowsMap(`
			SELECT DISTINCT
			    slave_instance.master_host, hostname_resolve_history.resolved_hostname
			FROM
			    database_instance slave_instance
			LEFT JOIN hostname_resolve ON (slave_instance.master_host = hostname_resolve.hostname)
			LEFT JOIN database_instance master_instance ON (
			    COALESCE(hostname_resolve.resolved_hostname, slave_instance.master_host) = master_instance.hostname
			    and slave_instance.master_port = master_instance.port
			) LEFT JOIN hostname_resolve_history ON (slave_instance.master_host = hostname_resolve_history.hostname)
			WHERE
			    master_instance.last_checked IS NULL
			    and slave_instance.master_host != ''
			    and slave_instance.master_host != '_'
			    and slave_instance.master_port > 0
			`, func(m sqlutils.RowMap) error {
		res[m.GetString("master_host")] = m.GetString("resolved_hostname")
		return nil
	})
	if err != nil {
		return res, log.Errore(err)
	}

	return res, nil
}

// ResolveUnknownMasterHostnameResolves fixes missing hostname resolves based on hostname_resolve_history
// The use case is replicas replicating from some unknown-hostname which cannot be otherwise found. This could
// happen due to an expire unresolve together with clearing up of hostname cache.
func ResolveUnknownMasterHostnameResolves() error {

	hostnameResolves, err := readUnknownMasterHostnameResolves()
	if err != nil {
		return err
	}
	for hostname, resolvedHostname := range hostnameResolves {
		UpdateResolvedHostname(hostname, resolvedHostname)
	}

	AuditOperation("resolve-unknown-masters", nil, fmt.Sprintf("Num resolved hostnames: %d", len(hostnameResolves)))
	return err
}

// ReadCountMySQLSnapshots is a utility method to return registered number of snapshots for a given list of hosts
func ReadCountMySQLSnapshots(hostnames []string) (map[string]int, error) {
	res := make(map[string]int)
	if !config.Config.ServeAgentsHttp {
		return res, nil
	}
	query := fmt.Sprintf(`
		select
			hostname,
			count_mysql_snapshots
		from
			host_agent
		where
			hostname in (%s)
		order by
			hostname
		`, sqlutils.InClauseStringValues(hostnames))

	err := db.QueryOrchestratorRowsMap(query, func(m sqlutils.RowMap) error {
		res[m.GetString("hostname")] = m.GetInt("count_mysql_snapshots")
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err
}

// PopulateInstancesAgents will fill in extra data acquired from agents for given instances
// At current this is the number of snapshots.
// This isn't too pretty; it's a push-into-instance-data-that-belongs-to-agent thing.
// Originally the need was to visually present the number of snapshots per host on the web/cluster page, which
// indeed proves to be useful in our experience.
func PopulateInstancesAgents(instances [](*Instance)) error {
	if len(instances) == 0 {
		return nil
	}
	hostnames := []string{}
	for _, instance := range instances {
		hostnames = append(hostnames, instance.Key.Hostname)
	}
	agentsCountMySQLSnapshots, err := ReadCountMySQLSnapshots(hostnames)
	if err != nil {
		return err
	}
	for _, instance := range instances {
		if count, ok := agentsCountMySQLSnapshots[instance.Key.Hostname]; ok {
			instance.CountMySQLSnapshots = count
		}
	}

	return nil
}

func GetClusterName(instanceKey *InstanceKey) (clusterName string, err error) {
	if clusterName, found := instanceKeyInformativeClusterName.Get(instanceKey.StringCode()); found {
		return clusterName.(string), nil
	}
	query := `
		select
			ifnull(max(cluster_name), '') as cluster_name
		from
			database_instance
		where
			hostname = ?
			and port = ?
			`
	err = db.QueryOrchestrator(query, sqlutils.Args(instanceKey.Hostname, instanceKey.Port), func(m sqlutils.RowMap) error {
		clusterName = m.GetString("cluster_name")
		instanceKeyInformativeClusterName.Set(instanceKey.StringCode(), clusterName, cache.DefaultExpiration)
		return nil
	})

	return clusterName, log.Errore(err)
}

// ReadClusters reads names of all known clusters
func ReadClusters() (clusterNames []string, err error) {
	clusters, err := ReadClustersInfo("")
	if err != nil {
		return clusterNames, err
	}
	for _, clusterInfo := range clusters {
		clusterNames = append(clusterNames, clusterInfo.ClusterName)
	}
	return clusterNames, nil
}

// ReadClusterInfo reads some info about a given cluster
func ReadClusterInfo(clusterName string) (*ClusterInfo, error) {
	clusters, err := ReadClustersInfo(clusterName)
	if err != nil {
		return &ClusterInfo{}, err
	}
	if len(clusters) != 1 {
		return &ClusterInfo{}, fmt.Errorf("No cluster info found for %s", clusterName)
	}
	return &(clusters[0]), nil
}

// ReadClustersInfo reads names of all known clusters and some aggregated info
func ReadClustersInfo(clusterName string) ([]ClusterInfo, error) {
	clusters := []ClusterInfo{}

	whereClause := ""
	args := sqlutils.Args()
	if clusterName != "" {
		whereClause = `where cluster_name = ?`
		args = append(args, clusterName)
	}
	query := fmt.Sprintf(`
		select
			cluster_name,
			count(*) as count_instances,
			ifnull(min(alias), cluster_name) as alias,
			ifnull(min(domain_name), '') as domain_name
		from
			database_instance
			left join cluster_alias using (cluster_name)
			left join cluster_domain_name using (cluster_name)
		%s
		group by
			cluster_name`, whereClause)

	err := db.QueryOrchestrator(query, args, func(m sqlutils.RowMap) error {
		clusterInfo := ClusterInfo{
			ClusterName:    m.GetString("cluster_name"),
			CountInstances: m.GetUint("count_instances"),
			ClusterAlias:   m.GetString("alias"),
			ClusterDomain:  m.GetString("domain_name"),
		}
		clusterInfo.ApplyClusterAlias()
		clusterInfo.ReadRecoveryInfo()

		clusters = append(clusters, clusterInfo)
		return nil
	})

	return clusters, err
}

// Get a listing of KVPair for clusters masters, for all clusters or for a specific cluster.
func GetMastersKVPairs(clusterName string) (kvPairs [](*kv.KVPair), err error) {

	clusterAliasMap := make(map[string]string)
	if clustersInfo, err := ReadClustersInfo(clusterName); err != nil {
		return kvPairs, err
	} else {
		for _, clusterInfo := range clustersInfo {
			clusterAliasMap[clusterInfo.ClusterName] = clusterInfo.ClusterAlias
		}
	}

	masters, err := ReadWriteableClustersMasters()
	if err != nil {
		return kvPairs, err
	}
	for _, master := range masters {
		clusterPairs := GetClusterMasterKVPairs(clusterAliasMap[master.ClusterName], &master.Key)
		kvPairs = append(kvPairs, clusterPairs...)
	}

	return kvPairs, err
}

// HeuristicallyApplyClusterDomainInstanceAttribute writes down the cluster-domain
// to master-hostname as a general attribute, by reading current topology and **trusting** it to be correct
func HeuristicallyApplyClusterDomainInstanceAttribute(clusterName string) (instanceKey *InstanceKey, err error) {
	clusterInfo, err := ReadClusterInfo(clusterName)
	if err != nil {
		return nil, err
	}

	if clusterInfo.ClusterDomain == "" {
		return nil, fmt.Errorf("Cannot find domain name for cluster %+v", clusterName)
	}

	masters, err := ReadClusterWriteableMaster(clusterName)
	if err != nil {
		return nil, err
	}
	if len(masters) != 1 {
		return nil, fmt.Errorf("Found %+v potential master for cluster %+v", len(masters), clusterName)
	}
	instanceKey = &masters[0].Key
	return instanceKey, attributes.SetGeneralAttribute(clusterInfo.ClusterDomain, instanceKey.StringCode())
}

// GetHeuristicClusterDomainInstanceAttribute attempts detecting the cluster domain
// for the given cluster, and return the instance key associated as writer with that domain
func GetHeuristicClusterDomainInstanceAttribute(clusterName string) (instanceKey *InstanceKey, err error) {
	clusterInfo, err := ReadClusterInfo(clusterName)
	if err != nil {
		return nil, err
	}

	if clusterInfo.ClusterDomain == "" {
		return nil, fmt.Errorf("Cannot find domain name for cluster %+v", clusterName)
	}

	writerInstanceName, err := attributes.GetGeneralAttribute(clusterInfo.ClusterDomain)
	if err != nil {
		return nil, err
	}
	return ParseRawInstanceKey(writerInstanceName)
}

// ReadAllInstanceKeys
func ReadAllInstanceKeys() ([]InstanceKey, error) {
	res := []InstanceKey{}
	query := `
		select
			hostname, port
		from
			database_instance
			`
	err := db.QueryOrchestrator(query, sqlutils.Args(), func(m sqlutils.RowMap) error {
		instanceKey, merr := NewResolveInstanceKey(m.GetString("hostname"), m.GetInt("port"))
		if merr != nil {
			log.Errore(merr)
		} else if !InstanceIsForgotten(instanceKey) {
			// only if not in "forget" cache
			res = append(res, *instanceKey)
		}
		return nil
	})
	return res, log.Errore(err)
}

// ReadAllInstanceKeysMasterKeys
func ReadAllMinimalInstances() ([]MinimalInstance, error) {
	res := []MinimalInstance{}
	query := `
		select
			hostname, port, master_host, master_port, cluster_name
		from
			database_instance
			`
	err := db.QueryOrchestrator(query, sqlutils.Args(), func(m sqlutils.RowMap) error {
		minimalInstance := MinimalInstance{}
		minimalInstance.Key = InstanceKey{
			Hostname: m.GetString("hostname"),
			Port:     m.GetInt("port"),
		}
		minimalInstance.MasterKey = InstanceKey{
			Hostname: m.GetString("master_host"),
			Port:     m.GetInt("master_port"),
		}
		minimalInstance.ClusterName = m.GetString("cluster_name")

		if !InstanceIsForgotten(&minimalInstance.Key) {
			// only if not in "forget" cache
			res = append(res, minimalInstance)
		}
		return nil
	})
	return res, log.Errore(err)
}

// ReadOutdatedInstanceKeys reads and returns keys for all instances that are not up to date (i.e.
// pre-configured time has passed since they were last checked)
// But we also check for the case where an attempt at instance checking has been made, that hasn't
// resulted in an actual check! This can happen when TCP/IP connections are hung, in which case the "check"
// never returns. In such case we multiply interval by a factor, so as not to open too many connections on
// the instance.
func ReadOutdatedInstanceKeys() ([]InstanceKey, error) {
	res := []InstanceKey{}
	query := `
		select
			hostname, port
		from
			database_instance
		where
			case
				when last_attempted_check <= last_checked
				then last_checked < now() - interval ? second
				else last_checked < now() - interval ? second
			end
			`
	args := sqlutils.Args(config.Config.InstancePollSeconds, 2*config.Config.InstancePollSeconds)

	err := db.QueryOrchestrator(query, args, func(m sqlutils.RowMap) error {
		instanceKey, merr := NewResolveInstanceKey(m.GetString("hostname"), m.GetInt("port"))
		if merr != nil {
			log.Errore(merr)
		} else if !InstanceIsForgotten(instanceKey) {
			// only if not in "forget" cache
			res = append(res, *instanceKey)
		}
		// We don;t return an error because we want to keep filling the outdated instances list.
		return nil
	})

	if err != nil {
		log.Errore(err)
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
	var ignore string = ""
	if insertIgnore {
		ignore = "ignore"
	}
	var valRow string = fmt.Sprintf("(%s)", strings.Join(values, ", "))
	var val bytes.Buffer
	val.WriteString(valRow)
	for i := 1; i < nrRows; i++ {
		val.WriteString(",\n                ") // indent VALUES, see below
		val.WriteString(valRow)
	}

	var col string = strings.Join(columns, ", ")
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

func mkInsertOdkuForInstances(instances []*Instance, instanceWasActuallyFound bool, updateLastSeen bool) (string, []interface{}, error) {
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
		"uptime",
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
		"log_slave_updates",
		"binary_log_file",
		"binary_log_pos",
		"master_host",
		"master_port",
		"slave_sql_running",
		"slave_io_running",
		"replication_sql_thread_state",
		"replication_io_thread_state",
		"has_replication_filters",
		"supports_oracle_gtid",
		"oracle_gtid",
		"master_uuid",
		"ancestry_uuid",
		"executed_gtid_set",
		"gtid_mode",
		"gtid_purged",
		"gtid_errant",
		"mariadb_gtid",
		"pseudo_gtid",
		"master_log_file",
		"read_master_log_pos",
		"relay_master_log_file",
		"exec_master_log_pos",
		"relay_log_file",
		"relay_log_pos",
		"last_sql_error",
		"last_io_error",
		"seconds_behind_master",
		"slave_lag_seconds",
		"sql_delay",
		"num_slave_hosts",
		"slave_hosts",
		"cluster_name",
		"suggested_cluster_alias",
		"data_center",
		"region",
		"physical_environment",
		"replication_depth",
		"is_co_master",
		"replication_credentials_available",
		"has_replication_credentials",
		"allow_tls",
		"semi_sync_enforced",
		"semi_sync_available",
		"semi_sync_master_enabled",
		"semi_sync_master_timeout",
		"semi_sync_master_wait_for_slave_count",
		"semi_sync_replica_enabled",
		"semi_sync_master_status",
		"semi_sync_master_clients",
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

	var values []string = make([]string, len(columns))
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

	var args []interface{}
	for _, instance := range instances {
		// number of columns minus 2 as last_checked and last_attempted_check
		// updated with NOW()
		args = append(args, instance.Key.Hostname)
		args = append(args, instance.Key.Port)
		args = append(args, instance.Uptime)
		args = append(args, instance.ServerID)
		args = append(args, instance.ServerUUID)
		args = append(args, instance.Version)
		args = append(args, instance.MajorVersionString())
		args = append(args, instance.VersionComment)
		args = append(args, instance.IsBinlogServer())
		args = append(args, instance.ReadOnly)
		args = append(args, instance.Binlog_format)
		args = append(args, instance.BinlogRowImage)
		args = append(args, instance.LogBinEnabled)
		args = append(args, instance.LogReplicationUpdatesEnabled)
		args = append(args, instance.SelfBinlogCoordinates.LogFile)
		args = append(args, instance.SelfBinlogCoordinates.LogPos)
		args = append(args, instance.MasterKey.Hostname)
		args = append(args, instance.MasterKey.Port)
		args = append(args, instance.ReplicationSQLThreadRuning)
		args = append(args, instance.ReplicationIOThreadRuning)
		args = append(args, instance.ReplicationSQLThreadState)
		args = append(args, instance.ReplicationIOThreadState)
		args = append(args, instance.HasReplicationFilters)
		args = append(args, instance.SupportsOracleGTID)
		args = append(args, instance.UsingOracleGTID)
		args = append(args, instance.MasterUUID)
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
		args = append(args, instance.SecondsBehindMaster)
		args = append(args, instance.ReplicationLagSeconds)
		args = append(args, instance.SQLDelay)
		args = append(args, len(instance.Replicas))
		args = append(args, instance.Replicas.ToJSONString())
		args = append(args, instance.ClusterName)
		args = append(args, instance.SuggestedClusterAlias)
		args = append(args, instance.DataCenter)
		args = append(args, instance.Region)
		args = append(args, instance.PhysicalEnvironment)
		args = append(args, instance.ReplicationDepth)
		args = append(args, instance.IsCoMaster)
		args = append(args, instance.ReplicationCredentialsAvailable)
		args = append(args, instance.HasReplicationCredentials)
		args = append(args, instance.AllowTLS)
		args = append(args, instance.SemiSyncEnforced)
		args = append(args, instance.SemiSyncAvailable)
		args = append(args, instance.SemiSyncMasterEnabled)
		args = append(args, instance.SemiSyncMasterTimeout)
		args = append(args, instance.SemiSyncMasterWaitForReplicaCount)
		args = append(args, instance.SemiSyncReplicaEnabled)
		args = append(args, instance.SemiSyncMasterStatus)
		args = append(args, instance.SemiSyncMasterClients)
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
		return sql, args, log.Errorf("Failed to build query: %v", err)
	}

	return sql, args, nil
}

// writeManyInstances stores instances in the orchestrator backend
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
	if _, err := db.ExecOrchestrator(sql, args...); err != nil {
		return err
	}
	return nil
}

type instanceUpdateObject struct {
	instance                 *Instance
	instanceWasActuallyFound bool
	lastError                error
}

// instances sorter by instanceKey
type byInstanceKey []*Instance

func (a byInstanceKey) Len() int           { return len(a) }
func (a byInstanceKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byInstanceKey) Less(i, j int) bool { return a[i].Key.SmallerThan(&a[j].Key) }

var instanceWriteBuffer chan instanceUpdateObject
var forceFlushInstanceWriteBuffer = make(chan bool)

func enqueueInstanceWrite(instance *Instance, instanceWasActuallyFound bool, lastError error) {
	if len(instanceWriteBuffer) == config.Config.InstanceWriteBufferSize {
		// Signal the "flushing" goroutine that there's work.
		// We prefer doing all bulk flushes from one goroutine.
		// Non blocking send to avoid blocking goroutines on sending a flush,
		// if the "flushing" goroutine is not able read is because a flushing is ongoing.
		select {
		case forceFlushInstanceWriteBuffer <- true:
		default:
		}
	}
	instanceWriteBuffer <- instanceUpdateObject{instance, instanceWasActuallyFound, lastError}
}

// flushInstanceWriteBuffer saves enqueued instances to Orchestrator Db
func flushInstanceWriteBuffer() {
	var instances []*Instance
	var lastseen []*Instance // instances to update with last_seen field

	defer func() {
		// reset stopwatches (TODO: .ResetAll())
		writeBufferLatency.Reset("wait")
		writeBufferLatency.Reset("write")
		writeBufferLatency.Start("wait") // waiting for next flush
	}()

	writeBufferLatency.Stop("wait")

	if len(instanceWriteBuffer) == 0 {
		return
	}

	// There are `DiscoveryMaxConcurrency` many goroutines trying to enqueue an instance into the buffer
	// when one instance is flushed from the buffer then one discovery goroutine is ready to enqueue a new instance
	// this is why we want to flush all instances in the buffer untill a max of `InstanceWriteBufferSize`.
	// Otherwise we can flush way more instances than what's expected.
	for i := 0; i < config.Config.InstanceWriteBufferSize && len(instanceWriteBuffer) > 0; i++ {
		upd := <-instanceWriteBuffer
		if upd.instanceWasActuallyFound && upd.lastError == nil {
			lastseen = append(lastseen, upd.instance)
		} else {
			instances = append(instances, upd.instance)
			log.Debugf("flushInstanceWriteBuffer: will not update database_instance.last_seen due to error: %+v", upd.lastError)
		}
	}

	writeBufferLatency.Start("write")

	// sort instances by instanceKey (table pk) to make locking predictable
	sort.Sort(byInstanceKey(instances))
	sort.Sort(byInstanceKey(lastseen))

	writeFunc := func() error {
		err := writeManyInstances(instances, true, false)
		if err != nil {
			return log.Errorf("flushInstanceWriteBuffer writemany: %v", err)
		}
		err = writeManyInstances(lastseen, true, true)
		if err != nil {
			return log.Errorf("flushInstanceWriteBuffer last_seen: %v", err)
		}

		writeInstanceCounter.Inc(int64(len(instances) + len(lastseen)))
		return nil
	}
	err := ExecDBWriteFunc(writeFunc)
	if err != nil {
		log.Errorf("flushInstanceWriteBuffer: %v", err)
	}

	writeBufferLatency.Stop("write")

	writeBufferMetrics.Append(&WriteBufferMetric{
		Timestamp:    time.Now(),
		WaitLatency:  writeBufferLatency.Elapsed("wait"),
		WriteLatency: writeBufferLatency.Elapsed("write"),
		Instances:    len(lastseen) + len(instances),
	})
}

// WriteInstance stores an instance in the orchestrator backend
func WriteInstance(instance *Instance, instanceWasActuallyFound bool, lastError error) error {
	if lastError != nil {
		log.Debugf("writeInstance: will not update database_instance due to error: %+v", lastError)
		return nil
	}
	return writeManyInstances([]*Instance{instance}, instanceWasActuallyFound, true)
}

// UpdateInstanceLastChecked updates the last_check timestamp in the orchestrator backed database
// for a given instance
func UpdateInstanceLastChecked(instanceKey *InstanceKey, partialSuccess bool) error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
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
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}

// UpdateInstanceLastAttemptedCheck updates the last_attempted_check timestamp in the orchestrator backed database
// for a given instance.
// This is used as a failsafe mechanism in case access to the instance gets hung (it happens), in which case
// the entire ReadTopology gets stuck (and no, connection timeout nor driver timeouts don't help. Don't look at me,
// the world is a harsh place to live in).
// And so we make sure to note down *before* we even attempt to access the instance; and this raises a red flag when we
// wish to access the instance again: if last_attempted_check is *newer* than last_checked, that's bad news and means
// we have a "hanging" issue.
func UpdateInstanceLastAttemptedCheck(instanceKey *InstanceKey) error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
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
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}

func InstanceIsForgotten(instanceKey *InstanceKey) bool {
	_, found := forgetInstanceKeys.Get(instanceKey.StringCode())
	return found
}

// ForgetInstance removes an instance entry from the orchestrator backed database.
// It may be auto-rediscovered through topology or requested for discovery by multiple means.
func ForgetInstance(instanceKey *InstanceKey) error {
	if instanceKey == nil {
		return log.Errorf("ForgetInstance(): nil instanceKey")
	}
	forgetInstanceKeys.Set(instanceKey.StringCode(), true, cache.DefaultExpiration)
	sqlResult, err := db.ExecOrchestrator(`
			delete
				from database_instance
			where
				hostname = ? and port = ?`,
		instanceKey.Hostname,
		instanceKey.Port,
	)
	if err != nil {
		return log.Errore(err)
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return log.Errore(err)
	}
	if rows == 0 {
		return log.Errorf("ForgetInstance(): instance %+v not found", *instanceKey)
	}
	AuditOperation("forget", instanceKey, "")
	return nil
}

// ForgetInstance removes an instance entry from the orchestrator backed database.
// It may be auto-rediscovered through topology or requested for discovery by multiple means.
func ForgetCluster(clusterName string) error {
	clusterInstances, err := ReadClusterInstances(clusterName)
	if err != nil {
		return err
	}
	if len(clusterInstances) == 0 {
		return nil
	}
	for _, instance := range clusterInstances {
		forgetInstanceKeys.Set(instance.Key.StringCode(), true, cache.DefaultExpiration)
		AuditOperation("forget", &instance.Key, "")
	}
	_, err = db.ExecOrchestrator(`
			delete
				from database_instance
			where
				cluster_name = ?`,
		clusterName,
	)
	return err
}

// ForgetLongUnseenInstances will remove entries of all instacnes that have long since been last seen.
func ForgetLongUnseenInstances() error {
	sqlResult, err := db.ExecOrchestrator(`
			delete
				from database_instance
			where
				last_seen < NOW() - interval ? hour`,
		config.Config.UnseenInstanceForgetHours,
	)
	if err != nil {
		return log.Errore(err)
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return log.Errore(err)
	}
	AuditOperation("forget-unseen", nil, fmt.Sprintf("Forgotten instances: %d", rows))
	return err
}

// SnapshotTopologies records topology graph for all existing topologies
func SnapshotTopologies() error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
        	insert ignore into
        		database_instance_topology_history (snapshot_unix_timestamp,
        			hostname, port, master_host, master_port, cluster_name, version)
        	select
        		UNIX_TIMESTAMP(NOW()),
        		hostname, port, master_host, master_port, cluster_name, version
			from
				database_instance
				`,
		)
		if err != nil {
			return log.Errore(err)
		}

		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

// ReadHistoryClusterInstances reads (thin) instances from history
func ReadHistoryClusterInstances(clusterName string, historyTimestampPattern string) ([](*Instance), error) {
	instances := [](*Instance){}

	query := `
		select
			*
		from
			database_instance_topology_history
		where
			snapshot_unix_timestamp rlike ?
			and cluster_name = ?
		order by
			hostname, port`

	err := db.QueryOrchestrator(query, sqlutils.Args(historyTimestampPattern, clusterName), func(m sqlutils.RowMap) error {
		instance := NewInstance()

		instance.Key.Hostname = m.GetString("hostname")
		instance.Key.Port = m.GetInt("port")
		instance.MasterKey.Hostname = m.GetString("master_host")
		instance.MasterKey.Port = m.GetInt("master_port")
		instance.ClusterName = m.GetString("cluster_name")

		instances = append(instances, instance)
		return nil
	})
	if err != nil {
		return instances, log.Errore(err)
	}
	return instances, err
}

// RecordInstanceCoordinatesHistory snapshots the binlog coordinates of instances
func RecordInstanceCoordinatesHistory() error {
	{
		writeFunc := func() error {
			_, err := db.ExecOrchestrator(`
        	delete from database_instance_coordinates_history
			where
				recorded_timestamp < NOW() - INTERVAL ? MINUTE
				`, (config.PseudoGTIDCoordinatesHistoryHeuristicMinutes + 2),
			)
			return log.Errore(err)
		}
		ExecDBWriteFunc(writeFunc)
	}
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
			insert into
				database_instance_coordinates_history (
					hostname, port,	last_seen, recorded_timestamp,
					binary_log_file, binary_log_pos, relay_log_file, relay_log_pos
				)
			select
				hostname, port, last_seen, NOW(),
				binary_log_file, binary_log_pos, relay_log_file, relay_log_pos
			from
				database_instance
			where
				(
					binary_log_file != ''
					or relay_log_file != ''
				)
				`,
		)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}

// GetHeuristiclyRecentCoordinatesForInstance returns valid and reasonably recent coordinates for given instance.
func GetHeuristiclyRecentCoordinatesForInstance(instanceKey *InstanceKey) (selfCoordinates *BinlogCoordinates, relayLogCoordinates *BinlogCoordinates, err error) {
	query := `
		select
			binary_log_file, binary_log_pos, relay_log_file, relay_log_pos
		from
			database_instance_coordinates_history
		where
			hostname = ?
			and port = ?
			and recorded_timestamp <= NOW() - INTERVAL ? MINUTE
		order by
			recorded_timestamp desc
			limit 1
			`
	err = db.QueryOrchestrator(query, sqlutils.Args(instanceKey.Hostname, instanceKey.Port, config.PseudoGTIDCoordinatesHistoryHeuristicMinutes), func(m sqlutils.RowMap) error {
		selfCoordinates = &BinlogCoordinates{LogFile: m.GetString("binary_log_file"), LogPos: m.GetInt64("binary_log_pos")}
		relayLogCoordinates = &BinlogCoordinates{LogFile: m.GetString("relay_log_file"), LogPos: m.GetInt64("relay_log_pos")}

		return nil
	})
	return selfCoordinates, relayLogCoordinates, err
}

// RecordInstanceCoordinatesHistory snapshots the binlog coordinates of instances
func RecordStaleInstanceBinlogCoordinates(instanceKey *InstanceKey, binlogCoordinates *BinlogCoordinates) error {
	args := sqlutils.Args(
		instanceKey.Hostname, instanceKey.Port,
		binlogCoordinates.LogFile, binlogCoordinates.LogPos,
	)
	_, err := db.ExecOrchestrator(`
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
		return log.Errore(err)
	}
	_, err = db.ExecOrchestrator(`
			insert ignore into
				database_instance_stale_binlog_coordinates (
					hostname, port,	binary_log_file, binary_log_pos, first_seen
				)
				values (
					?, ?, ?, ?, NOW()
				)`,
		args...)
	return log.Errore(err)
}

func ExpireStaleInstanceBinlogCoordinates() error {
	expireSeconds := config.Config.ReasonableReplicationLagSeconds * 2
	if expireSeconds < config.StaleInstanceCoordinatesExpireSeconds {
		expireSeconds = config.StaleInstanceCoordinatesExpireSeconds
	}
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
					delete from database_instance_stale_binlog_coordinates
					where first_seen < NOW() - INTERVAL ? SECOND
					`, expireSeconds,
		)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}

// GetPreviousKnownRelayLogCoordinatesForInstance returns known relay log coordinates, that are not the
// exact current coordinates
func GetPreviousKnownRelayLogCoordinatesForInstance(instance *Instance) (relayLogCoordinates *BinlogCoordinates, err error) {
	query := `
		select
			relay_log_file, relay_log_pos
		from
			database_instance_coordinates_history
		where
			hostname = ?
			and port = ?
			and (relay_log_file, relay_log_pos) < (?, ?)
			and relay_log_file != ''
			and relay_log_pos != 0
		order by
			recorded_timestamp desc
			limit 1
			`
	err = db.QueryOrchestrator(query, sqlutils.Args(
		instance.Key.Hostname,
		instance.Key.Port,
		instance.RelaylogCoordinates.LogFile,
		instance.RelaylogCoordinates.LogPos,
	), func(m sqlutils.RowMap) error {
		relayLogCoordinates = &BinlogCoordinates{LogFile: m.GetString("relay_log_file"), LogPos: m.GetInt64("relay_log_pos")}

		return nil
	})
	return relayLogCoordinates, err
}

// ResetInstanceRelaylogCoordinatesHistory forgets about the history of an instance. This action is desirable
// when relay logs become obsolete or irrelevant. Such is the case on `CHANGE MASTER TO`: servers gets compeltely
// new relay logs.
func ResetInstanceRelaylogCoordinatesHistory(instanceKey *InstanceKey) error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
			update database_instance_coordinates_history
				set relay_log_file='', relay_log_pos=0
			where
				hostname=? and port=?
				`, instanceKey.Hostname, instanceKey.Port,
		)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}

// FigureClusterName will make a best effort to deduce a cluster name using either a given alias
// or an instanceKey. First attempt is at alias, and if that doesn't work, we try instanceKey.
// - clusterHint may be an empty string
func FigureClusterName(clusterHint string, instanceKey *InstanceKey, thisInstanceKey *InstanceKey) (clusterName string, err error) {
	// Look for exact matches, first.

	if clusterHint != "" {
		// Exact cluster name match:
		if clusterInfo, err := ReadClusterInfo(clusterHint); err == nil && clusterInfo != nil {
			return clusterInfo.ClusterName, nil
		}
		// Exact cluster alias match:
		if clustersInfo, err := ReadClustersInfo(""); err == nil {
			for _, clusterInfo := range clustersInfo {
				if clusterInfo.ClusterAlias == clusterHint {
					return clusterInfo.ClusterName, nil
				}
			}
		}
	}

	clusterByInstanceKey := func(instanceKey *InstanceKey) (hasResult bool, clusterName string, err error) {
		if instanceKey == nil {
			return false, "", nil
		}
		instance, _, err := ReadInstance(instanceKey)
		if err != nil {
			return true, clusterName, log.Errore(err)
		}
		if instance != nil {
			if instance.ClusterName == "" {
				return true, clusterName, log.Errorf("Unable to determine cluster name for %+v, empty cluster name. clusterHint=%+v", instance.Key, clusterHint)
			}
			return true, instance.ClusterName, nil
		}
		return false, "", nil
	}
	// exact instance key:
	if hasResult, clusterName, err := clusterByInstanceKey(instanceKey); hasResult {
		return clusterName, err
	}
	// fuzzy instance key:
	if hasResult, clusterName, err := clusterByInstanceKey(ReadFuzzyInstanceKeyIfPossible(instanceKey)); hasResult {
		return clusterName, err
	}
	//  Let's see about _this_ instance
	if hasResult, clusterName, err := clusterByInstanceKey(thisInstanceKey); hasResult {
		return clusterName, err
	}
	return clusterName, log.Errorf("Unable to determine cluster name. clusterHint=%+v", clusterHint)
}

// FigureInstanceKey tries to figure out a key
func FigureInstanceKey(instanceKey *InstanceKey, thisInstanceKey *InstanceKey) (*InstanceKey, error) {
	if figuredKey := ReadFuzzyInstanceKeyIfPossible(instanceKey); figuredKey != nil {
		return figuredKey, nil
	}
	figuredKey := thisInstanceKey
	if figuredKey == nil {
		return nil, log.Errorf("Cannot deduce instance %+v", instanceKey)
	}
	return figuredKey, nil
}

// PopulateGroupReplicationInformation obtains information about Group Replication  for this host as well as other hosts
// who are members of the same group (if any).
func PopulateGroupReplicationInformation(instance *Instance, db *sql.DB) error {
	q := `
	SELECT
		MEMBER_ID,
		MEMBER_HOST,
		MEMBER_PORT,
		MEMBER_STATE,
		MEMBER_ROLE,
		@@global.group_replication_group_name,
		@@global.group_replication_single_primary_mode
	FROM
		performance_schema.replication_group_members
	`
	rows, err := db.Query(q)
	if err != nil {
		_, grNotSupported := GroupReplicationNotSupportedErrors[err.(*mysql.MySQLError).Number]
		if grNotSupported {
			return nil // If GR is not supported by the instance, just exit
		} else {
			// If we got here, the query failed but not because the server does not support group replication. Let's
			// log the error
			return log.Error("There was an error trying to check group replication information for instance "+
				"%+v: %+v", instance.Key, err)
		}
	}
	defer rows.Close()
	foundGroupPrimary := false
	// Loop over the query results and populate GR instance attributes from the row that matches the instance being
	// probed. In addition, figure out the group primary and also add it as attribute of the instance.
	for rows.Next() {
		var (
			uuid               string
			host               string
			port               uint16
			state              string
			role               string
			groupName          string
			singlePrimaryGroup bool
		)
		err := rows.Scan(&uuid, &host, &port, &state, &role, &groupName, &singlePrimaryGroup)
		if err == nil {
			// ToDo: add support for multi primary groups.
			if !singlePrimaryGroup {
				log.Debugf("This host seems to belong to a multi-primary replication group, which we don't " +
					"support")
				break
			}
			groupMemberKey, err := NewResolveInstanceKey(host, int(port))
			if err != nil {
				log.Errorf("Unable to resolve instance for group member %v:%v", host, port)
				continue
			}
			// Set the replication group primary from what we find in performance_schema.replication_group_members for
			// the instance being discovered.
			if role == GroupReplicationMemberRolePrimary && groupMemberKey != nil {
				instance.ReplicationGroupPrimaryInstanceKey = *groupMemberKey
				foundGroupPrimary = true
			}
			if uuid == instance.ServerUUID {
				instance.ReplicationGroupName = groupName
				instance.ReplicationGroupIsSinglePrimary = singlePrimaryGroup
				instance.ReplicationGroupMemberRole = role
				instance.ReplicationGroupMemberState = state
			} else {
				instance.AddGroupMemberKey(groupMemberKey) // This helps us keep info on all members of the same group as the instance
			}
		} else {
			log.Errorf("Unable to scan row  group replication information while processing %+v, skipping the "+
				"row and continuing: %+v", instance.Key, err)
		}
	}
	// If we did not manage to find the primary of the group in performance_schema.replication_group_members, we are
	// likely to have been expelled from the group. Still, try to find out the primary of the group and set it for the
	// instance being discovered, so that it is identified as part of the same cluster
	if !foundGroupPrimary {
		err = ReadReplicationGroupPrimary(instance)
		if err != nil {
			return log.Errorf("Unable to find the group primary of instance %+v even though it seems to be "+
				"part of a replication group", instance.Key)
		}
	}
	return nil
}

// RegisterInjectedPseudoGTID
func RegisterInjectedPseudoGTID(clusterName string) error {
	query := `
			insert into cluster_injected_pseudo_gtid (
					cluster_name,
					time_injected
				) values (?, now())
				on duplicate key update
					cluster_name=values(cluster_name),
					time_injected=now()
				`
	args := sqlutils.Args(clusterName)
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(query, args...)
		if err == nil {
			clusterInjectedPseudoGTIDCache.Set(clusterName, true, cache.DefaultExpiration)
		}
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}

// ExpireInjectedPseudoGTID
func ExpireInjectedPseudoGTID() error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
				delete from cluster_injected_pseudo_gtid
				where time_injected < NOW() - INTERVAL ? MINUTE
				`, config.PseudoGTIDExpireMinutes,
		)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}

// isInjectedPseudoGTID reads from backend DB / cache
func isInjectedPseudoGTID(clusterName string) (injected bool, err error) {
	if injectedValue, found := clusterInjectedPseudoGTIDCache.Get(clusterName); found {
		return injectedValue.(bool), err
	}
	query := `
			select
					count(*) as is_injected
				from
					cluster_injected_pseudo_gtid
				where
					cluster_name = ?
			`
	err = db.QueryOrchestrator(query, sqlutils.Args(clusterName), func(m sqlutils.RowMap) error {
		injected = m.GetBool("is_injected")
		return nil
	})
	clusterInjectedPseudoGTIDCache.Set(clusterName, injected, cache.DefaultExpiration)
	return injected, log.Errore(err)
}
