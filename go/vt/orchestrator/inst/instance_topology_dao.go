/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/db"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/orchestrator/util"
)

// Max concurrency for bulk topology operations
const topologyConcurrency = 128

var topologyConcurrencyChan = make(chan bool, topologyConcurrency)
var supportedAutoPseudoGTIDWriters *cache.Cache = cache.New(config.CheckAutoPseudoGTIDGrantsIntervalSeconds*time.Second, time.Second)

type OperationGTIDHint string

const (
	GTIDHintDeny    OperationGTIDHint = "NoGTID"
	GTIDHintNeutral OperationGTIDHint = "GTIDHintNeutral"
	GTIDHintForce   OperationGTIDHint = "GTIDHintForce"
)

const (
	Error1201CouldnotInitializeMasterInfoStructure = "Error 1201:"
)

// ExecInstance executes a given query on the given MySQL topology instance
func ExecInstance(instanceKey *InstanceKey, query string, args ...interface{}) (sql.Result, error) {
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return nil, err
	}
	return sqlutils.ExecNoPrepare(db, query, args...)
}

// ExecuteOnTopology will execute given function while maintaining concurrency limit
// on topology servers. It is safe in the sense that we will not leak tokens.
func ExecuteOnTopology(f func()) {
	topologyConcurrencyChan <- true
	defer func() { recover(); <-topologyConcurrencyChan }()
	f()
}

// ScanInstanceRow executes a read-a-single-row query on a given MySQL topology instance
func ScanInstanceRow(instanceKey *InstanceKey, query string, dest ...interface{}) error {
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return err
	}
	err = db.QueryRow(query).Scan(dest...)
	return err
}

// EmptyCommitInstance issues an empty COMMIT on a given instance
func EmptyCommitInstance(instanceKey *InstanceKey) error {
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return err
}

// RefreshTopologyInstance will synchronuously re-read topology instance
func RefreshTopologyInstance(instanceKey *InstanceKey) (*Instance, error) {
	_, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return nil, err
	}

	inst, found, err := ReadInstance(instanceKey)
	if err != nil || !found {
		return nil, err
	}

	return inst, nil
}

// RefreshTopologyInstances will do a blocking (though concurrent) refresh of all given instances
func RefreshTopologyInstances(instances [](*Instance)) {
	// use concurrency but wait for all to complete
	barrier := make(chan InstanceKey)
	for _, instance := range instances {
		instance := instance
		go func() {
			// Signal completed replica
			defer func() { barrier <- instance.Key }()
			// Wait your turn to read a replica
			ExecuteOnTopology(func() {
				log.Debugf("... reading instance: %+v", instance.Key)
				ReadTopologyInstance(&instance.Key)
			})
		}()
	}
	for range instances {
		<-barrier
	}
}

// GetReplicationRestartPreserveStatements returns a sequence of statements that make sure a replica is stopped
// and then returned to the same state. For example, if the replica was fully running, this will issue
// a STOP on both io_thread and sql_thread, followed by START on both. If one of them is not running
// at the time this function is called, said thread will be neither stopped nor started.
// The caller may provide an injected statememt, to be executed while the replica is stopped.
// This is useful for CHANGE MASTER TO commands, that unfortunately must take place while the replica
// is completely stopped.
func GetReplicationRestartPreserveStatements(instanceKey *InstanceKey, injectedStatement string) (statements []string, err error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return statements, err
	}
	if instance.ReplicationIOThreadRuning {
		statements = append(statements, SemicolonTerminated(`stop slave io_thread`))
	}
	if instance.ReplicationSQLThreadRuning {
		statements = append(statements, SemicolonTerminated(`stop slave sql_thread`))
	}
	if injectedStatement != "" {
		statements = append(statements, SemicolonTerminated(injectedStatement))
	}
	if instance.ReplicationSQLThreadRuning {
		statements = append(statements, SemicolonTerminated(`start slave sql_thread`))
	}
	if instance.ReplicationIOThreadRuning {
		statements = append(statements, SemicolonTerminated(`start slave io_thread`))
	}
	return statements, err
}

// FlushBinaryLogs attempts a 'FLUSH BINARY LOGS' statement on the given instance.
func FlushBinaryLogs(instanceKey *InstanceKey, count int) (*Instance, error) {
	if *config.RuntimeCLIFlags.Noop {
		return nil, fmt.Errorf("noop: aborting flush-binary-logs operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	for i := 0; i < count; i++ {
		_, err := ExecInstance(instanceKey, `flush binary logs`)
		if err != nil {
			return nil, log.Errore(err)
		}
	}

	log.Infof("flush-binary-logs count=%+v on %+v", count, *instanceKey)
	AuditOperation("flush-binary-logs", instanceKey, "success")

	return ReadTopologyInstance(instanceKey)
}

// FlushBinaryLogsTo attempts to 'FLUSH BINARY LOGS' until given binary log is reached
func FlushBinaryLogsTo(instanceKey *InstanceKey, logFile string) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	distance := instance.SelfBinlogCoordinates.FileNumberDistance(&BinlogCoordinates{LogFile: logFile})
	if distance < 0 {
		return nil, log.Errorf("FlushBinaryLogsTo: target log file %+v is smaller than current log file %+v", logFile, instance.SelfBinlogCoordinates.LogFile)
	}
	return FlushBinaryLogs(instanceKey, distance)
}

// purgeBinaryLogsTo attempts to 'PURGE BINARY LOGS' until given binary log is reached
func purgeBinaryLogsTo(instanceKey *InstanceKey, logFile string) (*Instance, error) {
	if *config.RuntimeCLIFlags.Noop {
		return nil, fmt.Errorf("noop: aborting purge-binary-logs operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	_, err := ExecInstance(instanceKey, "purge binary logs to ?", logFile)
	if err != nil {
		return nil, log.Errore(err)
	}

	log.Infof("purge-binary-logs to=%+v on %+v", logFile, *instanceKey)
	AuditOperation("purge-binary-logs", instanceKey, "success")

	return ReadTopologyInstance(instanceKey)
}

// TODO(sougou): implement count
func SetSemiSyncMaster(instanceKey *InstanceKey, enableMaster bool) error {
	if _, err := ExecInstance(instanceKey, `set global rpl_semi_sync_master_enabled = ?, global rpl_semi_sync_slave_enabled = ?`, enableMaster, false); err != nil {
		return log.Errore(err)
	}
	return nil
}

// TODO(sougou): This function may be used later for fixing semi-sync
func SetSemiSyncReplica(instanceKey *InstanceKey, enableReplica bool) error {
	if _, err := ExecInstance(instanceKey, `set global rpl_semi_sync_master_enabled = ?, global rpl_semi_sync_slave_enabled = ?`, false, enableReplica); err != nil {
		return log.Errore(err)
	}
	// Need to apply change by stopping starting IO thread
	ExecInstance(instanceKey, "stop slave io_thread")
	if _, err := ExecInstance(instanceKey, "start slave io_thread"); err != nil {
		return log.Errore(err)
	}
	return nil
}

func RestartReplicationQuick(instanceKey *InstanceKey) error {
	for _, cmd := range []string{`stop slave sql_thread`, `stop slave io_thread`, `start slave io_thread`, `start slave sql_thread`} {
		if _, err := ExecInstance(instanceKey, cmd); err != nil {
			return log.Errorf("%+v: RestartReplicationQuick: '%q' failed: %+v", *instanceKey, cmd, err)
		} else {
			log.Infof("%s on %+v as part of RestartReplicationQuick", cmd, *instanceKey)
		}
	}
	return nil
}

// StopReplicationNicely stops a replica such that SQL_thread and IO_thread are aligned (i.e.
// SQL_thread consumes all relay log entries)
// It will actually START the sql_thread even if the replica is completely stopped.
func StopReplicationNicely(instanceKey *InstanceKey, timeout time.Duration) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.ReplicationThreadsExist() {
		return instance, fmt.Errorf("instance is not a replica: %+v", instanceKey)
	}

	// stop io_thread, start sql_thread but catch any errors
	for _, cmd := range []string{`stop slave io_thread`, `start slave sql_thread`} {
		if _, err := ExecInstance(instanceKey, cmd); err != nil {
			return nil, log.Errorf("%+v: StopReplicationNicely: '%q' failed: %+v", *instanceKey, cmd, err)
		}
	}

	if instance.SQLDelay == 0 {
		// Otherwise we don't bother.
		if instance, err = WaitForSQLThreadUpToDate(instanceKey, timeout, 0); err != nil {
			return instance, err
		}
	}

	_, err = ExecInstance(instanceKey, `stop slave`)
	if err != nil {
		// Patch; current MaxScale behavior for STOP SLAVE is to throw an error if replica already stopped.
		if instance.isMaxScale() && err.Error() == "Error 1199: Slave connection is not running" {
			err = nil
		}
	}
	if err != nil {
		return instance, log.Errore(err)
	}

	instance, err = ReadTopologyInstance(instanceKey)
	log.Infof("Stopped replication nicely on %+v, Self:%+v, Exec:%+v", *instanceKey, instance.SelfBinlogCoordinates, instance.ExecBinlogCoordinates)
	return instance, err
}

func WaitForSQLThreadUpToDate(instanceKey *InstanceKey, overallTimeout time.Duration, staleCoordinatesTimeout time.Duration) (instance *Instance, err error) {
	// Otherwise we don't bother.
	var lastExecBinlogCoordinates BinlogCoordinates

	if overallTimeout == 0 {
		overallTimeout = 24 * time.Hour
	}
	if staleCoordinatesTimeout == 0 {
		staleCoordinatesTimeout = time.Duration(config.Config.ReasonableReplicationLagSeconds) * time.Second
	}
	generalTimer := time.NewTimer(overallTimeout)
	staleTimer := time.NewTimer(staleCoordinatesTimeout)
	for {
		instance, err := RetryInstanceFunction(func() (*Instance, error) {
			return ReadTopologyInstance(instanceKey)
		})
		if err != nil {
			return instance, log.Errore(err)
		}

		if instance.SQLThreadUpToDate() {
			// Woohoo
			return instance, nil
		}
		if instance.SQLDelay != 0 {
			return instance, log.Errorf("WaitForSQLThreadUpToDate: instance %+v has SQL Delay %+v. Operation is irrelevant", *instanceKey, instance.SQLDelay)
		}

		if !instance.ExecBinlogCoordinates.Equals(&lastExecBinlogCoordinates) {
			// means we managed to apply binlog events. We made progress...
			// so we reset the "staleness" timer
			if !staleTimer.Stop() {
				<-staleTimer.C
			}
			staleTimer.Reset(staleCoordinatesTimeout)
		}
		lastExecBinlogCoordinates = instance.ExecBinlogCoordinates

		select {
		case <-generalTimer.C:
			return instance, log.Errorf("WaitForSQLThreadUpToDate timeout on %+v after duration %+v", *instanceKey, overallTimeout)
		case <-staleTimer.C:
			return instance, log.Errorf("WaitForSQLThreadUpToDate stale coordinates timeout on %+v after duration %+v", *instanceKey, staleCoordinatesTimeout)
		default:
			log.Debugf("WaitForSQLThreadUpToDate waiting on %+v", *instanceKey)
			time.Sleep(retryInterval)
		}
	}
}

// StopReplicas will stop replication concurrently on given set of replicas.
// It will potentially do nothing, or attempt to stop _nicely_ or just stop normally, all according to stopReplicationMethod
func StopReplicas(replicas [](*Instance), stopReplicationMethod StopReplicationMethod, timeout time.Duration) [](*Instance) {
	if stopReplicationMethod == NoStopReplication {
		return replicas
	}
	refreshedReplicas := [](*Instance){}

	log.Debugf("Stopping %d replicas via %s", len(replicas), string(stopReplicationMethod))
	// use concurrency but wait for all to complete
	barrier := make(chan *Instance)
	for _, replica := range replicas {
		replica := replica
		go func() {
			updatedReplica := &replica
			// Signal completed replica
			defer func() { barrier <- *updatedReplica }()
			// Wait your turn to read a replica
			ExecuteOnTopology(func() {
				if stopReplicationMethod == StopReplicationNice {
					StopReplicationNicely(&replica.Key, timeout)
				}
				replica, _ = StopReplication(&replica.Key)
				updatedReplica = &replica
			})
		}()
	}
	for range replicas {
		refreshedReplicas = append(refreshedReplicas, <-barrier)
	}
	return refreshedReplicas
}

// StopReplicasNicely will attemt to stop all given replicas nicely, up to timeout
func StopReplicasNicely(replicas [](*Instance), timeout time.Duration) [](*Instance) {
	return StopReplicas(replicas, StopReplicationNice, timeout)
}

// StopReplication stops replication on a given instance
func StopReplication(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	_, err = ExecInstance(instanceKey, `stop slave`)
	if err != nil {
		// Patch; current MaxScale behavior for STOP SLAVE is to throw an error if replica already stopped.
		if instance.isMaxScale() && err.Error() == "Error 1199: Slave connection is not running" {
			err = nil
		}
	}
	if err != nil {
		return instance, log.Errore(err)
	}
	instance, err = ReadTopologyInstance(instanceKey)

	log.Infof("Stopped replication on %+v, Self:%+v, Exec:%+v", *instanceKey, instance.SelfBinlogCoordinates, instance.ExecBinlogCoordinates)
	return instance, err
}

// waitForReplicationState waits for both replication threads to be either running or not running, together.
// This is useful post- `start slave` operation, ensuring both threads are actually running,
// or post `stop slave` operation, ensuring both threads are not running.
func waitForReplicationState(instanceKey *InstanceKey, expectedState ReplicationThreadState) (expectationMet bool, err error) {
	waitDuration := time.Second
	waitInterval := 10 * time.Millisecond
	startTime := time.Now()

	for {
		// Since this is an incremental aggressive polling, it's OK if an occasional
		// error is observed. We don't bail out on a single error.
		if expectationMet, _ := expectReplicationThreadsState(instanceKey, expectedState); expectationMet {
			return true, nil
		}
		if time.Since(startTime)+waitInterval > waitDuration {
			break
		}
		time.Sleep(waitInterval)
		waitInterval = 2 * waitInterval
	}
	return false, nil
}

// StartReplication starts replication on a given instance.
func StartReplication(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.IsReplica() {
		return instance, fmt.Errorf("instance is not a replica: %+v", instanceKey)
	}

	_, err = ExecInstance(instanceKey, `start slave`)
	if err != nil {
		return instance, log.Errore(err)
	}
	log.Infof("Started replication on %+v", instanceKey)

	waitForReplicationState(instanceKey, ReplicationThreadStateRunning)

	instance, err = ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	if !instance.ReplicaRunning() {
		return instance, ReplicationNotRunningError
	}
	return instance, nil
}

// RestartReplication stops & starts replication on a given instance
func RestartReplication(instanceKey *InstanceKey) (instance *Instance, err error) {
	instance, err = StopReplication(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	instance, err = StartReplication(instanceKey)
	return instance, log.Errore(err)
}

// StartReplicas will do concurrent start-replica
func StartReplicas(replicas [](*Instance)) {
	// use concurrency but wait for all to complete
	log.Debugf("Starting %d replicas", len(replicas))
	barrier := make(chan InstanceKey)
	for _, instance := range replicas {
		instance := instance
		go func() {
			// Signal compelted replica
			defer func() { barrier <- instance.Key }()
			// Wait your turn to read a replica
			ExecuteOnTopology(func() { StartReplication(&instance.Key) })
		}()
	}
	for range replicas {
		<-barrier
	}
}

func WaitForExecBinlogCoordinatesToReach(instanceKey *InstanceKey, coordinates *BinlogCoordinates, maxWait time.Duration) (instance *Instance, exactMatch bool, err error) {
	startTime := time.Now()
	for {
		if maxWait != 0 && time.Since(startTime) > maxWait {
			return nil, exactMatch, fmt.Errorf("WaitForExecBinlogCoordinatesToReach: reached maxWait %+v on %+v", maxWait, *instanceKey)
		}
		instance, err = ReadTopologyInstance(instanceKey)
		if err != nil {
			return instance, exactMatch, log.Errore(err)
		}

		switch {
		case instance.ExecBinlogCoordinates.SmallerThan(coordinates):
			time.Sleep(retryInterval)
		case instance.ExecBinlogCoordinates.Equals(coordinates):
			return instance, true, nil
		case coordinates.SmallerThan(&instance.ExecBinlogCoordinates):
			return instance, false, nil
		}
	}
}

// StartReplicationUntilMasterCoordinates issuesa START SLAVE UNTIL... statement on given instance
func StartReplicationUntilMasterCoordinates(instanceKey *InstanceKey, masterCoordinates *BinlogCoordinates) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.IsReplica() {
		return instance, fmt.Errorf("instance is not a replica: %+v", instanceKey)
	}
	if !instance.ReplicationThreadsStopped() {
		return instance, fmt.Errorf("replication threads are not stopped: %+v", instanceKey)
	}

	log.Infof("Will start replication on %+v until coordinates: %+v", instanceKey, masterCoordinates)

	// MariaDB has a bug: a CHANGE MASTER TO statement does not work properly with prepared statement... :P
	// See https://mariadb.atlassian.net/browse/MDEV-7640
	// This is the reason for ExecInstance
	_, err = ExecInstance(instanceKey, "start slave until master_log_file=?, master_log_pos=?",
		masterCoordinates.LogFile, masterCoordinates.LogPos)
	if err != nil {
		return instance, log.Errore(err)
	}

	instance, exactMatch, err := WaitForExecBinlogCoordinatesToReach(instanceKey, masterCoordinates, 0)
	if err != nil {
		return instance, log.Errore(err)
	}
	if !exactMatch {
		return instance, fmt.Errorf("Start SLAVE UNTIL is past coordinates: %+v", instanceKey)
	}

	instance, err = StopReplication(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	return instance, err
}

// EnableMasterSSL issues CHANGE MASTER TO MASTER_SSL=1
func EnableMasterSSL(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if instance.ReplicationThreadsExist() && !instance.ReplicationThreadsStopped() {
		return instance, fmt.Errorf("EnableMasterSSL: Cannot enable SSL replication on %+v because replication threads are not stopped", *instanceKey)
	}
	log.Debugf("EnableMasterSSL: Will attempt enabling SSL replication on %+v", *instanceKey)

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting CHANGE MASTER TO MASTER_SSL=1 operation on %+v; signaling error but nothing went wrong.", *instanceKey)
	}
	_, err = ExecInstance(instanceKey, "change master to master_ssl=1")

	if err != nil {
		return instance, log.Errore(err)
	}

	log.Infof("EnableMasterSSL: Enabled SSL replication on %+v", *instanceKey)

	instance, err = ReadTopologyInstance(instanceKey)
	return instance, err
}

// See https://bugs.mysql.com/bug.php?id=83713
func workaroundBug83713(instanceKey *InstanceKey) {
	log.Debugf("workaroundBug83713: %+v", *instanceKey)
	queries := []string{
		`reset slave`,
		`start slave IO_THREAD`,
		`stop slave IO_THREAD`,
		`reset slave`,
	}
	for _, query := range queries {
		if _, err := ExecInstance(instanceKey, query); err != nil {
			log.Debugf("workaroundBug83713: error on %s: %+v", query, err)
		}
	}
}

// ChangeMasterTo changes the given instance's master according to given input.
// TODO(sougou): deprecate ReplicationCredentialsQuery, and all other credential discovery.
func ChangeMasterTo(instanceKey *InstanceKey, masterKey *InstanceKey, masterBinlogCoordinates *BinlogCoordinates, skipUnresolve bool, gtidHint OperationGTIDHint) (*Instance, error) {
	user, password := config.Config.MySQLReplicaUser, config.Config.MySQLReplicaPassword
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if instance.ReplicationThreadsExist() && !instance.ReplicationThreadsStopped() {
		return instance, fmt.Errorf("ChangeMasterTo: Cannot change master on: %+v because replication threads are not stopped", *instanceKey)
	}
	log.Debugf("ChangeMasterTo: will attempt changing master on %+v to %+v, %+v", *instanceKey, *masterKey, *masterBinlogCoordinates)
	changeToMasterKey := masterKey
	if !skipUnresolve {
		unresolvedMasterKey, nameUnresolved, err := UnresolveHostname(masterKey)
		if err != nil {
			log.Debugf("ChangeMasterTo: aborting operation on %+v due to resolving error on %+v: %+v", *instanceKey, *masterKey, err)
			return instance, err
		}
		if nameUnresolved {
			log.Debugf("ChangeMasterTo: Unresolved %+v into %+v", *masterKey, unresolvedMasterKey)
		}
		changeToMasterKey = &unresolvedMasterKey
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting CHANGE MASTER TO operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	originalMasterKey := instance.MasterKey
	originalExecBinlogCoordinates := instance.ExecBinlogCoordinates

	var changeMasterFunc func() error
	changedViaGTID := false
	if instance.UsingMariaDBGTID && gtidHint != GTIDHintDeny {
		// Keep on using GTID
		changeMasterFunc = func() error {
			_, err := ExecInstance(instanceKey, "change master to master_user=?, master_password=?, master_host=?, master_port=?",
				user, password, changeToMasterKey.Hostname, changeToMasterKey.Port)
			return err
		}
		changedViaGTID = true
	} else if instance.UsingMariaDBGTID && gtidHint == GTIDHintDeny {
		// Make sure to not use GTID
		changeMasterFunc = func() error {
			_, err = ExecInstance(instanceKey, "change master to master_user=?, master_password=?, master_host=?, master_port=?, master_log_file=?, master_log_pos=?, master_use_gtid=no",
				user, password, changeToMasterKey.Hostname, changeToMasterKey.Port, masterBinlogCoordinates.LogFile, masterBinlogCoordinates.LogPos)
			return err
		}
	} else if instance.IsMariaDB() && gtidHint == GTIDHintForce {
		// Is MariaDB; not using GTID, turn into GTID
		mariadbGTIDHint := "slave_pos"
		if !instance.ReplicationThreadsExist() {
			// This instance is currently a master. As per https://mariadb.com/kb/en/change-master-to/#master_use_gtid
			// we should be using current_pos.
			// See also:
			// - https://github.com/openark/orchestrator/issues/1146
			// - https://dba.stackexchange.com/a/234323
			mariadbGTIDHint = "current_pos"
		}
		changeMasterFunc = func() error {
			_, err = ExecInstance(instanceKey, fmt.Sprintf("change master to master_user=?, master_password=?, master_host=?, master_port=?, master_use_gtid=%s", mariadbGTIDHint),
				user, password, changeToMasterKey.Hostname, changeToMasterKey.Port)
			return err
		}
		changedViaGTID = true
	} else if instance.UsingOracleGTID && gtidHint != GTIDHintDeny {
		// Is Oracle; already uses GTID; keep using it.
		changeMasterFunc = func() error {
			_, err = ExecInstance(instanceKey, "change master to master_user=?, master_password=?, master_host=?, master_port=?",
				user, password, changeToMasterKey.Hostname, changeToMasterKey.Port)
			return err
		}
		changedViaGTID = true
	} else if instance.UsingOracleGTID && gtidHint == GTIDHintDeny {
		// Is Oracle; already uses GTID
		changeMasterFunc = func() error {
			_, err = ExecInstance(instanceKey, "change master to master_user=?, master_password=?, master_host=?, master_port=?, master_log_file=?, master_log_pos=?, master_auto_position=0",
				user, password, changeToMasterKey.Hostname, changeToMasterKey.Port, masterBinlogCoordinates.LogFile, masterBinlogCoordinates.LogPos)
			return err
		}
	} else if instance.SupportsOracleGTID && gtidHint == GTIDHintForce {
		// Is Oracle; not using GTID right now; turn into GTID
		changeMasterFunc = func() error {
			_, err = ExecInstance(instanceKey, "change master to master_user=?, master_password=?, master_host=?, master_port=?, master_auto_position=1",
				user, password, changeToMasterKey.Hostname, changeToMasterKey.Port)
			return err
		}
		changedViaGTID = true
	} else {
		// Normal binlog file:pos
		changeMasterFunc = func() error {
			_, err = ExecInstance(instanceKey, "change master to master_user=?, master_password=?, master_host=?, master_port=?, master_log_file=?, master_log_pos=?",
				user, password, changeToMasterKey.Hostname, changeToMasterKey.Port, masterBinlogCoordinates.LogFile, masterBinlogCoordinates.LogPos)
			return err
		}
	}
	err = changeMasterFunc()
	if err != nil && instance.UsingOracleGTID && strings.Contains(err.Error(), Error1201CouldnotInitializeMasterInfoStructure) {
		log.Debugf("ChangeMasterTo: got %+v", err)
		workaroundBug83713(instanceKey)
		err = changeMasterFunc()
	}
	if err != nil {
		return instance, log.Errore(err)
	}

	semiSync := ReplicaSemiSync(*masterKey, *instanceKey)
	if _, err := ExecInstance(instanceKey, `set global rpl_semi_sync_master_enabled = ?, global rpl_semi_sync_slave_enabled = ?`, false, semiSync); err != nil {
		return instance, log.Errore(err)
	}

	WriteMasterPositionEquivalence(&originalMasterKey, &originalExecBinlogCoordinates, changeToMasterKey, masterBinlogCoordinates)
	ResetInstanceRelaylogCoordinatesHistory(instanceKey)

	log.Infof("ChangeMasterTo: Changed master on %+v to: %+v, %+v. GTID: %+v", *instanceKey, masterKey, masterBinlogCoordinates, changedViaGTID)

	instance, err = ReadTopologyInstance(instanceKey)
	return instance, err
}

// SkipToNextBinaryLog changes master position to beginning of next binlog
// USE WITH CARE!
// Use case is binlog servers where the master was gone & replaced by another.
func SkipToNextBinaryLog(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	nextFileCoordinates, err := instance.ExecBinlogCoordinates.NextFileCoordinates()
	if err != nil {
		return instance, log.Errore(err)
	}
	nextFileCoordinates.LogPos = 4
	log.Debugf("Will skip replication on %+v to next binary log: %+v", instance.Key, nextFileCoordinates.LogFile)

	instance, err = ChangeMasterTo(&instance.Key, &instance.MasterKey, &nextFileCoordinates, false, GTIDHintNeutral)
	if err != nil {
		return instance, log.Errore(err)
	}
	AuditOperation("skip-binlog", instanceKey, fmt.Sprintf("Skipped replication to next binary log: %+v", nextFileCoordinates.LogFile))
	return StartReplication(instanceKey)
}

// ResetReplication resets a replica, breaking the replication
func ResetReplication(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if instance.ReplicationThreadsExist() && !instance.ReplicationThreadsStopped() {
		return instance, fmt.Errorf("Cannot reset replication on: %+v because replication threads are not stopped", instanceKey)
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting reset-replication operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	// MySQL's RESET SLAVE is done correctly; however SHOW SLAVE STATUS still returns old hostnames etc
	// and only resets till after next restart. This leads to orchestrator still thinking the instance replicates
	// from old host. We therefore forcibly modify the hostname.
	// RESET SLAVE ALL command solves this, but only as of 5.6.3
	_, err = ExecInstance(instanceKey, `change master to master_host='_'`)
	if err != nil {
		return instance, log.Errore(err)
	}
	_, err = ExecInstance(instanceKey, `reset slave /*!50603 all */`)
	if err != nil && strings.Contains(err.Error(), Error1201CouldnotInitializeMasterInfoStructure) {
		log.Debugf("ResetReplication: got %+v", err)
		workaroundBug83713(instanceKey)
		_, err = ExecInstance(instanceKey, `reset slave /*!50603 all */`)
	}
	if err != nil {
		return instance, log.Errore(err)
	}
	log.Infof("Reset replication %+v", instanceKey)

	instance, err = ReadTopologyInstance(instanceKey)
	return instance, err
}

// ResetMaster issues a RESET MASTER statement on given instance. Use with extreme care!
func ResetMaster(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if instance.ReplicationThreadsExist() && !instance.ReplicationThreadsStopped() {
		return instance, fmt.Errorf("Cannot reset master on: %+v because replication threads are not stopped", instanceKey)
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting reset-master operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	_, err = ExecInstance(instanceKey, `reset master`)
	if err != nil {
		return instance, log.Errore(err)
	}
	log.Infof("Reset master %+v", instanceKey)

	instance, err = ReadTopologyInstance(instanceKey)
	return instance, err
}

// skipQueryClassic skips a query in normal binlog file:pos replication
func setGTIDPurged(instance *Instance, gtidPurged string) error {
	if *config.RuntimeCLIFlags.Noop {
		return fmt.Errorf("noop: aborting set-gtid-purged operation on %+v; signalling error but nothing went wrong.", instance.Key)
	}

	_, err := ExecInstance(&instance.Key, `set global gtid_purged := ?`, gtidPurged)
	return err
}

// injectEmptyGTIDTransaction
func injectEmptyGTIDTransaction(instanceKey *InstanceKey, gtidEntry *OracleGtidSetEntry) error {
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Config.InstanceDBExecContextTimeoutSeconds)*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`SET GTID_NEXT="%s"`, gtidEntry.String())); err != nil {
		return err
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `SET GTID_NEXT="AUTOMATIC"`); err != nil {
		return err
	}
	return nil
}

// skipQueryClassic skips a query in normal binlog file:pos replication
func skipQueryClassic(instance *Instance) error {
	_, err := ExecInstance(&instance.Key, `set global sql_slave_skip_counter := 1`)
	return err
}

// skipQueryOracleGtid skips a single query in an Oracle GTID replicating replica, by injecting an empty transaction
func skipQueryOracleGtid(instance *Instance) error {
	nextGtid, err := instance.NextGTID()
	if err != nil {
		return err
	}
	if nextGtid == "" {
		return fmt.Errorf("Empty NextGTID() in skipQueryGtid() for %+v", instance.Key)
	}
	if _, err := ExecInstance(&instance.Key, `SET GTID_NEXT=?`, nextGtid); err != nil {
		return err
	}
	if err := EmptyCommitInstance(&instance.Key); err != nil {
		return err
	}
	if _, err := ExecInstance(&instance.Key, `SET GTID_NEXT='AUTOMATIC'`); err != nil {
		return err
	}
	return nil
}

// SkipQuery skip a single query in a failed replication instance
func SkipQuery(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.IsReplica() {
		return instance, fmt.Errorf("instance is not a replica: %+v", instanceKey)
	}
	if instance.ReplicationSQLThreadRuning {
		return instance, fmt.Errorf("Replication SQL thread is running on %+v", instanceKey)
	}
	if instance.LastSQLError == "" {
		return instance, fmt.Errorf("No SQL error on %+v", instanceKey)
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting skip-query operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	log.Debugf("Skipping one query on %+v", instanceKey)
	if instance.UsingOracleGTID {
		err = skipQueryOracleGtid(instance)
	} else if instance.UsingMariaDBGTID {
		return instance, log.Errorf("%+v is replicating with MariaDB GTID. To skip a query first disable GTID, then skip, then enable GTID again", *instanceKey)
	} else {
		err = skipQueryClassic(instance)
	}
	if err != nil {
		return instance, log.Errore(err)
	}
	AuditOperation("skip-query", instanceKey, "Skipped one query")
	return StartReplication(instanceKey)
}

// MasterPosWait issues a MASTER_POS_WAIT() an given instance according to given coordinates.
func MasterPosWait(instanceKey *InstanceKey, binlogCoordinates *BinlogCoordinates) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	_, err = ExecInstance(instanceKey, `select master_pos_wait(?, ?)`, binlogCoordinates.LogFile, binlogCoordinates.LogPos)
	if err != nil {
		return instance, log.Errore(err)
	}
	log.Infof("Instance %+v has reached coordinates: %+v", instanceKey, binlogCoordinates)

	instance, err = ReadTopologyInstance(instanceKey)
	return instance, err
}

// SetReadOnly sets or clears the instance's global read_only variable
func SetReadOnly(instanceKey *InstanceKey, readOnly bool) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting set-read-only operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	if _, err := ExecInstance(instanceKey, "set global read_only = ?", readOnly); err != nil {
		return instance, log.Errore(err)
	}
	if config.Config.UseSuperReadOnly {
		if _, err := ExecInstance(instanceKey, "set global super_read_only = ?", readOnly); err != nil {
			// We don't bail out here. super_read_only is only available on
			// MySQL 5.7.8 and Percona Server 5.6.21-70
			// At this time orchestrator does not verify whether a server supports super_read_only or not.
			// It makes a best effort to set it.
			log.Errore(err)
		}
	}
	instance, err = ReadTopologyInstance(instanceKey)

	log.Infof("instance %+v read_only: %t", instanceKey, readOnly)
	AuditOperation("read-only", instanceKey, fmt.Sprintf("set as %t", readOnly))

	return instance, err
}

// KillQuery stops replication on a given instance
func KillQuery(instanceKey *InstanceKey, process int64) (*Instance, error) {
	instance, err := ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting kill-query operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	_, err = ExecInstance(instanceKey, `kill query ?`, process)
	if err != nil {
		return instance, log.Errore(err)
	}

	instance, err = ReadTopologyInstance(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	log.Infof("Killed query on %+v", *instanceKey)
	AuditOperation("kill-query", instanceKey, fmt.Sprintf("Killed query %d", process))
	return instance, err
}

// injectPseudoGTID injects a Pseudo-GTID statement on a writable instance
func injectPseudoGTID(instance *Instance) (hint string, err error) {
	if *config.RuntimeCLIFlags.Noop {
		return hint, fmt.Errorf("noop: aborting inject-pseudo-gtid operation on %+v; signalling error but nothing went wrong.", instance.Key)
	}

	now := time.Now()
	randomHash := util.RandomHash()[0:16]
	hint = fmt.Sprintf("%.8x:%.8x:%s", now.Unix(), instance.ServerID, randomHash)
	query := fmt.Sprintf("drop view if exists `%s`.`_asc:%s`", config.PseudoGTIDSchema, hint)
	_, err = ExecInstance(&instance.Key, query)
	return hint, log.Errore(err)
}

// canInjectPseudoGTID checks orchestrator's grants to determine whether is has the
// privilege of auto-injecting pseudo-GTID
func canInjectPseudoGTID(instanceKey *InstanceKey) (canInject bool, err error) {
	if canInject, found := supportedAutoPseudoGTIDWriters.Get(instanceKey.StringCode()); found {
		return canInject.(bool), nil
	}
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return canInject, err
	}

	foundAll := false
	foundDropOnAll := false
	foundAllOnSchema := false
	foundDropOnSchema := false

	err = sqlutils.QueryRowsMap(db, `show grants for current_user()`, func(m sqlutils.RowMap) error {
		for _, grantData := range m {
			grant := grantData.String
			if strings.Contains(grant, `GRANT ALL PRIVILEGES ON *.*`) {
				foundAll = true
			}
			if strings.Contains(grant, `DROP`) && strings.Contains(grant, ` ON *.*`) {
				foundDropOnAll = true
			}
			if strings.Contains(grant, fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.*", config.PseudoGTIDSchema)) {
				foundAllOnSchema = true
			}
			if strings.Contains(grant, fmt.Sprintf(`GRANT ALL PRIVILEGES ON "%s".*`, config.PseudoGTIDSchema)) {
				foundAllOnSchema = true
			}
			if strings.Contains(grant, `DROP`) && strings.Contains(grant, fmt.Sprintf(" ON `%s`.*", config.PseudoGTIDSchema)) {
				foundDropOnSchema = true
			}
			if strings.Contains(grant, `DROP`) && strings.Contains(grant, fmt.Sprintf(` ON "%s".*`, config.PseudoGTIDSchema)) {
				foundDropOnSchema = true
			}
		}
		return nil
	})
	if err != nil {
		return canInject, err
	}

	canInject = foundAll || foundDropOnAll || foundAllOnSchema || foundDropOnSchema
	supportedAutoPseudoGTIDWriters.Set(instanceKey.StringCode(), canInject, cache.DefaultExpiration)

	return canInject, nil
}

// CheckAndInjectPseudoGTIDOnWriter checks whether pseudo-GTID can and
// should be injected on given instance, and if so, attempts to inject.
func CheckAndInjectPseudoGTIDOnWriter(instance *Instance) (injected bool, err error) {
	if instance == nil {
		return injected, log.Errorf("CheckAndInjectPseudoGTIDOnWriter: instance is nil")
	}
	if instance.ReadOnly {
		return injected, log.Errorf("CheckAndInjectPseudoGTIDOnWriter: instance is read-only: %+v", instance.Key)
	}
	if !instance.IsLastCheckValid {
		return injected, nil
	}
	canInject, err := canInjectPseudoGTID(&instance.Key)
	if err != nil {
		return injected, log.Errore(err)
	}
	if !canInject {
		if util.ClearToLog("CheckAndInjectPseudoGTIDOnWriter", instance.Key.StringCode()) {
			log.Warningf("AutoPseudoGTID enabled, but orchestrator has no priviliges on %+v to inject pseudo-gtid", instance.Key)
		}

		return injected, nil
	}
	if _, err := injectPseudoGTID(instance); err != nil {
		return injected, log.Errore(err)
	}
	injected = true
	if err := RegisterInjectedPseudoGTID(instance.ClusterName); err != nil {
		return injected, log.Errore(err)
	}
	return injected, nil
}

func GTIDSubtract(instanceKey *InstanceKey, gtidSet string, gtidSubset string) (gtidSubtract string, err error) {
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return gtidSubtract, err
	}
	err = db.QueryRow("select gtid_subtract(?, ?)", gtidSet, gtidSubset).Scan(&gtidSubtract)
	return gtidSubtract, err
}

func ShowMasterStatus(instanceKey *InstanceKey) (masterStatusFound bool, executedGtidSet string, err error) {
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return masterStatusFound, executedGtidSet, err
	}
	err = sqlutils.QueryRowsMap(db, "show master status", func(m sqlutils.RowMap) error {
		masterStatusFound = true
		executedGtidSet = m.GetStringD("Executed_Gtid_Set", "")
		return nil
	})
	return masterStatusFound, executedGtidSet, err
}

func ShowBinaryLogs(instanceKey *InstanceKey) (binlogs []string, err error) {
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return binlogs, err
	}
	err = sqlutils.QueryRowsMap(db, "show binary logs", func(m sqlutils.RowMap) error {
		binlogs = append(binlogs, m.GetString("Log_name"))
		return nil
	})
	return binlogs, err
}
