/*
Copyright 2019 The Vitess Authors.

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

/*
Handle creating replicas and setting up the replication streams.
*/

package mysqlctl

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/log"
)

// WaitForReplicationStart waits until the deadline for replication to start.
// This validates the current master is correct and can be connected to.
func WaitForReplicationStart(mysqld MysqlDaemon, replicaStartDeadline int) error {
	var rowMap map[string]string
	for replicaWait := 0; replicaWait < replicaStartDeadline; replicaWait++ {
		status, err := mysqld.ReplicationStatus()
		if err != nil {
			return err
		}

		if status.ReplicationRunning() {
			return nil
		}
		time.Sleep(time.Second)
	}

	errorKeys := []string{"Last_Error", "Last_IO_Error", "Last_SQL_Error"}
	errs := make([]string, 0, len(errorKeys))
	for _, key := range errorKeys {
		if rowMap[key] != "" {
			errs = append(errs, key+": "+rowMap[key])
		}
	}
	if len(errs) != 0 {
		return errors.New(strings.Join(errs, ", "))
	}
	return nil
}

// StartReplication starts replication.
func (mysqld *Mysqld) StartReplication(hookExtraEnv map[string]string) error {
	ctx := context.TODO()
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	if err := mysqld.executeSuperQueryListConn(ctx, conn, []string{conn.StartReplicationCommand()}); err != nil {
		return err
	}

	h := hook.NewSimpleHook("postflight_start_slave")
	h.ExtraEnv = hookExtraEnv
	return h.ExecuteOptional()
}

// StartReplicationUntilAfter starts replication until replication has come to `targetPos`, then it stops replication
func (mysqld *Mysqld) StartReplicationUntilAfter(ctx context.Context, targetPos mysql.Position) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	queries := []string{conn.StartReplicationUntilAfterCommand(targetPos)}

	return mysqld.executeSuperQueryListConn(ctx, conn, queries)
}

// StopReplication stops replication.
func (mysqld *Mysqld) StopReplication(hookExtraEnv map[string]string) error {
	h := hook.NewSimpleHook("preflight_stop_slave")
	h.ExtraEnv = hookExtraEnv
	if err := h.ExecuteOptional(); err != nil {
		return err
	}
	ctx := context.TODO()
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	return mysqld.executeSuperQueryListConn(ctx, conn, []string{conn.StopReplicationCommand()})
}

// StopIOThread stops a replica's IO thread only.
func (mysqld *Mysqld) StopIOThread(ctx context.Context) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	return mysqld.executeSuperQueryListConn(ctx, conn, []string{conn.StopIOThreadCommand()})
}

// RestartReplication stops, resets and starts replication.
func (mysqld *Mysqld) RestartReplication(hookExtraEnv map[string]string) error {
	h := hook.NewSimpleHook("preflight_stop_slave")
	h.ExtraEnv = hookExtraEnv
	if err := h.ExecuteOptional(); err != nil {
		return err
	}
	ctx := context.TODO()
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	if err := mysqld.executeSuperQueryListConn(ctx, conn, conn.RestartReplicationCommands()); err != nil {
		return err
	}

	h = hook.NewSimpleHook("postflight_start_slave")
	h.ExtraEnv = hookExtraEnv
	return h.ExecuteOptional()
}

// GetMysqlPort returns mysql port
func (mysqld *Mysqld) GetMysqlPort() (int32, error) {
	qr, err := mysqld.FetchSuperQuery(context.TODO(), "SHOW VARIABLES LIKE 'port'")
	if err != nil {
		return 0, err
	}
	if len(qr.Rows) != 1 {
		return 0, errors.New("no port variable in mysql")
	}
	utemp, err := evalengine.ToUint64(qr.Rows[0][1])
	if err != nil {
		return 0, err
	}
	return int32(utemp), nil
}

// IsReadOnly return true if the instance is read only
func (mysqld *Mysqld) IsReadOnly() (bool, error) {
	qr, err := mysqld.FetchSuperQuery(context.TODO(), "SHOW VARIABLES LIKE 'read_only'")
	if err != nil {
		return true, err
	}
	if len(qr.Rows) != 1 {
		return true, errors.New("no read_only variable in mysql")
	}
	if qr.Rows[0][1].ToString() == "ON" {
		return true, nil
	}
	return false, nil
}

// SetReadOnly set/unset the read_only flag
func (mysqld *Mysqld) SetReadOnly(on bool) error {
	query := "SET GLOBAL read_only = "
	if on {
		query += "ON"
	} else {
		query += "OFF"
	}
	return mysqld.ExecuteSuperQuery(context.TODO(), query)
}

var (
	// ErrNotMaster means there is no master status
	ErrNotMaster = errors.New("no master status")
)

// SetSuperReadOnly set/unset the super_read_only flag
func (mysqld *Mysqld) SetSuperReadOnly(on bool) error {
	query := "SET GLOBAL super_read_only = "
	if on {
		query += "ON"
	} else {
		query += "OFF"
	}
	return mysqld.ExecuteSuperQuery(context.TODO(), query)
}

// WaitMasterPos lets replicas wait to given replication position
func (mysqld *Mysqld) WaitMasterPos(ctx context.Context, targetPos mysql.Position) error {
	// Get a connection.
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	// First check if filePos flavored Position was passed in. If so, we can't defer to the flavor in the connection,
	// unless that flavor is also filePos.
	waitCommandName := "WaitUntilPositionCommand"
	var query string
	if targetPos.MatchesFlavor(mysql.FilePosFlavorID) {
		// If we are the master, WaitUntilFilePositionCommand will fail.
		// But position is most likely reached. So, check the position
		// first.
		mpos, err := conn.MasterFilePosition()
		if err != nil {
			return fmt.Errorf("WaitMasterPos: MasterFilePosition failed: %v", err)
		}
		if mpos.AtLeast(targetPos) {
			return nil
		}

		// Find the query to run, run it.
		query, err = conn.WaitUntilFilePositionCommand(ctx, targetPos)
		if err != nil {
			return err
		}
		waitCommandName = "WaitUntilFilePositionCommand"
	} else {
		// If we are the master, WaitUntilPositionCommand will fail.
		// But position is most likely reached. So, check the position
		// first.
		mpos, err := conn.MasterPosition()
		if err != nil {
			return fmt.Errorf("WaitMasterPos: MasterPosition failed: %v", err)
		}
		if mpos.AtLeast(targetPos) {
			return nil
		}

		// Find the query to run, run it.
		query, err = conn.WaitUntilPositionCommand(ctx, targetPos)
		if err != nil {
			return err
		}
	}

	qr, err := mysqld.FetchSuperQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("%v(%v) failed: %v", waitCommandName, query, err)
	}

	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return fmt.Errorf("unexpected result format from %v(%v): %#v", waitCommandName, query, qr)
	}
	result := qr.Rows[0][0]
	if result.IsNull() {
		return fmt.Errorf("%v(%v) failed: replication is probably stopped", waitCommandName, query)
	}
	if result.ToString() == "-1" {
		return fmt.Errorf("timed out waiting for position %v", targetPos)
	}
	return nil
}

// ReplicationStatus returns the server replication status
func (mysqld *Mysqld) ReplicationStatus() (mysql.ReplicationStatus, error) {
	conn, err := getPoolReconnect(context.TODO(), mysqld.dbaPool)
	if err != nil {
		return mysql.ReplicationStatus{}, err
	}
	defer conn.Recycle()

	return conn.ShowReplicationStatus()
}

// MasterStatus returns the master replication statuses
func (mysqld *Mysqld) MasterStatus(ctx context.Context) (mysql.MasterStatus, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return mysql.MasterStatus{}, err
	}
	defer conn.Recycle()

	return conn.ShowMasterStatus()
}

// MasterPosition returns the master replication position.
func (mysqld *Mysqld) MasterPosition() (mysql.Position, error) {
	conn, err := getPoolReconnect(context.TODO(), mysqld.dbaPool)
	if err != nil {
		return mysql.Position{}, err
	}
	defer conn.Recycle()

	return conn.MasterPosition()
}

// SetReplicationPosition sets the replication position at which the replica will resume
// when its replication is started.
func (mysqld *Mysqld) SetReplicationPosition(ctx context.Context, pos mysql.Position) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	cmds := conn.SetReplicationPositionCommands(pos)
	log.Infof("Executing commands to set replication position: %v", cmds)
	return mysqld.executeSuperQueryListConn(ctx, conn, cmds)
}

// SetMaster makes the provided host / port the master. It optionally
// stops replication before, and starts it after.
func (mysqld *Mysqld) SetMaster(ctx context.Context, masterHost string, masterPort int, replicationStopBefore bool, replicationStartAfter bool) error {
	params, err := mysqld.dbcfgs.ReplConnector().MysqlParams()
	if err != nil {
		return err
	}
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	cmds := []string{}
	if replicationStopBefore {
		cmds = append(cmds, conn.StopReplicationCommand())
	}
	smc := conn.SetMasterCommand(params, masterHost, masterPort, int(masterConnectRetry.Seconds()))
	cmds = append(cmds, smc)
	if replicationStartAfter {
		cmds = append(cmds, conn.StartReplicationCommand())
	}
	return mysqld.executeSuperQueryListConn(ctx, conn, cmds)
}

// ResetReplication resets all replication for this host.
func (mysqld *Mysqld) ResetReplication(ctx context.Context) error {
	conn, connErr := getPoolReconnect(ctx, mysqld.dbaPool)
	if connErr != nil {
		return connErr
	}
	defer conn.Recycle()

	cmds := conn.ResetReplicationCommands()
	return mysqld.executeSuperQueryListConn(ctx, conn, cmds)
}

// +------+---------+---------------------+------+-------------+------+----------------------------------------------------------------+------------------+
// | Id   | User    | Host                | db   | Command     | Time | State                                                          | Info             |
// +------+---------+---------------------+------+-------------+------+----------------------------------------------------------------+------------------+
// | 9792 | vt_repl | host:port           | NULL | Binlog Dump |   54 | Has sent all binlog to slave; waiting for binlog to be updated | NULL             |
// | 9797 | vt_dba  | localhost           | NULL | Query       |    0 | NULL                                                           | show processlist |
// +------+---------+---------------------+------+-------------+------+----------------------------------------------------------------+------------------+
//
// Array indices for the results of SHOW PROCESSLIST.
const (
	colConnectionID = iota //nolint
	colUsername            //nolint
	colClientAddr
	colDbName //nolint
	colCommand
)

const (
	// this is the command used by mysql replicas
	binlogDumpCommand = "Binlog Dump"
)

// FindReplicas gets IP addresses for all currently connected replicas.
func FindReplicas(mysqld MysqlDaemon) ([]string, error) {
	qr, err := mysqld.FetchSuperQuery(context.TODO(), "SHOW PROCESSLIST")
	if err != nil {
		return nil, err
	}
	addrs := make([]string, 0, 32)
	for _, row := range qr.Rows {
		// Check for prefix, since it could be "Binlog Dump GTID".
		if strings.HasPrefix(row[colCommand].ToString(), binlogDumpCommand) {
			host := row[colClientAddr].ToString()
			if host == "localhost" {
				// If we have a local binlog streamer, it will
				// show up as being connected
				// from 'localhost' through the local
				// socket. Ignore it.
				continue
			}
			host, _, err = netutil.SplitHostPort(host)
			if err != nil {
				return nil, fmt.Errorf("FindReplicas: malformed addr %v", err)
			}
			var ips []string
			ips, err = net.LookupHost(host)
			if err != nil {
				return nil, fmt.Errorf("FindReplicas: LookupHost failed %v", err)
			}
			addrs = append(addrs, ips...)
		}
	}

	return addrs, nil
}

// EnableBinlogPlayback prepares the server to play back events from a binlog stream.
// Whatever it does for a given flavor, it must be idempotent.
func (mysqld *Mysqld) EnableBinlogPlayback() error {
	// Get a connection.
	conn, err := getPoolReconnect(context.TODO(), mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	// See if we have a command to run, and run it.
	cmd := conn.EnableBinlogPlaybackCommand()
	if cmd == "" {
		return nil
	}
	if err := mysqld.ExecuteSuperQuery(context.TODO(), cmd); err != nil {
		log.Errorf("EnableBinlogPlayback: cannot run query '%v': %v", cmd, err)
		return fmt.Errorf("EnableBinlogPlayback: cannot run query '%v': %v", cmd, err)
	}

	log.Info("EnableBinlogPlayback: successfully ran %v", cmd)
	return nil
}

// DisableBinlogPlayback returns the server to the normal state after streaming.
// Whatever it does for a given flavor, it must be idempotent.
func (mysqld *Mysqld) DisableBinlogPlayback() error {
	// Get a connection.
	conn, err := getPoolReconnect(context.TODO(), mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	// See if we have a command to run, and run it.
	cmd := conn.DisableBinlogPlaybackCommand()
	if cmd == "" {
		return nil
	}
	if err := mysqld.ExecuteSuperQuery(context.TODO(), cmd); err != nil {
		log.Errorf("DisableBinlogPlayback: cannot run query '%v': %v", cmd, err)
		return fmt.Errorf("DisableBinlogPlayback: cannot run query '%v': %v", cmd, err)
	}

	log.Info("DisableBinlogPlayback: successfully ran '%v'", cmd)
	return nil
}

// SetSemiSyncEnabled enables or disables semi-sync replication for
// master and/or replica mode.
func (mysqld *Mysqld) SetSemiSyncEnabled(master, replica bool) error {
	log.Infof("Setting semi-sync mode: master=%v, replica=%v", master, replica)

	// Convert bool to int.
	var m, s int
	if master {
		m = 1
	}
	if replica {
		s = 1
	}

	err := mysqld.ExecuteSuperQuery(context.TODO(), fmt.Sprintf(
		"SET GLOBAL rpl_semi_sync_master_enabled = %v, GLOBAL rpl_semi_sync_slave_enabled = %v",
		m, s))
	if err != nil {
		return fmt.Errorf("can't set semi-sync mode: %v; make sure plugins are loaded in my.cnf", err)
	}
	return nil
}

// SemiSyncEnabled returns whether semi-sync is enabled for master or replica.
// If the semi-sync plugin is not loaded, we assume semi-sync is disabled.
func (mysqld *Mysqld) SemiSyncEnabled() (master, replica bool) {
	vars, err := mysqld.fetchVariables(context.TODO(), "rpl_semi_sync_%_enabled")
	if err != nil {
		return false, false
	}
	master = (vars["rpl_semi_sync_master_enabled"] == "ON")
	replica = (vars["rpl_semi_sync_slave_enabled"] == "ON")
	return master, replica
}

// SemiSyncReplicationStatus returns whether semi-sync is currently used by replication.
func (mysqld *Mysqld) SemiSyncReplicationStatus() (bool, error) {
	qr, err := mysqld.FetchSuperQuery(context.TODO(), "SHOW STATUS LIKE 'rpl_semi_sync_slave_status'")
	if err != nil {
		return false, err
	}
	if len(qr.Rows) != 1 {
		return false, errors.New("no rpl_semi_sync_slave_status variable in mysql")
	}
	if qr.Rows[0][1].ToString() == "ON" {
		return true, nil
	}
	return false, nil
}
