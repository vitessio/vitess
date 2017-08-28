/*
Copyright 2017 Google Inc.

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
	"strings"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/hook"
)

const (
	// SQLStartSlave is the SQl command issued to start MySQL replication
	SQLStartSlave = "START SLAVE"

	// SQLStopSlave is the SQl command issued to stop MySQL replication
	SQLStopSlave = "STOP SLAVE"
)

// WaitForSlaveStart waits until the deadline for replication to start.
// This validates the current master is correct and can be connected to.
func WaitForSlaveStart(mysqld MysqlDaemon, slaveStartDeadline int) error {
	var rowMap map[string]string
	for slaveWait := 0; slaveWait < slaveStartDeadline; slaveWait++ {
		status, err := mysqld.SlaveStatus()
		if err != nil {
			return err
		}

		if status.SlaveRunning() {
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

// StartSlave starts a slave on the provided MysqldDaemon
func StartSlave(md MysqlDaemon, hookExtraEnv map[string]string) error {
	if err := md.ExecuteSuperQueryList(context.TODO(), []string{SQLStartSlave}); err != nil {
		return err
	}

	h := hook.NewSimpleHook("postflight_start_slave")
	h.ExtraEnv = hookExtraEnv
	return h.ExecuteOptional()
}

// StopSlave stops a slave on the provided MysqldDaemon
func StopSlave(md MysqlDaemon, hookExtraEnv map[string]string) error {
	h := hook.NewSimpleHook("preflight_stop_slave")
	h.ExtraEnv = hookExtraEnv
	if err := h.ExecuteOptional(); err != nil {
		return err
	}

	return md.ExecuteSuperQueryList(context.TODO(), []string{SQLStopSlave})
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
	utemp, err := sqltypes.ToUint64(qr.Rows[0][1])
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

// WaitMasterPos lets slaves wait to given replication position
func (mysqld *Mysqld) WaitMasterPos(ctx context.Context, targetPos mysql.Position) error {
	// Get a connection.
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	// Find the query to run, run it.
	query, err := conn.WaitUntilPositionCommand(ctx, targetPos)
	if err != nil {
		return err
	}
	qr, err := mysqld.FetchSuperQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("WaitUntilPositionCommand(%v) failed: %v", query, err)
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return fmt.Errorf("unexpected result format from WaitUntilPositionCommand(%v): %#v", query, qr)
	}
	result := qr.Rows[0][0]
	if result.IsNull() {
		return fmt.Errorf("WaitUntilPositionCommand(%v) failed: gtid_mode is OFF", query)
	}
	if result.ToString() == "-1" {
		return fmt.Errorf("timed out waiting for position %v", targetPos)
	}
	return nil
}

// SlaveStatus returns the slave replication statuses
func (mysqld *Mysqld) SlaveStatus() (mysql.SlaveStatus, error) {
	conn, err := getPoolReconnect(context.TODO(), mysqld.dbaPool)
	if err != nil {
		return mysql.SlaveStatus{}, err
	}
	defer conn.Recycle()

	return conn.ShowSlaveStatus()
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

// SetSlavePosition sets the replication position at which the slave will resume
// when its replication is started.
func (mysqld *Mysqld) SetSlavePosition(ctx context.Context, pos mysql.Position) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	cmds := conn.SetSlavePositionCommands(pos)
	return mysqld.executeSuperQueryListConn(ctx, conn, cmds)
}

// SetMaster makes the provided host / port the master. It optionally
// stops replication before, and starts it after.
func (mysqld *Mysqld) SetMaster(ctx context.Context, masterHost string, masterPort int, slaveStopBefore bool, slaveStartAfter bool) error {
	params, err := dbconfigs.WithCredentials(&mysqld.dbcfgs.Repl)
	if err != nil {
		return err
	}
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	cmds := []string{}
	if slaveStopBefore {
		cmds = append(cmds, SQLStopSlave)
	}
	smc := conn.SetMasterCommand(&params, masterHost, masterPort, int(masterConnectRetry.Seconds()))
	cmds = append(cmds, smc)
	if slaveStartAfter {
		cmds = append(cmds, SQLStartSlave)
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
	colConnectionID = iota
	colUsername
	colClientAddr
	colDbName
	colCommand
)

const (
	// this is the command used by mysql slaves
	binlogDumpCommand = "Binlog Dump"
)

// FindSlaves gets IP addresses for all currently connected slaves.
func FindSlaves(mysqld MysqlDaemon) ([]string, error) {
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
				return nil, fmt.Errorf("FindSlaves: malformed addr %v", err)
			}
			addrs = append(addrs, host)
		}
	}

	return addrs, nil
}

// WaitBlpPosition will wait for the filtered replication to reach at least
// the provided position.
func WaitBlpPosition(ctx context.Context, mysqld MysqlDaemon, sql string, replicationPosition string) error {
	position, err := mysql.DecodePosition(replicationPosition)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		qr, err := mysqld.FetchSuperQuery(ctx, sql)
		if err != nil {
			return err
		}
		if len(qr.Rows) != 1 {
			return fmt.Errorf("QueryBlpCheckpoint(%v) returned unexpected row count: %v", sql, len(qr.Rows))
		}
		var pos mysql.Position
		if !qr.Rows[0][0].IsNull() {
			pos, err = mysql.DecodePosition(qr.Rows[0][0].ToString())
			if err != nil {
				return err
			}
		}
		if pos.AtLeast(position) {
			return nil
		}

		log.Infof("Sleeping 1 second waiting for binlog replication(%v) to catch up: %v != %v", sql, pos, position)
		time.Sleep(1 * time.Second)
	}
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
// master and/or slave mode.
func (mysqld *Mysqld) SetSemiSyncEnabled(master, slave bool) error {
	log.Infof("Setting semi-sync mode: master=%v, slave=%v", master, slave)

	// Convert bool to int.
	var m, s int
	if master {
		m = 1
	}
	if slave {
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

// SemiSyncEnabled returns whether semi-sync is enabled for master or slave.
// If the semi-sync plugin is not loaded, we assume semi-sync is disabled.
func (mysqld *Mysqld) SemiSyncEnabled() (master, slave bool) {
	vars, err := mysqld.fetchVariables(context.TODO(), "rpl_semi_sync_%_enabled")
	if err != nil {
		return false, false
	}
	master = (vars["rpl_semi_sync_master_enabled"] == "ON")
	slave = (vars["rpl_semi_sync_slave_enabled"] == "ON")
	return master, slave
}

// SemiSyncSlaveStatus returns whether semi-sync is currently used by replication.
func (mysqld *Mysqld) SemiSyncSlaveStatus() (bool, error) {
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
