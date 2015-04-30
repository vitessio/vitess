// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Handle creating replicas and setting up the replication streams.
*/

package mysqlctl

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"text/template"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

const (
	// SlaveStartDeadline is the deadline for starting a slave
	SlaveStartDeadline = 30
)

var masterPasswordStart = "  MASTER_PASSWORD = '"
var masterPasswordEnd = "',\n"

func fillStringTemplate(tmpl string, vars interface{}) (string, error) {
	myTemplate := template.Must(template.New("").Parse(tmpl))
	data := new(bytes.Buffer)
	if err := myTemplate.Execute(data, vars); err != nil {
		return "", err
	}
	return data.String(), nil
}

func changeMasterArgs2(params *sqldb.ConnParams, masterHost string, masterPort int, masterConnectRetry int) []string {
	var args []string
	args = append(args, fmt.Sprintf("MASTER_HOST = '%s'", masterHost))
	args = append(args, fmt.Sprintf("MASTER_PORT = %d", masterPort))
	args = append(args, fmt.Sprintf("MASTER_USER = '%s'", params.Uname))
	args = append(args, fmt.Sprintf("MASTER_PASSWORD = '%s'", params.Pass))
	args = append(args, fmt.Sprintf("MASTER_CONNECT_RETRY = %d", masterConnectRetry))

	if mysql.SslEnabled(params) {
		args = append(args, "MASTER_SSL = 1")
	}
	if params.SslCa != "" {
		args = append(args, fmt.Sprintf("MASTER_SSL_CA = '%s'", params.SslCa))
	}
	if params.SslCaPath != "" {
		args = append(args, fmt.Sprintf("MASTER_SSL_CAPATH = '%s'", params.SslCaPath))
	}
	if params.SslCert != "" {
		args = append(args, fmt.Sprintf("MASTER_SSL_CERT = '%s'", params.SslCert))
	}
	if params.SslKey != "" {
		args = append(args, fmt.Sprintf("MASTER_SSL_KEY = '%s'", params.SslKey))
	}
	return args
}

func changeMasterArgs(params *sqldb.ConnParams, status *proto.ReplicationStatus) []string {
	return changeMasterArgs2(params, status.MasterHost, status.MasterPort, status.MasterConnectRetry)
}

// parseSlaveStatus parses the common fields of SHOW SLAVE STATUS.
func parseSlaveStatus(fields map[string]string) *proto.ReplicationStatus {
	status := &proto.ReplicationStatus{
		MasterHost:      fields["Master_Host"],
		SlaveIORunning:  fields["Slave_IO_Running"] == "Yes",
		SlaveSQLRunning: fields["Slave_SQL_Running"] == "Yes",
	}
	parseInt, _ := strconv.ParseInt(fields["Master_Port"], 10, 0)
	status.MasterPort = int(parseInt)
	parseInt, _ = strconv.ParseInt(fields["Connect_Retry"], 10, 0)
	status.MasterConnectRetry = int(parseInt)
	parseUint, _ := strconv.ParseUint(fields["Seconds_Behind_Master"], 10, 0)
	status.SecondsBehindMaster = uint(parseUint)
	return status
}

// WaitForSlaveStart waits a slave until given deadline passed
func (mysqld *Mysqld) WaitForSlaveStart(slaveStartDeadline int) error {
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

// StartSlave starts a slave
func (mysqld *Mysqld) StartSlave(hookExtraEnv map[string]string) error {
	if err := mysqld.ExecuteSuperQuery("START SLAVE"); err != nil {
		return err
	}

	h := hook.NewSimpleHook("postflight_start_slave")
	h.ExtraEnv = hookExtraEnv
	return h.ExecuteOptional()
}

// StopSlave stops a slave
func (mysqld *Mysqld) StopSlave(hookExtraEnv map[string]string) error {
	h := hook.NewSimpleHook("preflight_stop_slave")
	h.ExtraEnv = hookExtraEnv
	if err := h.ExecuteOptional(); err != nil {
		return err
	}

	return mysqld.ExecuteSuperQuery("STOP SLAVE")
}

// GetMasterAddr returns master address
func (mysqld *Mysqld) GetMasterAddr() (string, error) {
	slaveStatus, err := mysqld.SlaveStatus()
	if err != nil {
		return "", err
	}
	return slaveStatus.MasterAddr(), nil
}

// GetMysqlPort returns mysql port
func (mysqld *Mysqld) GetMysqlPort() (int, error) {
	qr, err := mysqld.fetchSuperQuery("SHOW VARIABLES LIKE 'port'")
	if err != nil {
		return 0, err
	}
	if len(qr.Rows) != 1 {
		return 0, errors.New("no port variable in mysql")
	}
	utemp, err := strconv.ParseUint(qr.Rows[0][1].String(), 10, 16)
	if err != nil {
		return 0, err
	}
	return int(utemp), nil
}

// IsReadOnly return true if the instance is read only
func (mysqld *Mysqld) IsReadOnly() (bool, error) {
	qr, err := mysqld.fetchSuperQuery("SHOW VARIABLES LIKE 'read_only'")
	if err != nil {
		return true, err
	}
	if len(qr.Rows) != 1 {
		return true, errors.New("no read_only variable in mysql")
	}
	if qr.Rows[0][1].String() == "ON" {
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
	return mysqld.ExecuteSuperQuery(query)
}

var (
	// ErrNotSlave means there is no slave status
	ErrNotSlave = errors.New("no slave status")
	// ErrNotMaster means there is no master status
	ErrNotMaster = errors.New("no master status")
)

// ReparentPosition returns a replication state that will reparent a slave to the
// correct master for a specified position.
func (mysqld *Mysqld) ReparentPosition(slavePosition proto.ReplicationPosition) (rs *proto.ReplicationStatus, waitPosition proto.ReplicationPosition, reparentTime int64, err error) {
	qr, err := mysqld.fetchSuperQuery(fmt.Sprintf("SELECT time_created_ns, new_addr, new_position, wait_position FROM _vt.reparent_log WHERE last_position = '%v'", slavePosition))
	if err != nil {
		return
	}
	if len(qr.Rows) != 1 {
		err = fmt.Errorf("no reparent for position: %v", slavePosition)
		return
	}

	reparentTime, err = qr.Rows[0][0].ParseInt64()
	if err != nil {
		err = fmt.Errorf("bad reparent time: %v %v %v", slavePosition, qr.Rows[0][0], err)
		return
	}

	rs, err = proto.NewReplicationStatus(qr.Rows[0][1].String())
	if err != nil {
		return
	}
	flavor, err := mysqld.flavor()
	if err != nil {
		err = fmt.Errorf("can't parse replication position: %v", err)
		return
	}
	rs.Position, err = flavor.ParseReplicationPosition(qr.Rows[0][2].String())
	if err != nil {
		return
	}

	waitPosition, err = flavor.ParseReplicationPosition(qr.Rows[0][3].String())
	if err != nil {
		return
	}
	return
}

// WaitMasterPos lets slaves wait to given replication position
func (mysqld *Mysqld) WaitMasterPos(targetPos proto.ReplicationPosition, waitTimeout time.Duration) error {
	flavor, err := mysqld.flavor()
	if err != nil {
		return fmt.Errorf("WaitMasterPos needs flavor: %v", err)
	}
	return flavor.WaitMasterPos(mysqld, targetPos, waitTimeout)
}

// SlaveStatus returns the slave replication statuses
func (mysqld *Mysqld) SlaveStatus() (*proto.ReplicationStatus, error) {
	flavor, err := mysqld.flavor()
	if err != nil {
		return nil, fmt.Errorf("SlaveStatus needs flavor: %v", err)
	}
	return flavor.SlaveStatus(mysqld)
}

// MasterPosition returns master replication position
func (mysqld *Mysqld) MasterPosition() (rp proto.ReplicationPosition, err error) {
	flavor, err := mysqld.flavor()
	if err != nil {
		return rp, fmt.Errorf("MasterPosition needs flavor: %v", err)
	}
	return flavor.MasterPosition(mysqld)
}

// StartReplicationCommands starts a replication
func (mysqld *Mysqld) StartReplicationCommands(status *proto.ReplicationStatus) ([]string, error) {
	flavor, err := mysqld.flavor()
	if err != nil {
		return nil, fmt.Errorf("StartReplicationCommands needs flavor: %v", err)
	}
	params, err := dbconfigs.MysqlParams(mysqld.replParams)
	if err != nil {
		return nil, err
	}
	return flavor.StartReplicationCommands(&params, status)
}

// SetMasterCommands returns the commands to run to make the provided
// host / port the master.
func (mysqld *Mysqld) SetMasterCommands(masterHost string, masterPort int, masterConnectRetry int) ([]string, error) {
	flavor, err := mysqld.flavor()
	if err != nil {
		return nil, fmt.Errorf("SetMasterCommands needs flavor: %v", err)
	}
	params, err := dbconfigs.MysqlParams(mysqld.replParams)
	if err != nil {
		return nil, err
	}
	return flavor.SetMasterCommands(&params, masterHost, masterPort, masterConnectRetry)
}

// WaitForSlave waits for a slave if its lag is larger than given maxLag
func (mysqld *Mysqld) WaitForSlave(maxLag int) (err error) {
	// FIXME(msolomon) verify that slave started based on show slave status;
	var rowMap map[string]string
	for {
		rowMap, err = mysqld.fetchSuperQueryMap("SHOW SLAVE STATUS")
		if err != nil {
			return
		}

		if rowMap["Seconds_Behind_Master"] == "NULL" {
			break
		} else {
			lag, err := strconv.Atoi(rowMap["Seconds_Behind_Master"])
			if err != nil {
				break
			}
			if lag < maxLag {
				return nil
			}
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
	return errors.New("replication stopped, it will never catch up")
}

// ResetReplicationCommands returns the commands to run to reset all
// replication for this host.
func (mysqld *Mysqld) ResetReplicationCommands() ([]string, error) {
	flavor, err := mysqld.flavor()
	if err != nil {
		return nil, fmt.Errorf("ResetReplicationCommands needs flavor: %v", err)
	}
	return flavor.ResetReplicationCommands(), nil
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
func (mysqld *Mysqld) FindSlaves() ([]string, error) {
	qr, err := mysqld.fetchSuperQuery("SHOW PROCESSLIST")
	if err != nil {
		return nil, err
	}
	addrs := make([]string, 0, 32)
	for _, row := range qr.Rows {
		if row[colCommand].String() == binlogDumpCommand {
			host, _, err := netutil.SplitHostPort(row[colClientAddr].String())
			if err != nil {
				return nil, fmt.Errorf("FindSlaves: malformed addr %v", err)
			}
			addrs = append(addrs, host)
		}
	}

	return addrs, nil
}

// ValidateSnapshotPath is a helper function to make sure we can write to the local snapshot area, before we actually do any action
// (can be used for both partial and full snapshots)
func (mysqld *Mysqld) ValidateSnapshotPath() error {
	_path := path.Join(mysqld.SnapshotDir, "validate_test")
	if err := os.RemoveAll(_path); err != nil {
		return fmt.Errorf("ValidateSnapshotPath: Cannot validate snapshot directory: %v", err)
	}
	if err := os.MkdirAll(_path, 0775); err != nil {
		return fmt.Errorf("ValidateSnapshotPath: Cannot validate snapshot directory: %v", err)
	}
	if err := os.RemoveAll(_path); err != nil {
		return fmt.Errorf("ValidateSnapshotPath: Cannot validate snapshot directory: %v", err)
	}
	return nil
}

// WaitBlpPosition will wait for the filtered replication to reach at least
// the provided position.
func (mysqld *Mysqld) WaitBlpPosition(bp *blproto.BlpPosition, waitTimeout time.Duration) error {
	timeOut := time.Now().Add(waitTimeout)
	for {
		if time.Now().After(timeOut) {
			break
		}

		cmd := binlogplayer.QueryBlpCheckpoint(bp.Uid)
		qr, err := mysqld.fetchSuperQuery(cmd)
		if err != nil {
			return err
		}
		if len(qr.Rows) != 1 {
			return fmt.Errorf("QueryBlpCheckpoint(%v) returned unexpected row count: %v", bp.Uid, len(qr.Rows))
		}
		var pos proto.ReplicationPosition
		if !qr.Rows[0][0].IsNull() {
			pos, err = proto.DecodeReplicationPosition(qr.Rows[0][0].String())
			if err != nil {
				return err
			}
		}
		if pos.AtLeast(bp.Position) {
			return nil
		}

		log.Infof("Sleeping 1 second waiting for binlog replication(%v) to catch up: %v != %v", bp.Uid, pos, bp.Position)
		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("WaitBlpPosition(%v) timed out", bp.Uid)
}

// EnableBinlogPlayback prepares the server to play back events from a binlog stream.
// Whatever it does for a given flavor, it must be idempotent.
func (mysqld *Mysqld) EnableBinlogPlayback() error {
	flavor, err := mysqld.flavor()
	if err != nil {
		return fmt.Errorf("EnableBinlogPlayback needs flavor: %v", err)
	}
	return flavor.EnableBinlogPlayback(mysqld)
}

// DisableBinlogPlayback returns the server to the normal state after streaming.
// Whatever it does for a given flavor, it must be idempotent.
func (mysqld *Mysqld) DisableBinlogPlayback() error {
	flavor, err := mysqld.flavor()
	if err != nil {
		return fmt.Errorf("DisableBinlogPlayback needs flavor: %v", err)
	}
	return flavor.DisableBinlogPlayback(mysqld)
}
