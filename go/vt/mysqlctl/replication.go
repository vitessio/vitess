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
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"text/template"
	"time"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

const (
	SlaveStartDeadline = 30
)

var changeMasterCmd = `CHANGE MASTER TO
  MASTER_HOST = '{{.ReplicationState.MasterHost}}',
  MASTER_PORT = {{.ReplicationState.MasterPort}},
  MASTER_USER = '{{.MasterUser}}',
  MASTER_PASSWORD = '{{.MasterPassword}}',
  MASTER_LOG_FILE = '{{.ReplicationState.ReplicationPosition.MasterLogFile}}',
  MASTER_LOG_POS = {{.ReplicationState.ReplicationPosition.MasterLogPosition}},
  MASTER_CONNECT_RETRY = {{.ReplicationState.MasterConnectRetry}}
`

var masterPasswordStart = "  MASTER_PASSWORD = '"
var masterPasswordEnd = "',\n"

type newMasterData struct {
	ReplicationState *proto.ReplicationState
	MasterUser       string
	MasterPassword   string
}

func StartReplicationCommands(mysqld *Mysqld, replState *proto.ReplicationState) ([]string, error) {
	params, err := dbconfigs.MysqlParams(mysqld.replParams)
	if err != nil {
		return nil, err
	}
	nmd := &newMasterData{
		ReplicationState: replState,
		MasterUser:       params.Uname,
		MasterPassword:   params.Pass,
	}
	cmc, err := fillStringTemplate(changeMasterCmd, nmd)
	if err != nil {
		return nil, err
	}
	if params.SslEnabled() {
		cmc += ",\n  MASTER_SSL = 1"
	}
	if params.SslCa != "" {
		cmc += ",\n  MASTER_SSL_CA = '" + params.SslCa + "'"
	}
	if params.SslCaPath != "" {
		cmc += ",\n  MASTER_SSL_CAPATH = '" + params.SslCaPath + "'"
	}
	if params.SslCert != "" {
		cmc += ",\n  MASTER_SSL_CERT = '" + params.SslCert + "'"
	}
	if params.SslKey != "" {
		cmc += ",\n  MASTER_SSL_KEY = '" + params.SslKey + "'"
	}

	return []string{
		"STOP SLAVE",
		"RESET SLAVE",
		cmc,
		"START SLAVE"}, nil
}

func fillStringTemplate(tmpl string, vars interface{}) (string, error) {
	myTemplate := template.Must(template.New("").Parse(tmpl))
	data := new(bytes.Buffer)
	if err := myTemplate.Execute(data, vars); err != nil {
		return "", err
	}
	return data.String(), nil
}

func (mysqld *Mysqld) WaitForSlaveStart(slaveStartDeadline int) (err error) {
	var rowMap map[string]string
	for slaveWait := 0; slaveWait < slaveStartDeadline; slaveWait++ {
		rowMap, err = mysqld.slaveStatus()
		if err != nil {
			return
		}

		if rowMap["Slave_IO_Running"] == "Yes" && rowMap["Slave_SQL_Running"] == "Yes" {
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

func (mysqld *Mysqld) StartSlave(hookExtraEnv map[string]string) error {
	if err := mysqld.ExecuteSuperQuery("START SLAVE"); err != nil {
		return err
	}

	h := hook.NewSimpleHook("postflight_start_slave")
	h.ExtraEnv = hookExtraEnv
	return h.ExecuteOptional()
}

func (mysqld *Mysqld) StopSlave(hookExtraEnv map[string]string) error {
	h := hook.NewSimpleHook("preflight_stop_slave")
	h.ExtraEnv = hookExtraEnv
	if err := h.ExecuteOptional(); err != nil {
		return err
	}

	return mysqld.ExecuteSuperQuery("STOP SLAVE")
}

func (mysqld *Mysqld) GetMasterAddr() (string, error) {
	slaveStatus, err := mysqld.slaveStatus()
	if err != nil {
		return "", err
	}
	masterAddr := slaveStatus["Master_Host"] + ":" + slaveStatus["Master_Port"]
	return masterAddr, nil
}

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
	ErrNotSlave  = errors.New("no slave status")
	ErrNotMaster = errors.New("no master status")
)

func (mysqld *Mysqld) slaveStatus() (map[string]string, error) {
	qr, err := mysqld.fetchSuperQuery("SHOW SLAVE STATUS")
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 {
		return nil, ErrNotSlave
	}

	rowMap := make(map[string]string)
	for i, column := range qr.Rows[0] {
		if i >= len(showSlaveStatusColumnNames) {
			break
		}
		rowMap[showSlaveStatusColumnNames[i]] = column.String()
	}
	return rowMap, nil
}

// Return a replication state that will reparent a slave to the
// correct master for a specified position.
func (mysqld *Mysqld) ReparentPosition(slavePosition *proto.ReplicationPosition) (rs *proto.ReplicationState, waitPosition *proto.ReplicationPosition, reparentTime int64, err error) {
	qr, err := mysqld.fetchSuperQuery(fmt.Sprintf("SELECT time_created_ns, new_addr, new_position, wait_position FROM _vt.reparent_log WHERE last_position = '%v'", slavePosition.MapKey()))
	if err != nil {
		return
	}
	if len(qr.Rows) != 1 {
		err = fmt.Errorf("no reparent for position: %v", slavePosition.MapKey())
		return
	}

	reparentTime, err = qr.Rows[0][0].ParseInt64()
	if err != nil {
		err = fmt.Errorf("bad reparent time: %v %v %v", slavePosition.MapKey(), qr.Rows[0][0], err)
		return
	}

	file, pos, err := parseReplicationPosition(qr.Rows[0][2].String())
	if err != nil {
		return
	}
	rs, err = proto.NewReplicationState(qr.Rows[0][1].String())
	if err != nil {
		return
	}
	rs.ReplicationPosition.MasterLogFile = file
	rs.ReplicationPosition.MasterLogPosition = uint(pos)

	file, pos, err = parseReplicationPosition(qr.Rows[0][3].String())
	if err != nil {
		return
	}
	waitPosition = new(proto.ReplicationPosition)
	waitPosition.MasterLogFile = file
	waitPosition.MasterLogPosition = pos
	return
}

func parseReplicationPosition(rpos string) (filename string, pos uint, err error) {
	parts := strings.Split(rpos, ":")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("bad replication file position: %v", rpos)
	}
	_pos, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return "", 0, fmt.Errorf("bad replication file position: %v %v", rpos, err)
	}
	filename = parts[0]
	pos = uint(_pos)
	return
}

func (mysqld *Mysqld) WaitMasterPos(rp *proto.ReplicationPosition, waitTimeout time.Duration) error {
	var timeToWait int
	if waitTimeout > 0 {
		timeToWait = int(waitTimeout / time.Second)
		if timeToWait == 0 {
			timeToWait = 1
		}
	}
	cmd := fmt.Sprintf("SELECT MASTER_POS_WAIT('%v', %v, %v)",
		rp.MasterLogFile, rp.MasterLogPosition, timeToWait)
	qr, err := mysqld.fetchSuperQuery(cmd)
	if err != nil {
		return err
	}
	if len(qr.Rows) != 1 {
		return fmt.Errorf("WaitMasterPos returned unexpected row count: %v", len(qr.Rows))
	}
	if qr.Rows[0][0].IsNull() {
		return fmt.Errorf("WaitMasterPos failed: replication stopped")
	} else if qr.Rows[0][0].String() == "-1" {
		return fmt.Errorf("WaitMasterPos failed: timed out")
	}
	return nil
}

func (mysqld *Mysqld) WaitForMinimumReplicationPosition(targetGTID proto.GTID, waitTimeout time.Duration) (err error) {
	// TODO(enisoc): Use MySQL "wait for gtid" commands instead of comparing GTIDs.
	for remaining := waitTimeout; remaining > 0; remaining -= time.Second {
		pos, err := mysqld.SlaveStatus()
		if err != nil {
			return err
		}

		cmp, err := pos.MasterLogGTID.TryCompare(targetGTID)
		if err != nil {
			return err
		}
		if cmp >= 0 { // pos.MasterLogGTID >= targetGTID
			return nil
		}

		log.Infof("WaitForMinimumReplicationPosition got GTID %v, sleeping for 1s waiting for GTID %v", pos.MasterLogGTID, targetGTID)
		time.Sleep(time.Second)
	}
	return fmt.Errorf("timed out waiting for GTID %v", targetGTID)
}

func (mysqld *Mysqld) SlaveStatus() (*proto.ReplicationPosition, error) {
	fields, err := mysqld.slaveStatus()
	if err != nil {
		return nil, err
	}
	pos := new(proto.ReplicationPosition)
	// Use Relay_Master_Log_File for the SQL thread postion.
	pos.MasterLogFile = fields["Relay_Master_Log_File"]
	pos.MasterLogFileIo = fields["Master_Log_File"]
	temp, _ := strconv.ParseUint(fields["Exec_Master_Log_Pos"], 10, 0)
	pos.MasterLogPosition = uint(temp)
	temp, _ = strconv.ParseUint(fields["Read_Master_Log_Pos"], 10, 0)
	pos.MasterLogPositionIo = uint(temp)
	pos.MasterLogGTID.GTID, _ = mysqld.flavor.ParseGTID(fields["Exec_Master_Group_ID"])

	if fields["Slave_IO_Running"] == "Yes" && fields["Slave_SQL_Running"] == "Yes" {
		temp, _ = strconv.ParseUint(fields["Seconds_Behind_Master"], 10, 0)
		pos.SecondsBehindMaster = uint(temp)
	} else {
		// replications isn't running - report it as invalid since it won't resolve itself.
		pos.SecondsBehindMaster = proto.InvalidLagSeconds
	}
	return pos, nil
}

func (mysqld *Mysqld) MasterStatus() (rp *proto.ReplicationPosition, err error) {
	return mysqld.flavor.MasterStatus(mysqld)
}

/*
	mysql> show binlog info for 5\G
	*************************** 1. row ***************************
	Log_name: vt-0000041983-bin.000001
	Pos: 1194
	Server_ID: 41983
*/
func (mysqld *Mysqld) BinlogInfo(groupId int64) (rp *proto.ReplicationPosition, err error) {
	qr, err := mysqld.fetchSuperQuery(fmt.Sprintf("SHOW BINLOG INFO FOR %v", groupId))
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 {
		return nil, fmt.Errorf("no binlogs")
	}
	rp = &proto.ReplicationPosition{}
	rp.MasterLogFile = qr.Rows[0][0].String()
	temp, err := qr.Rows[0][1].ParseUint64()
	if err != nil {
		return nil, err
	}
	rp.MasterLogPosition = uint(temp)
	rp.MasterLogGTID.GTID, err = mysqld.flavor.ParseGTID(qr.Rows[0][1].String())
	if err != nil {
		return nil, err
	}

	// On the master, the SQL position and IO position are at
	// necessarily the same point.
	rp.MasterLogFileIo = rp.MasterLogFile
	rp.MasterLogPositionIo = rp.MasterLogPosition
	return rp, nil
}

func (mysqld *Mysqld) WaitForSlave(maxLag int) (err error) {
	// FIXME(msolomon) verify that slave started based on show slave status;
	var rowMap map[string]string
	for {
		rowMap, err = mysqld.slaveStatus()
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

/*
 Slave_IO_State: Waiting for master to send event
 Master_Host: voltron
 Master_User: vt_repl
 Master_Port: 6600
 Connect_Retry: 10
 Master_Log_File: vt-00000001-bin.000002
 Read_Master_Log_Pos: 106
 Relay_Log_File: vt-00000002-relay-bin.000003
 Relay_Log_Pos: 257
 Relay_Master_Log_File: vt-00000001-bin.000002
 Slave_IO_Running: Yes
 Slave_SQL_Running: Yes
 Replicate_Do_DB:
 Replicate_Ignore_DB:
 Replicate_Do_Table:
 Replicate_Ignore_Table:
 Replicate_Wild_Do_Table:
 Replicate_Wild_Ignore_Table:
 Last_Errno: 0
 Last_Error:
 Skip_Counter: 0
 Exec_Master_Log_Pos: 106
 Relay_Log_Space: 569
 Until_Condition: None
 Until_Log_File:
 Until_Log_Pos: 0
 Master_SSL_Allowed: No
 Master_SSL_CA_File:
 Master_SSL_CA_Path:
 Master_SSL_Cert:
 Master_SSL_Cipher:
 Master_SSL_Key:
 Seconds_Behind_Master: 0
 Master_SSL_Verify_Server_Cert: No
 Last_IO_Errno: 0
 Last_IO_Error:
 Last_SQL_Errno: 0
 Last_SQL_Error:
 Exec_Master_Group_ID: 14
 Connect_Using_Group_ID: No
*/
var showSlaveStatusColumnNames = []string{
	"Slave_IO_State",
	"Master_Host",
	"Master_User",
	"Master_Port",
	"Connect_Retry",
	"Master_Log_File",
	"Read_Master_Log_Pos",
	"Relay_Log_File",
	"Relay_Log_Pos",
	"Relay_Master_Log_File",
	"Slave_IO_Running",
	"Slave_SQL_Running",
	"Replicate_Do_DB",
	"Replicate_Ignore_DB",
	"Replicate_Do_Table",
	"Replicate_Ignore_Table",
	"Replicate_Wild_Do_Table",
	"Replicate_Wild_Ignore_Table",
	"Last_Errno",
	"Last_Error",
	"Skip_Counter",
	"Exec_Master_Log_Pos",
	"Relay_Log_Space",
	"Until_Condition",
	"Until_Log_File",
	"Until_Log_Pos",
	"Master_SSL_Allowed",
	"Master_SSL_CA_File",
	"Master_SSL_CA_Path",
	"Master_SSL_Cert",
	"Master_SSL_Cipher",
	"Master_SSL_Key",
	"Seconds_Behind_Master",
	"Master_SSL_Verify_Server_Cert",
	"Last_IO_Errno",
	"Last_IO_Error",
	"Last_SQL_Errno",
	"Last_SQL_Error",
	"Exec_Master_Group_ID",
	"Connect_Using_Group_ID",
}

func (mysqld *Mysqld) ExecuteSuperQuery(query string) error {
	return mysqld.ExecuteSuperQueryList([]string{query})
}

// FIXME(msolomon) should there be a query lock so we only
// run one admin action at a time?
func (mysqld *Mysqld) fetchSuperQuery(query string) (*mproto.QueryResult, error) {
	conn, connErr := mysqld.dbaPool.Get()
	if connErr != nil {
		return nil, connErr
	}
	defer conn.Recycle()
	log.V(6).Infof("fetch %v", query)
	qr, err := conn.ExecuteFetch(query, 10000, true)
	if err != nil {
		return nil, err
	}
	return qr, nil
}

func redactMasterPassword(input string) string {
	i := strings.Index(input, masterPasswordStart)
	if i == -1 {
		return input
	}
	j := strings.Index(input[i+len(masterPasswordStart):], masterPasswordEnd)
	if j == -1 {
		return input
	}
	return input[:i+len(masterPasswordStart)] + strings.Repeat("*", j) + input[i+len(masterPasswordStart)+j:]
}

// ExecuteSuperQueryList alows the user to execute queries at a super user.
func (mysqld *Mysqld) ExecuteSuperQueryList(queryList []string) error {
	conn, connErr := mysqld.dbaPool.Get()
	if connErr != nil {
		return connErr
	}
	defer conn.Recycle()
	for _, query := range queryList {
		log.Infof("exec %v", redactMasterPassword(query))
		if _, err := conn.ExecuteFetch(query, 10000, false); err != nil {
			return fmt.Errorf("ExecuteFetch(%v) failed: %v", redactMasterPassword(query), err.Error())
		}
	}
	return nil
}

// Force all slaves to error and stop. This is extreme, but helpful for emergencies
// and tests.
// Insert a row, block the propagation of its subsequent delete and reinsert it. This
// forces a failure on slaves only.
func (mysqld *Mysqld) BreakSlaves() error {
	now := time.Now().UnixNano()
	note := "force slave halt" // Any this is why we always leave a note...

	insertSql := fmt.Sprintf("INSERT INTO _vt.replication_log (time_created_ns, note) VALUES (%v, '%v')",
		now, note)
	deleteSql := fmt.Sprintf("DELETE FROM _vt.replication_log WHERE time_created_ns = %v", now)

	cmds := []string{
		insertSql,
		"SET sql_log_bin = 0",
		deleteSql,
		"SET sql_log_bin = 1",
		insertSql}

	return mysqld.ExecuteSuperQueryList(cmds)
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
	colConnectionId = iota
	colUsername
	colClientAddr
	colDbName
	colCommand
)

const (
	// this is the command used by mysql slaves
	binlogDumpCommand = "Binlog Dump"
)

// Get IP addresses for all currently connected slaves.
func (mysqld *Mysqld) FindSlaves() ([]string, error) {
	qr, err := mysqld.fetchSuperQuery("SHOW PROCESSLIST")
	if err != nil {
		return nil, err
	}
	addrs := make([]string, 0, 32)
	for _, row := range qr.Rows {
		if row[colCommand].String() == binlogDumpCommand {
			host, _, err := net.SplitHostPort(row[colClientAddr].String())
			if err != nil {
				return nil, fmt.Errorf("FindSlaves: malformed addr %v", err)
			}
			addrs = append(addrs, host)
		}
	}

	return addrs, nil
}

// Helper function to make sure we can write to the local snapshot area,
// before we actually do any action
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

// WaitBlpPos will wait for the filtered replication to reach at least
// the provided position.
func (mysqld *Mysqld) WaitBlpPos(bp *blproto.BlpPosition, waitTimeout time.Duration) error {
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
			return fmt.Errorf("WaitBlpPos(%v) returned unexpected row count: %v", bp.Uid, len(qr.Rows))
		}
		var gtid proto.GTID
		if !qr.Rows[0][0].IsNull() {
			gtid, err = mysqld.flavor.ParseGTID(qr.Rows[0][0].String())
			if err != nil {
				return err
			}
		}
		if gtid == bp.GTID {
			return nil
		}

		log.Infof("Sleeping 1 second waiting for binlog replication(%v) to catch up: %v != %v", bp.Uid, gtid, bp.GTID)
		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("WaitBlpPos(%v) timed out", bp.Uid)
}
