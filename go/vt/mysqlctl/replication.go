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

	"code.google.com/p/vitess/go/mysql/proto"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/key"
)

const (
	SlaveStartDeadline = 30
	InvalidLagSeconds  = 0xFFFFFFFF
)

type ReplicationPosition struct {
	MasterLogFile       string
	MasterLogPosition   uint
	MasterLogFileIo     string // how much has been read, but not applied
	MasterLogPositionIo uint
	SecondsBehindMaster uint
}

func (rp ReplicationPosition) MapKey() string {
	return fmt.Sprintf("%v:%d", rp.MasterLogFile, rp.MasterLogPosition)
}

func (rp ReplicationPosition) MapKeyIo() string {
	return fmt.Sprintf("%v:%d", rp.MasterLogFileIo, rp.MasterLogPositionIo)
}

type ReplicationState struct {
	// ReplicationPosition is not anonymous because the default json encoder has begun to fail here.
	ReplicationPosition ReplicationPosition
	MasterHost          string
	MasterPort          int
	MasterConnectRetry  int
}

func (rs ReplicationState) MasterAddr() string {
	return fmt.Sprintf("%v:%v", rs.MasterHost, rs.MasterPort)
}

func NewReplicationState(masterAddr string) (*ReplicationState, error) {
	addrPieces := strings.Split(masterAddr, ":")
	port, err := strconv.Atoi(addrPieces[1])
	if err != nil {
		return nil, err
	}
	return &ReplicationState{MasterConnectRetry: 10,
		MasterHost: addrPieces[0], MasterPort: port}, nil
}

var changeMasterCmd = `CHANGE MASTER TO
  MASTER_HOST = '{{.ReplicationState.MasterHost}}',
  MASTER_PORT = {{.ReplicationState.MasterPort}},
  MASTER_USER = '{{.MasterUser}}',
  MASTER_PASSWORD = '{{.MasterPassword}}',
  MASTER_LOG_FILE = '{{.ReplicationState.ReplicationPosition.MasterLogFile}}',
  MASTER_LOG_POS = {{.ReplicationState.ReplicationPosition.MasterLogPosition}},
  MASTER_CONNECT_RETRY = {{.ReplicationState.MasterConnectRetry}}
`

//  RELAY_LOG_FILE = '{{.ReplicationPosition.RelayLogFile}}',
//  RELAY_LOG_POS = {{.ReplicationPosition.RelayLogPosition}},

type newMasterData struct {
	ReplicationState *ReplicationState
	MasterUser       string
	MasterPassword   string
}

func StartReplicationCommands(mysqld *Mysqld, replState *ReplicationState) ([]string, error) {
	nmd := &newMasterData{ReplicationState: replState, MasterUser: mysqld.replParams.Uname, MasterPassword: mysqld.replParams.Pass}
	cmc, err := fillStringTemplate(changeMasterCmd, nmd)
	if err != nil {
		return nil, err
	}
	if mysqld.replParams.SslEnabled() {
		cmc += ",\n  MASTER_SSL = 1"
	}
	if mysqld.replParams.SslCa != "" {
		cmc += ",\n  MASTER_SSL_CA = '" + mysqld.replParams.SslCa + "'"
	}
	if mysqld.replParams.SslCaPath != "" {
		cmc += ",\n  MASTER_SSL_CAPATH = '" + mysqld.replParams.SslCaPath + "'"
	}
	if mysqld.replParams.SslCert != "" {
		cmc += ",\n  MASTER_SSL_CERT = '" + mysqld.replParams.SslCert + "'"
	}
	if mysqld.replParams.SslKey != "" {
		cmc += ",\n  MASTER_SSL_KEY = '" + mysqld.replParams.SslKey + "'"
	}

	return []string{
		"STOP SLAVE",
		"RESET SLAVE",
		cmc,
		"START SLAVE"}, nil
}

func StartSplitReplicationCommands(mysqld *Mysqld, replState *ReplicationState, keyRange key.KeyRange) ([]string, error) {
	nmd := &newMasterData{ReplicationState: replState, MasterUser: mysqld.replParams.Uname, MasterPassword: mysqld.replParams.Pass}
	startKey := string(keyRange.Start.Hex())
	endKey := string(keyRange.End.Hex())
	cmc, err := fillStringTemplate(changeMasterCmd, nmd)
	if err != nil {
		return nil, err
	}
	return []string{
		"SET GLOBAL vt_enable_binlog_splitter_rbr = 1",
		"SET GLOBAL vt_shard_key_range_start = \"" + startKey + "\"",
		"SET GLOBAL vt_shard_key_range_end = \"" + endKey + "\"",
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

func (mysqld *Mysqld) StartSlave() error {
	return mysqld.executeSuperQuery("SLAVE START")
}

func (mysqld *Mysqld) StopSlave() error {
	return mysqld.executeSuperQuery("SLAVE STOP")
}

func (mysqld *Mysqld) GetMasterAddr() (string, error) {
	slaveStatus, err := mysqld.slaveStatus()
	if err != nil {
		return "", err
	}
	masterAddr := slaveStatus["Master_Host"] + ":" + slaveStatus["Master_Port"]
	return masterAddr, nil
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
	return mysqld.executeSuperQuery(query)
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
func (mysqld *Mysqld) ReparentPosition(slavePosition *ReplicationPosition) (rs *ReplicationState, waitPosition *ReplicationPosition, reparentTime int64, err error) {
	qr, err := mysqld.fetchSuperQuery(fmt.Sprintf("SELECT time_created_ns, new_addr, new_position, wait_position FROM _vt.reparent_log WHERE last_position = '%v'", slavePosition.MapKey()))
	if err != nil {
		return
	}
	if len(qr.Rows) != 1 {
		err = fmt.Errorf("no reparent for position: %v", slavePosition.MapKey())
		return
	}

	reparentTime, err = strconv.ParseInt(qr.Rows[0][0].String(), 10, 64)
	if err != nil {
		err = fmt.Errorf("bad reparent time: %v %v %v", slavePosition.MapKey(), qr.Rows[0][0], err)
		return
	}

	file, pos, err := parseReplicationPosition(qr.Rows[0][2].String())
	if err != nil {
		return
	}
	rs, err = NewReplicationState(qr.Rows[0][1].String())
	if err != nil {
		return
	}
	rs.ReplicationPosition.MasterLogFile = file
	rs.ReplicationPosition.MasterLogPosition = uint(pos)

	file, pos, err = parseReplicationPosition(qr.Rows[0][3].String())
	if err != nil {
		return
	}
	waitPosition = new(ReplicationPosition)
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

func (mysqld *Mysqld) WaitMasterPos(rp *ReplicationPosition, waitTimeout int) error {
	cmd := fmt.Sprintf("SELECT MASTER_POS_WAIT('%v', %v, %v)",
		rp.MasterLogFile, rp.MasterLogPosition, waitTimeout)
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

func (mysqld *Mysqld) SlaveStatus() (*ReplicationPosition, error) {
	fields, err := mysqld.slaveStatus()
	if err != nil {
		return nil, err
	}
	pos := new(ReplicationPosition)
	// Use Relay_Master_Log_File for the SQL thread postion.
	pos.MasterLogFile = fields["Relay_Master_Log_File"]
	pos.MasterLogFileIo = fields["Master_Log_File"]
	temp, _ := strconv.ParseUint(fields["Exec_Master_Log_Pos"], 10, 0)
	pos.MasterLogPosition = uint(temp)
	temp, _ = strconv.ParseUint(fields["Read_Master_Log_Pos"], 10, 0)
	pos.MasterLogPositionIo = uint(temp)

	if fields["Slave_IO_Running"] == "Yes" && fields["Slave_SQL_Running"] == "Yes" {
		temp, _ = strconv.ParseUint(fields["Seconds_Behind_Master"], 10, 0)
		pos.SecondsBehindMaster = uint(temp)
	} else {
		// replications isn't running - report it as invalid since it won't resolve itself.
		pos.SecondsBehindMaster = InvalidLagSeconds
	}
	return pos, nil
}

/*
 mysql> show master status\G
 **************************** 1. row ***************************
 File: vt-000001c6-bin.000003
 Position: 106
 Binlog_Do_DB:
 Binlog_Ignore_DB:
*/
func (mysqld *Mysqld) MasterStatus() (rp *ReplicationPosition, err error) {
	qr, err := mysqld.fetchSuperQuery("SHOW MASTER STATUS")
	if err != nil {
		return
	}
	if len(qr.Rows) != 1 {
		return nil, ErrNotMaster
	}
	rp = &ReplicationPosition{}
	rp.MasterLogFile = qr.Rows[0][0].String()
	temp, err := strconv.ParseUint(qr.Rows[0][1].String(), 10, 0)
	rp.MasterLogPosition = uint(temp)
	// On the master, the SQL position and IO position are at
	// necessarily the same point.
	rp.MasterLogFileIo = rp.MasterLogFile
	rp.MasterLogPositionIo = rp.MasterLogPosition
	return
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
}

func (mysqld *Mysqld) executeSuperQuery(query string) error {
	return mysqld.executeSuperQueryList([]string{query})
}

// FIXME(msolomon) should there be a query lock so we only
// run one admin action at a time?
func (mysqld *Mysqld) fetchSuperQuery(query string) (*proto.QueryResult, error) {
	conn, connErr := mysqld.createConnection()
	if connErr != nil {
		return nil, connErr
	}
	defer conn.Close()
	relog.Info("fetch %v", query)
	qr, err := conn.ExecuteFetch(query, 10000, true)
	if err != nil {
		return nil, err
	}
	return qr, nil
}

func (mysqld *Mysqld) executeSuperQueryList(queryList []string) error {
	conn, connErr := mysqld.createConnection()
	if connErr != nil {
		return connErr
	}
	defer conn.Close()
	for _, query := range queryList {
		toLog := strings.Replace(query, mysqld.replParams.Pass, strings.Repeat("*", len(mysqld.replParams.Pass)), -1)
		relog.Info("exec %v", toLog)
		if _, err := conn.ExecuteFetch(query, 10000, false); err != nil {
			return fmt.Errorf("ExecuteFetch(%v) failed: %v", query, err.Error())
		}
	}
	return nil
}

func (mysqld *Mysqld) ConfigureKeyRange(startKey, endKey key.HexKeyspaceId) error {
	replicationCmds := []string{
		"SET GLOBAL vt_enable_binlog_splitter_rbr = 1",
		"SET GLOBAL vt_shard_key_range_start = \"0x" + string(startKey) + "\"",
		"SET GLOBAL vt_shard_key_range_end = \"0x" + string(endKey) + "\""}
	if err := mysqld.executeSuperQueryList(replicationCmds); err != nil {
		return err
	}
	return nil
}

func (mysqld *Mysqld) ResetKeyRange() error {
	replicationCmds := []string{
		"SET GLOBAL vt_shard_key_range_start = \"\"",
		"SET GLOBAL vt_shard_key_range_end = \"\""}
	if err := mysqld.executeSuperQueryList(replicationCmds); err != nil {
		return err
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

	return mysqld.executeSuperQueryList(cmds)
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
	colDbname
	colCommand
)

const (
	binlogDumpCommand = "Binlog Dump"
)

// Get IP addresses for all currently connected slaves.
// FIXME(msolomon) use command instead of user to find "rogue" slaves?
func (mysqld *Mysqld) FindSlaves() ([]string, error) {
	qr, err := mysqld.fetchSuperQuery("SHOW PROCESSLIST")
	if err != nil {
		return nil, err
	}
	addrs := make([]string, 0, 32)
	for _, row := range qr.Rows {
		if row[colUsername].String() == mysqld.replParams.Uname {
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
