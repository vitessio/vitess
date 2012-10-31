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
	"io/ioutil"
	"net"
	"path"
	"strconv"
	"strings"
	"text/template"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/key"
)

const (
	SlaveStartDeadline = 30
)

type ReplicationPosition struct {
	MasterLogFile       string
	MasterLogPosition   uint
	MasterLogFileIo     string // how much has been read, but not applied
	MasterLogPositionIo uint
}

func (rp ReplicationPosition) MapKey() string {
	return fmt.Sprintf("%v:%d", rp.MasterLogFile, rp.MasterLogPosition)
}

type ReplicationState struct {
	// ReplicationPosition is not anonymous because the default json encoder has begun to fail here.
	ReplicationPosition ReplicationPosition
	MasterHost          string
	MasterPort          int
	MasterUser          string
	MasterPassword      string
	MasterConnectRetry  int
}

func (rs ReplicationState) MasterAddr() string {
	return fmt.Sprintf("%v:%v", rs.MasterHost, rs.MasterPort)
}

func NewReplicationState(masterAddr, user, passwd string) *ReplicationState {
	addrPieces := strings.Split(masterAddr, ":")
	port, err := strconv.Atoi(addrPieces[1])
	if err != nil {
		panic(err)
	}
	return &ReplicationState{MasterUser: user, MasterPassword: passwd, MasterConnectRetry: 10,
		MasterHost: addrPieces[0], MasterPort: port}
}

type SnapshotFile struct {
	Path string
	Hash string // md5 sum of the compressed file
}

// This function returns the local file used to store the SnapshotFile,
// relative to the basePath.
// for instance, if the source path is something like:
// /vt/snapshot/vt_0000062344/data/vt_snapshot_test-MA,Mw/vt_insert_test.csv.gz
// we will get everything starting with 'data/...', append it to basepath,
// and remove the .gz extension. So with basePath=myPath, it will return:
// myPath/data/vt_snapshot_test-MA,Mw/vt_insert_test.csv
func (dataFile *SnapshotFile) getLocalFilename(basePath string) string {
	relativePath := strings.SplitN(dataFile.Path, "/", 5)[4]
	filename := path.Join(basePath, relativePath)
	// trim compression extension
	filename = filename[:len(filename)-len(path.Ext(filename))]
	return filename
}

type ReplicaSource struct {
	Addr string // this is the address of the tabletserver, not mysql

	DbName string
	Files  []SnapshotFile

	// ReplicationState is not anonymous because the default json encoder has begun to fail here.
	ReplicationState *ReplicationState
}

func NewReplicaSource(addr, mysqlAddr, user, passwd, dbName string, files []SnapshotFile, pos *ReplicationPosition) *ReplicaSource {
	rs := &ReplicaSource{
		Addr:             addr,
		DbName:           dbName,
		Files:            files,
		ReplicationState: NewReplicationState(mysqlAddr, user, passwd)}
	rs.ReplicationState.ReplicationPosition = *pos
	return rs
}

var changeMasterCmd = `CHANGE MASTER TO
  MASTER_HOST = '{{.MasterHost}}',
  MASTER_PORT = {{.MasterPort}},
  MASTER_USER = '{{.MasterUser}}',
  MASTER_PASSWORD = '{{.MasterPassword}}',
  MASTER_LOG_FILE = '{{.ReplicationPosition.MasterLogFile}}',
  MASTER_LOG_POS = {{.ReplicationPosition.MasterLogPosition}},
  MASTER_CONNECT_RETRY = {{.MasterConnectRetry}}`

func StartReplicationCommands(replState *ReplicationState) []string {
	return []string{
		"RESET SLAVE",
		mustFillStringTemplate(changeMasterCmd, replState),
		"START SLAVE"}
}

// Read replication state from local files.
func ReadReplicationState(mysqld *Mysqld) (*ReplicationState, error) {
	relayInfo, err := ioutil.ReadFile(mysqld.config.RelayLogInfoPath)
	if err != nil {
		return nil, err
	}
	// FIXME(msolomon) not sure i'll need this data
	masterInfo, err := ioutil.ReadFile(mysqld.config.MasterInfoFile)
	if err != nil {
		return nil, err
	}
	relayParts := strings.Split(string(relayInfo), " ")
	masterParts := strings.Split(string(masterInfo), " ")
	// FIXME(msolomon) does the file supply port?
	addr := fmt.Sprintf("%v:%v", masterParts[3], 3306)
	replState := NewReplicationState(addr, mysqld.replParams.Uname, mysqld.replParams.Pass)
	replState.ReplicationPosition.MasterLogFile = relayParts[2]
	temp, _ := strconv.ParseUint(relayParts[3], 10, 0)
	replState.ReplicationPosition.MasterLogPosition = uint(temp)
	replState.MasterUser = masterParts[4]
	replState.MasterPassword = masterParts[5]
	return replState, nil
}

func mustFillStringTemplate(tmpl string, vars interface{}) string {
	myTemplate := template.Must(template.New("").Parse(tmpl))
	data := new(bytes.Buffer)
	if err := myTemplate.Execute(data, vars); err != nil {
		panic(err)
	}
	return data.String()
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
		time.Sleep(1e9)
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
	rows, err := mysqld.fetchSuperQuery("SHOW VARIABLES LIKE 'read_only'")
	if err != nil {
		return true, err
	}
	if len(rows) != 1 {
		return true, errors.New("no read_only variable in mysql")
	}
	if rows[0][1].(string) == "ON" {
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

var ErrNotSlave = errors.New("no slave status")

func (mysqld *Mysqld) slaveStatus() (map[string]string, error) {
	rows, err := mysqld.fetchSuperQuery("SHOW SLAVE STATUS")
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, ErrNotSlave
	}

	rowMap := make(map[string]string)
	for i, column := range rows[0] {
		if i >= len(showSlaveStatusColumnNames) {
			break
		}
		if column == nil {
			rowMap[showSlaveStatusColumnNames[i]] = ""
		} else {
			rowMap[showSlaveStatusColumnNames[i]] = column.(string)
		}
	}
	return rowMap, nil
}

func (mysqld *Mysqld) WaitMasterPos(rp *ReplicationPosition, waitTimeout int) error {
	cmd := fmt.Sprintf("SELECT MASTER_POS_WAIT('%v', %v, %v)",
		rp.MasterLogFile, rp.MasterLogPosition, waitTimeout)
	rows, err := mysqld.fetchSuperQuery(cmd)
	if err != nil {
		return err
	}
	if len(rows) != 1 {
		return fmt.Errorf("WaitMasterPos returned unexpected row count: %v", len(rows))
	}
	if rows[0][0] == nil {
		return fmt.Errorf("WaitMasterPos failed: replication stopped")
	} else if rows[0][0].(string) == "-1" {
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
	// Use Relay_Master_Log_File because we care where the SQL thread
	// is, not the IO thread.
	pos.MasterLogFile = fields["Relay_Master_Log_File"]
	pos.MasterLogFileIo = fields["Master_Log_File"]
	temp, _ := strconv.ParseUint(fields["Exec_Master_Log_Pos"], 10, 0)
	pos.MasterLogPosition = uint(temp)
	temp, _ = strconv.ParseUint(fields["Read_Master_Log_Pos"], 10, 0)
	pos.MasterLogPositionIo = uint(temp)
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
func (mysqld *Mysqld) MasterStatus() (position *ReplicationPosition, err error) {
	rows, err := mysqld.fetchSuperQuery("SHOW MASTER STATUS")
	if err != nil {
		return
	}
	if len(rows) != 1 {
		err = errors.New("unexpected result for show master status")
		return
	}
	position = &ReplicationPosition{}
	position.MasterLogFile = rows[0][0].(string)
	temp, err := strconv.ParseUint(rows[0][1].(string), 10, 0)
	position.MasterLogPosition = uint(temp)
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
		time.Sleep(1e9)
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
func (mysqld *Mysqld) fetchSuperQuery(query string) ([][]interface{}, error) {
	conn, connErr := mysqld.createConnection()
	if connErr != nil {
		return nil, connErr
	}
	defer conn.Close()
	relog.Info("fetch %v", query)
	qr, err := conn.ExecuteFetch([]byte(query), 10000, false)
	if err != nil {
		return nil, err
	}
	return qr.Rows, nil
}

func (mysqld *Mysqld) executeSuperQueryList(queryList []string) error {
	conn, connErr := mysqld.createConnection()
	if connErr != nil {
		return connErr
	}
	defer conn.Close()
	for _, query := range queryList {
		relog.Info("exec %v", query)
		if _, err := conn.ExecuteFetch([]byte(query), 10000, false); err != nil {
			return err
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
	rows, err := mysqld.fetchSuperQuery("SHOW PROCESSLIST")
	if err != nil {
		return nil, err
	}
	addrs := make([]string, 0, 32)
	for _, row := range rows {
		if row[colUsername] == mysqld.replParams.Uname {
			host, _, err := net.SplitHostPort(row[colClientAddr].(string))
			if err != nil {
				return nil, fmt.Errorf("FindSlaves: malformed addr %v", err)
			}
			addrs = append(addrs, host)
		}
	}

	return addrs, nil
}
