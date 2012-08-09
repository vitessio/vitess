// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Handle creating replicas and setting up the replication streams.
*/

package mysqlctl

import (
	"bytes"
	"code.google.com/p/vitess/go/relog"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"text/template"
	"time"
)

const (
	SlaveStartDeadline = 30
)

type ReplicationPosition struct {
	MasterLogFile     string
	MasterLogPosition uint
	ReadMasterLogPosition uint // how much has been read, but not applied
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

// FIXME(msolomon) wrong place for default credentials
func NewReplicationState(masterAddr string) *ReplicationState {
	addrPieces := strings.Split(masterAddr, ":")
	port, err := strconv.Atoi(addrPieces[1])
	if err != nil {
		panic(err)
	}
	return &ReplicationState{MasterUser: "vt_repl", MasterPassword: "", MasterConnectRetry: 10,
		MasterHost: addrPieces[0], MasterPort: port}
}

type ReplicaSource struct {
	Addr string // this is the address of the tabletserver, not mysql

	DbName   string
	FileList []string
	HashList []string // md5 sum of the compressed file
	*ReplicationState
}

func NewReplicaSource(addr, mysqlAddr string) *ReplicaSource {
	return &ReplicaSource{
		Addr:             addr,
		FileList:         make([]string, 0, 256),
		HashList:         make([]string, 0, 256),
		ReplicationState: NewReplicationState(mysqlAddr)}
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
	relayInfo, err := ioutil.ReadFile(mysqld.config.RelayLogInfoPath())
	if err != nil {
		return nil, err
	}
	// FIXME(msolomon) not sure i'll need this data
	masterInfo, err := ioutil.ReadFile(mysqld.config.MasterInfoPath())
	if err != nil {
		return nil, err
	}
	relayParts := strings.Split(string(relayInfo), " ")
	masterParts := strings.Split(string(masterInfo), " ")
	// FIXME(msolomon) does the file supply port?
	addr := fmt.Sprintf("%v:%v", masterParts[3], 3306)
	replState := NewReplicationState(addr)
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

func (mysqld *Mysqld) ValidateReplicaSource() error {
	rows, err := mysqld.fetchSuperQuery("SHOW PROCESSLIST")
	if err != nil {
		return err
	}
	if len(rows) > 8 {
		return errors.New("too many processes")
	}

	slaveStatus, err := mysqld.slaveStatus()
	if err != nil {
		if err != ERR_NOT_SLAVE {
			return err
		}
	} else {
		lagSeconds, _ := strconv.Atoi(slaveStatus["seconds_behind_master"])
		if lagSeconds > 5 {
			return errors.New("lag_seconds exceed maximum tolerance")
		}
	}
	// FIXME(msolomon) check free space based on an estimate of the current
	// size of the db files.
	// Also, check that we aren't already cloning/compressing or acting as a
	// source. Mysqld being down isn't enough, presumably that will be
	// restarted as soon as the snapshot is taken.
	return nil
}

func (mysqld *Mysqld) ValidateReplicaTarget() error {
	rows, err := mysqld.fetchSuperQuery("SHOW PROCESSLIST")
	if err != nil {
		return err
	}
	if len(rows) > 4 {
		return errors.New("too many active db processes")
	}

	rows, err = mysqld.fetchSuperQuery("SHOW DATABASES")
	if err != nil {
		return err
	}

	for _, row := range rows {
		if strings.HasPrefix(row[0].(string), "vt_") {
			dbName := row[0].(string)
			tableRows, err := mysqld.fetchSuperQuery("SHOW TABLES FROM " + dbName)
			if err != nil {
				relog.Error("failed checking tables %v", err)
			} else if len(tableRows) == 0 {
				// no tables == empty db, all is well
				continue
			}
			return errors.New("found active db named " + dbName)
		}
	}

	return nil
}

func (mysqld *Mysqld) ValidateSplitReplicaTarget() error {
	rows, err := mysqld.fetchSuperQuery("SHOW PROCESSLIST")
	if err != nil {
		return err
	}
	if len(rows) > 4 {
		return errors.New("too many active db processes")
	}

	rows, err = mysqld.fetchSuperQuery("SHOW DATABASES")
	if err != nil {
		return err
	}

	// NOTE: we expect that database was already created during tablet
	// assignment.
	return nil
}

/*
 This piece runs on the machine acting as the source in the create replica
 action.

 get master/slave status
 shutdown mysql
 compress /vt/vt_[0-9a-f]+/data/vt_.+
 compute md5() sums
 place in /vt/clone_src where they will be served by http server (not rpc)
 restart mysql
*/
func (mysqld *Mysqld) CreateReplicaSource(dbName, replicaSourcePath, sourceAddr string, allowHierarchicalReplication bool) (_replicaSource *ReplicaSource, err error) {
	if dbName == "" {
		err = errors.New("no database name provided")
		return
	}
	if err = mysqld.ValidateReplicaSource(); err != nil {
		return
	}

	// FIXME(msolomon) bleh, must match patterns in mycnf - probably belongs
	// in there as derived paths.
	cloneSourcePath := path.Join(replicaSourcePath, "data", dbName)
	cloneInnodbDataSourcePath := path.Join(replicaSourcePath, innodbDataSubdir)
	cloneInnodbLogSourcePath := path.Join(replicaSourcePath, innodbLogSubdir)
	// clean out and start fresh	
	for _, _path := range []string{cloneSourcePath, cloneInnodbDataSourcePath, cloneInnodbLogSourcePath} {
		if err = os.RemoveAll(_path); err != nil {
			return
		}
		if err = os.MkdirAll(_path, 0775); err != nil {
			return
		}
	}

	dbDataDir := path.Join(mysqld.config.DataDir, dbName)
	dbFiles, dirErr := ioutil.ReadDir(dbDataDir)
	if dirErr != nil {
		return nil, dirErr
	}
	innodbDataFiles, dirErr := ioutil.ReadDir(mysqld.config.InnodbDataHomeDir)
	if dirErr != nil {
		return nil, dirErr
	}
	innodbLogFiles, dirErr := ioutil.ReadDir(mysqld.config.InnodbLogGroupHomeDir)
	if dirErr != nil {
		return nil, dirErr
	}

	// if the source is a slave use the master replication position,
	// unless we are allowing hierachical replicas.
	masterAddr := ""
	replicationPosition, statusErr := mysqld.SlaveStatus()
	if statusErr != nil {
		if statusErr != ERR_NOT_SLAVE {
			// this is a real error
			return nil, statusErr
		}
		// we are really a master, so we need that position
		replicationPosition, statusErr = mysqld.MasterStatus()
		if statusErr != nil {
			return nil, statusErr
		}
		masterAddr = mysqld.Addr()
	} else {
		// we are a slave, check our replication strategy
		if allowHierarchicalReplication {
			masterAddr = mysqld.Addr()
		} else {
			masterAddr, err = mysqld.GetMasterAddr()
			if err != nil {
				return
			}
		}
	}

	replicaSource := NewReplicaSource(sourceAddr, masterAddr)
	replicaSource.Addr = sourceAddr
	replicaSource.DbName = dbName
	replicaSource.ReplicationPosition = *replicationPosition

	// save initial state so we can restore on Start()
	slaveStartRequired := false
	if slaveStatus, slaveErr := mysqld.slaveStatus(); slaveErr == nil {
		slaveStartRequired = (slaveStatus["Slave_IO_Running"] == "Yes" && slaveStatus["Slave_SQL_Running"] == "Yes")
	}
	readOnly := true
	if readOnly, err = mysqld.IsReadOnly(); err != nil {
		return
	}
	if err = Shutdown(mysqld, true); err != nil {
		return
	}

	// FIXME(msolomon) should mysqld just restart on any failure?
	compressFiles := func(filenames []os.FileInfo, srcDir, dstDir string) error {
		for _, fileInfo := range filenames {
			if !fileInfo.IsDir() {
				srcPath := path.Join(srcDir, fileInfo.Name())
				dstPath := path.Join(dstDir, fileInfo.Name()+".gz")
				if compressErr := compressFile(srcPath, dstPath); err != nil {
					return compressErr
				}
				hash, hashErr := md5File(dstPath)
				if hashErr != nil {
					return hashErr
				}
				replicaSource.FileList = append(replicaSource.FileList, dstPath)
				replicaSource.HashList = append(replicaSource.HashList, hash)
				relog.Info("%v:%v ready", dstPath, hash)
			}
		}
		return nil
	}

	if err = compressFiles(dbFiles, dbDataDir, cloneSourcePath); err != nil {
		return
	}
	if err = compressFiles(innodbDataFiles, mysqld.config.InnodbDataHomeDir, cloneInnodbDataSourcePath); err != nil {
		return
	}
	if err = compressFiles(innodbLogFiles, mysqld.config.InnodbLogGroupHomeDir, cloneInnodbLogSourcePath); err != nil {
		return
	}

	if err = Start(mysqld); err != nil {
		return
	}

	// restore original mysqld state that we saved above
	if slaveStartRequired {
		if err = mysqld.StartSlave(); err != nil {
			return
		}
		// this should be quick, but we might as well just wait
		if err = mysqld.WaitForSlaveStart(5); err != nil {
			return
		}
	}
	if err = mysqld.SetReadOnly(readOnly); err != nil {
		return
	}

	// ok, copy over the pointer on success
	_replicaSource = replicaSource
	return
}

/*
 This piece runs on the presumably empty machine acting as the target in the
 create replica action.

 validate target (self)
 shutdown_mysql()
 create temp data directory /vt/target/vt_<keyspace>
 copy compressed data files via HTTP
 verify md5sum of compressed files
 uncompress into /vt/vt_<target-uid>/data/vt_<keyspace>
 start_mysql()
 clean up compressed files
*/
func (mysqld *Mysqld) CreateReplicaTarget(replicaSource *ReplicaSource, tempStoragePath string) (err error) {
	if replicaSource == nil {
		return errors.New("nil replicaSource")
	}
	if err = mysqld.ValidateReplicaTarget(); err != nil {
		return
	}

	if err = Shutdown(mysqld, true); err != nil {
		return
	}

	replicaDbPath := path.Join(mysqld.config.DataDir, replicaSource.DbName)

	cleanDirs := []string{tempStoragePath, replicaDbPath, mysqld.config.InnodbDataHomeDir, mysqld.config.InnodbLogGroupHomeDir}

	// clean out and start fresh
	// FIXME(msolomon) this might be changed to allow partial recovery
	for _, dir := range cleanDirs {
		if err = os.RemoveAll(dir); err != nil {
			return
		}
		if err = os.MkdirAll(dir, 0775); err != nil {
			return
		}
	}

	httpConn, connErr := net.Dial("tcp", replicaSource.Addr)
	if connErr != nil {
		return connErr
	}
	defer httpConn.Close()

	fileClient := httputil.NewClientConn(httpConn, nil)
	defer fileClient.Close()
	// FIXME(msolomon) parallelize
	// FIXME(msolomon) pull out simple URL fetch?
	// FIXME(msolomon) automatically retry a file transfer at least once
	// FIXME(msolomon) deadlines?
	for i, srcPath := range replicaSource.FileList {
		srcHash := replicaSource.HashList[i]
		urlstr := "http://" + replicaSource.Addr + srcPath
		urlobj, parseErr := url.Parse(urlstr)
		if parseErr != nil {
			return errors.New("failed to create url " + urlstr)
		}
		req := &http.Request{Method: "GET",
			Host: replicaSource.Addr,
			URL:  urlobj}
		err = fileClient.Write(req)
		if err != nil {
			return errors.New("failed requesting " + urlstr)
		}
		var response *http.Response
		response, err = fileClient.Read(req)
		if err != nil {
			return errors.New("failed fetching " + urlstr)
		}
		if response.StatusCode != 200 {
			return errors.New("failed fetching " + urlstr + ": " + response.Status)
		}
		relativePath := strings.SplitN(srcPath, "/", 5)[4]
		gzFilename := path.Join(tempStoragePath, relativePath)
		filename := path.Join(mysqld.config.TabletDir, relativePath)
		// trim .gz
		filename = filename[:len(filename)-3]

		dir, _ := path.Split(gzFilename)
		if dirErr := os.MkdirAll(dir, 0775); dirErr != nil {
			return dirErr
		}

		// FIXME(msolomon) buffer output?
		file, fileErr := os.OpenFile(gzFilename, os.O_CREATE|os.O_WRONLY, 0664)
		if fileErr != nil {
			return fileErr
		}
		defer file.Close()

		_, err = io.Copy(file, response.Body)
		if err != nil {
			return
		}
		file.Close()

		hash, hashErr := md5File(gzFilename)
		if hashErr != nil {
			return hashErr
		}
		if srcHash != hash {
			return errors.New("hash mismatch for " + gzFilename + ", " + srcHash + " != " + hash)
		}

		if err = uncompressFile(gzFilename, filename); err != nil {
			return
		}
		if err = os.Remove(gzFilename); err != nil {
			// don't stop the process for this error
			relog.Error("failed to remove %v", gzFilename)
		}
		relog.Info("%v ready", filename)
	}

	if err = Start(mysqld); err != nil {
		return
	}

	cmdList := StartReplicationCommands(replicaSource.ReplicationState)
	relog.Info("StartReplicationCommands %#v", cmdList)
	if err = mysqld.executeSuperQueryList(cmdList); err != nil {
		return
	}

	err = mysqld.WaitForSlaveStart(SlaveStartDeadline)
	return
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

var ERR_NOT_SLAVE = errors.New("no slave status")

func (mysqld *Mysqld) slaveStatus() (map[string]string, error) {
	rows, err := mysqld.fetchSuperQuery("SHOW SLAVE STATUS")
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, ERR_NOT_SLAVE
	}

	rowMap := make(map[string]string)
	for i, column := range rows[0] {
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
	pos.MasterLogFile = fields["Relay_Master_Log_File"]
	temp, _ := strconv.ParseUint(fields["Exec_Master_Log_Pos"], 10, 0)
	pos.MasterLogPosition = uint(temp)
	temp, _ = strconv.ParseUint(fields["Read_Master_Log_Pos"], 10, 0)
	pos.ReadMasterLogPosition = uint(temp)
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

func (mysqld *Mysqld) ConfigureKeyRange(startKey string, endKey string) error {
	replicationCmds := []string{
		"SET GLOBAL vt_enable_binlog_splitter_rbr = 1",
		"SET GLOBAL vt_shard_key_range_start = \"0x" + startKey + "\"",
		"SET GLOBAL vt_shard_key_range_end = \"0x" + endKey + "\""}
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
// Insert a row, block the propogation of its subsequent delete and reinsert it. This 
// forces a failue on slaves only.
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

	return mysqld.executeSuperQueryList(cmds);
}
