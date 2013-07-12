// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package mysqlctl

import (
	"expvar"
	"fmt"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/rpcwrap"
	estats "code.google.com/p/vitess/go/stats" // stats is a private type defined somewhere else in this package, so it would conflict
	"code.google.com/p/vitess/go/sync2"
	"code.google.com/p/vitess/go/vt/dbconfigs"
)

/* API and config for UpdateStream Service */

const (
	DISABLED = iota
	ENABLED
)

type UpdateStream struct {
	mycnf          *Mycnf
	tabletType     string
	state          sync2.AtomicInt32
	states         *estats.States
	actionLock     sync.Mutex
	binlogPrefix   string
	logsDir        string
	usingRelayLogs bool
	mysqld         *Mysqld
	stateWaitGroup sync.WaitGroup
	dbname         string
}

type UpdateStreamRequest struct {
	StartPosition BinlogPosition
}

var UpdateStreamRpcService *UpdateStream

func RegisterUpdateStreamService(mycnf *Mycnf) {
	if UpdateStreamRpcService != nil {
		//relog.Warning("Update Stream service already initialized")
		return
	}

	UpdateStreamRpcService = &UpdateStream{mycnf: mycnf}
	UpdateStreamRpcService.states = estats.NewStates("", []string{
		"Disabled",
		"Enabled",
	}, time.Now(), DISABLED)
	rpcwrap.RegisterAuthenticated(UpdateStreamRpcService)
	expvar.Publish("UpdateStreamRpcService", estats.StrFunc(func() string { return UpdateStreamRpcService.statsJSON() }))
}

func logError() {
	if x := recover(); x != nil {
		relog.Error("%s", x.(error).Error())
	}
}

func isReplicaType(tabletType string) bool {
	switch tabletType {
	case "replica", "rdonly", "batch":
		return true
	}
	return false
}

func dbcfgsCorrect(tabletType string, dbcfgs dbconfigs.DBConfigs) bool {
	switch tabletType {
	case "master":
		if dbcfgs.Dba.UnixSocket != "" {
			return true
		}
	case "replica", "rdonly", "batch":
		if dbcfgs.Dba.UnixSocket != "" && dbcfgs.Repl.UnixSocket != "" {
			return true
		}
	}
	return false
}

func EnableUpdateStreamService(tabletType string, dbcfgs dbconfigs.DBConfigs) {
	defer logError()
	UpdateStreamRpcService.actionLock.Lock()
	defer UpdateStreamRpcService.actionLock.Unlock()

	if !dbcfgsCorrect(tabletType, dbcfgs) {
		relog.Warning("missing/incomplete db configs file, cannot enable update stream service")
		return
	}

	if UpdateStreamRpcService.isServiceEnabled() {
		relog.Warning("Update stream service is already enabled")
		return
	}

	UpdateStreamRpcService.setState(ENABLED)

	UpdateStreamRpcService.mysqld = NewMysqld(UpdateStreamRpcService.mycnf, dbcfgs.Dba, dbcfgs.Repl)
	UpdateStreamRpcService.dbname = dbcfgs.App.Dbname
	relog.Info("dbcfgs.App.Dbname %v DbName %v", dbcfgs.App.Dbname, UpdateStreamRpcService.dbname)
	relog.Info("mycnf.BinLogPath %v mycnf.RelayLogPath %v", UpdateStreamRpcService.mycnf.BinLogPath, UpdateStreamRpcService.mycnf.RelayLogPath)
	UpdateStreamRpcService.tabletType = tabletType
	UpdateStreamRpcService.usingRelayLogs = false
	if tabletType == "master" {
		UpdateStreamRpcService.binlogPrefix = UpdateStreamRpcService.mycnf.BinLogPath
	} else if isReplicaType(tabletType) {
		// If the replica server has log-slave-updates turned on
		// use binlogs, if not then use relay logs.
		if UpdateStreamRpcService.mycnf.BinLogPath != "" {
			UpdateStreamRpcService.binlogPrefix = UpdateStreamRpcService.mycnf.BinLogPath
		} else {
			UpdateStreamRpcService.binlogPrefix = UpdateStreamRpcService.mycnf.RelayLogPath
			UpdateStreamRpcService.usingRelayLogs = true
		}
	}
	UpdateStreamRpcService.logsDir = path.Dir(UpdateStreamRpcService.binlogPrefix)

	relog.Info("Update Stream enabled, logsDir %v, usingRelayLogs: %v", UpdateStreamRpcService.logsDir, UpdateStreamRpcService.usingRelayLogs)
}

func DisableUpdateStreamService() {
	//If the service is already disabled, just return
	if !UpdateStreamRpcService.isServiceEnabled() {
		return
	}
	UpdateStreamRpcService.actionLock.Lock()
	defer UpdateStreamRpcService.actionLock.Unlock()
	disableUpdateStreamService()
	relog.Info("Update Stream Disabled")
}

func IsUpdateStreamEnabled() bool {
	return UpdateStreamRpcService.isServiceEnabled()
}

func IsUpdateStreamUsingRelayLogs() bool {
	return UpdateStreamRpcService.usingRelayLogs
}

func disableUpdateStreamService() {
	defer logError()

	UpdateStreamRpcService.setState(DISABLED)
	UpdateStreamRpcService.stateWaitGroup.Wait()
}

func (updateStream *UpdateStream) isServiceEnabled() bool {
	return updateStream.state.Get() == ENABLED
}

func (updateStream *UpdateStream) setState(state int32) {
	updateStream.state.Set(state)
	updateStream.states.SetState(int(state))
}

// expvar.Var interface.
// NOTE(szopa): It's unexported to avoid rpc logspam.
func (updateStream *UpdateStream) statsJSON() string {
	return fmt.Sprintf("{"+
		"\"States\": %v"+
		"}", updateStream.states.String())
}

func LogsDir() string {
	return UpdateStreamRpcService.logsDir
}

func ServeUpdateStream(req *UpdateStreamRequest, sendReply SendUpdateStreamResponse) error {
	return UpdateStreamRpcService.ServeUpdateStream(req, sendReply)
}

func (updateStream *UpdateStream) ServeUpdateStream(req *UpdateStreamRequest, sendReply SendUpdateStreamResponse) error {
	defer func() {
		if x := recover(); x != nil {
			//Send the error to the client.
			//panic(x)
			SendError(sendReply, x.(error), nil)
		}
	}()

	if !updateStream.isServiceEnabled() {
		relog.Warning("Unable to serve client request: Update stream service is not enabled yet")
		return fmt.Errorf("Update stream service is not enabled yet")
	}

	if !IsStartPositionValid(&req.StartPosition) {
		return fmt.Errorf("Invalid start position, cannot serve the stream")
	}
	relog.Info("ServeUpdateStream starting @ %v", req.StartPosition.String())

	startCoordinates := &req.StartPosition.Position
	blp := NewBlp(startCoordinates, updateStream)

	//locate the relay filename and position based on the masterPosition map
	if !updateStream.usingRelayLogs {
		if !IsMasterPositionValid(startCoordinates) {
			return fmt.Errorf("Invalid start position %v", req.StartPosition)
		}
	} else {
		if !IsRelayPositionValid(startCoordinates, UpdateStreamRpcService.logsDir) {
			return fmt.Errorf("Could not locate the start position %v", req.StartPosition)
		}
	}

	updateStream.actionLock.Lock()
	updateStream.stateWaitGroup.Add(1)
	updateStream.actionLock.Unlock()
	defer updateStream.clientDone()

	blp.StreamBinlog(sendReply, updateStream.binlogPrefix)
	return nil
}

func (updateStream *UpdateStream) clientDone() {
	updateStream.stateWaitGroup.Done()
}

func (updateStream *UpdateStream) getReplicationPosition() (rc *ReplicationCoordinates, err error) {
	rc = new(ReplicationCoordinates)
	if updateStream.usingRelayLogs {
		rowMap, err := updateStream.mysqld.slaveStatus()
		if err != nil {
			return nil, err
		}
		rc.MasterFilename = rowMap["Relay_Master_Log_File"]
		temp, _ := strconv.ParseUint(rowMap["Exec_Master_Log_Pos"], 10, 0)
		rc.MasterPosition = uint64(temp)
		rc.RelayFilename = rowMap["Relay_Log_File"]
		temp, _ = strconv.ParseUint(rowMap["Relay_Log_Pos"], 10, 0)
		rc.RelayPosition = uint64(temp)
	} else {
		rp, err := updateStream.mysqld.MasterStatus()
		if err != nil {
			return nil, err
		}
		rc.MasterFilename = rp.MasterLogFile
		rc.MasterPosition = uint64(rp.MasterLogPosition)
	}
	return rc, err
}

func GetReplicationPosition() (rc *ReplicationCoordinates, err error) {
	return UpdateStreamRpcService.getReplicationPosition()
}

func IsMasterPositionValid(startCoordinates *ReplicationCoordinates) bool {
	if startCoordinates.MasterFilename == "" || startCoordinates.MasterPosition <= 0 {
		return false
	}
	return true
}

//This verifies the correctness of the start position.
//The seek for relay logs depends on the RelayFilename and correct MasterFilename and Position.
func IsRelayPositionValid(startCoordinates *ReplicationCoordinates, logsDir string) bool {
	if startCoordinates.RelayFilename == "" || startCoordinates.MasterFilename == "" || startCoordinates.MasterPosition <= 0 {
		return false
	}
	var relayFile string
	d, f := path.Split(startCoordinates.RelayFilename)
	if d == "" {
		relayFile = path.Join(logsDir, f)
	} else {
		relayFile = startCoordinates.RelayFilename
	}
	_, err := os.Open(relayFile)
	if err != nil {
		return false
	}
	return true
}

func IsStartPositionValid(startPos *BinlogPosition) bool {
	startCoord := &startPos.Position
	return IsMasterPositionValid(startCoord)
}
