// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package mysqlctl

import (
	"encoding/json"
	"expvar"
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/rpcwrap"
	estats "code.google.com/p/vitess/go/stats" // stats is a private type defined somewhere else in this package, so it would conflict
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
	state          int32
	states         *estats.States
	actionLock     sync.Mutex
	binlogPrefix   string
	logsDir        string
	usingRelayLogs bool
	logMetadata    *SlaveMetadata
	mysqld         *Mysqld
	clients        []*Blp
	stateWaitGroup sync.WaitGroup
	dbname         string
}

type UpdateStreamRequest struct {
	StartPosition string
}

var UpdateStreamRpcService *UpdateStream

func RegisterUpdateStreamService(mycnf *Mycnf) {
	if UpdateStreamRpcService != nil {
		relog.Warning("Update Stream service already initialized")
		return
	}

	UpdateStreamRpcService = &UpdateStream{mycnf: mycnf}
	UpdateStreamRpcService.clients = make([]*Blp, 10)
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
		disableUpdateStreamService()
	}

	UpdateStreamRpcService.setState(ENABLED)

	UpdateStreamRpcService.mysqld = NewMysqld(UpdateStreamRpcService.mycnf, dbcfgs.Dba, dbcfgs.Repl)
	UpdateStreamRpcService.dbname = dbcfgs.Repl.Dbname
	UpdateStreamRpcService.tabletType = tabletType
	UpdateStreamRpcService.usingRelayLogs = false
	if tabletType == "master" {
		UpdateStreamRpcService.binlogPrefix = UpdateStreamRpcService.mycnf.BinLogPath
	} else if isReplicaType(tabletType) {
		UpdateStreamRpcService.binlogPrefix = UpdateStreamRpcService.mycnf.RelayLogPath
		UpdateStreamRpcService.usingRelayLogs = true
	}
	UpdateStreamRpcService.logsDir = path.Dir(UpdateStreamRpcService.binlogPrefix)

	if UpdateStreamRpcService.usingRelayLogs {
		UpdateStreamRpcService.logMetadata = NewSlaveMetadata(UpdateStreamRpcService.logsDir, UpdateStreamRpcService.mycnf.RelayLogInfoPath)
	}
}

func DisableUpdateStreamService() {
	//If the service is already disabled, just return
	if !UpdateStreamRpcService.isServiceEnabled() {
		return
	}
	UpdateStreamRpcService.actionLock.Lock()
	defer UpdateStreamRpcService.actionLock.Unlock()
	disableUpdateStreamService()
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
	return atomic.LoadInt32(&updateStream.state) == ENABLED
}

func (updateStream *UpdateStream) setState(state int32) {
	atomic.StoreInt32(&UpdateStreamRpcService.state, state)
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
			SendError(sendReply, req.StartPosition, x.(error), nil)
		}
	}()

	if !updateStream.isServiceEnabled() {
		relog.Warning("Unable to serve client request: Update stream service is not enabled yet")
		return fmt.Errorf("Update stream service is not enabled yet")
	}

	relog.Info("ServeUpdateStream starting @ %v", req.StartPosition)
	if req.StartPosition == "" {
		return fmt.Errorf("Invalid start position, cannot serve the stream")
	}
	startCoordinates, err := DecodePositionToCoordinates(req.StartPosition)
	if err != nil {
		return fmt.Errorf("Invalid start position %v, cannot serve the stream, err %v", req.StartPosition, err)
	}

	blp := NewBlp(startCoordinates, updateStream)
	updateStream.clients = append(updateStream.clients, blp)
	updateStream.actionLock.Lock()
	updateStream.stateWaitGroup.Add(1)
	updateStream.actionLock.Unlock()

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

	blp.StreamBinlog(sendReply, updateStream.binlogPrefix)
	updateStream.stateWaitGroup.Done()
	return nil
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

func GetCurrentReplicationPosition() (repl *ReplicationCoordinates, err error) {
	if !UpdateStreamRpcService.isServiceEnabled() {
		return nil, fmt.Errorf("UpdateStream Service is not enabled, cannot determine current replication position")
	}
	if !UpdateStreamRpcService.usingRelayLogs {
		return nil, fmt.Errorf("Server is a non-replicating type")
	}
	return UpdateStreamRpcService.logMetadata.GetCurrentReplicationPosition()
}

func EncodeCoordinatesToPosition(coordinates *ReplicationCoordinates) (string, error) {
	pos, err := json.Marshal(coordinates)
	if err != nil {
		return "", err
	}
	return string(pos), err
}

func DecodePositionToCoordinates(pos string) (repl *ReplicationCoordinates, err error) {
	repl = new(ReplicationCoordinates)
	err = json.Unmarshal([]byte(pos), repl)
	if err != nil {
		return nil, err
	}
	return repl, err
}
