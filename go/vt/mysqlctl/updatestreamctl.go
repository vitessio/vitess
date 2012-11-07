// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package mysqlctl

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/rpcwrap"
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
	actionLock     sync.Mutex
	binlogPrefix   string
	logsDir        string
	usingRelayLogs bool
	logMetadata    *SlaveMetadata
	mysqld         *Mysqld
	clients        []*Blp
	stateWaitGroup sync.WaitGroup
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
	rpcwrap.RegisterAuthenticated(UpdateStreamRpcService)
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
	updateStream := UpdateStreamRpcService
	updateStream.actionLock.Lock()
	defer updateStream.actionLock.Unlock()

	if !dbcfgsCorrect(tabletType, dbcfgs) {
		relog.Warning("missing/incomplete db configs file, cannot enable update stream service")
		return
	}

	if updateStream.isServiceEnabled() {
		disableUpdateStreamService()
	}

	atomic.StoreInt32(&updateStream.state, ENABLED)

	updateStream.mysqld = NewMysqld(updateStream.mycnf, dbcfgs.Dba, dbcfgs.Repl)
	updateStream.tabletType = tabletType
	updateStream.usingRelayLogs = false
	if tabletType == "master" {
		updateStream.binlogPrefix = updateStream.mycnf.BinLogPath
	} else if isReplicaType(tabletType) {
		updateStream.binlogPrefix = updateStream.mycnf.RelayLogPath
		updateStream.usingRelayLogs = true
	}
	updateStream.logsDir = path.Dir(updateStream.binlogPrefix)

	if updateStream.usingRelayLogs {
		updateStream.logMetadata = NewSlaveMetadata(updateStream.logsDir, updateStream.mycnf.RelayLogInfoPath)
		go updateStream.buildSlaveMetadata()
	}
}

func DisableUpdateStreamService() {
	UpdateStreamRpcService.actionLock.Lock()
	defer UpdateStreamRpcService.actionLock.Unlock()
	disableUpdateStreamService()
}

func disableUpdateStreamService() {
	defer logError()
	updateStream := UpdateStreamRpcService

	atomic.StoreInt32(&updateStream.state, DISABLED)
	updateStream.stateWaitGroup.Wait()
}

func (updateStream *UpdateStream) isServiceEnabled() bool {
	state := atomic.LoadInt32(&updateStream.state)
	if state == ENABLED {
		return true
	}
	return false
}

func (updateStream *UpdateStream) buildSlaveMetadata() {
	var err error

	//perform one lookup immediately
	if err = updateStream.logMetadata.BuildMetadata(); err != nil {
		relog.Error("Error in building slave metadata %v", err)
	}
	timerChannel := time.Tick(30 * time.Second)
	for _ = range timerChannel {
		if !updateStream.isServiceEnabled() {
			return
		}

		if err = updateStream.logMetadata.BuildMetadata(); err != nil {
			relog.Error("Error in building slave metadata %v", err)
		}
	}
}

func (updateStream *UpdateStream) ServeUpdateStream(req *UpdateStreamRequest, sendReply SendUpdateStreamResponse) error {
	defer func() {
		if x := recover(); x != nil {
			//Send the error to the client.
			SendError(sendReply, req.StartPosition, x.(error), nil)
		}
	}()

	if !updateStream.isServiceEnabled() {
		relog.Warning("Update stream service is not enabled yet")
		return fmt.Errorf("Update stream service is not enabled yet")
	}

	//relog.Info("ServeUpdateStream starting @ %v", req.StartPosition)
	// path is something like /vt/vt-xxxxxx-bin-log:position
	pieces := strings.SplitN(req.StartPosition, ":", 2)
	filename := pieces[0]
	if filename == "" {
		return fmt.Errorf("Invalid start position, %v", req.StartPosition)
	}
	pos, err := strconv.ParseUint(pieces[1], 10, 64)
	if err != nil {
		return fmt.Errorf("Could not locate the start position %v, err: %v", req.StartPosition, err)
	}

	blp := NewBlp(filename, pos, updateStream)
	updateStream.clients = append(updateStream.clients, blp)
	updateStream.stateWaitGroup.Add(1)

	//locate the relay filename and position based on the masterPosition map
	if updateStream.usingRelayLogs {
		//updateStream.logMetadata.display()
		replCoordinates, err := updateStream.logMetadata.LocateReplicationCoordinates(filename, pos)
		if err != nil {
			return fmt.Errorf("Could not locate the start position %v, err: %v", req.StartPosition, err)
		}
		//relog.Info("Located correct pos, %v %v", replCoordinates.RelayFilename, replCoordinates.RelayPosition)
		blp.repl = &replCoordinates
	}

	blp.StreamBinlog(sendReply, updateStream.binlogPrefix)
	updateStream.stateWaitGroup.Done()
	return nil
}
