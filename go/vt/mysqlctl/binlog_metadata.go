// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package mysqlctl

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"sync"

	"code.google.com/p/vitess/go/relog"
)

/*
The code in this file helps to track the position metadata
for binlogs.
*/

var RELAY_INFO_FILE = "relay-log.info"

type ReplicationCoordinates struct {
	RelayFilename  string
	RelayPosition  uint64
	MasterFilename string
	MasterPosition uint64
}

func NewReplicationCoordinates(slaveLog string, slavePosition uint64, masterFile string, masterPos uint64) *ReplicationCoordinates {
	return &ReplicationCoordinates{RelayFilename: slaveLog, RelayPosition: slavePosition, MasterFilename: masterFile, MasterPosition: masterPos}
}

type SlaveMetadata struct {
	logsDir           string
	relayInfoFile     string
	mLock             sync.Mutex
	SlavePositionList []*ReplicationCoordinates
}

func NewSlaveMetadata(logsDir string) *SlaveMetadata {
	metadata := &SlaveMetadata{logsDir: logsDir}
	metadata.relayInfoFile = path.Join(logsDir, RELAY_INFO_FILE)
	metadata.SlavePositionList = make([]*ReplicationCoordinates, 0, 20)
	return metadata
}

//This builds and sanitizes the list of replication coordinates.
func (metadata *SlaveMetadata) BuildMetadata() (err error) {
	if err = metadata.addLatestPosition(); err != nil {
		return err
	}
	if err = metadata.purgeOld(); err != nil {
		return err
	}
	return nil
}

//This gets the current replication position.
func (metadata *SlaveMetadata) GetCurrentReplicationPosition() (repl *ReplicationCoordinates, err error) {
	return metadata.getCurrentReplicationPosition()
}

//Given a masterfilename, position this locates the closest replication position.
func (metadata *SlaveMetadata) LocateReplicationCoordinates(masterFilename string, masterPos uint64) (replCoordinates ReplicationCoordinates, err error) {
	masterMap, err := metadata.constructMasterMap()
	if err != nil {
		return replCoordinates, err
	}
	metadata.mLock.Lock()
	defer metadata.mLock.Unlock()

	for filename, posList := range masterMap {
		if filename != masterFilename {
			continue
		}
		for i, repl := range posList {
			if i == 0 {
				if masterPos < repl.MasterPosition {
					return replCoordinates, fmt.Errorf("Earliest known master position: %v", repl.MasterPosition)
				}
			}
			if masterPos == repl.MasterPosition {
				return *repl, nil
			} else if masterPos < repl.MasterPosition {
				return *posList[i-1], nil
			}
		}
		return *posList[len(posList)-1], nil
	}

	err = fmt.Errorf("MasterFilename not known.")
	return
}

//This adds the latest replication position to the replication position list.
func (metadata *SlaveMetadata) addLatestPosition() (err error) {
	metadata.mLock.Lock()
	defer metadata.mLock.Unlock()

	repl, err := metadata.getCurrentReplicationPosition()
	if err != nil {
		return fmt.Errorf("Error in getting current replication position %v", err)
	}

	metadata.SlavePositionList = append(metadata.SlavePositionList, repl)
	return nil
}

//This purges the old stale entries from the list, for which the relay logs don't exist.
func (metadata *SlaveMetadata) purgeOld() error {
	metadata.mLock.Lock()
	defer metadata.mLock.Unlock()

	var staleFile string
	startIndex := 0

	for i, repl := range metadata.SlavePositionList {
		if staleFile != "" && staleFile == repl.RelayFilename {
			startIndex = i + 1
		} else {
			relayFile, err := os.Open(repl.RelayFilename)
			if err != nil {
				staleFile = repl.RelayFilename
				startIndex = i + 1
			} else {
				relayFile.Close()
				break
			}
		}
	}

	if startIndex > 0 && startIndex < len(metadata.SlavePositionList) {
		metadata.SlavePositionList = metadata.SlavePositionList[startIndex:]
	} else if startIndex == len(metadata.SlavePositionList) {
		metadata.SlavePositionList = metadata.SlavePositionList[0:0]
	}
	return nil
}

//This constructs a master position map which is used to locate master->replication position.
//Format map[MasterFilename]->[]Replication Positions in increasing order of positions.
func (metadata *SlaveMetadata) constructMasterMap() (masterFileMap map[string][]*ReplicationCoordinates, err error) {
	metadata.mLock.Lock()
	defer metadata.mLock.Unlock()

	masterFileMap = make(map[string][]*ReplicationCoordinates, len(metadata.SlavePositionList))

	for _, repl := range metadata.SlavePositionList {
		if positionList, ok := masterFileMap[repl.MasterFilename]; ok {
			positionList = append(positionList, repl)
		} else {
			positionList = make([]*ReplicationCoordinates, 0, len(metadata.SlavePositionList))
			positionList = append(positionList, repl)
			masterFileMap[repl.MasterFilename] = positionList
		}
	}

	return masterFileMap, nil
}

//This reads the current replication position from relay-log.info file.
func (metadata *SlaveMetadata) getCurrentReplicationPosition() (repl *ReplicationCoordinates, err error) {
	relayInfo, err := os.Open(metadata.relayInfoFile)
	if err != nil {
		return nil, fmt.Errorf("Error in reading file %s", metadata.relayInfoFile)
	}
	defer relayInfo.Close()
	fileData := make([]byte, 4096)
	_, err = relayInfo.Read(fileData)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("Error reading the file")
	}

	lines := bytes.Split(fileData, []byte("\n"))
	if len(lines) < 4 {
		return nil, fmt.Errorf("Invalid relay.info format")
	}
	relayFile := path.Join(metadata.logsDir, string(lines[0]))
	relayPos, err := strconv.ParseUint(string(lines[1]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("Invalid relay position, %v", err)
	}
	masterFilename := lines[2]
	masterPos, err := strconv.ParseUint(string(lines[3]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("Invalid master position, %v", err)
	}

	replCoordinates := NewReplicationCoordinates(relayFile, relayPos, string(masterFilename), masterPos)
	return replCoordinates, nil
}

//debug function, can be removed later
//this is an external function since it needs to be called from outside the package.
func (metadata *SlaveMetadata) Display() {
	var err error
	//relog.Info("len of slave position list %v", metadata.SlavePositionList.Len())
	masterFileMap, err := metadata.constructMasterMap()
	if err != nil {
		relog.Error("Error in constructing master map,  ", err)
	}
	for masterFile, positionList := range masterFileMap {
		for _, replCoord := range positionList {
			relog.Info("Master %v:%v => Slave %v:%v\n", masterFile, replCoord.MasterPosition, replCoord.RelayFilename, replCoord.RelayPosition)
		}
	}
}
