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
)

/*
The code in this file helps to track the position metadata
for binlogs.
*/

type ReplicationCoordinates struct {
	RelayFilename  string
	RelayPosition  uint64
	MasterFilename string
	MasterPosition uint64
}

func NewReplicationCoordinates(slaveLog string, slavePosition uint64, masterFile string, masterPos uint64) *ReplicationCoordinates {
	return &ReplicationCoordinates{RelayFilename: slaveLog, RelayPosition: slavePosition, MasterFilename: masterFile, MasterPosition: masterPos}
}

func (repl *ReplicationCoordinates) String() string {
	return fmt.Sprintf("Master %v:%v Relay %v:%v", repl.MasterFilename, repl.MasterPosition, repl.RelayFilename, repl.RelayPosition)
}

type SlaveMetadata struct {
	logsDir       string
	relayInfoFile string
}

func NewSlaveMetadata(logsDir string, relayInfo string) *SlaveMetadata {
	metadata := &SlaveMetadata{logsDir: logsDir, relayInfoFile: relayInfo}
	return metadata
}

//This gets the current replication position.
func (metadata *SlaveMetadata) GetCurrentReplicationPosition() (repl *ReplicationCoordinates, err error) {
	return metadata.getCurrentReplicationPosition()
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
	var relayFile string
	relayPath := string(lines[0])
	d, f := path.Split(relayPath)
	if d == "" {
		relayFile = path.Join(metadata.logsDir, f)
	} else {
		relayFile = relayPath
	}
	relayPos, err := strconv.ParseUint(string(lines[1]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("Invalid relay position, %v", err)
	}
	masterFilename := lines[2]
	masterPos, err := strconv.ParseUint(string(lines[3]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("Invalid master position, %v", err)
	}

	//relog.Info("repl Coord %v %v %v %v", relayFile, relayPos, string(masterFilename), masterPos)
	replCoordinates := NewReplicationCoordinates(relayFile, relayPos, string(masterFilename), masterPos)
	return replCoordinates, nil
}
