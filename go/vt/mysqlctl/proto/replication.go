// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	// InvalidLagSeconds is a special value for SecondsBehindMaster
	// that means replication is not running
	InvalidLagSeconds = 0xFFFFFFFF
)

// ReplicationPosition tracks the replication position on both a master
// and a slave.
type ReplicationPosition struct {
	// MasterLogFile, MasterLogPosition and MasterLogGroupId are
	// the position on the logs for transactions that have been
	// applied (SQL position):
	// - on the master, it's File, Position and Group_ID from
	//   'show master status'.
	// - on the slave, it's Relay_Master_Log_File, Exec_Master_Log_Pos
	//   and Exec_Master_Group_ID from 'show slave status'.
	MasterLogFile     string
	MasterLogPosition uint
	MasterLogGroupId  int64

	// MasterLogFileIo and MasterLogPositionIo are the position on the logs
	// that have been downloaded from the master (IO position),
	// but not necessarely applied yet:
	// - on the master, same as MasterLogFile and MasterLogPosition.
	// - on the slave, it's Master_Log_File and Read_Master_Log_Pos
	//   from 'show slave status'.
	MasterLogFileIo     string
	MasterLogPositionIo uint

	// SecondsBehindMaster is how far behind we are in applying logs in
	// replication. If equal to InvalidLagSeconds, it means replication
	// is not running.
	SecondsBehindMaster uint
}

func (rp *ReplicationPosition) MapKey() string {
	return fmt.Sprintf("%v:%d", rp.MasterLogFile, rp.MasterLogPosition)
}

func (rp *ReplicationPosition) MapKeyIo() string {
	return fmt.Sprintf("%v:%d", rp.MasterLogFileIo, rp.MasterLogPositionIo)
}

type ReplicationState struct {
	// ReplicationPosition is not anonymous because the default json encoder has begun to fail here.
	ReplicationPosition ReplicationPosition
	MasterHost          string
	MasterPort          int
	MasterConnectRetry  int
}

func (rs *ReplicationState) MasterAddr() string {
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

// Binlog server / player replication structures

type BlpPosition struct {
	Uid     uint32
	GroupId int64
}

type BlpPositionList struct {
	Entries []BlpPosition
}

func (bpl *BlpPositionList) FindBlpPositionById(id uint32) (*BlpPosition, error) {
	for _, pos := range bpl.Entries {
		if pos.Uid == id {
			return &pos, nil
		}
	}
	return nil, fmt.Errorf("BlpPosition for id %v not found", id)
}
