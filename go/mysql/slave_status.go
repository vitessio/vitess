/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysql

import (
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	"vitess.io/vitess/go/vt/vterrors"
)

// SlaveStatus holds replication information from SHOW SLAVE STATUS.
type SlaveStatus struct {
	Position Position
	// RelayLogPosition is the Position that the replica would be at if it
	// were to finish executing everything that's currently in its relay log.
	// However, some MySQL flavors don't expose this information,
	// in which case RelayLogPosition.IsZero() will be true.
	RelayLogPosition     Position
	FilePosition         Position
	FileRelayLogPosition Position
	MasterServerID       uint
	SlaveIORunning       bool
	SlaveSQLRunning      bool
	SecondsBehindMaster  uint
	MasterHost           string
	MasterPort           int
	MasterConnectRetry   int
}

// SlaveRunning returns true iff both the Slave IO and Slave SQL threads are
// running.
func (s *SlaveStatus) SlaveRunning() bool {
	return s.SlaveIORunning && s.SlaveSQLRunning
}

// SlaveStatusToProto translates a Status to proto3.
func SlaveStatusToProto(s SlaveStatus) *replicationdatapb.Status {
	return &replicationdatapb.Status{
		Position:             EncodePosition(s.Position),
		RelayLogPosition:     EncodePosition(s.RelayLogPosition),
		FilePosition:         EncodePosition(s.FilePosition),
		FileRelayLogPosition: EncodePosition(s.FileRelayLogPosition),
		MasterServerId:       uint32(s.MasterServerID),
		SlaveIoRunning:       s.SlaveIORunning,
		SlaveSqlRunning:      s.SlaveSQLRunning,
		SecondsBehindMaster:  uint32(s.SecondsBehindMaster),
		MasterHost:           s.MasterHost,
		MasterPort:           int32(s.MasterPort),
		MasterConnectRetry:   int32(s.MasterConnectRetry),
	}
}

// ProtoToSlaveStatus translates a proto Status, or panics.
func ProtoToSlaveStatus(s *replicationdatapb.Status) SlaveStatus {
	pos, err := DecodePosition(s.Position)
	if err != nil {
		panic(vterrors.Wrapf(err, "cannot decode Position"))
	}
	relayPos, err := DecodePosition(s.RelayLogPosition)
	if err != nil {
		panic(vterrors.Wrapf(err, "cannot decode RelayLogPosition"))
	}
	filePos, err := DecodePosition(s.FilePosition)
	if err != nil {
		panic(vterrors.Wrapf(err, "cannot decode FilePosition"))
	}
	fileRelayPos, err := DecodePosition(s.FileRelayLogPosition)
	if err != nil {
		panic(vterrors.Wrapf(err, "cannot decode FileRelayLogPosition"))
	}
	return SlaveStatus{
		Position:             pos,
		RelayLogPosition:     relayPos,
		FilePosition:         filePos,
		FileRelayLogPosition: fileRelayPos,
		MasterServerID:       uint(s.MasterServerId),
		SlaveIORunning:       s.SlaveIoRunning,
		SlaveSQLRunning:      s.SlaveSqlRunning,
		SecondsBehindMaster:  uint(s.SecondsBehindMaster),
		MasterHost:           s.MasterHost,
		MasterPort:           int(s.MasterPort),
		MasterConnectRetry:   int(s.MasterConnectRetry),
	}
}
