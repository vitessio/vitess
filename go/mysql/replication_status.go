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
	"fmt"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	"vitess.io/vitess/go/vt/vterrors"
)

// ReplicationStatus holds replication information from SHOW SLAVE STATUS.
type ReplicationStatus struct {
	// Position is the current position of the replica. For GTID replication implementations
	// it is the executed GTID set. For file replication implementation, it is same as
	// FilePosition
	Position Position
	// RelayLogPosition is the Position that the replica would be at if it
	// were to finish executing everything that's currently in its relay log.
	// However, some MySQL flavors don't expose this information,
	// in which case RelayLogPosition.IsZero() will be true.
	// If ReplicationLagUnknown is true then we should not rely on the seconds
	// behind value and we can instead try to calculate the lag ourselves when
	// appropriate. For MySQL GTID replication implementation it is the union of
	// executed GTID set and retrieved GTID set. For file replication implementation,
	// it is same as RelayLogSourceBinlogEquivalentPosition
	RelayLogPosition Position
	// FilePosition stores the position of the source tablets binary log
	// upto which the SQL thread of the replica has run.
	FilePosition Position
	// RelayLogSourceBinlogEquivalentPosition stores the position of the source tablets binary log
	// upto which the IO thread has read and added to the relay log
	RelayLogSourceBinlogEquivalentPosition Position
	// RelayLogFilePosition stores the position in the relay log file
	RelayLogFilePosition  Position
	SourceServerID        uint32
	IOState               ReplicationState
	LastIOError           string
	SQLState              ReplicationState
	LastSQLError          string
	ReplicationLagSeconds uint32
	ReplicationLagUnknown bool
	SourceHost            string
	SourcePort            int32
	SourceUser            string
	ConnectRetry          int32
	SourceUUID            SID
	SQLDelay              uint32
	AutoPosition          bool
	UsingGTID             bool
	HasReplicationFilters bool
	SSLAllowed            bool
}

// Running returns true if both the IO and SQL threads are running.
func (s *ReplicationStatus) Running() bool {
	return s.IOState == ReplicationStateRunning && s.SQLState == ReplicationStateRunning
}

// Healthy returns true if both the SQL IO components are healthy
func (s *ReplicationStatus) Healthy() bool {
	return s.SQLHealthy() && s.IOHealthy()
}

// IOHealthy returns true if the IO thread is running OR, the
// IO thread is connecting AND there's no IO error from the last
// attempt to connect to the source.
func (s *ReplicationStatus) IOHealthy() bool {
	return s.IOState == ReplicationStateRunning ||
		(s.IOState == ReplicationStateConnecting && s.LastIOError == "")
}

// SQLHealthy returns true if the SQLState is running.
// For consistency and to support altering this calculation in the future.
func (s *ReplicationStatus) SQLHealthy() bool {
	return s.SQLState == ReplicationStateRunning
}

// ReplicationStatusToProto translates a Status to proto3.
func ReplicationStatusToProto(s ReplicationStatus) *replicationdatapb.Status {
	replstatuspb := &replicationdatapb.Status{
		Position:                               EncodePosition(s.Position),
		RelayLogPosition:                       EncodePosition(s.RelayLogPosition),
		FilePosition:                           EncodePosition(s.FilePosition),
		RelayLogSourceBinlogEquivalentPosition: EncodePosition(s.RelayLogSourceBinlogEquivalentPosition),
		SourceServerId:                         s.SourceServerID,
		ReplicationLagSeconds:                  s.ReplicationLagSeconds,
		ReplicationLagUnknown:                  s.ReplicationLagUnknown,
		SqlDelay:                               s.SQLDelay,
		RelayLogFilePosition:                   EncodePosition(s.RelayLogFilePosition),
		SourceHost:                             s.SourceHost,
		SourceUser:                             s.SourceUser,
		SourcePort:                             s.SourcePort,
		ConnectRetry:                           s.ConnectRetry,
		SourceUuid:                             s.SourceUUID.String(),
		IoState:                                int32(s.IOState),
		LastIoError:                            s.LastIOError,
		SqlState:                               int32(s.SQLState),
		LastSqlError:                           s.LastSQLError,
		SslAllowed:                             s.SSLAllowed,
		HasReplicationFilters:                  s.HasReplicationFilters,
		AutoPosition:                           s.AutoPosition,
		UsingGtid:                              s.UsingGTID,
	}
	return replstatuspb
}

// ProtoToReplicationStatus translates a proto Status, or panics.
func ProtoToReplicationStatus(s *replicationdatapb.Status) ReplicationStatus {
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
	fileRelayPos, err := DecodePosition(s.RelayLogSourceBinlogEquivalentPosition)
	if err != nil {
		panic(vterrors.Wrapf(err, "cannot decode RelayLogSourceBinlogEquivalentPosition"))
	}
	relayFilePos, err := DecodePosition(s.RelayLogFilePosition)
	if err != nil {
		panic(vterrors.Wrapf(err, "cannot decode RelayLogFilePosition"))
	}
	var sid SID
	if s.SourceUuid != "" {
		sid, err = ParseSID(s.SourceUuid)
		if err != nil {
			panic(vterrors.Wrapf(err, "cannot decode SourceUUID"))
		}
	}
	replstatus := ReplicationStatus{
		Position:                               pos,
		RelayLogPosition:                       relayPos,
		FilePosition:                           filePos,
		RelayLogSourceBinlogEquivalentPosition: fileRelayPos,
		RelayLogFilePosition:                   relayFilePos,
		SourceServerID:                         s.SourceServerId,
		ReplicationLagSeconds:                  s.ReplicationLagSeconds,
		ReplicationLagUnknown:                  s.ReplicationLagUnknown,
		SQLDelay:                               s.SqlDelay,
		SourceHost:                             s.SourceHost,
		SourceUser:                             s.SourceUser,
		SourcePort:                             s.SourcePort,
		ConnectRetry:                           s.ConnectRetry,
		SourceUUID:                             sid,
		IOState:                                ReplicationState(s.IoState),
		LastIOError:                            s.LastIoError,
		SQLState:                               ReplicationState(s.SqlState),
		LastSQLError:                           s.LastSqlError,
		SSLAllowed:                             s.SslAllowed,
		HasReplicationFilters:                  s.HasReplicationFilters,
		AutoPosition:                           s.AutoPosition,
		UsingGTID:                              s.UsingGtid,
	}
	return replstatus
}

// FindErrantGTIDs can be used to find errant GTIDs in the receiver's relay log, by comparing it against all known replicas,
// provided as a list of ReplicationStatus's. This method only works if the flavor for all retrieved ReplicationStatus's is MySQL.
// The result is returned as a Mysql56GTIDSet, each of whose elements is a found errant GTID.
// This function is best effort in nature. If it marks something as errant, then it is for sure errant. But there may be cases of errant GTIDs, which aren't caught by this function.
func (s *ReplicationStatus) FindErrantGTIDs(otherReplicaStatuses []*ReplicationStatus) (Mysql56GTIDSet, error) {
	if len(otherReplicaStatuses) == 0 {
		// If there is nothing to compare this replica against, then we must assume that its GTID set is the correct one.
		return nil, nil
	}

	relayLogSet, ok := s.RelayLogPosition.GTIDSet.(Mysql56GTIDSet)
	if !ok {
		return nil, fmt.Errorf("errant GTIDs can only be computed on the MySQL flavor")
	}

	otherSets := make([]Mysql56GTIDSet, 0, len(otherReplicaStatuses))
	for _, status := range otherReplicaStatuses {
		otherSet, ok := status.RelayLogPosition.GTIDSet.(Mysql56GTIDSet)
		if !ok {
			panic("The receiver ReplicationStatus contained a Mysql56GTIDSet in its relay log, but a replica's ReplicationStatus is of another flavor. This should never happen.")
		}
		otherSets = append(otherSets, otherSet)
	}

	// Copy set for final diffSet so we don't mutate receiver.
	diffSet := make(Mysql56GTIDSet, len(relayLogSet))
	for sid, intervals := range relayLogSet {
		if sid == s.SourceUUID {
			continue
		}
		diffSet[sid] = intervals
	}

	for _, otherSet := range otherSets {
		diffSet = diffSet.Difference(otherSet)
	}

	if len(diffSet) == 0 {
		// If diffSet is empty, then we have no errant GTIDs.
		return nil, nil
	}

	return diffSet, nil
}
