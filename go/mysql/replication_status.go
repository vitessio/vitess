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
	Position Position
	// RelayLogPosition is the Position that the replica would be at if it
	// were to finish executing everything that's currently in its relay log.
	// However, some MySQL flavors don't expose this information,
	// in which case RelayLogPosition.IsZero() will be true.
	RelayLogPosition      Position
	FilePosition          Position
	FileRelayLogPosition  Position
	SourceServerID        uint
	IOThreadRunning       bool
	SQLThreadRunning      bool
	ReplicationLagSeconds uint
	SourceHost            string
	SourcePort            int
	ConnectRetry          int
	SourceUUID            SID
}

// ReplicationRunning returns true iff both the IO and SQL threads are
// running.
func (s *ReplicationStatus) ReplicationRunning() bool {
	return s.IOThreadRunning && s.SQLThreadRunning
}

// ReplicationStatusToProto translates a Status to proto3.
func ReplicationStatusToProto(s ReplicationStatus) *replicationdatapb.Status {
	return &replicationdatapb.Status{
		Position:              EncodePosition(s.Position),
		RelayLogPosition:      EncodePosition(s.RelayLogPosition),
		FilePosition:          EncodePosition(s.FilePosition),
		FileRelayLogPosition:  EncodePosition(s.FileRelayLogPosition),
		SourceServerId:        uint32(s.SourceServerID),
		IoThreadRunning:       s.IOThreadRunning,
		SqlThreadRunning:      s.SQLThreadRunning,
		ReplicationLagSeconds: uint32(s.ReplicationLagSeconds),
		SourceHost:            s.SourceHost,
		SourcePort:            int32(s.SourcePort),
		ConnectRetry:          int32(s.ConnectRetry),
		SourceUuid:            s.SourceUUID.String(),
	}
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
	fileRelayPos, err := DecodePosition(s.FileRelayLogPosition)
	if err != nil {
		panic(vterrors.Wrapf(err, "cannot decode FileRelayLogPosition"))
	}
	var sid SID
	if s.SourceUuid != "" {
		sid, err = ParseSID(s.SourceUuid)
		if err != nil {
			panic(vterrors.Wrapf(err, "cannot decode SourceUUID"))
		}
	}
	return ReplicationStatus{
		Position:              pos,
		RelayLogPosition:      relayPos,
		FilePosition:          filePos,
		FileRelayLogPosition:  fileRelayPos,
		SourceServerID:        uint(s.SourceServerId),
		IOThreadRunning:       s.IoThreadRunning,
		SQLThreadRunning:      s.SqlThreadRunning,
		ReplicationLagSeconds: uint(s.ReplicationLagSeconds),
		SourceHost:            s.SourceHost,
		SourcePort:            int(s.SourcePort),
		ConnectRetry:          int(s.ConnectRetry),
		SourceUUID:            sid,
	}
}

// FindErrantGTIDs can be used to find errant GTIDs in the receiver's relay log, by comparing it against all known replicas,
// provided as a list of ReplicationStatus's. This method only works if the flavor for all retrieved ReplicationStatus's is MySQL.
// The result is returned as a Mysql56GTIDSet, each of whose elements is a found errant GTID.
func (s *ReplicationStatus) FindErrantGTIDs(otherReplicaStatuses []*ReplicationStatus) (Mysql56GTIDSet, error) {
	set, ok := s.RelayLogPosition.GTIDSet.(Mysql56GTIDSet)
	if !ok {
		return nil, fmt.Errorf("errant GTIDs can only be computed on the MySQL flavor")
	}

	otherSets := make([]Mysql56GTIDSet, 0, len(otherReplicaStatuses))
	for _, status := range otherReplicaStatuses {
		otherSet, ok := status.RelayLogPosition.GTIDSet.(Mysql56GTIDSet)
		if !ok {
			panic("The receiver ReplicationStatus contained a Mysql56GTIDSet in its relay log, but a replica's ReplicationStatus is of another flavor. This should never happen.")
		}
		// Copy and throw out master SID from consideration, so we don't mutate input.
		otherSetNoMasterSID := make(Mysql56GTIDSet, len(otherSet))
		for sid, intervals := range otherSet {
			if sid == status.SourceUUID {
				continue
			}
			otherSetNoMasterSID[sid] = intervals
		}

		otherSets = append(otherSets, otherSetNoMasterSID)
	}

	// Copy set for final diffSet so we don't mutate receiver.
	diffSet := make(Mysql56GTIDSet, len(set))
	for sid, intervals := range set {
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
