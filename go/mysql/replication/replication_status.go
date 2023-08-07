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

package replication

import (
	"fmt"
	"strconv"

	"vitess.io/vitess/go/vt/log"
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

func ParseMysqlReplicationStatus(resultMap map[string]string) (ReplicationStatus, error) {
	status := ParseReplicationStatus(resultMap)
	uuidString := resultMap["Master_UUID"]
	if uuidString != "" {
		sid, err := ParseSID(uuidString)
		if err != nil {
			return ReplicationStatus{}, vterrors.Wrapf(err, "cannot decode SourceUUID")
		}
		status.SourceUUID = sid
	}

	var err error
	status.Position.GTIDSet, err = ParseMysql56GTIDSet(resultMap["Executed_Gtid_Set"])
	if err != nil {
		return ReplicationStatus{}, vterrors.Wrapf(err, "ReplicationStatus can't parse MySQL 5.6 GTID (Executed_Gtid_Set: %#v)", resultMap["Executed_Gtid_Set"])
	}
	relayLogGTIDSet, err := ParseMysql56GTIDSet(resultMap["Retrieved_Gtid_Set"])
	if err != nil {
		return ReplicationStatus{}, vterrors.Wrapf(err, "ReplicationStatus can't parse MySQL 5.6 GTID (Retrieved_Gtid_Set: %#v)", resultMap["Retrieved_Gtid_Set"])
	}
	// We take the union of the executed and retrieved gtidset, because the retrieved gtidset only represents GTIDs since
	// the relay log has been reset. To get the full Position, we need to take a union of executed GTIDSets, since these would
	// have been in the relay log's GTIDSet in the past, prior to a reset.
	status.RelayLogPosition.GTIDSet = status.Position.GTIDSet.Union(relayLogGTIDSet)

	return status, nil
}

func ParseMariadbReplicationStatus(resultMap map[string]string) (ReplicationStatus, error) {
	status := ParseReplicationStatus(resultMap)

	var err error
	status.Position.GTIDSet, err = ParseMariadbGTIDSet(resultMap["Gtid_Slave_Pos"])
	if err != nil {
		return ReplicationStatus{}, vterrors.Wrapf(err, "ReplicationStatus can't parse MariaDB GTID (Gtid_Slave_Pos: %#v)", resultMap["Gtid_Slave_Pos"])
	}

	return status, nil
}

func ParseFilePosReplicationStatus(resultMap map[string]string) (ReplicationStatus, error) {
	status := ParseReplicationStatus(resultMap)

	status.Position = status.FilePosition
	status.RelayLogPosition = status.RelayLogSourceBinlogEquivalentPosition

	return status, nil
}

func ParseFilePosPrimaryStatus(resultMap map[string]string) (PrimaryStatus, error) {
	status := ParsePrimaryStatus(resultMap)

	status.Position = status.FilePosition

	return status, nil
}

// ParseReplicationStatus parses the common (non-flavor-specific) fields of ReplicationStatus
func ParseReplicationStatus(fields map[string]string) ReplicationStatus {
	// The field names in the map are identical to what we receive from the database
	// Hence the names still contain Master
	status := ReplicationStatus{
		SourceHost:            fields["Master_Host"],
		SourceUser:            fields["Master_User"],
		SSLAllowed:            fields["Master_SSL_Allowed"] == "Yes",
		AutoPosition:          fields["Auto_Position"] == "1",
		UsingGTID:             fields["Using_Gtid"] != "No" && fields["Using_Gtid"] != "",
		HasReplicationFilters: (fields["Replicate_Do_DB"] != "") || (fields["Replicate_Ignore_DB"] != "") || (fields["Replicate_Do_Table"] != "") || (fields["Replicate_Ignore_Table"] != "") || (fields["Replicate_Wild_Do_Table"] != "") || (fields["Replicate_Wild_Ignore_Table"] != ""),
		// These fields are returned from the underlying DB and cannot be renamed
		IOState:      ReplicationStatusToState(fields["Slave_IO_Running"]),
		LastIOError:  fields["Last_IO_Error"],
		SQLState:     ReplicationStatusToState(fields["Slave_SQL_Running"]),
		LastSQLError: fields["Last_SQL_Error"],
	}
	parseInt, _ := strconv.ParseInt(fields["Master_Port"], 10, 32)
	status.SourcePort = int32(parseInt)
	parseInt, _ = strconv.ParseInt(fields["Connect_Retry"], 10, 32)
	status.ConnectRetry = int32(parseInt)
	parseUint, err := strconv.ParseUint(fields["Seconds_Behind_Master"], 10, 32)
	if err != nil {
		// we could not parse the value into a valid uint32 -- most commonly because the value is NULL from the
		// database -- so let's reflect that the underlying value was unknown on our last check
		status.ReplicationLagUnknown = true
	} else {
		status.ReplicationLagUnknown = false
		status.ReplicationLagSeconds = uint32(parseUint)
	}
	parseUint, _ = strconv.ParseUint(fields["Master_Server_Id"], 10, 32)
	status.SourceServerID = uint32(parseUint)
	parseUint, _ = strconv.ParseUint(fields["SQL_Delay"], 10, 32)
	status.SQLDelay = uint32(parseUint)

	executedPosStr := fields["Exec_Master_Log_Pos"]
	file := fields["Relay_Master_Log_File"]
	if file != "" && executedPosStr != "" {
		status.FilePosition.GTIDSet, err = ParseFilePosGTIDSet(fmt.Sprintf("%s:%s", file, executedPosStr))
		if err != nil {
			log.Warningf("Error parsing GTID set %s:%s: %v", file, executedPosStr, err)
		}
	}

	readPosStr := fields["Read_Master_Log_Pos"]
	file = fields["Master_Log_File"]
	if file != "" && readPosStr != "" {
		status.RelayLogSourceBinlogEquivalentPosition.GTIDSet, err = ParseFilePosGTIDSet(fmt.Sprintf("%s:%s", file, readPosStr))
		if err != nil {
			log.Warningf("Error parsing GTID set %s:%s: %v", file, readPosStr, err)
		}
	}

	relayPosStr := fields["Relay_Log_Pos"]
	file = fields["Relay_Log_File"]
	if file != "" && relayPosStr != "" {
		status.RelayLogFilePosition.GTIDSet, err = ParseFilePosGTIDSet(fmt.Sprintf("%s:%s", file, relayPosStr))
		if err != nil {
			log.Warningf("Error parsing GTID set %s:%s: %v", file, relayPosStr, err)
		}
	}
	return status
}
