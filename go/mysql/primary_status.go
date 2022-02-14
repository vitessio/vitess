package mysql

import (
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
)

// PrimaryStatus holds replication information from SHOW MASTER STATUS.
type PrimaryStatus struct {
	// Position represents the server's GTID based position.
	Position Position
	// FilePosition represents the server's file based position.
	FilePosition Position
}

// PrimaryStatusToProto translates a PrimaryStatus to proto3.
func PrimaryStatusToProto(s PrimaryStatus) *replicationdatapb.PrimaryStatus {
	return &replicationdatapb.PrimaryStatus{
		Position:     EncodePosition(s.Position),
		FilePosition: EncodePosition(s.FilePosition),
	}
}
