/*
Copyright 2020 The Vitess Authors.

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
)

// MasterStatus holds replication information from SHOW MASTER STATUS.
type MasterStatus struct {
	// Position represents the master's GTID based position.
	Position Position
	// FilePosition represents the master's file based position.
	FilePosition Position
}

// MasterStatusToProto translates a MasterStatus to proto3.
func MasterStatusToProto(s MasterStatus) *replicationdatapb.MasterStatus {
	return &replicationdatapb.MasterStatus{
		Position:     EncodePosition(s.Position),
		FilePosition: EncodePosition(s.FilePosition),
	}
}
