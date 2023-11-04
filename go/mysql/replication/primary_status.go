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

package replication

import (
	"fmt"

	"vitess.io/vitess/go/vt/log"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	"vitess.io/vitess/go/vt/vterrors"
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

func ParseMysqlPrimaryStatus(resultMap map[string]string) (PrimaryStatus, error) {
	status := ParsePrimaryStatus(resultMap)

	var err error
	status.Position.GTIDSet, err = ParseMysql56GTIDSet(resultMap["Executed_Gtid_Set"])
	if err != nil {
		return PrimaryStatus{}, vterrors.Wrapf(err, "PrimaryStatus can't parse MySQL 5.6 GTID (Executed_Gtid_Set: %#v)", resultMap["Executed_Gtid_Set"])
	}

	return status, nil
}

// ParsePrimaryStatus parses the common fields of SHOW MASTER STATUS.
func ParsePrimaryStatus(fields map[string]string) PrimaryStatus {
	status := PrimaryStatus{}

	fileExecPosStr := fields["Position"]
	file := fields["File"]
	if file != "" && fileExecPosStr != "" {
		var err error
		status.FilePosition.GTIDSet, err = ParseFilePosGTIDSet(fmt.Sprintf("%s:%s", file, fileExecPosStr))
		if err != nil {
			log.Warningf("Error parsing GTID set %s:%s: %v", file, fileExecPosStr, err)
		}
	}

	return status
}
