/*
Copyright 2017 Google Inc.

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

package mysqlctl

import (
	"fmt"

	"github.com/youtube/vitess/go/mysql"
	replicationdatapb "github.com/youtube/vitess/go/vt/proto/replicationdata"
)

// StatusToProto translates a Status to proto3
func StatusToProto(r Status) *replicationdatapb.Status {
	return &replicationdatapb.Status{
		Position:            mysql.EncodePosition(r.Position),
		SlaveIoRunning:      r.SlaveIORunning,
		SlaveSqlRunning:     r.SlaveSQLRunning,
		SecondsBehindMaster: uint32(r.SecondsBehindMaster),
		MasterHost:          r.MasterHost,
		MasterPort:          int32(r.MasterPort),
		MasterConnectRetry:  int32(r.MasterConnectRetry),
	}
}

// ProtoToStatus translates a proto Status, or panics
func ProtoToStatus(r *replicationdatapb.Status) Status {
	pos, err := mysql.DecodePosition(r.Position)
	if err != nil {
		panic(fmt.Errorf("cannot decode Position: %v", err))
	}
	return Status{
		Position:            pos,
		SlaveIORunning:      r.SlaveIoRunning,
		SlaveSQLRunning:     r.SlaveSqlRunning,
		SecondsBehindMaster: uint(r.SecondsBehindMaster),
		MasterHost:          r.MasterHost,
		MasterPort:          int(r.MasterPort),
		MasterConnectRetry:  int(r.MasterConnectRetry),
	}
}
