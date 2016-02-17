// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package replication

import (
	"fmt"

	replicationdatapb "github.com/youtube/vitess/go/vt/proto/replicationdata"
)

// StatusToProto translates a Status to proto3
func StatusToProto(r Status) *replicationdatapb.Status {
	return &replicationdatapb.Status{
		Position:            EncodePosition(r.Position),
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
	pos, err := DecodePosition(r.Position)
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
