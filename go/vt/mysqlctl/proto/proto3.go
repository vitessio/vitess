// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"

	pb "github.com/youtube/vitess/go/vt/proto/replicationdata"
)

// ReplicationStatusToProto translates a ReplicationStatus to
// proto, or panics
func ReplicationStatusToProto(r ReplicationStatus) *pb.Status {
	return &pb.Status{
		Position:            EncodeReplicationPosition(r.Position),
		SlaveIoRunning:      r.SlaveIORunning,
		SlaveSqlRunning:     r.SlaveSQLRunning,
		SecondsBehindMaster: uint32(r.SecondsBehindMaster),
		MasterHost:          r.MasterHost,
		MasterPort:          int32(r.MasterPort),
		MasterConnectRetry:  int32(r.MasterConnectRetry),
	}
}

// ProtoToReplicationStatus translates a proto ReplicationStatus, or panics
func ProtoToReplicationStatus(r *pb.Status) ReplicationStatus {
	pos, err := DecodeReplicationPosition(r.Position)
	if err != nil {
		panic(fmt.Errorf("cannot decode Position: %v", err))
	}
	return ReplicationStatus{
		Position:            pos,
		SlaveIORunning:      r.SlaveIoRunning,
		SlaveSQLRunning:     r.SlaveSqlRunning,
		SecondsBehindMaster: uint(r.SecondsBehindMaster),
		MasterHost:          r.MasterHost,
		MasterPort:          int(r.MasterPort),
		MasterConnectRetry:  int(r.MasterConnectRetry),
	}
}
