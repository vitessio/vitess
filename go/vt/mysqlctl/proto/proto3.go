// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"

	pb "github.com/youtube/vitess/go/vt/proto/replicationdata"
)

// ReplicationPositionToProto translates a ReplicationPosition to
// proto, or panics
func ReplicationPositionToProto(rp ReplicationPosition) *pb.Position {
	switch gtid := rp.GTIDSet.(type) {
	case MariadbGTID:
		return &pb.Position{
			MariadbGtid: &pb.MariadbGtid{
				Domain:   gtid.Domain,
				Server:   gtid.Server,
				Sequence: gtid.Sequence,
			},
		}
	case Mysql56GTIDSet:
		result := &pb.Position{
			MysqlGtidSet: &pb.MysqlGtidSet{},
		}
		for k, v := range gtid {
			s := &pb.MysqlGtidSet_MysqlUuidSet{
				Interval: make([]*pb.MysqlGtidSet_MysqlInterval, len(v)),
			}
			s.Uuid = make([]byte, len(k))
			for i, b := range k {
				s.Uuid[i] = b
			}
			for i, in := range v {
				s.Interval[i] = &pb.MysqlGtidSet_MysqlInterval{
					First: uint64(in.start),
					Last:  uint64(in.end),
				}
			}
			result.MysqlGtidSet.UuidSet = append(result.MysqlGtidSet.UuidSet, s)
		}
		return result
	default:
		panic(fmt.Errorf("can't convert ReplicationPosition to proto: %#v", rp))
	}
}

// ProtoToReplicationPosition translates a proto ReplicationPosition, or panics
func ProtoToReplicationPosition(rp *pb.Position) ReplicationPosition {
	if rp.MariadbGtid != nil {
		return ReplicationPosition{
			GTIDSet: MariadbGTID{
				Domain:   rp.MariadbGtid.Domain,
				Server:   rp.MariadbGtid.Server,
				Sequence: rp.MariadbGtid.Sequence,
			},
		}
	}
	if rp.MysqlGtidSet != nil {
		result := ReplicationPosition{
			GTIDSet: Mysql56GTIDSet(make(map[SID][]interval)),
		}
		for _, s := range rp.MysqlGtidSet.UuidSet {
			if len(s.Uuid) != 16 {
				panic(fmt.Errorf("invalid MysqlGtidSet Uuid length: %v", len(s.Uuid)))
			}
			var sid SID
			for i, b := range s.Uuid {
				sid[i] = b
			}
			ins := make([]interval, len(s.Interval))
			for i, in := range s.Interval {
				ins[i].start = int64(in.First)
				ins[i].end = int64(in.Last)
			}
		}
		return result
	}

	panic(fmt.Errorf("can't convert ReplicationPosition from proto: %#v", rp))
}
