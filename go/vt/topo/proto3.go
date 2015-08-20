// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"fmt"
	"strings"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains the methods to convert topo structures to proto3.
// Eventually we will use the proto3 data structures directly.

// TabletTypeToProto turns a TabletType into a proto
func TabletTypeToProto(t TabletType) pb.TabletType {
	if result, err := topoproto.ParseTabletType(string(t)); err != nil {
		panic(fmt.Errorf("unknown tablet type: %v", t))
	} else {
		return result
	}
}

// ProtoToTabletType turns a proto to a TabletType
func ProtoToTabletType(t pb.TabletType) TabletType {
	return TabletType(strings.ToLower(pb.TabletType_name[int32(t)]))
}

// SrvKeyspaceToProto turns a Tablet into a proto
func SrvKeyspaceToProto(s *SrvKeyspace) *pb.SrvKeyspace {
	result := &pb.SrvKeyspace{
		ShardingColumnName: s.ShardingColumnName,
		ShardingColumnType: key.KeyspaceIdTypeToProto(s.ShardingColumnType),
		SplitShardCount:    s.SplitShardCount,
	}
	for tt, p := range s.Partitions {
		partition := &pb.SrvKeyspace_KeyspacePartition{
			ServedType: TabletTypeToProto(tt),
		}
		for _, sr := range p.ShardReferences {
			partition.ShardReferences = append(partition.ShardReferences, &pb.ShardReference{
				Name:     sr.Name,
				KeyRange: key.KeyRangeToProto(sr.KeyRange),
			})
		}
		result.Partitions = append(result.Partitions, partition)
	}
	for tt, k := range s.ServedFrom {
		result.ServedFrom = append(result.ServedFrom, &pb.SrvKeyspace_ServedFrom{
			TabletType: TabletTypeToProto(tt),
			Keyspace:   k,
		})
	}
	return result
}

// ProtoToSrvKeyspace turns a proto to a Tablet
func ProtoToSrvKeyspace(s *pb.SrvKeyspace) *SrvKeyspace {
	result := &SrvKeyspace{
		Partitions:         make(map[TabletType]*KeyspacePartition),
		ShardingColumnName: s.ShardingColumnName,
		ShardingColumnType: key.ProtoToKeyspaceIdType(s.ShardingColumnType),
		SplitShardCount:    s.SplitShardCount,
	}
	for _, p := range s.Partitions {
		tt := ProtoToTabletType(p.ServedType)
		partition := &KeyspacePartition{}
		for _, sr := range p.ShardReferences {
			partition.ShardReferences = append(partition.ShardReferences, ShardReference{
				Name:     sr.Name,
				KeyRange: key.ProtoToKeyRange(sr.KeyRange),
			})
		}
		result.Partitions[tt] = partition
	}
	if len(s.ServedFrom) > 0 {
		result.ServedFrom = make(map[TabletType]string)
		for _, sf := range s.ServedFrom {
			tt := ProtoToTabletType(sf.TabletType)
			result.ServedFrom[tt] = sf.Keyspace
		}
	}
	return result
}
