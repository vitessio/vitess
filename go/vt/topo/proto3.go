// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"fmt"
	"strings"

	"github.com/youtube/vitess/go/vt/key"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains the methods to convert topo structures to proto3.
// Eventually we will use the proto3 data structures directly.

// TabletAliasToProto turns a TabletAlias into a proto
func TabletAliasToProto(t TabletAlias) *pb.TabletAlias {
	return &pb.TabletAlias{
		Cell: t.Cell,
		Uid:  t.Uid,
	}
}

// ProtoToTabletAlias turns a proto to a TabletAlias
func ProtoToTabletAlias(t *pb.TabletAlias) TabletAlias {
	if t == nil {
		return TabletAlias{}
	}
	return TabletAlias{
		Cell: t.Cell,
		Uid:  t.Uid,
	}
}

// TabletTypeToProto turns a TabletType into a proto
func TabletTypeToProto(t TabletType) pb.TabletType {
	if result, ok := pb.TabletType_value[strings.ToUpper(string(t))]; ok {
		return pb.TabletType(result)
	}
	panic(fmt.Errorf("unknown tablet type: %v", t))
}

// ProtoToTabletType turns a proto to a TabletType
func ProtoToTabletType(t pb.TabletType) TabletType {
	return TabletType(strings.ToLower(pb.TabletType_name[int32(t)]))
}

// TabletToProto turns a Tablet into a proto
func TabletToProto(t *Tablet) *pb.Tablet {
	result := &pb.Tablet{
		Alias:          TabletAliasToProto(t.Alias),
		Hostname:       t.Hostname,
		Ip:             t.IPAddr,
		PortMap:        make(map[string]int32),
		Keyspace:       t.Keyspace,
		Shard:          t.Shard,
		KeyRange:       key.KeyRangeToProto(t.KeyRange),
		Type:           TabletTypeToProto(t.Type),
		DbNameOverride: t.DbNameOverride,
		Tags:           t.Tags,
		HealthMap:      t.Health,
	}
	for k, v := range t.Portmap {
		result.PortMap[k] = int32(v)
	}
	return result
}

// ProtoToTablet turns a proto to a Tablet
func ProtoToTablet(t *pb.Tablet) *Tablet {
	result := &Tablet{
		Alias:          ProtoToTabletAlias(t.Alias),
		Hostname:       t.Hostname,
		IPAddr:         t.Ip,
		Portmap:        make(map[string]int),
		Keyspace:       t.Keyspace,
		Shard:          t.Shard,
		KeyRange:       key.ProtoToKeyRange(t.KeyRange),
		Type:           ProtoToTabletType(t.Type),
		DbNameOverride: t.DbNameOverride,
		Tags:           t.Tags,
		Health:         t.HealthMap,
	}
	for k, v := range t.PortMap {
		result.Portmap[k] = int(v)
	}
	return result
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
