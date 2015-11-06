// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package key

import pb "github.com/youtube/vitess/go/vt/proto/topodata"

// This file contains the functions to convert topo data to and from proto3

// KeyRangeToProto translates a KeyRange to proto, or panics
func KeyRangeToProto(k KeyRange) *pb.KeyRange {
	return &pb.KeyRange{
		Start: []byte(k.Start),
		End:   []byte(k.End),
	}
}

// ProtoToKeyRange translates a proto KeyRange, or panics
func ProtoToKeyRange(k *pb.KeyRange) KeyRange {
	if k == nil {
		return KeyRange{}
	}
	return KeyRange{
		Start: KeyspaceId(k.Start),
		End:   KeyspaceId(k.End),
	}
}

// KeyRangesToProto translates an array of KeyRange to proto
func KeyRangesToProto(ks []KeyRange) []*pb.KeyRange {
	if len(ks) == 0 {
		return nil
	}
	result := make([]*pb.KeyRange, len(ks))
	for i, k := range ks {
		result[i] = KeyRangeToProto(k)
	}
	return result
}

// ProtoToKeyRanges translates a proto into an array of KeyRanges
func ProtoToKeyRanges(ks []*pb.KeyRange) []KeyRange {
	if len(ks) == 0 {
		return nil
	}
	result := make([]KeyRange, len(ks))
	for i, k := range ks {
		result[i] = ProtoToKeyRange(k)
	}
	return result
}

// KeyspaceIdsToProto translates an array of KeyspaceId to proto
func KeyspaceIdsToProto(l []KeyspaceId) [][]byte {
	if len(l) == 0 {
		return nil
	}
	result := make([][]byte, len(l))
	for i, k := range l {
		result[i] = []byte(k)
	}
	return result
}

// ProtoToKeyspaceIds translates a proto into an array of KeyspaceIds
func ProtoToKeyspaceIds(l [][]byte) []KeyspaceId {
	if len(l) == 0 {
		return nil
	}
	result := make([]KeyspaceId, len(l))
	for i, k := range l {
		result[i] = KeyspaceId(k)
	}
	return result
}
