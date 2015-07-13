// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package key

import (
	"fmt"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains the functions to convert topo data to and from proto3

// KeyspaceIdTypeToProto translates a KeyspaceIdType to proto, or panics
func KeyspaceIdTypeToProto(k KeyspaceIdType) pb.KeyspaceIdType {
	switch k {
	case KIT_UNSET:
		return pb.KeyspaceIdType_UNSET
	case KIT_UINT64:
		return pb.KeyspaceIdType_UINT64
	case KIT_BYTES:
		return pb.KeyspaceIdType_BYTES
	}
	panic(fmt.Errorf("Invalid value for KeyspaceIdType: %v", k))
}

// ProtoToKeyspaceIdType translates a proto KeyspaceIdType, or panics
func ProtoToKeyspaceIdType(k pb.KeyspaceIdType) KeyspaceIdType {
	switch k {
	case pb.KeyspaceIdType_UNSET:
		return KIT_UNSET
	case pb.KeyspaceIdType_UINT64:
		return KIT_UINT64
	case pb.KeyspaceIdType_BYTES:
		return KIT_BYTES
	}
	panic(fmt.Errorf("Invalid value for KeyspaceIdType: %v", k))
}

// KeyRangeToProto translates a KeyRange to proto, or panics
func KeyRangeToProto(k KeyRange) *pb.KeyRange {
	return &pb.KeyRange{
		Start: []byte(k.Start),
		End:   []byte(k.End),
	}
}

// ProtoToKeyRange translates a proto KeyRange, or panics
func ProtoToKeyRange(k *pb.KeyRange) KeyRange {
	return KeyRange{
		Start: KeyspaceId(k.Start),
		End:   KeyspaceId(k.End),
	}
}
