// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package key

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

//
// Uint64Key definitions
//

// Uint64Key is a uint64 that can be converted into a KeyspaceId.
type Uint64Key uint64

func (i Uint64Key) String() string {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint64(i))
	return buf.String()
}

// Bytes returns the keyspace id (as bytes) associated with a Uint64Key.
func (i Uint64Key) Bytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint64(i))
	return buf.Bytes()
}

//
// KeyspaceIdType helper methods
//

// ParseKeyspaceIDType parses the keyspace id type into the enum
func ParseKeyspaceIDType(param string) (pb.KeyspaceIdType, error) {
	if param == "" {
		return pb.KeyspaceIdType_UNSET, nil
	}
	value, ok := pb.KeyspaceIdType_value[strings.ToUpper(param)]
	if !ok {
		return pb.KeyspaceIdType_UNSET, fmt.Errorf("unknown KeyspaceIdType %v", param)
	}
	return pb.KeyspaceIdType(value), nil
}

//
// KeyRange helper methods
//

// KeyRangeContains returns true if the provided id is in the keyrange.
func KeyRangeContains(kr *pb.KeyRange, id []byte) bool {
	if kr == nil {
		return true
	}
	return string(kr.Start) <= string(id) &&
		(len(kr.End) == 0 || string(id) < string(kr.End))
}

// ParseKeyRangeParts parses a start and end hex values and build a proto KeyRange
func ParseKeyRangeParts(start, end string) (*pb.KeyRange, error) {
	s, err := hex.DecodeString(start)
	if err != nil {
		return nil, err
	}
	e, err := hex.DecodeString(end)
	if err != nil {
		return nil, err
	}
	return &pb.KeyRange{Start: s, End: e}, nil
}

// KeyRangeString prints a pb.KeyRange
func KeyRangeString(k *pb.KeyRange) string {
	if k == nil {
		return "<nil>"
	}
	return hex.EncodeToString(k.Start) + "-" + hex.EncodeToString(k.End)
}

// KeyRangeIsPartial returns true if the KeyRange does not cover the entire space.
func KeyRangeIsPartial(kr *pb.KeyRange) bool {
	if kr == nil {
		return false
	}
	return !(len(kr.Start) == 0 && len(kr.End) == 0)
}

// KeyRangeEqual returns true if both key ranges cover the same area
func KeyRangeEqual(left, right *pb.KeyRange) bool {
	if left == nil {
		return right == nil || (len(right.Start) == 0 && len(right.End) == 0)
	}
	if right == nil {
		return len(left.Start) == 0 && len(left.End) == 0
	}
	return string(left.Start) == string(right.Start) &&
		string(left.End) == string(right.End)
}

// KeyRangeStartEqual returns true if both key ranges have the same start
func KeyRangeStartEqual(left, right *pb.KeyRange) bool {
	if left == nil {
		return right == nil || len(right.Start) == 0
	}
	if right == nil {
		return len(left.Start) == 0
	}
	return string(left.Start) == string(right.Start)
}

// KeyRangeEndEqual returns true if both key ranges have the same end
func KeyRangeEndEqual(left, right *pb.KeyRange) bool {
	if left == nil {
		return right == nil || len(right.End) == 0
	}
	if right == nil {
		return len(left.End) == 0
	}
	return string(left.End) == string(right.End)
}

// For more info on the following functions, see:
// See: http://stackoverflow.com/questions/4879315/what-is-a-tidy-algorithm-to-find-overlapping-intervals
// two segments defined as (a,b) and (c,d) (with a<b and c<d):
// intersects = (b > c) && (a < d)
// overlap = min(b, d) - max(c, a)

// KeyRangesIntersect returns true if some Keyspace values exist in both ranges.
func KeyRangesIntersect(first, second *pb.KeyRange) bool {
	if first == nil || second == nil {
		return true
	}
	return (len(first.End) == 0 || string(second.Start) < string(first.End)) &&
		(len(second.End) == 0 || string(first.Start) < string(second.End))
}

// KeyRangesOverlap returns the overlap between two KeyRanges.
// They need to overlap, otherwise an error is returned.
func KeyRangesOverlap(first, second *pb.KeyRange) (*pb.KeyRange, error) {
	if !KeyRangesIntersect(first, second) {
		return nil, fmt.Errorf("KeyRanges %v and %v don't overlap", first, second)
	}
	if first == nil {
		return second, nil
	}
	if second == nil {
		return first, nil
	}
	// compute max(c,a) and min(b,d)
	// start with (a,b)
	result := &(*first)
	// if c > a, then use c
	if string(second.Start) > string(first.Start) {
		result.Start = second.Start
	}
	// if b is maxed out, or
	// (d is not maxed out and d < b)
	//                           ^ valid test as neither b nor d are max
	// then use d
	if len(first.End) == 0 || (len(second.End) != 0 && string(second.End) < string(first.End)) {
		result.End = second.End
	}
	return result, nil
}

// ParseShardingSpec parses a string that describes a sharding
// specification. a-b-c-d will be parsed as a-b, b-c, c-d. The empty
// string may serve both as the start and end of the keyspace: -a-b-
// will be parsed as start-a, a-b, b-end.
func ParseShardingSpec(spec string) ([]*pb.KeyRange, error) {
	parts := strings.Split(spec, "-")
	if len(parts) == 1 {
		return nil, fmt.Errorf("malformed spec: doesn't define a range: %q", spec)
	}
	old := parts[0]
	ranges := make([]*pb.KeyRange, len(parts)-1)

	for i, p := range parts[1:] {
		if p == "" && i != (len(parts)-2) {
			return nil, fmt.Errorf("malformed spec: MinKey/MaxKey cannot be in the middle of the spec: %q", spec)
		}
		if p != "" && p <= old {
			return nil, fmt.Errorf("malformed spec: shard limits should be in order: %q", spec)
		}
		s, err := hex.DecodeString(old)
		if err != nil {
			return nil, err
		}
		if len(s) == 0 {
			s = nil
		}
		e, err := hex.DecodeString(p)
		if err != nil {
			return nil, err
		}
		if len(e) == 0 {
			e = nil
		}
		ranges[i] = &pb.KeyRange{Start: s, End: e}
		old = p
	}
	return ranges, nil
}
