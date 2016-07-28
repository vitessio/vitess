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

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

//
// Uint64Key definitions
//

// Uint64Key is a uint64 that can be converted into a KeyspaceId.
type Uint64Key uint64

func (i Uint64Key) String() string {
	return string(i.Bytes())
}

// Bytes returns the keyspace id (as bytes) associated with a Uint64Key.
func (i Uint64Key) Bytes() []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

//
// KeyspaceIdType helper methods
//

// ParseKeyspaceIDType parses the keyspace id type into the enum
func ParseKeyspaceIDType(param string) (topodatapb.KeyspaceIdType, error) {
	if param == "" {
		return topodatapb.KeyspaceIdType_UNSET, nil
	}
	value, ok := topodatapb.KeyspaceIdType_value[strings.ToUpper(param)]
	if !ok {
		return topodatapb.KeyspaceIdType_UNSET, fmt.Errorf("unknown KeyspaceIdType %v", param)
	}
	return topodatapb.KeyspaceIdType(value), nil
}

//
// KeyRange helper methods
//

// KeyRangeContains returns true if the provided id is in the keyrange.
func KeyRangeContains(kr *topodatapb.KeyRange, id []byte) bool {
	if kr == nil {
		return true
	}
	return bytes.Compare(kr.Start, id) <= 0 &&
		(len(kr.End) == 0 || bytes.Compare(id, kr.End) < 0)
}

// ParseKeyRangeParts parses a start and end hex values and build a proto KeyRange
func ParseKeyRangeParts(start, end string) (*topodatapb.KeyRange, error) {
	s, err := hex.DecodeString(start)
	if err != nil {
		return nil, err
	}
	e, err := hex.DecodeString(end)
	if err != nil {
		return nil, err
	}
	return &topodatapb.KeyRange{Start: s, End: e}, nil
}

// KeyRangeString prints a topodatapb.KeyRange
func KeyRangeString(k *topodatapb.KeyRange) string {
	if k == nil {
		return "<nil>"
	}
	return hex.EncodeToString(k.Start) + "-" + hex.EncodeToString(k.End)
}

// KeyRangeIsPartial returns true if the KeyRange does not cover the entire space.
func KeyRangeIsPartial(kr *topodatapb.KeyRange) bool {
	if kr == nil {
		return false
	}
	return !(len(kr.Start) == 0 && len(kr.End) == 0)
}

// KeyRangeEqual returns true if both key ranges cover the same area
func KeyRangeEqual(left, right *topodatapb.KeyRange) bool {
	if left == nil {
		return right == nil || (len(right.Start) == 0 && len(right.End) == 0)
	}
	if right == nil {
		return len(left.Start) == 0 && len(left.End) == 0
	}
	return bytes.Compare(left.Start, right.Start) == 0 &&
		bytes.Compare(left.End, right.End) == 0
}

// KeyRangeStartEqual returns true if both key ranges have the same start
func KeyRangeStartEqual(left, right *topodatapb.KeyRange) bool {
	if left == nil {
		return right == nil || len(right.Start) == 0
	}
	if right == nil {
		return len(left.Start) == 0
	}
	return bytes.Compare(left.Start, right.Start) == 0
}

// KeyRangeEndEqual returns true if both key ranges have the same end
func KeyRangeEndEqual(left, right *topodatapb.KeyRange) bool {
	if left == nil {
		return right == nil || len(right.End) == 0
	}
	if right == nil {
		return len(left.End) == 0
	}
	return bytes.Compare(left.End, right.End) == 0
}

// For more info on the following functions, see:
// See: http://stackoverflow.com/questions/4879315/what-is-a-tidy-algorithm-to-find-overlapping-intervals
// two segments defined as (a,b) and (c,d) (with a<b and c<d):
// intersects = (b > c) && (a < d)
// overlap = min(b, d) - max(c, a)

// KeyRangesIntersect returns true if some Keyspace values exist in both ranges.
func KeyRangesIntersect(first, second *topodatapb.KeyRange) bool {
	if first == nil || second == nil {
		return true
	}
	return (len(first.End) == 0 || bytes.Compare(second.Start, first.End) < 0) &&
		(len(second.End) == 0 || bytes.Compare(first.Start, second.End) < 0)
}

// KeyRangesOverlap returns the overlap between two KeyRanges.
// They need to overlap, otherwise an error is returned.
func KeyRangesOverlap(first, second *topodatapb.KeyRange) (*topodatapb.KeyRange, error) {
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
	result := *first
	// if c > a, then use c
	if bytes.Compare(second.Start, first.Start) > 0 {
		result.Start = second.Start
	}
	// if b is maxed out, or
	// (d is not maxed out and d < b)
	//                           ^ valid test as neither b nor d are max
	// then use d
	if len(first.End) == 0 || (len(second.End) != 0 && bytes.Compare(second.End, first.End) < 0) {
		result.End = second.End
	}
	return &result, nil
}

// KeyRangeIncludes returns true if the first provided KeyRange, big,
// contains the second KeyRange, small. If they intersect, but small
// spills out, this returns false.
func KeyRangeIncludes(big, small *topodatapb.KeyRange) bool {
	if big == nil {
		// The outside one covers everything, we're good.
		return true
	}
	if small == nil {
		// The smaller one covers everything, better have the
		// bigger one also cover everything.
		return len(big.Start) == 0 && len(big.End) == 0
	}
	// Now we check small.Start >= big.Start, and small.End <= big.End
	if len(big.Start) != 0 && bytes.Compare(small.Start, big.Start) < 0 {
		return false
	}
	if len(big.End) != 0 && (len(small.End) == 0 || bytes.Compare(small.End, big.End) > 0) {
		return false
	}
	return true
}

// ParseShardingSpec parses a string that describes a sharding
// specification. a-b-c-d will be parsed as a-b, b-c, c-d. The empty
// string may serve both as the start and end of the keyspace: -a-b-
// will be parsed as start-a, a-b, b-end.
func ParseShardingSpec(spec string) ([]*topodatapb.KeyRange, error) {
	parts := strings.Split(spec, "-")
	if len(parts) == 1 {
		return nil, fmt.Errorf("malformed spec: doesn't define a range: %q", spec)
	}
	old := parts[0]
	ranges := make([]*topodatapb.KeyRange, len(parts)-1)

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
		ranges[i] = &topodatapb.KeyRange{Start: s, End: e}
		old = p
	}
	return ranges, nil
}
