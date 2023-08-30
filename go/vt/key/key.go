/*
Copyright 2019 The Vitess Authors.

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

package key

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"

	"google.golang.org/protobuf/proto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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
// KeyRange helper methods
//

// EvenShardsKeyRange returns a key range definition for a shard at index "i",
// assuming range based sharding with "n" equal-width shards in total.
// i starts at 0.
//
// Example: (1, 2) returns the second out of two shards in total i.e. "80-".
//
// This function must not be used in the Vitess code base because Vitess also
// supports shards with different widths. In that case, the output of this
// function would be wrong.
//
// Note: start and end values have trailing zero bytes omitted.
// For example, "80-" has only the first byte (0x80) set.
// We do this to produce the same KeyRange objects as ParseKeyRangeParts() does.
// Because it's using the Go hex methods, it's omitting trailing zero bytes as
// well.
func EvenShardsKeyRange(i, n int) (*topodatapb.KeyRange, error) {
	if n <= 0 {
		return nil, fmt.Errorf("the shard count must be > 0: %v", n)
	}
	if i >= n {
		return nil, fmt.Errorf("the index of the shard must be less than the total number of shards: %v < %v", i, n)
	}
	if n&(n-1) != 0 {
		return nil, fmt.Errorf("the shard count must be a power of two: %v", n)
	}

	// Determine the number of bytes which are required to represent any
	// KeyRange start or end for the given n.
	// This is required to trim the returned values to the same length e.g.
	// (256, 512) should return 8000-8080 as shard key range.
	minBytes := 0
	for nn := Uint64Key(n - 1); nn > 0; nn >>= 8 {
		minBytes++
	}

	width := Uint64Key(math.MaxUint64)/Uint64Key(n) + 1
	start := Uint64Key(i) * width
	end := start + width

	// Note: The byte value is empty if start or end is the min or the max
	// respectively.
	startBytes := start.Bytes()[:minBytes]
	endBytes := end.Bytes()[:minBytes]
	if start == 0 {
		startBytes = []byte{}
	}
	if end == 0 {
		// Always set the end except for the last shard. In that case, the
		// end value (2^64) flows over and is the same as 0.
		endBytes = []byte{}
	}
	return &topodatapb.KeyRange{Start: startBytes, End: endBytes}, nil
}

// KeyRangeAdd adds two adjacent keyranges into a single value.
// If the values are not adjacent, it returns false.
func KeyRangeAdd(first, second *topodatapb.KeyRange) (*topodatapb.KeyRange, bool) {
	if first == nil || second == nil {
		return nil, false
	}
	if len(first.End) != 0 && bytes.Equal(first.End, second.Start) {
		return &topodatapb.KeyRange{Start: first.Start, End: second.End}, true
	}
	if len(second.End) != 0 && bytes.Equal(second.End, first.Start) {
		return &topodatapb.KeyRange{Start: second.Start, End: first.End}, true
	}
	return nil, false
}

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
		return "-"
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
	return bytes.Equal(addPadding(left.Start), addPadding(right.Start)) &&
		bytes.Equal(addPadding(left.End), addPadding(right.End))
}

// addPadding adds padding to make sure keyrange represents an 8 byte integer.
// From Vitess docs:
// A hash vindex produces an 8-byte number.
// This means that all numbers less than 0x8000000000000000 will fall in shard -80.
// Any number with the highest bit set will be >= 0x8000000000000000, and will therefore
// belong to shard 80-.
// This means that from a keyrange perspective -80 == 00-80 == 0000-8000 == 000000-800000
// If we don't add this padding, we could run into issues when transitioning from keyranges
// that use 2 bytes to 4 bytes.
func addPadding(kr []byte) []byte {
	paddedKr := make([]byte, 8)

	for i := 0; i < len(kr); i++ {
		paddedKr = append(paddedKr, kr[i])
	}

	for i := len(kr); i < 8; i++ {
		paddedKr = append(paddedKr, 0)
	}
	return paddedKr
}

// KeyRangeStartSmaller returns true if right's keyrange start is _after_ left's start
func KeyRangeStartSmaller(left, right *topodatapb.KeyRange) bool {
	if left == nil {
		return right != nil
	}
	if right == nil {
		return false
	}
	return bytes.Compare(left.Start, right.Start) < 0
}

// KeyRangeStartEqual returns true if both key ranges have the same start
func KeyRangeStartEqual(left, right *topodatapb.KeyRange) bool {
	if left == nil {
		return right == nil || len(right.Start) == 0
	}
	if right == nil {
		return len(left.Start) == 0
	}
	return bytes.Equal(addPadding(left.Start), addPadding(right.Start))
}

// KeyRangeContiguous returns true if the end of the left key range exactly
// matches the start of the right key range (i.e they are contigious)
func KeyRangeContiguous(left, right *topodatapb.KeyRange) bool {
	if left == nil {
		return right == nil || (len(right.Start) == 0 && len(right.End) == 0)
	}
	if right == nil {
		return len(left.Start) == 0 && len(left.End) == 0
	}
	return bytes.Equal(addPadding(left.End), addPadding(right.Start))
}

// KeyRangeEndEqual returns true if both key ranges have the same end
func KeyRangeEndEqual(left, right *topodatapb.KeyRange) bool {
	if left == nil {
		return right == nil || len(right.End) == 0
	}
	if right == nil {
		return len(left.End) == 0
	}
	return bytes.Equal(addPadding(left.End), addPadding(right.End))
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
	result := proto.Clone(first).(*topodatapb.KeyRange)
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
	return result, nil
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
// "0" is treated as "-", to allow us to not have to special-case
// client code.
func ParseShardingSpec(spec string) ([]*topodatapb.KeyRange, error) {
	parts := strings.Split(spec, "-")
	if len(parts) == 1 {
		if spec == "0" {
			parts = []string{"", ""}
		} else {
			return nil, fmt.Errorf("malformed spec: doesn't define a range: %q", spec)
		}
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

var krRegexp = regexp.MustCompile(`^[0-9a-fA-F]*-[0-9a-fA-F]*$`)

// IsKeyRange returns true if the string represents a keyrange.
func IsKeyRange(kr string) bool {
	return krRegexp.MatchString(kr)
}

// GenerateShardRanges returns shard ranges assuming a keyspace with N shards.
func GenerateShardRanges(shards int) ([]string, error) {
	var format string
	var maxShards int

	switch {
	case shards <= 0:
		return nil, errors.New("shards must be greater than zero")
	case shards <= 256:
		format = "%02x"
		maxShards = 256
	case shards <= 65536:
		format = "%04x"
		maxShards = 65536
	default:
		return nil, errors.New("this function does not support more than 65336 shards in a single keyspace")
	}

	rangeFormatter := func(start, end int) string {
		var (
			startKid string
			endKid   string
		)

		if start != 0 {
			startKid = fmt.Sprintf(format, start)
		}

		if end != maxShards {
			endKid = fmt.Sprintf(format, end)
		}

		return fmt.Sprintf("%s-%s", startKid, endKid)
	}

	start := 0
	end := 0

	// If shards does not divide evenly into maxShards, then there is some lossiness,
	// where each shard is smaller than it should technically be (if, for example, size == 25.6).
	// If we choose to keep everything in ints, then we have two choices:
	// 	- Have every shard in #numshards be a uniform size, tack on an additional shard
	//	  at the end of the range to account for the loss. This is bad because if you ask for
	//	  7 shards, you'll actually get 7 uniform shards with 1 small shard, for 8 total shards.
	//	  It's also bad because one shard will have much different data distribution than the rest.
	//	- Expand the final shard to include whatever is left in the keyrange. This will give the
	//	  correct number of shards, which is good, but depending on how lossy each individual shard is,
	//	  you could end with that final shard being significantly larger than the rest of the shards,
	//	  so this doesn't solve the data distribution problem.
	//
	// By tracking the "real" end (both in the real number sense, and in the truthfulness of the value sense),
	// we can re-truncate the integer end on each iteration, which spreads the lossiness more
	// evenly across the shards.
	//
	// This implementation has no impact on shard numbers that are powers of 2, even at large numbers,
	// which you can see in the tests.
	size := float64(maxShards) / float64(shards)
	realEnd := float64(0)
	shardRanges := make([]string, 0, shards)

	for i := 1; i <= shards; i++ {
		realEnd = float64(i) * size

		end = int(realEnd)
		shardRanges = append(shardRanges, rangeFormatter(start, end))
		start = end
	}

	return shardRanges, nil
}
