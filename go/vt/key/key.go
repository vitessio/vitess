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

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	KeyRangePattern = regexp.MustCompile(`^(0|([0-9a-fA-F]{2})*-([0-9a-fA-F]{2})*)$`)
)

//
// Uint64Key definitions
//

// Uint64Key is a uint64 that can be converted into a keyspace id.
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

// Helper methods for keyspace id values.

// Normalize removes any trailing zero bytes from id. This allows two id values to be compared even if they are
// different lengths.
// From a key range perspective, -80 == 00-80 == 0000-8000 == 000000-800000, etc. and they should
// always be treated the same even if they are different lengths.
func Normalize(id []byte) []byte {
	trailingZeroes := 0
	for i := len(id) - 1; i >= 0 && id[i] == 0x00; i-- {
		trailingZeroes += 1
	}

	return id[:len(id)-trailingZeroes]
}

// Compare compares two keyspace IDs while taking care to normalize them; returns -1 if a<b, 1 if a>b, 0 if equal.
func Compare(a, b []byte) int {
	return bytes.Compare(Normalize(a), Normalize(b))
}

// Less returns true if a is less than b.
func Less(a, b []byte) bool {
	return Compare(a, b) < 0
}

// Equal returns true if a is equal to b.
func Equal(a, b []byte) bool {
	return Compare(a, b) == 0
}

// Empty returns true if id is an empty keyspace ID.
func Empty(id []byte) bool {
	return len(Normalize(id)) == 0
}

//
// KeyRange helper methods
//

// KeyRangeAdd adds two adjacent KeyRange values (in any order) into a single value. If the values are not adjacent,
// it returns false.
func KeyRangeAdd(a, b *topodatapb.KeyRange) (*topodatapb.KeyRange, bool) {
	if a == nil || b == nil {
		return nil, false
	}
	if !Empty(a.End) && Equal(a.End, b.Start) {
		return &topodatapb.KeyRange{Start: a.Start, End: b.End}, true
	}
	if !Empty(b.End) && Equal(b.End, a.Start) {
		return &topodatapb.KeyRange{Start: b.Start, End: a.End}, true
	}
	return nil, false
}

// KeyRangeContains returns true if the provided id is in the keyrange.
func KeyRangeContains(keyRange *topodatapb.KeyRange, id []byte) bool {
	if KeyRangeIsComplete(keyRange) {
		return true
	}
	return (Empty(keyRange.Start) || Compare(id, keyRange.Start) >= 0) && (Empty(keyRange.End) || Compare(id, keyRange.End) < 0)
}

// ParseKeyRangeParts parses a Start and End as hex encoded strings and builds a proto KeyRange.
func ParseKeyRangeParts(start, end string) (*topodatapb.KeyRange, error) {
	startKey, err := hex.DecodeString(start)
	if err != nil {
		return nil, err
	}
	endKey, err := hex.DecodeString(end)
	if err != nil {
		return nil, err
	}
	return &topodatapb.KeyRange{Start: startKey, End: endKey}, nil
}

// KeyRangeString formats a topodatapb.KeyRange into a hex encoded string.
func KeyRangeString(keyRange *topodatapb.KeyRange) string {
	if KeyRangeIsComplete(keyRange) {
		return "-"
	}
	return hex.EncodeToString(keyRange.Start) + "-" + hex.EncodeToString(keyRange.End)
}

// KeyRangeStartCompare compares the Start of two KeyRange values using semantics unique to Start values where an
// empty Start means the *minimum* value; returns -1 if a<b, 1 if a>b, 0 if equal.
func KeyRangeStartCompare(a, b *topodatapb.KeyRange) int {
	aIsMinimum := a == nil || Empty(a.Start)
	bIsMinimum := b == nil || Empty(b.Start)

	if aIsMinimum && bIsMinimum {
		return 0
	} else if aIsMinimum {
		return -1
	} else if bIsMinimum {
		return 1
	}

	return Compare(a.Start, b.Start)
}

// KeyRangeStartEqual returns true if both KeyRange values have the same Start.
func KeyRangeStartEqual(a, b *topodatapb.KeyRange) bool {
	return KeyRangeStartCompare(a, b) == 0
}

// KeyRangeEndCompare compares the End of two KeyRange values using semantics unique to End values where an
// empty End means the *maximum* value; returns -1 if a<b, 1 if a>b, 0 if equal.
func KeyRangeEndCompare(a, b *topodatapb.KeyRange) int {
	aIsMaximum := a == nil || Empty(a.End)
	bIsMaximum := b == nil || Empty(b.End)

	if aIsMaximum && bIsMaximum {
		return 0
	} else if aIsMaximum {
		return 1
	} else if bIsMaximum {
		return -1
	}

	return Compare(a.End, b.End)
}

// KeyRangeEndEqual returns true if both KeyRange values have the same End.
func KeyRangeEndEqual(a, b *topodatapb.KeyRange) bool {
	return KeyRangeEndCompare(a, b) == 0
}

// KeyRangeCompare compares two KeyRange values, taking into account both the Start and End fields and their
// field-specific comparison logic; returns -1 if a<b, 1 if a>b, 0 if equal. Specifically:
//
//   - The Start-specific KeyRangeStartCompare and End-specific KeyRangeEndCompare are used for proper comparison
//     of an empty value for either Start or End.
//   - The Start is compared first and End is only compared if Start is equal.
func KeyRangeCompare(a, b *topodatapb.KeyRange) int {
	// First, compare the Start field.
	if v := KeyRangeStartCompare(a, b); v != 0 {
		// The Start field for a and b differ, and that is enough; return that comparison.
		return v
	}

	// The Start field was equal, so compare the End field and return that comparison.
	return KeyRangeEndCompare(a, b)
}

// KeyRangeEqual returns true if a and b are equal.
func KeyRangeEqual(a, b *topodatapb.KeyRange) bool {
	return KeyRangeCompare(a, b) == 0
}

// KeyRangeLess returns true if a is less than b.
func KeyRangeLess(a, b *topodatapb.KeyRange) bool {
	return KeyRangeCompare(a, b) < 0
}

// KeyRangeIsComplete returns true if the KeyRange covers the entire keyspace.
func KeyRangeIsComplete(keyRange *topodatapb.KeyRange) bool {
	return keyRange == nil || (Empty(keyRange.Start) && Empty(keyRange.End))
}

// KeyRangeIsPartial returns true if the KeyRange does not cover the entire keyspace.
func KeyRangeIsPartial(keyRange *topodatapb.KeyRange) bool {
	return !KeyRangeIsComplete(keyRange)
}

// KeyRangeContiguous returns true if the End of KeyRange a is equivalent to the Start of the KeyRange b,
// which means that they are contiguous.
func KeyRangeContiguous(a, b *topodatapb.KeyRange) bool {
	if KeyRangeIsComplete(a) || KeyRangeIsComplete(b) {
		return false // no two KeyRange values can be contiguous if either is the complete range
	}

	return Equal(a.End, b.Start)
}

// For more info on the following functions, see:
//   http://stackoverflow.com/questions/4879315/what-is-a-tidy-algorithm-to-find-overlapping-intervals
// Two segments defined as (a,b) and (c,d) (with a<b and c<d):
//   * intersects = (b > c) && (a < d)
//   * overlap = min(b, d) - max(c, a)

// KeyRangeIntersect returns true if some part of KeyRange a and b overlap, meaning that some keyspace ID values
// exist in both a and b.
func KeyRangeIntersect(a, b *topodatapb.KeyRange) bool {
	if KeyRangeIsComplete(a) || KeyRangeIsComplete(b) {
		return true // if either KeyRange is complete, there must be an intersection
	}

	return (Empty(a.End) || Less(b.Start, a.End)) && (Empty(b.End) || Less(a.Start, b.End))
}

// KeyRangeContainsKeyRange returns true if KeyRange a fully contains KeyRange b.
func KeyRangeContainsKeyRange(a, b *topodatapb.KeyRange) bool {
	// If a covers the entire KeyRange, it always contains b.
	if KeyRangeIsComplete(a) {
		return true
	}

	// If b covers the entire KeyRange, a must also cover the entire KeyRange.
	if KeyRangeIsComplete(b) {
		return KeyRangeIsComplete(a)
	}

	// Ensure b.Start >= a.Start and b.End <= a.End.
	if KeyRangeStartCompare(b, a) >= 0 && KeyRangeEndCompare(b, a) <= 0 {
		return true
	}

	return false
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

// IsValidKeyRange returns true if the string represents a valid key range.
func IsValidKeyRange(keyRangeString string) bool {
	return KeyRangePattern.MatchString(keyRangeString)
}

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
