// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package key

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

var MinKey = KeyspaceId("")
var MaxKey = KeyspaceId("")

// NOTE(msolomon) not sure about all these types - feels like it will create
// hoops later.
type Uint64Key uint64

func (i Uint64Key) String() string {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint64(i))
	return buf.String()
}

func (i Uint64Key) KeyspaceId() KeyspaceId {
	return KeyspaceId(i.String())
}

type KeyspaceId string

func (kid KeyspaceId) Hex() HexKeyspaceId {
	return HexKeyspaceId(strings.ToUpper(hex.EncodeToString([]byte(kid))))
}

func (kid KeyspaceId) MarshalJSON() ([]byte, error) {
	return []byte("\"" + string(kid.Hex()) + "\""), nil
}

func (kid *KeyspaceId) UnmarshalJSON(data []byte) (err error) {
	*kid, err = HexKeyspaceId(data[1 : len(data)-1]).Unhex()
	return err
}

type HexKeyspaceId string

func (hkid HexKeyspaceId) Unhex() (KeyspaceId, error) {
	b, err := hex.DecodeString(string(hkid))
	if err != nil {
		return KeyspaceId(""), err
	}
	return KeyspaceId(string(b)), nil
}

type KeyRange struct {
	Start KeyspaceId
	End   KeyspaceId
}

func (kr KeyRange) MapKey() string {
	return string(kr.Start) + "-" + string(kr.End)
}

func (kr KeyRange) Contains(i KeyspaceId) bool {
	return kr.Start <= i && (kr.End == MaxKey || i < kr.End)
}

func (kr KeyRange) String() string {
	return fmt.Sprintf("{Start: %v, End: %v}", string(kr.Start.Hex()), string(kr.End.Hex()))
}

func (kr KeyRange) IsPartial() bool {
	return !(kr.Start == MinKey && kr.End == MaxKey)
}

type KeyspaceRange struct {
	Keyspace string
	KeyRange
}

type KeyspaceIdArray []KeyspaceId

func (p KeyspaceIdArray) Len() int { return len(p) }

func (p KeyspaceIdArray) Less(i, j int) bool {
	return p[i] < p[j]
}

func (p KeyspaceIdArray) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p KeyspaceIdArray) Sort() { sort.Sort(p) }

type KeyRangeArray []KeyRange

func (p KeyRangeArray) Len() int { return len(p) }

func (p KeyRangeArray) Less(i, j int) bool {
	return p[i].Start < p[j].Start
}

func (p KeyRangeArray) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p KeyRangeArray) Sort() { sort.Sort(p) }

// ParseShardingSpec parses a string that describes a sharding
// specification. a-b-c-d will be parsed as a-b, b-c, c-d. The empty
// string may serve both as the start and end of the keyspace: -a-b-
// will be parsed as start-a, a-b, b-end.
func ParseShardingSpec(spec string) (KeyRangeArray, error) {
	parts := strings.Split(spec, "-")
	if len(parts) == 1 {
		return nil, fmt.Errorf("malformed spec: doesn't define a range: %q", spec)
	}
	old := parts[0]
	ranges := make([]KeyRange, len(parts)-1)

	for i, p := range parts[1:] {
		if p == "" && i != (len(parts)-2) {
			return nil, fmt.Errorf("malformed spec: MinKey/MaxKey cannot be in the middle of the spec: %q", spec)
		}
		if p != "" && p <= old {
			return nil, fmt.Errorf("malformed spec: shard limits should be in order: %q", spec)
		}
		s, err := HexKeyspaceId(old).Unhex()
		if err != nil {
			return nil, err
		}
		e, err := HexKeyspaceId(p).Unhex()
		if err != nil {
			return nil, err
		}
		ranges[i] = KeyRange{Start: s, End: e}
		old = p
	}
	return ranges, nil
}
