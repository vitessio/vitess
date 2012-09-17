// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package key

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"strings"
)

var MinKey = KeyspaceId("")
var MaxKey = KeyspaceId(strings.Repeat("\xff", 64))

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
	return HexKeyspaceId(hex.EncodeToString([]byte(kid)))
}

func (kid KeyspaceId) MarshalJSON() ([]byte, error) {
	return []byte("\"" + string(kid.Hex()) + "\""), nil
}

func (kid *KeyspaceId) UnmarshalJSON(data []byte) error {
	*kid = HexKeyspaceId(data[1 : len(data)-1]).Unhex()
	return nil
}

type HexKeyspaceId string

func (hkid HexKeyspaceId) Unhex() KeyspaceId {
	b, err := hex.DecodeString(string(hkid))
	if err != nil {
		panic(err)
	}
	return KeyspaceId(string(b))
}

type KeyRange struct {
	Start KeyspaceId
	End   KeyspaceId
}

func (kr KeyRange) MapKey() string {
	return string(kr.Start) + "-" + string(kr.End)
}

func (kr KeyRange) Contains(i KeyspaceId) bool {
	return kr.Start < i && i <= kr.End
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
