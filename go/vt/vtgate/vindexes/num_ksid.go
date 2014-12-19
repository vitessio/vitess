// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"encoding/binary"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

var (
	_ planbuilder.Unique     = NumKSID{}
	_ planbuilder.Reversible = NumKSID{}
)

type NumKSID struct{}

func NewNumKSID(_ map[string]interface{}) (planbuilder.Vindex, error) {
	return NumKSID{}, nil
}

func (_ NumKSID) Cost() int {
	return 0
}

func (_ NumKSID) Verify(_ planbuilder.VCursor, id interface{}, ks key.KeyspaceId) (bool, error) {
	var keybytes [8]byte
	num, err := getNumber(id)
	if err != nil {
		return false, err
	}
	binary.BigEndian.PutUint64(keybytes[:], uint64(num))
	return key.KeyspaceId(keybytes[:]) == ks, nil
}

func (_ NumKSID) Map(_ planbuilder.VCursor, ids []interface{}) ([]key.KeyspaceId, error) {
	var keybytes [8]byte
	out := make([]key.KeyspaceId, 0, len(ids))
	for _, id := range ids {
		num, err := getNumber(id)
		if err != nil {
			return nil, err
		}
		binary.BigEndian.PutUint64(keybytes[:], uint64(num))
		out = append(out, key.KeyspaceId(keybytes[:]))
	}
	return out, nil
}

func (_ NumKSID) ReverseMap(_ planbuilder.VCursor, k key.KeyspaceId) (interface{}, error) {
	return binary.BigEndian.Uint64([]byte(k)), nil
}

func init() {
	planbuilder.Register("num_ksid", NewNumKSID)
}
