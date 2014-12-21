// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"encoding/binary"
	"fmt"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

var (
	_ planbuilder.Unique     = NumKSID{}
	_ planbuilder.Reversible = NumKSID{}
)

// NumKSID defines a bit-pattern mapping of a uint64 to the KeyspaceId.
// It's Unique and Reversible.
type NumKSID struct{}

// NewNumKSID creates a NumKSID vindex.
func NewNumKSID(_ map[string]interface{}) (planbuilder.Vindex, error) {
	return NumKSID{}, nil
}

// Cost returns the cost of this vindex as 0.
func (NumKSID) Cost() int {
	return 0
}

// Verify returns true if id and ksid match.
func (NumKSID) Verify(_ planbuilder.VCursor, id interface{}, ksid key.KeyspaceId) (bool, error) {
	var keybytes [8]byte
	num, err := getNumber(id)
	if err != nil {
		return false, fmt.Errorf("NumKSID.Verify: %v", err)
	}
	binary.BigEndian.PutUint64(keybytes[:], uint64(num))
	return key.KeyspaceId(keybytes[:]) == ksid, nil
}

// Map returns the associated keyspae ids for the given ids.
func (NumKSID) Map(_ planbuilder.VCursor, ids []interface{}) ([]key.KeyspaceId, error) {
	var keybytes [8]byte
	out := make([]key.KeyspaceId, 0, len(ids))
	for _, id := range ids {
		num, err := getNumber(id)
		if err != nil {
			return nil, fmt.Errorf("NumKSID.Map: %v", err)
		}
		binary.BigEndian.PutUint64(keybytes[:], uint64(num))
		out = append(out, key.KeyspaceId(keybytes[:]))
	}
	return out, nil
}

// ReverseMap returns the associated id for the ksid.
func (NumKSID) ReverseMap(_ planbuilder.VCursor, ksid key.KeyspaceId) (interface{}, error) {
	if len(ksid) != 8 {
		return nil, fmt.Errorf("NumKSID.ReverseMap: length of keyspace is not 8: %d", len(ksid))
	}
	return binary.BigEndian.Uint64([]byte(ksid)), nil
}

func init() {
	planbuilder.Register("num_ksid", NewNumKSID)
}
