// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
)

// Numeric defines a bit-pattern mapping of a uint64 to the KeyspaceId.
// It's Unique and Reversible.
type Numeric struct{}

// NewNumeric creates a Numeric vindex.
func NewNumeric(_ map[string]interface{}) (planbuilder.Vindex, error) {
	return Numeric{}, nil
}

// Cost returns the cost of this vindex as 0.
func (Numeric) Cost() int {
	return 0
}

// Verify returns true if id and ksid match.
func (Numeric) Verify(_ planbuilder.VCursor, id interface{}, ksid []byte) (bool, error) {
	var keybytes [8]byte
	num, err := getNumber(id)
	if err != nil {
		return false, fmt.Errorf("Numeric.Verify: %v", err)
	}
	binary.BigEndian.PutUint64(keybytes[:], uint64(num))
	return bytes.Compare(keybytes[:], ksid) == 0, nil
}

// Map returns the associated keyspae ids for the given ids.
func (Numeric) Map(_ planbuilder.VCursor, ids []interface{}) ([][]byte, error) {
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		num, err := getNumber(id)
		if err != nil {
			return nil, fmt.Errorf("Numeric.Map: %v", err)
		}
		var keybytes [8]byte
		binary.BigEndian.PutUint64(keybytes[:], uint64(num))
		out = append(out, []byte(keybytes[:]))
	}
	return out, nil
}

// ReverseMap returns the associated id for the ksid.
func (Numeric) ReverseMap(_ planbuilder.VCursor, ksid []byte) (interface{}, error) {
	if len(ksid) != 8 {
		return nil, fmt.Errorf("Numeric.ReverseMap: length of keyspace is not 8: %d", len(ksid))
	}
	return binary.BigEndian.Uint64([]byte(ksid)), nil
}

func init() {
	planbuilder.Register("numeric", NewNumeric)
}
