// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"
)

// Stores the mapping of keys
type NumericLookupTable map[uint64]uint64

// Similar to vindex Numeric but first attempts a lookup via a json file
type NumericStaticMap struct {
	name   string
	lookup NumericLookupTable
}

// NewNumericStaticMap creates a NumericStaticMap vindex.
func NewNumericStaticMap(name string, params map[string]string) (Vindex, error) {
	jsonPath, ok := params["json_path"]
	if !ok {
		return nil, errors.New("NumericStaticMap: Could not find `json_path` param in vschema")
	}

	lt, err := loadNumericLookupTable(jsonPath)
	if err != nil {
		return nil, err
	}
	return &NumericStaticMap{
		name:   name,
		lookup: lt,
	}, nil
}

// String returns the name of the vindex.
func (vind *NumericStaticMap) String() string {
	return vind.name
}

// Cost returns the cost of this vindex as 1.
func (*NumericStaticMap) Cost() int {
	return 1
}

// Verify returns true if id and ksid match.
func (vind *NumericStaticMap) Verify(_ VCursor, id interface{}, ksid []byte) (bool, error) {
	var keybytes [8]byte
	num, err := getNumber(id)
	if err != nil {
		return false, fmt.Errorf("NumericStaticMap.Verify: %v", err)
	}
	
	lookupNum, ok := vind.lookup[uint64(num)]
	if ok {
		num = int64(lookupNum)
	}
	
	binary.BigEndian.PutUint64(keybytes[:], uint64(num))
	return bytes.Compare(keybytes[:], ksid) == 0, nil
}

// Map returns the associated keyspace ids for the given ids.
func (vind *NumericStaticMap) Map(_ VCursor, ids []interface{}) ([][]byte, error) {
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		num, err := getNumber(id)
		lookupNum, ok := vind.lookup[uint64(num)]
		if ok {
			num = int64(lookupNum)
		}
		if err != nil {
			return nil, fmt.Errorf("NumericStaticMap.Map: %v", err)
		}
		var keybytes [8]byte
		binary.BigEndian.PutUint64(keybytes[:], uint64(num))
		out = append(out, keybytes[:])
	}
	return out, nil
}

// ReverseMap returns the associated id for the ksid.
func (vind *NumericStaticMap) ReverseMap(_ VCursor, ksid []byte) (interface{}, error) {
	if len(ksid) != 8 {
		return nil, fmt.Errorf("NumericStaticMap.ReverseMap: length of keyspace is not 8: %d", len(ksid))
	}
	id := binary.BigEndian.Uint64([]byte(ksid))
	for k, v := range vind.lookup {
		if int64(id) == int64(v) {
			id = vind.lookup[k]
		}
	}
	return id, nil
}

func init() {
	Register("numeric_static_map", NewNumericStaticMap)
}

func loadNumericLookupTable(path string) (NumericLookupTable, error) {
	var m map[string]uint64
	lt := make(map[uint64]uint64)
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return lt, err
	}
	err = json.Unmarshal(data, &m)
	if err != nil {
		return lt, err
	}
	for k, v := range m {
		newK, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			return lt, err
		}
		lt[newK] = v
	}

	return lt, nil
}
