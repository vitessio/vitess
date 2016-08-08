// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vindexes

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
)

// Stores the mapping of keys
type NumericLookupTable map[int64]int64

// Similar to vindex Numeric but first attempts a lookup via a json file
type NumericStaticMap struct {
	name   string
	lookup NumericLookupTable
}

// NewNumericStaticMap creates a NumericStaticMap vindex.
func NewNumericStaticMap(name string, params map[string]string) (Vindex, error) {
	jsonPath, ok := params["json_path"]
	if !ok {
		panic("NumericStaticMap: Could not find `json_path` param in vschema")
	}

	lt, error := loadNumericLookupTable(jsonPath)
	if error != nil {
		return nil, error
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
	lookupNum, ok := vind.lookup[num]
	if ok {
		num = lookupNum
	}
	if err != nil {
		return false, fmt.Errorf("NumericStaticMap.Verify: %v", err)
	}
	binary.BigEndian.PutUint64(keybytes[:], uint64(num))
	return bytes.Compare(keybytes[:], ksid) == 0, nil
}

// Map returns the associated keyspace ids for the given ids.
func (vind *NumericStaticMap) Map(_ VCursor, ids []interface{}) ([][]byte, error) {
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		num, err := getNumber(id)
		lookupNum, ok := vind.lookup[num]
		if ok {
			num = lookupNum
		}
		if err != nil {
			return nil, fmt.Errorf("NumericStaticMap.Map: %v", err)
		}
		var keybytes [8]byte
		binary.BigEndian.PutUint64(keybytes[:], uint64(num))
		out = append(out, []byte(keybytes[:]))
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
		if int64(id) == v {
			id = uint64(vind.lookup[k])
		}
	}
	return id, nil
}

func init() {
	Register("numeric_static_map", NewNumericStaticMap)
}

func loadNumericLookupTable(path string) (NumericLookupTable, error) {
	var m map[string]int64
	lt := make(map[int64]int64)
	data, error := ioutil.ReadFile(path)
	if error != nil {
		return lt, error
	}
	error = json.Unmarshal(data, &m)
	for k, v := range m {
		newK, _ := strconv.ParseInt(k, 10, 64)
		lt[newK] = v
	}

	return lt, nil
}
