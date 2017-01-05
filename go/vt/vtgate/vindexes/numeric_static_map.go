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

// NumericLookupTable stores the mapping of keys.
type NumericLookupTable map[uint64]uint64

// NumericStaticMap is similar to vindex Numeric but first attempts a lookup via
// a JSON file.
type NumericStaticMap struct {
	name   string
	lookup NumericLookupTable
}

func init() {
	Register("numeric_static_map", NewNumericStaticMap)
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

// Verify returns true if ids and ksids match.
func (vind *NumericStaticMap) Verify(_ VCursor, ids []interface{}, ksids [][]byte) (bool, error) {
	if len(ids) != len(ksids) {
		return false, fmt.Errorf("NumericStaticMap.Verify: length of ids %v doesn't match length of ksids %v", len(ids), len(ksids))
	}
	for rowNum := range ids {
		var keybytes [8]byte
		num, err := getNumber(ids[rowNum])
		if err != nil {
			return false, fmt.Errorf("NumericStaticMap.Verify: %v", err)
		}
		lookupNum, ok := vind.lookup[uint64(num)]
		if ok {
			num = int64(lookupNum)
		}
		binary.BigEndian.PutUint64(keybytes[:], uint64(num))
		if bytes.Compare(keybytes[:], ksids[rowNum]) != 0 {
			return false, nil
		}
	}
	return true, nil
}

// Map returns the associated keyspace ids for the given ids.
func (vind *NumericStaticMap) Map(_ VCursor, ids []interface{}) ([][]byte, error) {
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		num, err := getNumber(id)
		if err != nil {
			return nil, fmt.Errorf("NumericStaticMap.Map: %v", err)
		}
		lookupNum, ok := vind.lookup[uint64(num)]
		if ok {
			num = int64(lookupNum)
		}
		var keybytes [8]byte
		binary.BigEndian.PutUint64(keybytes[:], uint64(num))
		out = append(out, keybytes[:])
	}
	return out, nil
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
