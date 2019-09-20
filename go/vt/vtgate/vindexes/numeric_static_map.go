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

package vindexes

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io/ioutil"
	"strconv"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	_ Vindex = (*NumericStaticMap)(nil)
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

// IsUnique returns true since the Vindex is unique.
func (vind *NumericStaticMap) IsUnique() bool {
	return true
}

// IsFunctional returns true since the Vindex is functional.
func (vind *NumericStaticMap) IsFunctional() bool {
	return true
}

// Verify returns true if ids and ksids match.
func (vind *NumericStaticMap) Verify(_ VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		var keybytes [8]byte
		num, err := sqltypes.ToUint64(ids[i])
		if err != nil {
			return nil, vterrors.Wrap(err, "NumericStaticMap.Verify")
		}
		lookupNum, ok := vind.lookup[num]
		if ok {
			num = lookupNum
		}
		binary.BigEndian.PutUint64(keybytes[:], num)
		out[i] = bytes.Equal(keybytes[:], ksids[i])
	}
	return out, nil
}

// Map can map ids to key.Destination objects.
func (vind *NumericStaticMap) Map(cursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	for _, id := range ids {
		num, err := sqltypes.ToUint64(id)
		if err != nil {
			out = append(out, key.DestinationNone{})
			continue
		}
		lookupNum, ok := vind.lookup[num]
		if ok {
			num = lookupNum
		}
		var keybytes [8]byte
		binary.BigEndian.PutUint64(keybytes[:], num)
		out = append(out, key.DestinationKeyspaceID(keybytes[:]))
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
