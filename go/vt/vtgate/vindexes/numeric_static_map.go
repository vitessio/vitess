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
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"strconv"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

const (
	numericStaticMapParamJSON         = "json"
	numericStaticMapParamJSONPath     = "json_path"
	numericStaticMapParamFallbackType = "fallback_type"
)

var (
	_ SingleColumn    = (*NumericStaticMap)(nil)
	_ Hashing         = (*NumericStaticMap)(nil)
	_ ParamValidating = (*NumericStaticMap)(nil)

	numericStaticMapParams = []string{
		numericStaticMapParamJSON,
		numericStaticMapParamJSONPath,
		numericStaticMapParamFallbackType,
	}
)

// NumericLookupTable stores the mapping of keys.
type NumericLookupTable map[uint64]uint64

// NumericStaticMap is similar to vindex Numeric but first attempts a lookup via
// a JSON file.
type NumericStaticMap struct {
	name          string
	hashVdx       Hashing
	lookup        NumericLookupTable
	unknownParams []string
}

func init() {
	Register("numeric_static_map", newNumericStaticMap)
}

// newNumericStaticMap creates a NumericStaticMap vindex.
func newNumericStaticMap(name string, params map[string]string) (Vindex, error) {
	jsonStr, jsok := params[numericStaticMapParamJSON]
	jsonPath, jpok := params[numericStaticMapParamJSONPath]

	if !jsok && !jpok {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "NumericStaticMap: Could not find either `json_path` or `json` params in vschema")
	}

	if jsok && jpok {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "NumericStaticMap: Found both `json` and `json_path` params in vschema")
	}

	var err error
	var lt NumericLookupTable

	if jpok {
		lt, err = loadNumericLookupTable(jsonPath)
		if err != nil {
			return nil, err
		}
	}

	if jsok {
		lt, err = parseNumericLookupTable([]byte(jsonStr))
		if err != nil {
			return nil, err
		}
	}

	var hashVdx Hashing

	if s, ok := params[numericStaticMapParamFallbackType]; ok {
		vindex, err := CreateVindex(s, name+"_hash", map[string]string{})
		if err != nil {
			return nil, err
		}
		hashVdx, _ = vindex.(Hashing) // We know this will not fail
	}

	return &NumericStaticMap{
		hashVdx:       hashVdx,
		lookup:        lt,
		name:          name,
		unknownParams: FindUnknownParams(params, numericStaticMapParams),
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

// NeedsVCursor satisfies the Vindex interface.
func (vind *NumericStaticMap) NeedsVCursor() bool {
	return false
}

// Verify returns true if ids and ksids match.
func (vind *NumericStaticMap) Verify(ctx context.Context, vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, 0, len(ids))
	for i, id := range ids {
		ksid, err := vind.Hash(id)
		if err != nil {
			return nil, err
		}
		out = append(out, bytes.Equal(ksid, ksids[i]))
	}
	return out, nil
}

// Map can map ids to key.Destination objects.
func (vind *NumericStaticMap) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	for _, id := range ids {
		ksid, err := vind.Hash(id)
		if err != nil {
			out = append(out, key.DestinationNone{})
			continue
		}
		out = append(out, key.DestinationKeyspaceID(ksid))
	}
	return out, nil
}

func (vind *NumericStaticMap) Hash(id sqltypes.Value) ([]byte, error) {
	num, err := evalengine.ToUint64(id)
	if err != nil {
		return nil, err
	}
	lookupNum, ok := vind.lookup[num]
	if !ok {
		// Not in lookup, use fallback hash
		if vind.hashVdx != nil {
			return vind.hashVdx.Hash(id)
		}
	} else {
		num = lookupNum
	}

	var keybytes [8]byte
	binary.BigEndian.PutUint64(keybytes[:], num)
	return keybytes[:], nil
}

// UnknownParams implements the ParamValidating interface.
func (vind *NumericStaticMap) UnknownParams() []string {
	return vind.unknownParams
}

func loadNumericLookupTable(path string) (NumericLookupTable, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseNumericLookupTable(data)
}

func parseNumericLookupTable(data []byte) (NumericLookupTable, error) {
	var m map[string]uint64
	lt := make(map[uint64]uint64)
	err := json.Unmarshal(data, &m)
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
