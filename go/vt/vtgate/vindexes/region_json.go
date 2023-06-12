/*
Copyright 2020 The Vitess Authors.

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
	"fmt"
	"os"
	"strconv"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
)

const (
	regionJSONParamRegionBytes = "region_bytes"
	regionJSONParamRegionMap   = "region_map"
)

var (
	_ MultiColumn = (*RegionJSON)(nil)

	regionJSONParams = []string{
		regionJSONParamRegionBytes,
		regionJSONParamRegionMap,
	}
)

func init() {
	Register("region_json", newRegionJSON)
}

// RegionMap is used to store mapping of country to region
type RegionMap map[string]uint64

// RegionJSON is a multi-column unique vindex
// The first column is used to lookup the prefix part of the keyspace id, the second column is hashed,
// and the two values are combined to produce the keyspace id.
// RegionJson can be used for geo-partitioning because the first column can denote a region,
// and it will dictate the shard range for that region.
type RegionJSON struct {
	name          string
	regionMap     RegionMap
	regionBytes   int
	unknownParams []string
}

// newRegionJSON creates a RegionJson vindex.
// The supplied map requires all the fields of "RegionExperimental".
// Additionally, it requires a region_map argument representing the path to a json file
// containing a map of country to region.
func newRegionJSON(name string, m map[string]string) (Vindex, error) {
	rmPath := m[regionJSONParamRegionMap]
	rmap := make(map[string]uint64)
	data, err := os.ReadFile(rmPath)
	if err != nil {
		return nil, err
	}
	log.Infof("Loaded Region map from: %s", rmPath)
	err = json.Unmarshal(data, &rmap)
	if err != nil {
		return nil, err
	}
	rb, err := strconv.Atoi(m[regionJSONParamRegionBytes])
	if err != nil {
		return nil, err
	}
	switch rb {
	case 1, 2:
	default:
		return nil, fmt.Errorf("region_bytes must be 1 or 2: %v", rb)
	}

	return &RegionJSON{
		name:          name,
		regionMap:     rmap,
		regionBytes:   rb,
		unknownParams: FindUnknownParams(m, regionJSONParams),
	}, nil
}

// String returns the name of the vindex.
func (rv *RegionJSON) String() string {
	return rv.name
}

// Cost returns the cost of this index as 1.
func (rv *RegionJSON) Cost() int {
	return 1
}

// IsUnique returns true since the Vindex is unique.
func (rv *RegionJSON) IsUnique() bool {
	return true
}

// NeedsVCursor satisfies the Vindex interface.
func (rv *RegionJSON) NeedsVCursor() bool {
	return false
}

// Map satisfies MultiColumn.
func (rv *RegionJSON) Map(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value) ([]key.Destination, error) {
	destinations := make([]key.Destination, 0, len(rowsColValues))
	for _, row := range rowsColValues {
		if len(row) != 2 {
			destinations = append(destinations, key.DestinationNone{})
			continue
		}
		// Compute hash.
		hn, err := evalengine.ToUint64(row[0])
		if err != nil {
			destinations = append(destinations, key.DestinationNone{})
			continue
		}
		h := vhash(hn)

		rn, ok := rv.regionMap[row[1].ToString()]
		if !ok {
			destinations = append(destinations, key.DestinationNone{})
			continue
		}
		r := make([]byte, 2)
		binary.BigEndian.PutUint16(r, uint16(rn))

		// Concatenate and add to destinations.
		if rv.regionBytes == 1 {
			r = r[1:]
		}
		dest := append(r, h...)
		destinations = append(destinations, key.DestinationKeyspaceID(dest))
	}
	return destinations, nil
}

// Verify satisfies MultiColumn
func (rv *RegionJSON) Verify(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte) ([]bool, error) {
	result := make([]bool, len(rowsColValues))
	destinations, _ := rv.Map(ctx, vcursor, rowsColValues)
	for i, dest := range destinations {
		destksid, ok := dest.(key.DestinationKeyspaceID)
		if !ok {
			continue
		}
		result[i] = bytes.Equal([]byte(destksid), ksids[i])
	}
	return result, nil
}

func (rv *RegionJSON) PartialVindex() bool {
	return false
}

// UnknownParams implements the ParamValidating interface.
func (rv *RegionJSON) UnknownParams() []string {
	return rv.unknownParams
}
