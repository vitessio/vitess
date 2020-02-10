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
	"encoding/binary"
	"encoding/json"
	"io/ioutil"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
)

var (
	_ MultiColumn = (*RegionJson)(nil)
)

func init() {
	Register("region_json", NewRegionJson)
}

// RegionMap is used to store mapping of country to region
type RegionMap map[string]uint64

// RegionJson defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// Unique and a Lookup.
type RegionJson struct {
	name        string
	regionMap   RegionMap
	regionBytes int
}

// NewRegionJson creates a RegionJson vindex.
// The supplied map requires all the fields of "RegionExperimental".
// Additionally, it requires a region_map argument representing the path to a json file
// containing a map of country to region.
func NewRegionJson(name string, m map[string]string) (Vindex, error) {
	rmPath := m["region_map"]
	rmap := make(map[string]uint64)
	data, err := ioutil.ReadFile(rmPath)
	if err != nil {
		return nil, err
	}
	log.Infof("Loaded Region map from: %s", rmPath)
	err = json.Unmarshal(data, &rmap)
	if err != nil {
		return nil, err
	}

	return &RegionJson{
		name:      name,
		regionMap: rmap,
	}, nil
}

// String returns the name of the vindex.
func (rv *RegionJson) String() string {
	return rv.name
}

// Cost returns the cost of this index as 1.
func (rv *RegionJson) Cost() int {
	return 1
}

// IsUnique returns true since the Vindex is unique.
func (rv *RegionJson) IsUnique() bool {
	return true
}

// Map satisfies MultiColumn.
func (rv *RegionJson) Map(vcursor VCursor, rowsColValues [][]sqltypes.Value) ([]key.Destination, error) {
	destinations := make([]key.Destination, 0, len(rowsColValues))
	for _, row := range rowsColValues {
		if len(row) != 2 {
			destinations = append(destinations, key.DestinationNone{})
			continue
		}
		// Compute hash.
		hn, err := sqltypes.ToUint64(row[0])
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
func (rv *RegionJson) Verify(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte) ([]bool, error) {
	result := make([]bool, len(rowsColValues))
	destinations, _ := rv.Map(vcursor, rowsColValues)
	for i, dest := range destinations {
		destksid, ok := dest.(key.DestinationKeyspaceID)
		if !ok {
			continue
		}
		result[i] = bytes.Equal([]byte(destksid), ksids[i])
	}
	return result, nil
}

// NeedVCursor satisfies the Vindex interface.
func (rv *RegionJson) NeedsVCursor() bool {
	return false
}
