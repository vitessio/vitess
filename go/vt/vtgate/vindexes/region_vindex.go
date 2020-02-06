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
	"io/ioutil"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
)

var (
	_ MultiColumn = (*RegionVindex)(nil)
)

func init() {
	Register("region_vindex", NewRegionVindex)
}

// RegionMap is used to store mapping of country to region
type RegionMap map[string]uint64

// RegionVindex defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// Unique and a Lookup.
type RegionVindex struct {
	name        string
	regionMap   RegionMap
	regionBytes int
}

// NewRegionVindex creates a RegionVindex vindex.
// The supplied map requires all the fields of "region_experimental".
// Additionally, it requires a region_map argument representing the path to a json file
// containing a map of country to region.
func NewRegionVindex(name string, m map[string]string) (Vindex, error) {
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

	return &RegionVindex{
		name:      name,
		regionMap: rmap,
	}, nil
}

// String returns the name of the vindex.
func (rv *RegionVindex) String() string {
	return rv.name
}

// Cost returns the cost of this index as 1.
func (rv *RegionVindex) Cost() int {
	return 1
}

// IsUnique returns true since the Vindex is unique.
func (rv *RegionVindex) IsUnique() bool {
	return true
}

// Map satisfies MultiColumn.
func (rv *RegionVindex) Map(vcursor VCursor, rowsColValues [][]sqltypes.Value) ([]key.Destination, error) {
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

		log.Infof("Region map: %v", rv.regionMap)
		// Compute region prefix.
		log.Infof("Country: %v", row[1].ToString())
		rn, ok := rv.regionMap[row[1].ToString()]
		log.Infof("Found region %v, true/false: %v", rn, ok)
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
func (rv *RegionVindex) Verify(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte) ([]bool, error) {
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

// NeedVCursor staisfies the Vindex interface.
func (rv *RegionVindex) NeedsVCursor() bool {
	return false
}
