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
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var (
	_ MultiColumn = (*RegionExperimental)(nil)
)

func init() {
	Register("region_experimental", NewRegionExperimental)
}

// RegionExperimental defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// Unique and a Lookup.
type RegionExperimental struct {
	name        string
	regionBytes int
}

// NewRegionExperimental creates a RegionExperimental vindex.
// The supplied map requires all the fields of "consistent_lookup_unique".
// Additionally, it requires a region_bytes argument whose value can be "1", or "2".
func NewRegionExperimental(name string, m map[string]string) (Vindex, error) {
	rbs, ok := m["region_bytes"]
	if !ok {
		return nil, fmt.Errorf("region_experimental missing region_bytes param")
	}
	var rb int
	switch rbs {
	case "1":
		rb = 1
	case "2":
		rb = 2
	default:
		return nil, fmt.Errorf("region_bits must be 1 or 2: %v", rbs)
	}
	return &RegionExperimental{
		name:        name,
		regionBytes: rb,
	}, nil
}

// String returns the name of the vindex.
func (ge *RegionExperimental) String() string {
	return ge.name
}

// Cost returns the cost of this index as 1.
func (ge *RegionExperimental) Cost() int {
	return 1
}

// IsUnique returns true since the Vindex is unique.
func (ge *RegionExperimental) IsUnique() bool {
	return true
}

// NeedsVCursor satisfies the Vindex interface.
func (ge *RegionExperimental) NeedsVCursor() bool {
	return false
}

// Map satisfies MultiColumn.
func (ge *RegionExperimental) Map(vcursor VCursor, rowsColValues [][]sqltypes.Value) ([]key.Destination, error) {
	destinations := make([]key.Destination, 0, len(rowsColValues))
	for _, row := range rowsColValues {
		if len(row) != 2 {
			destinations = append(destinations, key.DestinationNone{})
			continue
		}
		// Compute region prefix.
		rn, err := sqltypes.ToUint64(row[0])
		if err != nil {
			destinations = append(destinations, key.DestinationNone{})
			continue
		}
		r := make([]byte, 2, 2+8)
		binary.BigEndian.PutUint16(r, uint16(rn))

		// Compute hash.
		hn, err := sqltypes.ToUint64(row[1])
		if err != nil {
			destinations = append(destinations, key.DestinationNone{})
			continue
		}
		h := vhash(hn)

		// Concatenate and add to destinations.
		if ge.regionBytes == 1 {
			r = r[1:]
		}
		dest := append(r, h...)
		destinations = append(destinations, key.DestinationKeyspaceID(dest))
	}
	return destinations, nil
}

// Verify satisfies MultiColumn.
func (ge *RegionExperimental) Verify(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte) ([]bool, error) {
	result := make([]bool, len(rowsColValues))
	destinations, _ := ge.Map(vcursor, rowsColValues)
	for i, dest := range destinations {
		destksid, ok := dest.(key.DestinationKeyspaceID)
		if !ok {
			continue
		}
		result[i] = bytes.Equal([]byte(destksid), ksids[i])
	}
	return result, nil
}
