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
	_ Vindex        = (*GeoExperimental)(nil)
	_ Lookup        = (*GeoExperimental)(nil)
	_ WantOwnerInfo = (*GeoExperimental)(nil)
	_ MultiColumn   = (*GeoExperimental)(nil)
)

func init() {
	Register("geo_experimental", NewGeoExperimental)
}

// GeoExperimental defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// Unique and a Lookup.
type GeoExperimental struct {
	regionBytes int
	*ConsistentLookupUnique
}

// NewGeoExperimental creates a GeoExperimental vindex.
// The supplied map requires all the fields of "consistent_lookup_unique".
// Additionally, it requires a region_bytes argument whose value can be "1", or "2".
func NewGeoExperimental(name string, m map[string]string) (Vindex, error) {
	rbs, ok := m["region_bytes"]
	if !ok {
		return nil, fmt.Errorf("geo_experimental missing region_bytes param")
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
	vindex, err := NewConsistentLookupUnique(name, m)
	if err != nil {
		// Unreachable.
		return nil, err
	}
	cl := vindex.(*ConsistentLookupUnique)
	if len(cl.lkp.FromColumns) != 2 {
		return nil, fmt.Errorf("two columns are required for geo_experimental: %v", cl.lkp.FromColumns)
	}
	return &GeoExperimental{
		regionBytes:            rb,
		ConsistentLookupUnique: cl,
	}, nil
}

// MapMulti satisfies MultiColumn.
func (ge *GeoExperimental) MapMulti(vcursor VCursor, rowsColValues [][]sqltypes.Value) ([]key.Destination, error) {
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

		// Compute region prefix.
		rn, err := sqltypes.ToUint64(row[1])
		if err != nil {
			destinations = append(destinations, key.DestinationNone{})
			continue
		}
		r := make([]byte, 2)
		binary.BigEndian.PutUint16(r, uint16(rn))

		// Concatenate and add to destinations.
		if ge.regionBytes == 1 {
			r = r[1:]
		}
		dest := append(r, h...)
		destinations = append(destinations, key.DestinationKeyspaceID(dest))
	}
	return destinations, nil
}

// VerifyMulti satisfies MultiColumn.
func (ge *GeoExperimental) VerifyMulti(vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte) ([]bool, error) {
	result := make([]bool, len(rowsColValues))
	destinations, _ := ge.MapMulti(vcursor, rowsColValues)
	for i, dest := range destinations {
		destksid, ok := dest.(key.DestinationKeyspaceID)
		if !ok {
			continue
		}
		result[i] = bytes.Equal([]byte(destksid), ksids[i])
	}
	return result, nil
}
