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
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	regionExperimentalParamRegionBytes = "region_bytes"
)

var (
	_ MultiColumn     = (*RegionExperimental)(nil)
	_ ParamValidating = (*RegionExperimental)(nil)

	regionExperimentalParams = []string{
		regionExperimentalParamRegionBytes,
	}
)

func init() {
	Register("region_experimental", newRegionExperimental)
}

// RegionExperimental is a multi-column unique vindex. The first column is prefixed
// to the hash of the second column to produce the keyspace id.
// RegionExperimental can be used for geo-partitioning because the first column can denote a region,
// and its value will dictate the shard for that region.
type RegionExperimental struct {
	name          string
	regionBytes   int
	unknownParams []string
}

// newRegionExperimental creates a RegionExperimental vindex.
// The supplied map requires all the fields of "consistent_lookup_unique".
// Additionally, it requires a region_bytes argument whose value can be "1", or "2".
func newRegionExperimental(name string, m map[string]string) (Vindex, error) {
	rbs, ok := m[regionExperimentalParamRegionBytes]
	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, fmt.Sprintf("region_experimental missing %s param", regionExperimentalParamRegionBytes))
	}
	var rb int
	switch rbs {
	case "1":
		rb = 1
	case "2":
		rb = 2
	default:
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "region_bytes must be 1 or 2: %v", rbs)
	}
	return &RegionExperimental{
		name:          name,
		regionBytes:   rb,
		unknownParams: FindUnknownParams(m, regionExperimentalParams),
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
func (ge *RegionExperimental) Map(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value) ([]key.Destination, error) {
	destinations := make([]key.Destination, 0, len(rowsColValues))
	for _, row := range rowsColValues {
		if len(row) > 2 {
			destinations = append(destinations, key.DestinationNone{})
			continue
		}
		// Compute region prefix.
		rn, err := row[0].ToCastUint64()
		if err != nil {
			destinations = append(destinations, key.DestinationNone{})
			continue
		}
		r := make([]byte, 2, 2+8)
		binary.BigEndian.PutUint16(r, uint16(rn))

		// Concatenate and add to destinations.
		if ge.regionBytes == 1 {
			r = r[1:]
		}

		dest := r
		if len(row) == 2 {
			// Compute hash.
			hn, err := row[1].ToCastUint64()
			if err != nil {
				destinations = append(destinations, key.DestinationNone{})
				continue
			}
			h := vhash(hn)
			dest = append(dest, h...)
			destinations = append(destinations, key.DestinationKeyspaceID(dest))
		} else {
			destinations = append(destinations, NewKeyRangeFromPrefix(dest))
		}
	}
	return destinations, nil
}

// Verify satisfies MultiColumn.
func (ge *RegionExperimental) Verify(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte) ([]bool, error) {
	result := make([]bool, len(rowsColValues))
	destinations, _ := ge.Map(ctx, vcursor, rowsColValues)
	for i, dest := range destinations {
		destksid, ok := dest.(key.DestinationKeyspaceID)
		if !ok {
			continue
		}
		result[i] = bytes.Equal([]byte(destksid), ksids[i])
	}
	return result, nil
}

func (ge *RegionExperimental) PartialVindex() bool {
	return true
}

// UnknownParams implements the ParamValidating interface.
func (ge *RegionExperimental) UnknownParams() []string {
	return ge.unknownParams
}
