/*
Copyright 2022 The Vitess Authors.

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

/*

A Vindex which uses a mapping lookup table `placement_map` to set the first `placement_prefix_bytes` of the Keyspace ID
and another Vindex type `placement_sub_vindex_type` (which must support Hashing) as a sub-Vindex to set the rest.
This is suitable for regional sharding (like region_json or region_experimental) but does not require a mapping file,
and can support non-integer types for the sharding column. All parameters are prefixed with `placement_` so as to avoid
conflict, because the `params` map is passed to the sub-Vindex as well.

*/

package vindexes

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var (
	_ MultiColumn = (*Placement)(nil)

	PlacementRequiredParams = []string{
		"placement_map",
		"placement_prefix_bytes",
		"placement_sub_vindex_type",
	}
)

func init() {
	Register("placement", NewPlacement)
}

type PlacementMap map[string]uint64

type Placement struct {
	name          string
	placementMap  PlacementMap
	subVindex     Vindex
	subVindexType string
	subVindexName string
	prefixBytes   int
}

// Parse a string containing a list of delimited string:integer key-value pairs, e.g. "foo:1,bar:2".
func parsePlacementMap(s string) (*PlacementMap, error) {
	placementMap := make(PlacementMap)
	for _, entry := range strings.Split(s, ",") {
		if entry == "" {
			continue
		}

		kv := strings.Split(entry, ":")
		if len(kv) != 2 {
			return nil, fmt.Errorf("entry: %v; expected key:value", entry)
		}
		if kv[0] == "" {
			return nil, fmt.Errorf("entry: %v; unexpected empty key", entry)
		}
		if kv[1] == "" {
			return nil, fmt.Errorf("entry: %v; unexpected empty value", entry)
		}

		value, err := strconv.ParseUint(kv[1], 0, 64)
		if err != nil {
			return nil, fmt.Errorf("entry: %v; %v", entry, err)
		}
		placementMap[kv[0]] = value
	}
	return &placementMap, nil
}

func NewPlacement(name string, params map[string]string) (Vindex, error) {
	var missingParams []string
	for _, param := range PlacementRequiredParams {
		if params[param] == "" {
			missingParams = append(missingParams, param)
		}
	}

	if len(missingParams) > 0 {
		return nil, fmt.Errorf("missing params: %s", strings.Join(missingParams, ", "))
	}

	placementMap, parseError := parsePlacementMap(params["placement_map"])
	if parseError != nil {
		return nil, fmt.Errorf("malformed placement_map; %v", parseError)
	}

	prefixBytes, prefixError := strconv.Atoi(params["placement_prefix_bytes"])
	if prefixError != nil {
		return nil, prefixError
	}

	if prefixBytes < 1 || prefixBytes > 7 {
		return nil, fmt.Errorf("invalid placement_prefix_bytes: %v; expected integer between 1 and 7", prefixBytes)
	}

	subVindexType := params["placement_sub_vindex_type"]
	subVindexName := fmt.Sprintf("%s_sub_vindex", name)
	subVindex, createVindexError := CreateVindex(subVindexType, subVindexName, params)
	if createVindexError != nil {
		return nil, fmt.Errorf("invalid placement_sub_vindex_type: %v", createVindexError)
	}

	// TODO: Should we support MultiColumn Vindex?
	if _, subVindexSupportsHashing := subVindex.(Hashing); !subVindexSupportsHashing {
		return nil, fmt.Errorf("invalid placement_sub_vindex_type: %v; does not support the Hashing interface", createVindexError)
	}

	return &Placement{
		name:          name,
		placementMap:  *placementMap,
		subVindex:     subVindex,
		subVindexType: subVindexType,
		subVindexName: subVindexName,
		prefixBytes:   prefixBytes,
	}, nil
}

func (p *Placement) String() string {
	return p.name
}

func (p *Placement) Cost() int {
	return 1
}

func (p *Placement) IsUnique() bool {
	return true
}

func (p *Placement) NeedsVCursor() bool {
	return false
}

func (p *Placement) PartialVindex() bool {
	return true
}

func makeDestinationPrefix(value uint64, prefixBytes int) []byte {
	destinationPrefix := make([]byte, 8)
	binary.BigEndian.PutUint64(destinationPrefix, value)
	if prefixBytes < 8 {
		// Shorten the prefix to the desired length.
		destinationPrefix = destinationPrefix[(8 - prefixBytes):]
	}

	return destinationPrefix
}

func (p *Placement) Map(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value) ([]key.Destination, error) {
	destinations := make([]key.Destination, 0, len(rowsColValues))

	for _, row := range rowsColValues {
		if len(row) != 1 && len(row) != 2 {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "wrong number of column values were passed: expected 1-2, got %d", len(row))
		}

		// Calculate the destination prefix from the placement key which will be the same whether this is a partial
		// or full usage of the Vindex.
		placementKey := row[0].ToString()
		placementDestinationValue, placementMappingFound := p.placementMap[placementKey]
		if !placementMappingFound {
			destinations = append(destinations, key.DestinationNone{})
			continue
		}

		placementDestinationPrefix := makeDestinationPrefix(placementDestinationValue, p.prefixBytes)

		if len(row) == 1 { // Partial Vindex usage with only the placement column provided.
			destinations = append(destinations, NewKeyRangeFromPrefix(placementDestinationPrefix))
		} else if len(row) == 2 { // Full Vindex usage with the placement column and subVindex column provided.
			subVindexValue, hashingError := p.subVindex.(Hashing).Hash(row[1])
			if hashingError != nil {
				return nil, hashingError // TODO: Should we be less fatal here and use DestinationNone?
			}

			// Concatenate and add to destinations.
			rowDestination := append(placementDestinationPrefix, subVindexValue...)
			destinations = append(destinations, key.DestinationKeyspaceID(rowDestination[0:8]))
		}
	}

	return destinations, nil
}

func (p *Placement) Verify(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, keyspaceIDs [][]byte) ([]bool, error) {
	result := make([]bool, len(rowsColValues))
	destinations, _ := p.Map(ctx, vcursor, rowsColValues)
	for i, destination := range destinations {
		switch d := destination.(type) {
		case key.DestinationKeyspaceID:
			result[i] = bytes.Equal(d, keyspaceIDs[i])
		default:
			result[i] = false
		}
	}
	return result, nil
}
