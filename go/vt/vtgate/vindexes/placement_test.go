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

package vindexes

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

func createBasicPlacementVindex(t *testing.T) (Vindex, error) {
	return CreateVindex("placement", "placement", map[string]string{
		"table":                     "t",
		"from":                      "f1,f2",
		"to":                        "toc",
		"placement_prefix_bytes":    "1",
		"placement_map":             "foo:1,bar:2",
		"placement_sub_vindex_type": "xxhash",
	})
}

func TestPlacementName(t *testing.T) {
	vindex, err := createBasicPlacementVindex(t)
	require.NoError(t, err)
	assert.Equal(t, "placement", vindex.String())
}

func TestPlacementCost(t *testing.T) {
	vindex, err := createBasicPlacementVindex(t)
	require.NoError(t, err)
	assert.Equal(t, 1, vindex.Cost())
}

func TestPlacementIsUnique(t *testing.T) {
	vindex, err := createBasicPlacementVindex(t)
	require.NoError(t, err)
	assert.True(t, vindex.IsUnique())
}

func TestPlacementNeedsVCursor(t *testing.T) {
	vindex, err := createBasicPlacementVindex(t)
	require.NoError(t, err)
	assert.False(t, vindex.NeedsVCursor())
}

func TestPlacementNoParams(t *testing.T) {
	_, err := CreateVindex("placement", "placement", nil)
	assert.EqualError(t, err, "missing params: placement_map, placement_prefix_bytes, placement_sub_vindex_type")
}

func TestPlacementPlacementMapMissing(t *testing.T) {
	_, err := CreateVindex("placement", "placement", map[string]string{
		"placement_prefix_bytes":    "1",
		"placement_sub_vindex_type": "hash",
	})
	assert.EqualError(t, err, "missing params: placement_map")
}

func TestPlacementPlacementMapMalformedMap(t *testing.T) {
	_, err := CreateVindex("placement", "placement", map[string]string{
		"placement_map":             "xyz",
		"placement_prefix_bytes":    "1",
		"placement_sub_vindex_type": "hash",
	})
	assert.EqualError(t, err, "malformed placement_map; entry: xyz; expected key:value")
}

func TestPlacementPlacementMapMissingKey(t *testing.T) {
	_, err := CreateVindex("placement", "placement", map[string]string{
		"placement_map":             "abc:1,:2,ghi:3",
		"placement_prefix_bytes":    "1",
		"placement_sub_vindex_type": "hash",
	})
	assert.EqualError(t, err, "malformed placement_map; entry: :2; unexpected empty key")
}

func TestPlacementPlacementMapMissingValue(t *testing.T) {
	_, err := CreateVindex("placement", "placement", map[string]string{
		"placement_map":             "abc:1,def:,ghi:3",
		"placement_prefix_bytes":    "1",
		"placement_sub_vindex_type": "hash",
	})
	assert.EqualError(t, err, "malformed placement_map; entry: def:; unexpected empty value")
}

func TestPlacementPlacementMapMalformedValue(t *testing.T) {
	_, err := CreateVindex("placement", "placement", map[string]string{
		"placement_map":             "abc:xyz",
		"placement_prefix_bytes":    "1",
		"placement_sub_vindex_type": "hash",
	})
	assert.EqualError(t, err, "malformed placement_map; entry: abc:xyz; strconv.ParseUint: parsing \"xyz\": invalid syntax")
}

func TestPlacementPrefixBytesMissing(t *testing.T) {
	_, err := CreateVindex("placement", "placement", map[string]string{
		"placement_map":             "foo:1,bar:2",
		"placement_sub_vindex_type": "hash",
	})
	assert.EqualError(t, err, "missing params: placement_prefix_bytes")
}

func TestPlacementPrefixBytesTooLow(t *testing.T) {
	_, err := CreateVindex("placement", "placement", map[string]string{
		"placement_map":             "foo:1,bar:2",
		"placement_prefix_bytes":    "0",
		"placement_sub_vindex_type": "hash",
	})
	assert.EqualError(t, err, "invalid placement_prefix_bytes: 0; expected integer between 1 and 7")
}

func TestPlacementPrefixBytesTooHigh(t *testing.T) {
	_, err := CreateVindex("placement", "placement", map[string]string{
		"placement_map":             "foo:1,bar:2",
		"placement_prefix_bytes":    "17",
		"placement_sub_vindex_type": "hash",
	})
	assert.EqualError(t, err, "invalid placement_prefix_bytes: 17; expected integer between 1 and 7")
}

func TestPlacementSubVindexTypeMissing(t *testing.T) {
	_, err := CreateVindex("placement", "placement", map[string]string{
		"placement_map":          "foo:1,bar:2",
		"placement_prefix_bytes": "1",
	})
	assert.EqualError(t, err, "missing params: placement_sub_vindex_type")
}

func TestPlacementSubVindexTypeIncorrect(t *testing.T) {
	_, err := CreateVindex("placement", "placement", map[string]string{
		"placement_map":             "foo:1,bar:2",
		"placement_prefix_bytes":    "1",
		"placement_sub_vindex_type": "doesnotexist",
	})
	assert.EqualError(t, err, "invalid placement_sub_vindex_type: vindexType \"doesnotexist\" not found")
}

func TestPlacementMapSqlTypeVarChar(t *testing.T) {
	vindex, err := CreateVindex("placement", "placement", map[string]string{
		"table":                     "t",
		"from":                      "f1,f2",
		"to":                        "toc",
		"placement_prefix_bytes":    "1",
		"placement_map":             "foo:1,bar:2",
		"placement_sub_vindex_type": "xxhash",
	})
	assert.NoError(t, err)
	actualDestinations, err := vindex.(MultiColumn).Map(context.Background(), nil, [][]sqltypes.Value{{
		sqltypes.NewVarChar("foo"), sqltypes.NewVarChar("hello world"),
	}, {
		sqltypes.NewVarChar("bar"), sqltypes.NewVarChar("hello world"),
	}, {
		sqltypes.NewVarChar("xyz"), sqltypes.NewVarChar("hello world"),
	}})
	assert.NoError(t, err)

	expectedDestinations := []key.Destination{
		key.DestinationKeyspaceID{0x01, 0x68, 0x69, 0x1e, 0xb2, 0x34, 0x67, 0xab},
		key.DestinationKeyspaceID{0x02, 0x68, 0x69, 0x1e, 0xb2, 0x34, 0x67, 0xab},
		key.DestinationNone{},
	}
	assert.Equal(t, expectedDestinations, actualDestinations)
}

func TestPlacementMapSqlTypeInt64(t *testing.T) {
	vindex, err := CreateVindex("placement", "placement", map[string]string{
		"table":                     "t",
		"from":                      "f1,f2",
		"to":                        "toc",
		"placement_prefix_bytes":    "1",
		"placement_map":             "foo:1,bar:2",
		"placement_sub_vindex_type": "xxhash",
	})
	assert.NoError(t, err)
	actualDestinations, err := vindex.(MultiColumn).Map(context.Background(), nil, [][]sqltypes.Value{{
		sqltypes.NewVarChar("foo"), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewVarChar("bar"), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewVarChar("xyz"), sqltypes.NewInt64(1),
	}})
	assert.NoError(t, err)

	expectedDestinations := []key.Destination{
		key.DestinationKeyspaceID{0x01, 0xd4, 0x64, 0x05, 0x36, 0x76, 0x12, 0xb4},
		key.DestinationKeyspaceID{0x02, 0xd4, 0x64, 0x05, 0x36, 0x76, 0x12, 0xb4},
		key.DestinationNone{},
	}
	assert.Equal(t, expectedDestinations, actualDestinations)
}

func TestPlacementMapWithHexValues(t *testing.T) {
	vindex, err := CreateVindex("placement", "placement", map[string]string{
		"table":                     "t",
		"from":                      "f1,f2",
		"to":                        "toc",
		"placement_prefix_bytes":    "1",
		"placement_map":             "foo:0x01,bar:0x02",
		"placement_sub_vindex_type": "xxhash",
	})
	assert.NoError(t, err)
	actualDestinations, err := vindex.(MultiColumn).Map(context.Background(), nil, [][]sqltypes.Value{{
		sqltypes.NewVarChar("foo"), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewVarChar("bar"), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewVarChar("xyz"), sqltypes.NewInt64(1),
	}})
	assert.NoError(t, err)

	expectedDestinations := []key.Destination{
		key.DestinationKeyspaceID{0x01, 0xd4, 0x64, 0x05, 0x36, 0x76, 0x12, 0xb4},
		key.DestinationKeyspaceID{0x02, 0xd4, 0x64, 0x05, 0x36, 0x76, 0x12, 0xb4},
		key.DestinationNone{},
	}
	assert.Equal(t, expectedDestinations, actualDestinations)
}

func TestPlacementMapMultiplePrefixBytes(t *testing.T) {
	vindex, err := CreateVindex("placement", "placement", map[string]string{
		"table":                     "t",
		"from":                      "f1,f2",
		"to":                        "toc",
		"placement_prefix_bytes":    "4",
		"placement_map":             "foo:1,bar:2",
		"placement_sub_vindex_type": "xxhash",
	})
	assert.NoError(t, err)
	actualDestinations, err := vindex.(MultiColumn).Map(context.Background(), nil, [][]sqltypes.Value{{
		sqltypes.NewVarChar("foo"), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewVarChar("bar"), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewVarChar("xyz"), sqltypes.NewInt64(1),
	}})
	assert.NoError(t, err)

	expectedDestinations := []key.Destination{
		key.DestinationKeyspaceID{0x00, 0x00, 0x00, 0x01, 0xd4, 0x64, 0x05, 0x36},
		key.DestinationKeyspaceID{0x00, 0x00, 0x00, 0x02, 0xd4, 0x64, 0x05, 0x36},
		key.DestinationNone{},
	}
	assert.Equal(t, expectedDestinations, actualDestinations)
}

func TestPlacementSubVindexNumeric(t *testing.T) {
	vindex, err := CreateVindex("placement", "placement", map[string]string{
		"table":                     "t",
		"from":                      "f1,f2",
		"to":                        "toc",
		"placement_prefix_bytes":    "1",
		"placement_map":             "foo:1,bar:2",
		"placement_sub_vindex_type": "numeric",
	})
	assert.NoError(t, err)
	actualDestinations, err := vindex.(MultiColumn).Map(context.Background(), nil, [][]sqltypes.Value{{
		sqltypes.NewVarChar("foo"), sqltypes.NewInt64(0x01234567deadbeef),
	}, {
		sqltypes.NewVarChar("bar"), sqltypes.NewInt64(0x01234567deadbeef),
	}, {
		sqltypes.NewVarChar("xyz"), sqltypes.NewInt64(0x01234567deadbeef),
	}})
	assert.NoError(t, err)

	expectedDestinations := []key.Destination{
		key.DestinationKeyspaceID{0x01, 0x01, 0x23, 0x45, 0x67, 0xde, 0xad, 0xbe},
		key.DestinationKeyspaceID{0x02, 0x01, 0x23, 0x45, 0x67, 0xde, 0xad, 0xbe},
		key.DestinationNone{},
	}
	assert.Equal(t, expectedDestinations, actualDestinations)
}

func TestPlacementMapPrefixOnly(t *testing.T) {
	vindex, err := CreateVindex("placement", "placement", map[string]string{
		"table":                     "t",
		"from":                      "f1,f2",
		"to":                        "toc",
		"placement_prefix_bytes":    "1",
		"placement_map":             "foo:1,bar:2",
		"placement_sub_vindex_type": "xxhash",
	})
	assert.NoError(t, err)
	actualDestinations, err := vindex.(MultiColumn).Map(context.Background(), nil, [][]sqltypes.Value{{
		sqltypes.NewVarChar("foo"),
	}, {
		sqltypes.NewVarChar("bar"),
	}, {
		sqltypes.NewVarChar("xyz"),
	}})
	assert.NoError(t, err)

	expectedDestinations := []key.Destination{
		NewKeyRangeFromPrefix([]byte{0x01}),
		NewKeyRangeFromPrefix([]byte{0x02}),
		key.DestinationNone{},
	}
	assert.Equal(t, expectedDestinations, actualDestinations)
}

func TestPlacementMapTooManyColumns(t *testing.T) {
	vindex, err := CreateVindex("placement", "placement", map[string]string{
		"table":                     "t",
		"from":                      "f1,f2",
		"to":                        "toc",
		"placement_prefix_bytes":    "1",
		"placement_map":             "foo:1,bar:2",
		"placement_sub_vindex_type": "xxhash",
	})
	assert.NoError(t, err)
	actualDestinations, err := vindex.(MultiColumn).Map(context.Background(), nil, [][]sqltypes.Value{{
		// Too many columns; expecting two, providing three.
		sqltypes.NewVarChar("a"), sqltypes.NewVarChar("b"), sqltypes.NewVarChar("c"),
	}})
	assert.EqualError(t, err, "wrong number of column values were passed: expected 1-2, got 3")

	assert.Nil(t, actualDestinations)
}

func TestPlacementMapNoColumns(t *testing.T) {
	vindex, err := CreateVindex("placement", "placement", map[string]string{
		"table":                     "t",
		"from":                      "f1,f2",
		"to":                        "toc",
		"placement_prefix_bytes":    "1",
		"placement_map":             "foo:1,bar:2",
		"placement_sub_vindex_type": "xxhash",
	})
	assert.NoError(t, err)
	actualDestinations, err := vindex.(MultiColumn).Map(context.Background(), nil, [][]sqltypes.Value{{
		// Empty column list.
	}})
	assert.EqualError(t, err, "wrong number of column values were passed: expected 1-2, got 0")

	assert.Nil(t, actualDestinations)
}
