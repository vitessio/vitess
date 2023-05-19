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
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// createVindex creates the "numeric_static_map" vindex object which is used by
// each test.
func createVindex() (SingleColumn, error) {
	m := make(map[string]string)
	m["json_path"] = "testdata/numeric_static_map_test.json"
	vindex, err := CreateVindex("numeric_static_map", "numericStaticMap", m)
	if err != nil {
		panic(err)
	}
	return vindex.(SingleColumn), nil
}

func numericStaticMapCreateVindexTestCase(
	testName string,
	vindexParams map[string]string,
	expectErr error,
	expectUnknownParams []string,
) createVindexTestCase {
	return createVindexTestCase{
		testName: testName,

		vindexType:   "numeric_static_map",
		vindexName:   "numeric_static_map",
		vindexParams: vindexParams,

		expectCost:          1,
		expectErr:           expectErr,
		expectIsUnique:      true,
		expectNeedsVCursor:  false,
		expectString:        "numeric_static_map",
		expectUnknownParams: expectUnknownParams,
	}
}

func TestNumericStaticMapCreateVindex(t *testing.T) {
	cases := []createVindexTestCase{
		numericStaticMapCreateVindexTestCase(
			"no params invalid, require either json_path or json",
			nil,
			vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "NumericStaticMap: Could not find either `json_path` or `json` params in vschema"),
			nil,
		),
		numericStaticMapCreateVindexTestCase(
			"empty params invalid, require either json_path or json",
			map[string]string{},
			vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "NumericStaticMap: Could not find either `json_path` or `json` params in vschema"),
			nil,
		),
		numericStaticMapCreateVindexTestCase(
			"json_path and json mutually exclusive",
			map[string]string{
				"json":      "{}",
				"json_path": "/path/to/map.json",
			},
			vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "NumericStaticMap: Found both `json` and `json_path` params in vschema"),
			nil,
		),
		numericStaticMapCreateVindexTestCase(
			"json_path must exist",
			map[string]string{
				"json_path": "/path/to/map.json",
			},
			errors.New("open /path/to/map.json: no such file or directory"),
			nil,
		),
		numericStaticMapCreateVindexTestCase(
			"json ok",
			map[string]string{
				"json": "{}",
			},
			nil,
			nil,
		),
		numericStaticMapCreateVindexTestCase(
			"json must be valid syntax",
			map[string]string{
				"json": "{]",
			},
			errors.New("invalid character ']' looking for beginning of object key string"),
			nil,
		),
		numericStaticMapCreateVindexTestCase(
			"fallback_type ok",
			map[string]string{
				"json":          "{}",
				"fallback_type": "binary",
			},
			nil,
			nil,
		),
		numericStaticMapCreateVindexTestCase(
			"fallback_type must be valid vindex type",
			map[string]string{
				"json":          "{}",
				"fallback_type": "not_found",
			},
			vterrors.Errorf(vtrpc.Code_NOT_FOUND, "vindexType %q not found", "not_found"),
			nil,
		),
		numericStaticMapCreateVindexTestCase(
			"unknown params",
			map[string]string{
				"json":  "{}",
				"hello": "world",
			},
			nil,
			[]string{"hello"},
		),
	}

	testCreateVindexes(t, cases)
}

func TestNumericStaticMapMap(t *testing.T) {
	numericStaticMap, err := createVindex()
	if err != nil {
		t.Fatalf("failed to create vindex: %v", err)
	}
	got, err := numericStaticMap.Map(context.Background(), nil, []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
		sqltypes.NewInt64(3),
		sqltypes.NewFloat64(1.1),
		sqltypes.NewInt64(4),
		sqltypes.NewInt64(5),
		sqltypes.NewInt64(6),
		sqltypes.NewInt64(7),
		sqltypes.NewInt64(8),
		sqltypes.NULL,
	})
	require.NoError(t, err)

	// in the third slice, we expect 2 instead of 3 as numeric_static_map_test.json
	// has 3 mapped to 2
	want := []key.Destination{
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x01")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x02")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x02")),
		key.DestinationNone{},
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x04")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x05")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x06")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x07")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x08")),
		key.DestinationNone{},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %+v, want %+v", got, want)
	}
}

func TestNumericStaticMapVerify(t *testing.T) {
	numericStaticMap, err := createVindex()
	if err != nil {
		t.Fatalf("failed to create vindex: %v", err)
	}
	got, err := numericStaticMap.Verify(context.Background(), nil, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, [][]byte{[]byte("\x00\x00\x00\x00\x00\x00\x00\x01"), []byte("\x00\x00\x00\x00\x00\x00\x00\x01")})
	require.NoError(t, err)
	want := []bool{true, false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("lhu.Verify(match): %v, want %v", got, want)
	}

	// Failure test
	_, err = numericStaticMap.Verify(context.Background(), nil, []sqltypes.Value{sqltypes.NewVarBinary("aa")}, [][]byte{nil})
	require.EqualError(t, err, "could not parse value: 'aa'")
}

func TestNumericStaticMapWithJsonVdx(t *testing.T) {
	withFallbackVdx, err := CreateVindex(
		"numeric_static_map",
		t.Name(),
		map[string]string{
			"json": "{\"1\":2,\"3\":4,\"5\":6}",
		},
	)

	require.NoError(t, err)
	require.Empty(t, withFallbackVdx.(ParamValidating).UnknownParams())
	assert.Equal(t, 1, withFallbackVdx.Cost())
	assert.Equal(t, t.Name(), withFallbackVdx.String())
	assert.True(t, withFallbackVdx.IsUnique())
	assert.False(t, withFallbackVdx.NeedsVCursor())

	// Bad format tests
	_, err = CreateVindex(
		"numeric_static_map",
		t.Name(),
		map[string]string{
			"json": "{\"1\":2,\"3\":4,\"5\":6:8,\"10\":11}",
		},
	)
	require.EqualError(t, err, "invalid character ':' after object key:value pair")

	// Letters in key or value not allowed
	_, err = CreateVindex(
		"numeric_static_map",
		t.Name(),
		map[string]string{"json": "{\"1\":a}"},
	)
	require.EqualError(t, err, "invalid character 'a' looking for beginning of value")
	_, err = CreateVindex(
		"numeric_static_map",
		t.Name(),
		map[string]string{"json": "{\"a\":1}"},
	)
	require.EqualError(t, err, "strconv.ParseUint: parsing \"a\": invalid syntax")
}

// Test mapping of vindex, both for specified map keys and underlying xxhash
func TestNumericStaticMapWithFallback(t *testing.T) {
	mapWithFallbackVdx, err := CreateVindex(
		"numeric_static_map",
		t.Name(),
		map[string]string{
			"json":          "{\"1\":2,\"3\":4,\"4\":5,\"5\":6,\"6\":7,\"7\":8,\"8\":9,\"10\":18446744073709551615}",
			"fallback_type": "xxhash",
		},
	)
	if err != nil {
		t.Fatalf("failed to create vindex: %v", err)
	}
	require.Empty(t, mapWithFallbackVdx.(ParamValidating).UnknownParams())
	singleCol := mapWithFallbackVdx.(SingleColumn)
	got, err := singleCol.Map(context.Background(), nil, []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
		sqltypes.NewInt64(3),
		sqltypes.NewFloat64(1.1),
		sqltypes.NewVarChar("test1"),
		sqltypes.NewInt64(4),
		sqltypes.NewInt64(5),
		sqltypes.NewInt64(6),
		sqltypes.NewInt64(7),
		sqltypes.NewInt64(8),
		sqltypes.NewInt64(10),
		sqltypes.NULL,
	})
	require.NoError(t, err)

	want := []key.Destination{
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x02")),
		key.DestinationKeyspaceID([]byte("\x8b\x59\x80\x16\x62\xb5\x21\x60")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x04")),
		key.DestinationNone{},
		key.DestinationNone{}, // strings do not map
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x05")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x06")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x07")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x08")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x00\x00\x00\x00\x00\x09")),
		key.DestinationKeyspaceID([]byte("\xff\xff\xff\xff\xff\xff\xff\xff")),
		key.DestinationNone{},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map()\ngot: %+v\nwant: %+v", got, want)
	}
}

func TestNumericStaticMapWithFallbackVerify(t *testing.T) {
	mapWithFallbackVdx, err := CreateVindex(
		"numeric_static_map",
		t.Name(),
		map[string]string{
			"json":          "{\"1\":2,\"3\":4,\"4\":5,\"5\":6,\"6\":7,\"7\":8,\"8\":9,\"10\":18446744073709551615}",
			"fallback_type": "xxhash",
		},
	)
	if err != nil {
		t.Fatalf("failed to create vindex: %v", err)
	}
	require.Empty(t, mapWithFallbackVdx.(ParamValidating).UnknownParams())
	singleCol := mapWithFallbackVdx.(SingleColumn)
	got, err := singleCol.Verify(context.Background(), nil, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2), sqltypes.NewInt64(11), sqltypes.NewInt64(10)}, [][]byte{[]byte("\x00\x00\x00\x00\x00\x00\x00\x02"), []byte("\x8b\x59\x80\x16\x62\xb5\x21\x60"), []byte("\xff\xff\xff\xff\xff\xff\xff\xff"), []byte("\xff\xff\xff\xff\xff\xff\xff\xff")})
	require.NoError(t, err)
	want := []bool{true, true, false, true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Verify(match): %v, want %v", got, want)
	}

	// Failure test
	_, err = singleCol.Verify(context.Background(), nil, []sqltypes.Value{sqltypes.NewVarBinary("aa")}, [][]byte{nil})
	require.EqualError(t, err, "could not parse value: 'aa'")
}
