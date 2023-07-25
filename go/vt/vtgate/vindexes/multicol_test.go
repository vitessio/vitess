/*
Copyright 2021 The Vitess Authors.

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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func multicolCreateVindexTestCase(
	testName string,
	vindexParams map[string]string,
	expectCost int,
	expectErr error,
	expectUnknownParams []string,
) createVindexTestCase {
	return createVindexTestCase{
		testName: testName,

		vindexType:   "multicol",
		vindexName:   "multicol",
		vindexParams: vindexParams,

		expectCost:          expectCost,
		expectErr:           expectErr,
		expectIsUnique:      true,
		expectNeedsVCursor:  false,
		expectString:        "multicol",
		expectUnknownParams: expectUnknownParams,
	}
}

func TestMulticolCreateVindex(t *testing.T) {
	cases := []createVindexTestCase{
		multicolCreateVindexTestCase(
			"column count 0 invalid",
			map[string]string{
				"column_count": "0",
			},
			0,
			vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "number of columns should be between 1 and 8 in the parameter 'column_count'"),
			nil,
		),
		multicolCreateVindexTestCase(
			"column count 3 ok",
			map[string]string{
				"column_count": "3",
			},
			3,
			nil,
			nil,
		),
		multicolCreateVindexTestCase(
			"column count 9 invalid",
			map[string]string{
				"column_count": "9",
			},
			0,
			vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "number of columns should be between 1 and 8 in the parameter 'column_count'"),
			nil,
		),
		multicolCreateVindexTestCase(
			"column bytes ok",
			map[string]string{
				"column_count": "3",
				"column_bytes": "1,2,3",
			},
			3,
			nil,
			nil,
		),
		multicolCreateVindexTestCase(
			"column bytes more than column count invalid",
			map[string]string{
				"column_count": "3",
				"column_bytes": "1,2,3,4",
			},
			0,
			vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "number of column bytes provided are more than column count in the parameter 'column_bytes'"),
			nil,
		),
		multicolCreateVindexTestCase(
			"column bytes exceeds keyspace id length",
			map[string]string{
				"column_count": "3",
				"column_bytes": "100,200,300",
			},
			3,
			vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "column bytes count exceeds the keyspace id length (total bytes count cannot exceed 8 bytes) in the parameter 'column_bytes'"),
			nil,
		),
		multicolCreateVindexTestCase(
			"column vindex ok",
			map[string]string{
				"column_count":  "3",
				"column_bytes":  "1,2,3",
				"column_vindex": "binary,binary,binary",
			},
			0,
			nil,
			nil,
		),
		multicolCreateVindexTestCase(
			"column vindex more than column count",
			map[string]string{
				"column_count":  "3",
				"column_bytes":  "1,2,3",
				"column_vindex": "binary,binary,binary,binary",
			},
			0,
			vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "number of vindex function provided are more than column count in the parameter 'column_vindex'"),
			nil,
		),
		multicolCreateVindexTestCase(
			"column vindex non-hashing invalid",
			map[string]string{
				"column_count":  "3",
				"column_bytes":  "1,2,3",
				"column_vindex": "binary,binary,null",
			},
			0,
			vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "multicol vindex supports vindexes that exports hashing function, are unique and are non-lookup vindex, passed vindex 'null' is invalid"),
			nil,
		),
		multicolCreateVindexTestCase(
			"column vindex non-unique invalid",
			map[string]string{
				"column_count":  "3",
				"column_bytes":  "1,2,3",
				"column_vindex": "binary,binary,cfc",
			},
			0,
			vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "multicol vindex supports vindexes that exports hashing function, are unique and are non-lookup vindex, passed vindex 'cfc' is invalid"),
			nil,
		),
		multicolCreateVindexTestCase(
			"column vindex lookup or needs vcursor invalid",
			map[string]string{
				"column_count":  "3",
				"column_bytes":  "1,2,3",
				"column_vindex": "binary,binary,lookup",
			},
			0,
			vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "multicol vindex supports vindexes that exports hashing function, are unique and are non-lookup vindex, passed vindex 'lookup' is invalid"),
			nil,
		),
		multicolCreateVindexTestCase(
			"no params",
			nil,
			0,
			vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "number of columns not provided in the parameter 'column_count'"),
			nil,
		),
		multicolCreateVindexTestCase(
			"empty params",
			map[string]string{},
			0,
			vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "number of columns not provided in the parameter 'column_count'"),
			nil,
		),
		multicolCreateVindexTestCase(
			"allow unknown params",
			map[string]string{
				"column_count": "1",
				"hello":        "world",
			},
			1,
			nil,
			nil,
		),
	}

	testCreateVindexes(t, cases)
}

func TestMultiColMisc(t *testing.T) {
	vindex, err := CreateVindex("multicol", "multicol_misc", map[string]string{
		"column_count": "3",
	})
	require.NoError(t, err)
	_, ok := vindex.(ParamValidating)
	require.False(t, ok)

	multiColVdx, isMultiColVdx := vindex.(*MultiCol)
	assert.True(t, isMultiColVdx)

	assert.Equal(t, 3, multiColVdx.Cost())
	assert.Equal(t, "multicol_misc", multiColVdx.String())
	assert.True(t, multiColVdx.IsUnique())
	assert.False(t, multiColVdx.NeedsVCursor())
	assert.True(t, multiColVdx.PartialVindex())
}

func TestMultiColMap(t *testing.T) {
	vindex, err := CreateVindex("multicol", "multicol_map", map[string]string{
		"column_count": "3",
	})
	require.NoError(t, err)
	mutiCol := vindex.(MultiColumn)

	got, err := mutiCol.Map(context.Background(), nil, [][]sqltypes.Value{{
		sqltypes.NewInt64(1), sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewInt64(255), sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewInt64(256), sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		// only one column provided, partial column for key range mapping.
		sqltypes.NewInt64(1),
	}, {
		// only two column provided, partial column for key range mapping.
		sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		// Invalid column value type.
		sqltypes.NewVarBinary("abcd"), sqltypes.NewInt64(256), sqltypes.NewInt64(256),
	}, {
		// Invalid column value type.
		sqltypes.NewInt64(256), sqltypes.NewInt64(256), sqltypes.NewVarBinary("abcd"),
	}})
	assert.NoError(t, err)

	want := []key.Destination{
		key.DestinationKeyspaceID("\x16\x6b\x40\x16\x6b\x40\x16\x6b"),
		key.DestinationKeyspaceID("\x25\x4e\x88\x16\x6b\x40\x16\x6b"),
		key.DestinationKeyspaceID("\xdd\x7c\x0b\x16\x6b\x40\x16\x6b"),
		key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x16\x6b\x40"), End: []byte("\x16\x6b\x41")}},
		key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x16\x6b\x40\x16\x6b\x40"), End: []byte("\x16\x6b\x40\x16\x6b\x41")}},
		key.DestinationNone{},
		key.DestinationNone{},
	}
	assert.Equal(t, want, got)
}
