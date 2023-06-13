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
	"strconv"
	"testing"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

func regionExperimentalCreateVindexTestCase(
	testName string,
	vindexParams map[string]string,
	expectErr error,
	expectUnknownParams []string,
) createVindexTestCase {
	return createVindexTestCase{
		testName: testName,

		vindexType:   "region_experimental",
		vindexName:   "region_experimental",
		vindexParams: vindexParams,

		expectCost:          1,
		expectErr:           expectErr,
		expectIsUnique:      true,
		expectNeedsVCursor:  false,
		expectString:        "region_experimental",
		expectUnknownParams: expectUnknownParams,
	}
}

func TestRegionExperimentalCreateVindex(t *testing.T) {
	cases := []createVindexTestCase{
		regionExperimentalCreateVindexTestCase(
			"no params invalid: region_bytes required",
			nil,
			vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "region_experimental missing region_bytes param"),
			nil,
		),
		regionExperimentalCreateVindexTestCase(
			"empty params invalid: region_bytes required",
			map[string]string{},
			vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "region_experimental missing region_bytes param"),
			nil,
		),
		regionExperimentalCreateVindexTestCase(
			"region_bytes may not be 0",
			map[string]string{
				"region_bytes": "0",
			},
			vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "region_bytes must be 1 or 2: 0"),
			nil,
		),
		regionExperimentalCreateVindexTestCase(
			"region_bytes may be 1",
			map[string]string{
				"region_bytes": "1",
			},
			nil,
			nil,
		),
		regionExperimentalCreateVindexTestCase(
			"region_bytes may be 2",
			map[string]string{
				"region_bytes": "2",
			},
			nil,
			nil,
		),
		regionExperimentalCreateVindexTestCase(
			"region_bytes may not be 3",
			map[string]string{
				"region_bytes": "3",
			},
			vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "region_bytes must be 1 or 2: 3"),
			nil,
		),
		regionExperimentalCreateVindexTestCase(
			"unknown params",
			map[string]string{
				"region_bytes": "1",
				"hello":        "world",
			},
			nil,
			[]string{"hello"},
		),
	}

	testCreateVindexes(t, cases)
}

func TestRegionExperimentalMisc(t *testing.T) {
	ge, err := createRegionVindex(t, "region_experimental", "f1,f2", 1)
	require.NoError(t, err)
	assert.Equal(t, 1, ge.Cost())
	assert.Equal(t, "region_experimental", ge.String())
	assert.True(t, ge.IsUnique())
	assert.False(t, ge.NeedsVCursor())
}

func TestRegionExperimentalMap(t *testing.T) {
	vindex, err := createRegionVindex(t, "region_experimental", "f1,f2", 1)
	assert.NoError(t, err)
	ge := vindex.(MultiColumn)
	got, err := ge.Map(context.Background(), nil, [][]sqltypes.Value{{
		sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewInt64(255), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewInt64(256), sqltypes.NewInt64(1),
	}, {
		// only region id provided, partial column for key range mapping.
		sqltypes.NewInt64(1),
	}, {
		// Invalid region.
		sqltypes.NewVarBinary("abcd"), sqltypes.NewInt64(256),
	}, {
		// Invalid id.
		sqltypes.NewInt64(1), sqltypes.NewVarBinary("abcd"),
	}})
	assert.NoError(t, err)

	want := []key.Destination{
		key.DestinationKeyspaceID([]byte("\x01\x16k@\xb4J\xbaK\xd6")),
		key.DestinationKeyspaceID([]byte("\xff\x16k@\xb4J\xbaK\xd6")),
		key.DestinationKeyspaceID([]byte("\x00\x16k@\xb4J\xbaK\xd6")),
		key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{Start: []byte("\x01"), End: []byte("\x02")}},
		key.DestinationNone{},
		key.DestinationNone{},
	}
	assert.Equal(t, want, got)
}

func TestRegionExperimentalMapMulti2(t *testing.T) {
	vindex, err := createRegionVindex(t, "region_experimental", "f1,f2", 2)
	assert.NoError(t, err)
	ge := vindex.(MultiColumn)
	got, err := ge.Map(context.Background(), nil, [][]sqltypes.Value{{
		sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewInt64(255), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewInt64(256), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewInt64(0x10000), sqltypes.NewInt64(1),
	}})
	assert.NoError(t, err)

	want := []key.Destination{
		key.DestinationKeyspaceID([]byte("\x00\x01\x16k@\xb4J\xbaK\xd6")),
		key.DestinationKeyspaceID([]byte("\x00\xff\x16k@\xb4J\xbaK\xd6")),
		key.DestinationKeyspaceID([]byte("\x01\x00\x16k@\xb4J\xbaK\xd6")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x16k@\xb4J\xbaK\xd6")),
	}
	assert.Equal(t, want, got)
}

func TestRegionExperimentalVerifyMulti(t *testing.T) {
	vindex, err := createRegionVindex(t, "region_experimental", "f1,f2", 1)
	assert.NoError(t, err)
	ge := vindex.(MultiColumn)
	vals := [][]sqltypes.Value{{
		// One for match
		sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		// One for mismatch
		sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		// One invalid value
		sqltypes.NewInt64(1),
	}}
	ksids := [][]byte{
		[]byte("\x01\x16k@\xb4J\xbaK\xd6"),
		[]byte("no match"),
		[]byte(""),
	}

	want := []bool{true, false, false}
	got, err := ge.Verify(context.Background(), nil, vals, ksids)
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func createRegionVindex(t *testing.T, name, from string, rb int) (Vindex, error) {
	return CreateVindex(name, name, map[string]string{
		"region_bytes": strconv.Itoa(rb),
	})
}
