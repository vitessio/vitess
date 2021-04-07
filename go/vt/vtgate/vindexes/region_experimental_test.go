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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

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
	got, err := ge.Map(nil, [][]sqltypes.Value{{
		sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewInt64(255), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewInt64(256), sqltypes.NewInt64(1),
	}, {
		// Invalid length.
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
		key.DestinationNone{},
		key.DestinationNone{},
		key.DestinationNone{},
	}
	assert.Equal(t, want, got)
}

func TestRegionExperimentalMapMulti2(t *testing.T) {
	vindex, err := createRegionVindex(t, "region_experimental", "f1,f2", 2)
	assert.NoError(t, err)
	ge := vindex.(MultiColumn)
	got, err := ge.Map(nil, [][]sqltypes.Value{{
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
	got, err := ge.Verify(nil, vals, ksids)
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestRegionExperimentalCreateErrors(t *testing.T) {
	_, err := createRegionVindex(t, "region_experimental", "f1,f2", 3)
	assert.EqualError(t, err, "region_bits must be 1 or 2: 3")
	_, err = CreateVindex("region_experimental", "region_experimental", nil)
	assert.EqualError(t, err, "region_experimental missing region_bytes param")
}

func createRegionVindex(t *testing.T, name, from string, rb int) (Vindex, error) {
	return CreateVindex(name, name, map[string]string{
		"region_bytes": strconv.Itoa(rb),
		"table":        "t",
		"from":         from,
		"to":           "toc",
	})
}
