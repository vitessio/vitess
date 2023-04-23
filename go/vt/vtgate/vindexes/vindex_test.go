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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

func init() {
	Register("allow_unknown_params", &vindexFactory{
		allowUnknownParams: true,
		create: func(_ string, _ map[string]string) (Vindex, []VindexWarning, error) {
			return nil, nil, nil
		},
		params: []VindexParam{
			&vindexParam{name: "option1"},
			&vindexParam{name: "option2"},
		},
	})
	Register("warn_unknown_params", &vindexFactory{
		allowUnknownParams: false,
		create: func(_ string, _ map[string]string) (Vindex, []VindexWarning, error) {
			return nil, nil, nil
		},
		params: []VindexParam{
			&vindexParam{name: "option1"},
			&vindexParam{name: "option2"},
		},
	})
}

func TestVindexMap(t *testing.T) {
	ge, _, err := createRegionVindex(t, "region_experimental", "f1,f2", 1)
	assert.NoError(t, err)

	got, err := Map(context.Background(), ge, nil, [][]sqltypes.Value{{
		sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}})
	assert.NoError(t, err)

	want := []key.Destination{
		key.DestinationKeyspaceID([]byte("\x01\x16k@\xb4J\xbaK\xd6")),
	}
	assert.Equal(t, want, got)

	hash, _, err := CreateVindex("hash", "hash", nil)
	assert.NoError(t, err)
	got, err = Map(context.Background(), hash, nil, [][]sqltypes.Value{{
		sqltypes.NewInt64(1),
	}})
	assert.NoError(t, err)
	want = []key.Destination{
		key.DestinationKeyspaceID([]byte("\x16k@\xb4J\xbaK\xd6")),
	}
	assert.Equal(t, want, got)
}

func TestVindexVerify(t *testing.T) {
	ge, _, err := createRegionVindex(t, "region_experimental", "f1,f2", 1)
	assert.NoError(t, err)

	got, err := Verify(context.Background(), ge, nil, [][]sqltypes.Value{{
		sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}}, [][]byte{
		[]byte("\x01\x16k@\xb4J\xbaK\xd6"),
	})
	assert.NoError(t, err)

	want := []bool{true}
	assert.Equal(t, want, got)

	hash, _, err := CreateVindex("hash", "hash", nil)
	assert.NoError(t, err)
	got, err = Verify(context.Background(), hash, nil, [][]sqltypes.Value{{
		sqltypes.NewInt64(1),
	}}, [][]byte{
		[]byte("\x16k@\xb4J\xbaK\xd6"),
	})
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestCreateVindexAllowUnknownParams(t *testing.T) {
	vindex, warnings, err := CreateVindex(
		"allow_unknown_params",
		"allow_unknown_params",
		map[string]string{
			"option1": "value1",
			"option2": "value2",
			"option3": "value3",
			"option4": "value4",
		},
	)

	require.Nil(t, vindex)
	require.NoError(t, err)
	require.Len(t, warnings, 0)
}

func TestCreateVindexWarnUnknownParams(t *testing.T) {
	vindex, warnings, err := CreateVindex(
		"warn_unknown_params",
		"warn_unknown_params",
		map[string]string{
			"option1": "value1",
			"option2": "value2",
			"option3": "value3",
			"option4": "value4",
		},
	)

	require.Nil(t, vindex)
	require.NoError(t, err)

	require.Len(t, warnings, 2)
	require.EqualError(t, warnings[0], "unknown param 'option3'")
	require.EqualError(t, warnings[1], "unknown param 'option4'")
}
