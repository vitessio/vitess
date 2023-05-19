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

type testVindex struct {
	allowUnknownParams bool
	knownParams        []*Param
	params             map[string]string
}

func (v *testVindex) Cost() int {
	return 0
}

func (v *testVindex) String() string {
	return ""
}

func (v *testVindex) IsUnique() bool {
	return false
}

func (v *testVindex) NeedsVCursor() bool {
	return false
}

func (v *testVindex) InvalidParamErrors() []error {
	return ValidateParams(v.params, &ParamValidationOpts{AllowUnknown: v.allowUnknownParams, Params: v.knownParams})
}

func init() {
	Register("allow_unknown_params", func(_ string, params map[string]string) (Vindex, error) {
		return &testVindex{
			allowUnknownParams: true,
			knownParams: []*Param{
				{Name: "option1"},
				{Name: "option2"},
			},
			params: params,
		}, nil
	})
	Register("warn_unknown_params", func(_ string, params map[string]string) (Vindex, error) {
		return &testVindex{
			allowUnknownParams: false,
			knownParams: []*Param{
				{Name: "option1"},
				{Name: "option2"},
			},
			params: params,
		}, nil
	})
}

func TestVindexMap(t *testing.T) {
	ge, err := createRegionVindex(t, "region_experimental", "f1,f2", 1)
	assert.NoError(t, err)

	got, err := Map(context.Background(), ge, nil, [][]sqltypes.Value{{
		sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}})
	assert.NoError(t, err)

	want := []key.Destination{
		key.DestinationKeyspaceID([]byte("\x01\x16k@\xb4J\xbaK\xd6")),
	}
	assert.Equal(t, want, got)

	hash, err := CreateVindex("hash", "hash", nil)
	assert.NoError(t, err)
	require.Empty(t, hash.(ParamValidating).InvalidParamErrors())
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
	ge, err := createRegionVindex(t, "region_experimental", "f1,f2", 1)
	assert.NoError(t, err)
	require.Empty(t, ge.(ParamValidating).InvalidParamErrors())

	got, err := Verify(context.Background(), ge, nil, [][]sqltypes.Value{{
		sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}}, [][]byte{
		[]byte("\x01\x16k@\xb4J\xbaK\xd6"),
	})
	assert.NoError(t, err)

	want := []bool{true}
	assert.Equal(t, want, got)

	hash, err := CreateVindex("hash", "hash", nil)
	require.Empty(t, hash.(ParamValidating).InvalidParamErrors())
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
	vindex, err := CreateVindex(
		"allow_unknown_params",
		"allow_unknown_params",
		map[string]string{
			"option1": "value1",
			"option2": "value2",
			"option3": "value3",
			"option4": "value4",
		},
	)

	require.NotNil(t, vindex)
	require.NoError(t, err)
}

func TestCreateVindexWarnUnknownParams(t *testing.T) {
	vindex, err := CreateVindex(
		"warn_unknown_params",
		"warn_unknown_params",
		map[string]string{
			"option1": "value1",
			"option2": "value2",
			"option3": "value3",
			"option4": "value4",
		},
	)

	require.NotNil(t, vindex)
	require.NoError(t, err)

	warnings := vindex.(ParamValidating).InvalidParamErrors()
	require.Len(t, warnings, 2)
	for _, msg := range []string{"unknown param 'option3'", "unknown param 'option4'"} {
		if msg == warnings[0].Error() || msg == warnings[1].Error() {
			continue
		}
		require.Fail(t, "expected one warning to have error message: %s", msg)
	}
}
