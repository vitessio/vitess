/*
Copyright 2023 The Vitess Authors.

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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type createVindexTestCase struct {
	testName string

	vindexType   string
	vindexName   string
	vindexParams map[string]string

	expectCost          int
	expectErr           error
	expectIsUnique      bool
	expectNeedsVCursor  bool
	expectString        string
	expectUnknownParams []string
}

func assertEqualVtError(t *testing.T, expected, actual error) {
	// vterrors.Errorf returns a struct containing a stacktrace, which fails
	// assert.EqualError since the stacktrace would be guaranteed to be different.
	// so just check the error message
	if expected == nil {
		assert.NoError(t, actual)
	} else {
		assert.EqualError(t, actual, expected.Error())
	}
}

func testCreateVindex(
	t *testing.T,
	tc createVindexTestCase,
	fns ...func(createVindexTestCase, Vindex, []error, error),
) {
	t.Run(tc.testName, func(t *testing.T) {
		vdx, err := CreateVindex(
			tc.vindexType,
			tc.vindexName,
			tc.vindexParams,
		)
		assertEqualVtError(t, tc.expectErr, err)
		if err == nil {
			assert.NotNil(t, vdx)
		}
		paramValidating, ok := vdx.(ParamValidating)
		var unknownParams []string
		if ok {
			unknownParams = paramValidating.UnknownParams()
		}
		require.Equal(t, len(tc.expectUnknownParams), len(unknownParams))
		sort.Strings(tc.expectUnknownParams)
		sort.Strings(unknownParams)
		require.Equal(t, tc.expectUnknownParams, unknownParams)
		if vdx != nil {
			assert.Equal(t, tc.expectString, vdx.String())
			assert.Equal(t, tc.expectCost, vdx.Cost())
			assert.Equal(t, tc.expectIsUnique, vdx.IsUnique())
			assert.Equal(t, tc.expectNeedsVCursor, vdx.NeedsVCursor())
		}
	})
}

func testCreateVindexes(
	t *testing.T,
	tcs []createVindexTestCase,
	fns ...func(createVindexTestCase, Vindex, []error, error),
) {
	for _, tc := range tcs {
		testCreateVindex(t, tc, fns...)
	}
}
