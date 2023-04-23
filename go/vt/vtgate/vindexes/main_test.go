package vindexes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type createVindexTestCase struct {
	testName string

	vindexType   string
	vindexName   string
	vindexParams map[string]string

	expectCost         int
	expectErr          error
	expectIsUnique     bool
	expectNeedsVCursor bool
	expectString       string
	expectWarnings     []VindexWarning
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
	fns ...func(createVindexTestCase, Vindex, []VindexWarning, error),
) {
	t.Run(tc.testName, func(t *testing.T) {
		vdx, warnings, err := CreateVindex(
			tc.vindexType,
			tc.vindexName,
			tc.vindexParams,
		)
		assertEqualVtError(t, tc.expectErr, err)
		for _, expectWarning := range tc.expectWarnings {
			found := false
			for _, warning := range warnings {
				if warning.Error() == expectWarning.Error() {
					found = true
				}
			}
			if !found {
				require.Fail(t, "expected a warning with error message", expectWarning.Error())
			}
		}
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
	fns ...func(createVindexTestCase, Vindex, []VindexWarning, error),
) {
	for _, tc := range tcs {
		testCreateVindex(t, tc, fns...)
	}
}
