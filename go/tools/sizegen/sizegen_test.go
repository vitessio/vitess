package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFullGeneration(t *testing.T) {
	result, err := GenerateSizeHelpers([]string{"./integration/..."}, []string{"vitess.io/vitess/go/tools/sizegen/integration.*"})
	require.NoError(t, err)

	verifyErrors := VerifyFilesOnDisk(result)
	require.Empty(t, verifyErrors)

	for _, file := range result {
		contents := fmt.Sprintf("%#v", file)
		require.Contains(t, contents, "http://www.apache.org/licenses/LICENSE-2.0")
		require.Contains(t, contents, "type cachedObject interface")
		require.Contains(t, contents, "//go:nocheckptr")
	}
}
