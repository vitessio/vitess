package asthelpergen

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFullGeneration(t *testing.T) {
	result, err := GenerateASTHelpers([]string{"./integration/..."}, "vitess.io/vitess/go/tools/asthelpergen/integration.AST", "*NoCloneType")
	require.NoError(t, err)

	verifyErrors := VerifyFilesOnDisk(result)
	require.Empty(t, verifyErrors)

	for _, file := range result {
		contents := fmt.Sprintf("%#v", file)
		require.Contains(t, contents, "http://www.apache.org/licenses/LICENSE-2.0")
		applyIdx := strings.Index(contents, "func (a *application) apply(parent, node AST, replacer replacerFunc)")
		cloneIdx := strings.Index(contents, "CloneAST(in AST) AST")
		if applyIdx == 0 && cloneIdx == 0 {
			t.Fatalf("file doesn't contain expected contents")
		}
	}
}
