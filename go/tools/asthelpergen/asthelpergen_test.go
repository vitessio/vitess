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
