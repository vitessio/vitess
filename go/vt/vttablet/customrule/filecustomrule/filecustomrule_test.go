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

package filecustomrule

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
)

var customRule1 = `[
				{
					"Name": "r1",
					"Description": "disallow bindvar 'asdfg'",
					"BindVarConds":[{
						"Name": "asdfg",
						"OnAbsent": false,
						"Operator": ""
					}]
				}
			]`

func TestFileCustomRule(t *testing.T) {
	tqsc := tabletservermock.NewController()

	var qrs *rules.Rules
	rulepath := path.Join(os.TempDir(), ".customrule.json")
	// Set r1 and try to get it back
	err := os.WriteFile(rulepath, []byte(customRule1), os.FileMode(0o644))
	require.NoErrorf(t, err, "Cannot write r1 to rule file %s, err=%v", rulepath, err)

	fcr := NewFileCustomRule()
	// Let FileCustomRule to build rule from the local file
	err = fcr.Open(tqsc, rulepath)
	require.NoError(t, err)
	// Fetch query rules we built to verify correctness
	qrs, _, err = fcr.GetRules()
	require.NoError(t, err)
	qr := qrs.Find("r1")
	require.NotNilf(t, qr, "Expect custom rule r1 to be found, but got nothing, qrs=%v", qrs)
}

// TestActivateFileCustomRulesLoadFailure covers what happens at vttablet
// startup when the custom rules file cannot be loaded: by default the
// tablet keeps serving without file based custom rules (fail-open), and with
// --filecustomrules-strict the load error is fatal instead.
// See https://github.com/vitessio/vitess/issues/20522.
func TestActivateFileCustomRulesLoadFailure(t *testing.T) {
	badRulePath := path.Join(t.TempDir(), "bad-customrule.json")
	err := os.WriteFile(badRulePath, []byte("{ not valid json"), os.FileMode(0o644))
	require.NoError(t, err)

	goodRulePath := path.Join(t.TempDir(), "good-customrule.json")
	err = os.WriteFile(goodRulePath, []byte(customRule1), os.FileMode(0o644))
	require.NoError(t, err)

	// setup points the package-level state at the given rules file and strict
	// setting, records calls to exitFunc, and restores everything on cleanup.
	setup := func(t *testing.T, rulePath string, strict bool) (exitCodes *[]int) {
		origPath, origStrict, origWatch := fileRulePath, fileRuleStrict, fileRuleShouldWatch
		origExit, origRule := exitFunc, fileCustomRule
		t.Cleanup(func() {
			fileRulePath, fileRuleStrict, fileRuleShouldWatch = origPath, origStrict, origWatch
			exitFunc, fileCustomRule = origExit, origRule
		})

		var codes []int
		fileRulePath = rulePath
		fileRuleStrict = strict
		fileRuleShouldWatch = false
		fileCustomRule = NewFileCustomRule()
		exitFunc = func(code int) { codes = append(codes, code) }
		return &codes
	}

	t.Run("load failure without strict keeps serving", func(t *testing.T) {
		exitCodes := setup(t, badRulePath, false)
		ActivateFileCustomRules(tabletservermock.NewController())
		require.Empty(t, *exitCodes)
	})

	t.Run("load failure with strict is fatal", func(t *testing.T) {
		exitCodes := setup(t, badRulePath, true)
		ActivateFileCustomRules(tabletservermock.NewController())
		require.Equal(t, []int{1}, *exitCodes)
	})

	t.Run("valid file with strict starts normally", func(t *testing.T) {
		exitCodes := setup(t, goodRulePath, true)
		ActivateFileCustomRules(tabletservermock.NewController())
		require.Empty(t, *exitCodes)

		qrs, _, err := fileCustomRule.GetRules()
		require.NoError(t, err)
		require.NotNil(t, qrs.Find("r1"))
	})
}
