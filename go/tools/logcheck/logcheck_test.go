/*
Copyright 2026 The Vitess Authors.

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

package logcheck

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/analysis/analysistest"
)

// TestAnalyzer checks that the analyzer reports exactly the findings marked
// with "// want" comments in testdata/src, which has stubs of go/vt/log and
// go/vt/logutil with the API they have today, plus ThrottledLogger's
// structured Info, Warn and Error methods. Packages without such comments
// (lookalikes, nottracked, testfiles, generated, and the stubs themselves)
// must produce no findings.
func TestAnalyzer(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), Analyzer,
		"constants",
		"targets",
		"funcvalues",
		"calls",
		"prefix",
		"testfiles",
		"lookalikes",
		"nottracked",
		"generated",
		"slogdirect",
		"vitess.io/vitess/go/vt/log",
		"vitess.io/vitess/go/vt/logutil",
	)
}

// TestDiagnosticRanges checks that a finding covers the whole message or
// prefix argument, or the whole qualified function used as a value.
func TestDiagnosticRanges(t *testing.T) {
	var got []string
	for _, result := range analysistest.Run(t, analysistest.TestData(), Analyzer, "funcvalues", "prefix", "calls") {
		for _, d := range result.Diagnostics {
			start, end := result.Pass.Fset.Position(d.Pos), result.Pass.Fset.Position(d.End)
			src, err := os.ReadFile(start.Filename)
			require.NoError(t, err)
			got = append(got, string(src[start.Offset:end.Offset]))
		}
	}
	assert.ElementsMatch(t, []string{
		"log.Error",
		"log.ErrorDepth",
		"log.Info",
		"pl.Warn",
		"tl.Error",
		"tl.Infof",
		"v.Info",
		"log.Info",
		"log.Warn",
		"log.DefaultHooks.OnError",
		"log.NewPrefixedLogger",
		"name",
		"component + name",
		`fmt.Sprintf("bad %s", name)`,
		"name",
		"(*logutil.ThrottledLogger).Errorf",
		"b.Warningf",
		"b.Warn",
		"(*log.PrefixedLogger).Error",
		"name", "name", "name", "name", "name", "name", "name", "name", "name", "name",
	}, got)
}

// TestAnalyzerWithoutGlog runs against a go/vt/log without glog's V, Verbose
// and Flush, and with Enabled, the API it has once glog support is removed.
func TestAnalyzerWithoutGlog(t *testing.T) {
	analysistest.Run(t, filepath.Join(analysistest.TestData(), "noglog"), Analyzer, "enabled")
}
