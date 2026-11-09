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
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/analysis/analysistest"
	"golang.org/x/tools/go/packages"
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

// TestRealLoggingFunctions checks that messageParam matches the logging
// functions of the real go/vt/log and go/vt/logutil, which TestAnalyzer
// doesn't see, so that a change to their signatures can't turn the message
// rule off unnoticed. log.V(n).Info is left out because removing glog
// support removes it.
func TestRealLoggingFunctions(t *testing.T) {
	pkgs, err := packages.Load(&packages.Config{
		Context: t.Context(),
		Mode:    packages.NeedName | packages.NeedTypes,
	}, logPkgPath, logutilPkgPath)
	require.NoError(t, err)
	byPath := make(map[string]*types.Package)
	for _, pkg := range pkgs {
		require.Empty(t, pkg.Errors, pkg.PkgPath)
		byPath[pkg.PkgPath] = pkg.Types
	}

	for path, names := range map[string][]string{
		logPkgPath: {
			"Debug", "Info", "Warn", "Error",
			"DebugDepth", "InfoDepth", "WarnDepth", "ErrorDepth",
			"PrefixedLogger.Info", "PrefixedLogger.Warn", "PrefixedLogger.Error",
			"PrefixedLogger.InfoDepth", "PrefixedLogger.WarnDepth", "PrefixedLogger.ErrorDepth",
		},
		logutilPkgPath: {
			"ThrottledLogger.Info", "ThrottledLogger.Warn", "ThrottledLogger.Error",
		},
	} {
		pkg := byPath[path]
		require.NotNil(t, pkg, path)
		for _, name := range names {
			obj := lookup(pkg, name)
			if assert.NotNil(t, obj, "%s.%s not found", path, name) {
				assert.GreaterOrEqual(t, messageParam(obj), 0, "%s.%s is not a logging function", path, name)
			}
		}
	}

	obj := lookup(byPath[logPkgPath], "NewPrefixedLogger")
	assert.True(t, obj != nil && isNewPrefixedLogger(obj), "log.NewPrefixedLogger not found")
}

// lookup returns the package-level object name, or the method of *T for a
// name of the form T.M.
func lookup(pkg *types.Package, name string) types.Object {
	typeName, method, ok := strings.Cut(name, ".")
	if !ok {
		return pkg.Scope().Lookup(name)
	}
	tn, _ := pkg.Scope().Lookup(typeName).(*types.TypeName)
	if tn == nil {
		return nil
	}
	obj, _, _ := types.LookupFieldOrMethod(types.NewPointer(tn.Type()), false, pkg, method)
	return obj
}

// TestAnalyzerWithoutGlog runs against a go/vt/log without glog's V, Verbose
// and Flush, and with Enabled, the API it has once glog support is removed.
func TestAnalyzerWithoutGlog(t *testing.T) {
	analysistest.Run(t, filepath.Join(analysistest.TestData(), "noglog"), Analyzer, "enabled")
}
