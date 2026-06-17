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

package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	fixtureRoot = "testdata/root"
	pkgA        = "vitess.io/vitess/go/test/endtoend/pkga"
	pkgB        = "vitess.io/vitess/go/test/endtoend/pkgb"
	pkgEmpty    = "vitess.io/vitess/go/test/endtoend/empty"
)

// requireProblem asserts that at least one problem contains all the given substrings.
func requireProblem(t *testing.T, problems []string, substrs ...string) {
	t.Helper()
	for _, p := range problems {
		all := true
		for _, s := range substrs {
			if !strings.Contains(p, s) {
				all = false
				break
			}
		}
		if all {
			return
		}
	}
	require.Failf(t, "expected problem not reported", "no problem contains all of %q in:\n%s", substrs, strings.Join(problems, "\n"))
}

func TestIsTestName(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"Test", true},
		{"TestFoo", true},
		{"Test_foo", true},
		{"Test1", true},
		{"Testfoo", false},
		{"Testïdent", false},
		{"TestÏdent", true},
		{"TeFoo", false},
		{"BenchmarkFoo", false},
		{"testFoo", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isTestName(tc.name))
		})
	}
}

func TestSplitRegexp(t *testing.T) {
	tests := []struct {
		pattern string
		want    []string
	}{
		{"TestFoo", []string{"TestFoo"}},
		{"A/B", []string{"A", "B"}},
		{"[a/b]", []string{"[a/b]"}},
		{"(a/b)", []string{"(a/b)"}},
		{"A/[b/c]/d", []string{"A", "[b/c]", "d"}},
		{`a\/b`, []string{`a\/b`}},
		{"", []string{""}},
	}
	for _, tc := range tests {
		t.Run(tc.pattern, func(t *testing.T) {
			require.Equal(t, tc.want, splitRegexp(tc.pattern))
		})
	}
}

func TestRunPattern(t *testing.T) {
	t.Run("separate value", func(t *testing.T) {
		pat, found, err := runPattern([]string{"-run", "TestFoo", "-timeout", "15m"})
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "TestFoo", pat)
	})
	t.Run("equals form", func(t *testing.T) {
		pat, found, err := runPattern([]string{"-run=TestFoo"})
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "TestFoo", pat)
	})
	t.Run("absent", func(t *testing.T) {
		_, found, err := runPattern([]string{"-timeout", "15m"})
		require.NoError(t, err)
		require.False(t, found)
	})
	t.Run("no args", func(t *testing.T) {
		_, found, err := runPattern(nil)
		require.NoError(t, err)
		require.False(t, found)
	})
	t.Run("dangling -run", func(t *testing.T) {
		_, _, err := runPattern([]string{"-run"})
		require.ErrorContains(t, err, "-run")
	})
}

func TestListTests(t *testing.T) {
	t.Run("pkga", func(t *testing.T) {
		tests, err := listTests(fixtureRoot + "/go/test/endtoend/pkga")
		require.NoError(t, err)
		require.Equal(t, []string{"TestBar", "TestExternal", "TestFoo", "TestLinuxOnly"}, tests)
	})
	t.Run("pkgb TestMain with *testing.T is a regular test", func(t *testing.T) {
		tests, err := listTests(fixtureRoot + "/go/test/endtoend/pkgb")
		require.NoError(t, err)
		require.Equal(t, []string{"TestB1", "TestMain"}, tests)
	})
	t.Run("no test functions", func(t *testing.T) {
		tests, err := listTests(fixtureRoot + "/go/test/endtoend/empty")
		require.NoError(t, err)
		require.Empty(t, tests)
	})
	t.Run("missing dir", func(t *testing.T) {
		_, err := listTests(fixtureRoot + "/go/test/endtoend/nosuchpkg")
		require.Error(t, err)
	})
}

func TestLoadConfigs(t *testing.T) {
	entries, problems := loadConfigs([]string{"testdata/config_a.json", "testdata/config_b.json"})
	require.Len(t, entries, 3)
	requireProblem(t, problems, "dup_entry", "testdata/config_a.json", "testdata/config_b.json")
	require.Len(t, problems, 1)
}

func entryFor(name string, test *Test) entry {
	return entry{configFile: "test/config.json", name: name, test: test}
}

func TestRunChecks(t *testing.T) {
	t.Run("entry without -run covers all tests", func(t *testing.T) {
		problems := runChecks(fixtureRoot, []entry{
			entryFor("all_pkga", &Test{Packages: []string{pkgA}}),
		})
		require.Empty(t, problems)
	})

	t.Run("dead regex is reported", func(t *testing.T) {
		problems := runChecks(fixtureRoot, []entry{
			entryFor("all_pkga", &Test{Packages: []string{pkgA}}),
			entryFor("dead", &Test{Packages: []string{pkgA}, Args: []string{"-run", "TestNope"}}),
		})
		requireProblem(t, problems, "dead", "TestNope", pkgA)
		require.Len(t, problems, 1)
	})

	t.Run("invalid regex is reported", func(t *testing.T) {
		problems := runChecks(fixtureRoot, []entry{
			entryFor("all_pkga", &Test{Packages: []string{pkgA}}),
			entryFor("bad", &Test{Packages: []string{pkgA}, Args: []string{"-run", "Test["}}),
		})
		requireProblem(t, problems, "bad", "Test[")
		require.Len(t, problems, 1)
	})

	t.Run("dangling -run is reported", func(t *testing.T) {
		problems := runChecks(fixtureRoot, []entry{
			entryFor("all_pkga", &Test{Packages: []string{pkgA}}),
			entryFor("dangling", &Test{Packages: []string{pkgA}, Args: []string{"-run"}}),
		})
		requireProblem(t, problems, "dangling", "-run")
		require.Len(t, problems, 1)
	})

	t.Run("orphaned tests are reported", func(t *testing.T) {
		problems := runChecks(fixtureRoot, []entry{
			entryFor("foo_only", &Test{Packages: []string{pkgA}, Args: []string{"-run", "TestFoo"}}),
		})
		requireProblem(t, problems, pkgA, "TestBar")
		requireProblem(t, problems, pkgA, "TestExternal")
		requireProblem(t, problems, pkgA, "TestLinuxOnly")
		require.Len(t, problems, 3)
	})

	t.Run("only the first slash-separated regex element selects top-level tests", func(t *testing.T) {
		problems := runChecks(fixtureRoot, []entry{
			entryFor("all_pkga", &Test{Packages: []string{pkgA}}),
			entryFor("subtest", &Test{Packages: []string{pkgA}, Args: []string{"-run", "TestFoo/sub/case"}}),
		})
		require.Empty(t, problems)
	})

	t.Run("multi-package entry must match in every package", func(t *testing.T) {
		problems := runChecks(fixtureRoot, []entry{
			entryFor("all_pkga", &Test{Packages: []string{pkgA}}),
			entryFor("all_pkgb", &Test{Packages: []string{pkgB}}),
			entryFor("multi", &Test{Packages: []string{pkgA, pkgB}, Args: []string{"-run", "TestFoo"}}),
		})
		requireProblem(t, problems, "multi", "TestFoo", pkgB)
		require.Len(t, problems, 1)
	})

	t.Run("missing package directory is reported", func(t *testing.T) {
		problems := runChecks(fixtureRoot, []entry{
			entryFor("missing", &Test{Packages: []string{"vitess.io/vitess/go/test/endtoend/nosuchpkg"}}),
		})
		requireProblem(t, problems, "missing", "nosuchpkg")
		require.Len(t, problems, 1)
	})

	t.Run("package import path outside the module is reported", func(t *testing.T) {
		problems := runChecks(fixtureRoot, []entry{
			entryFor("foreign", &Test{Packages: []string{"github.com/foo/bar"}}),
		})
		requireProblem(t, problems, "foreign", "github.com/foo/bar")
		require.Len(t, problems, 1)
	})

	t.Run("package with no test functions is reported", func(t *testing.T) {
		problems := runChecks(fixtureRoot, []entry{
			entryFor("empty", &Test{Packages: []string{pkgEmpty}}),
		})
		requireProblem(t, problems, "empty", pkgEmpty)
		require.Len(t, problems, 1)
	})

	t.Run("entry without packages is skipped", func(t *testing.T) {
		problems := runChecks(fixtureRoot, []entry{
			entryFor("command_only", &Test{Command: []string{"some/script.sh"}}),
		})
		require.Empty(t, problems)
	})

	t.Run("manual entries neither provide coverage nor pull packages into scope", func(t *testing.T) {
		// A manual entry alone does not bring pkga into scope: no problems.
		problems := runChecks(fixtureRoot, []entry{
			entryFor("manual_all", &Test{Packages: []string{pkgA}, Manual: true}),
		})
		require.Empty(t, problems)

		// A manual entry does not count as coverage for a package brought
		// into scope by a non-manual entry.
		problems = runChecks(fixtureRoot, []entry{
			entryFor("manual_all", &Test{Packages: []string{pkgA}, Manual: true}),
			entryFor("foo_only", &Test{Packages: []string{pkgA}, Args: []string{"-run", "TestFoo"}}),
		})
		requireProblem(t, problems, pkgA, "TestBar")
		requireProblem(t, problems, pkgA, "TestExternal")
		requireProblem(t, problems, pkgA, "TestLinuxOnly")
		require.Len(t, problems, 3)
	})
}
