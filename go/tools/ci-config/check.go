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
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

type (
	// Test mirrors the entry schema of test/config*.json (see the Test
	// struct in test.go at the repository root).
	Test struct {
		File     string
		Args     []string
		Command  []string
		Packages []string
		Manual   bool
		Shard    string
		Tags     []string
	}

	// Config is the overall object serialized in test/config*.json.
	Config struct {
		Tests map[string]*Test
	}

	// AllowedOrphan is a test function that is intentionally not run by any
	// config entry. Reason is mandatory: JSON has no comments, so the
	// justification must live in the data.
	AllowedOrphan struct {
		Package string
		Test    string
		Reason  string
	}

	// Allowlist is the object serialized in test/ci_orphaned_tests.json.
	Allowlist struct {
		Orphans []AllowedOrphan
	}

	// entry is a single named test entry together with the config file it
	// was loaded from.
	entry struct {
		configFile string
		name       string
		test       *Test
	}
)

const modulePrefix = "vitess.io/vitess/"

// run loads the config files and the allowlist and returns all problems
// found. It only returns an error for I/O-level failures reading the
// allowlist; config problems are reported as strings.
func run(root string, configPaths []string, allowlistPath string) ([]string, error) {
	entries, problems := loadConfigs(configPaths)
	allow, err := loadAllowlist(allowlistPath)
	if err != nil {
		return nil, err
	}
	problems = append(problems, runChecks(root, entries, allow)...)
	return problems, nil
}

// loadConfigs reads every config file and returns the flattened entries in
// deterministic order. Duplicate entry names across files are reported as
// problems: test.go merges the configs with maps.Copy, which would silently
// drop one of the two entries.
func loadConfigs(paths []string) ([]entry, []string) {
	var entries []entry
	var problems []string
	firstSeen := make(map[string]string)
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			problems = append(problems, fmt.Sprintf("cannot read %s: %v", path, err))
			continue
		}
		config := &Config{}
		if err := json.Unmarshal(content, config); err != nil {
			problems = append(problems, fmt.Sprintf("cannot parse %s: %v", path, err))
			continue
		}
		names := make([]string, 0, len(config.Tests))
		for name := range config.Tests {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if prev, ok := firstSeen[name]; ok {
				problems = append(problems, fmt.Sprintf("duplicate entry %q defined in both %s and %s", name, prev, path))
			} else {
				firstSeen[name] = path
			}
			entries = append(entries, entry{configFile: path, name: name, test: config.Tests[name]})
		}
	}
	return entries, problems
}

// loadAllowlist reads the orphan allowlist. A missing file is an empty
// allowlist, so the tool keeps working on release branches that predate it.
func loadAllowlist(path string) (Allowlist, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Allowlist{}, nil
		}
		return Allowlist{}, err
	}
	var allow Allowlist
	if err := json.Unmarshal(content, &allow); err != nil {
		return Allowlist{}, fmt.Errorf("cannot parse %s: %w", path, err)
	}
	return allow, nil
}

// pkgDir maps a package import path from a config entry to a directory
// under root.
func pkgDir(root, importPath string) (string, error) {
	rel, ok := strings.CutPrefix(importPath, modulePrefix)
	if !ok {
		return "", fmt.Errorf("package %s is not under %s", importPath, modulePrefix)
	}
	return filepath.Join(root, filepath.FromSlash(rel)), nil
}

// listTests returns the names of all top-level test functions declared in
// the *_test.go files of dir, sorted and deduplicated. It mirrors how cmd/go
// discovers tests (isTestFunc/isTest in cmd/go/internal/load/test.go):
// internal and external test packages both count, TestMain(m *testing.M) is
// the test entrypoint rather than a test, and a TestMain(t *testing.T) is a
// regular test. Build constraints are deliberately ignored: CI runs on
// linux, and a tag-guarded orphan still deserves triage.
func listTests(dir string) ([]string, error) {
	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	seen := make(map[string]bool)
	for _, de := range dirEntries {
		if de.IsDir() || !strings.HasSuffix(de.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, de.Name()), nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil {
				continue
			}
			if isTestName(fn.Name.Name) && isTestFunc(fn, "T") {
				seen[fn.Name.Name] = true
			}
		}
	}
	tests := make([]string, 0, len(seen))
	for name := range seen {
		tests = append(tests, name)
	}
	sort.Strings(tests)
	return tests, nil
}

// isTestName reports whether name is a test function name as defined by
// cmd/go's isTest: the prefix "Test" followed by nothing or by a rune that
// is not lowercase.
func isTestName(name string) bool {
	rest, ok := strings.CutPrefix(name, "Test")
	if !ok {
		return false
	}
	if rest == "" {
		return true
	}
	r, _ := utf8.DecodeRuneInString(rest)
	return !unicode.IsLower(r)
}

// isTestFunc reports whether fn has the signature func(*X) for a type whose
// name is arg ("T" for tests), mirroring cmd/go's isTestFunc: only the final
// name of the parameter type is checked, because the testing package may be
// imported under any name.
func isTestFunc(fn *ast.FuncDecl, arg string) bool {
	if fn.Type.Results != nil && len(fn.Type.Results.List) > 0 ||
		fn.Type.Params.List == nil ||
		len(fn.Type.Params.List) != 1 ||
		len(fn.Type.Params.List[0].Names) > 1 {
		return false
	}
	ptr, ok := fn.Type.Params.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	if name, ok := ptr.X.(*ast.Ident); ok && name.Name == arg {
		return true
	}
	if sel, ok := ptr.X.(*ast.SelectorExpr); ok && sel.Sel.Name == arg {
		return true
	}
	return false
}

// runPattern extracts the value of the -run flag from a config entry's Args.
func runPattern(args []string) (pattern string, found bool, err error) {
	for i, arg := range args {
		if arg == "-run" || arg == "--run" {
			if i+1 >= len(args) {
				return "", false, errors.New("-run flag has no value")
			}
			return args[i+1], true, nil
		}
		for _, prefix := range []string{"-run=", "--run="} {
			if value, ok := strings.CutPrefix(arg, prefix); ok {
				return value, true, nil
			}
		}
	}
	return "", false, nil
}

// splitRegexp splits a -run pattern on slashes that are not inside a
// bracket or parenthesis group, mirroring splitRegexp in testing/match.go.
// Only the first element selects top-level tests; the rest select subtests.
func splitRegexp(s string) []string {
	a := make([]string, 0, strings.Count(s, "/")+1)
	cs := 0
	cp := 0
	for i := 0; i < len(s); {
		switch s[i] {
		case '[':
			cs++
		case ']':
			if cs--; cs < 0 { // An unmatched ']' is legal.
				cs = 0
			}
		case '(':
			if cs == 0 {
				cp++
			}
		case ')':
			if cs == 0 {
				cp--
			}
		case '\\':
			i++
		case '/':
			if cs == 0 && cp == 0 {
				a = append(a, s[:i])
				s = s[i+1:]
				i = 0
				continue
			}
		}
		i++
	}
	return append(a, s)
}

// runChecks validates the config entries against the test functions that
// actually exist in the tree:
//
//   - every entry's packages must exist and contain at least one test function;
//   - every -run regex must select at least one test function in each of the
//     entry's packages (a regex that matches nothing means the entry silently
//     runs no tests);
//   - every test function in a configured package must be selected by at
//     least one entry, or be listed in the allowlist with a reason;
//   - every allowlist row must reference an existing, still-unselected test,
//     so the allowlist shrinks as orphans are re-enabled.
//
// Manual entries are skipped entirely: they neither provide coverage (they
// don't run in CI) nor pull their packages into scope.
func runChecks(root string, entries []entry, allow Allowlist) []string {
	var problems []string
	addf := func(format string, a ...any) {
		problems = append(problems, fmt.Sprintf(format, a...))
	}

	testsCache := make(map[string][]string)
	testsFor := func(pkg string) ([]string, error) {
		if tests, ok := testsCache[pkg]; ok {
			return tests, nil
		}
		dir, err := pkgDir(root, pkg)
		if err != nil {
			return nil, err
		}
		tests, err := listTests(dir)
		if err != nil {
			return nil, err
		}
		testsCache[pkg] = tests
		return tests, nil
	}

	// covered maps package -> test name -> name of one entry that runs it.
	covered := make(map[string]map[string]string)
	inScope := make(map[string]bool)
	cover := func(pkg, test, entryName string) {
		if covered[pkg] == nil {
			covered[pkg] = make(map[string]string)
		}
		if _, ok := covered[pkg][test]; !ok {
			covered[pkg][test] = entryName
		}
	}

	for _, e := range entries {
		if e.test.Manual || len(e.test.Packages) == 0 {
			continue
		}

		pattern, hasPattern, err := runPattern(e.test.Args)
		patternOK := err == nil
		if err != nil {
			addf("entry %q (%s): %v", e.name, e.configFile, err)
		}
		var re *regexp.Regexp
		if patternOK && hasPattern {
			// Only the first slash-separated element filters top-level
			// tests; matching is unanchored, exactly like go test.
			re, err = regexp.Compile(splitRegexp(pattern)[0])
			if err != nil {
				patternOK = false
				addf("entry %q (%s): invalid -run regex %q: %v", e.name, e.configFile, pattern, err)
			}
		}

		for _, pkg := range e.test.Packages {
			tests, err := testsFor(pkg)
			if err != nil {
				addf("entry %q (%s): %v", e.name, e.configFile, err)
				continue
			}
			if len(tests) == 0 {
				addf("entry %q (%s): package %s contains no test functions", e.name, e.configFile, pkg)
				continue
			}
			inScope[pkg] = true
			if !patternOK {
				continue
			}
			if !hasPattern {
				for _, test := range tests {
					cover(pkg, test, e.name)
				}
				continue
			}
			matched := false
			for _, test := range tests {
				if re.MatchString(test) {
					cover(pkg, test, e.name)
					matched = true
				}
			}
			if !matched {
				addf("entry %q (%s): -run regex %q matches no test function in package %s", e.name, e.configFile, pattern, pkg)
			}
		}
	}

	allowed := make(map[string]map[string]bool)
	for _, orphan := range allow.Orphans {
		if allowed[orphan.Package] == nil {
			allowed[orphan.Package] = make(map[string]bool)
		}
		allowed[orphan.Package][orphan.Test] = true
	}

	pkgs := make([]string, 0, len(inScope))
	for pkg := range inScope {
		pkgs = append(pkgs, pkg)
	}
	sort.Strings(pkgs)
	for _, pkg := range pkgs {
		for _, test := range testsCache[pkg] {
			if _, ok := covered[pkg][test]; ok {
				continue
			}
			if allowed[pkg][test] {
				continue
			}
			addf("package %s: %s is not run by any entry in test/config*.json; add or broaden an entry to run it, or add it to test/ci_orphaned_tests.json with a reason", pkg, test)
		}
	}

	seenRows := make(map[string]bool)
	for _, orphan := range allow.Orphans {
		key := orphan.Package + "." + orphan.Test
		if seenRows[key] {
			addf("allowlist: duplicate row for %s in package %s", orphan.Test, orphan.Package)
			continue
		}
		seenRows[key] = true
		if orphan.Reason == "" {
			addf("allowlist: row for %s in package %s has no Reason", orphan.Test, orphan.Package)
		}
		if !inScope[orphan.Package] {
			addf("allowlist: package %s is not referenced by any test/config*.json entry; remove its rows", orphan.Package)
			continue
		}
		tests := testsCache[orphan.Package]
		idx := sort.SearchStrings(tests, orphan.Test)
		if idx >= len(tests) || tests[idx] != orphan.Test {
			addf("allowlist: %s does not exist in package %s; remove the row", orphan.Test, orphan.Package)
			continue
		}
		if entryName, ok := covered[orphan.Package][orphan.Test]; ok {
			addf("allowlist: %s in package %s is now covered by entry %q; remove the row", orphan.Test, orphan.Package, entryName)
		}
	}

	return problems
}
