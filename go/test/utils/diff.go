/*
Copyright 2020 The Vitess Authors.

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

package utils

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

// MustMatchFn is used to create a common diff function for a test file
// Usage in *_test.go file:
//
// Top declaration:
//
// var mustMatch = testutils.MustMatchFn(
// 	[]interface{}{  // types with unexported fields
// 		type1{},
// 		type2{},
// 		...
// 		typeN{},
// 	},
// 	[]string{  // ignored fields
// 		".id",        // id numbers are unstable
// 		".createAt",  // created dates might not be interesting to compare
// 	},
// )
//
// In Test*() function:
//
// mustMatch(t, want, got, "something doesn't match")
func MustMatchFn(allowUnexportedTypes []interface{}, ignoredFields []string, extraOpts ...cmp.Option) func(t *testing.T, want, got interface{}, errMsg ...string) {
	diffOpts := append([]cmp.Option{
		cmp.AllowUnexported(allowUnexportedTypes...),
		cmpIgnoreFields(ignoredFields...),
	}, extraOpts...)
	// Diffs want/got and fails with errMsg on any failure.
	return func(t *testing.T, want, got interface{}, errMsg ...string) {
		t.Helper()
		diff := cmp.Diff(want, got, diffOpts...)
		if diff != "" {
			t.Fatalf("%v: (-want +got)\n%v", errMsg, diff)
		}
	}
}

// MustMatch is a convenience version of MustMatchFn with no overrides.
// Usage in Test*() function:
//
// testutils.MustMatch(t, want, got, "something doesn't match")
var MustMatch = MustMatchFn(nil, nil)

// Skips fields of pathNames for cmp.Diff.
// Similar to standard cmpopts.IgnoreFields, but allows unexported fields.
func cmpIgnoreFields(pathNames ...string) cmp.Option {
	skipFields := make(map[string]bool, len(pathNames))
	for _, name := range pathNames {
		skipFields[name] = true
	}

	return cmp.FilterPath(func(path cmp.Path) bool {
		for _, ps := range path {
			if skipFields[ps.String()] {
				return true
			}
		}
		return false
	}, cmp.Ignore())
}
