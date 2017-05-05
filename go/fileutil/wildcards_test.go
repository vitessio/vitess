/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fileutil

import (
	"testing"
)

func testWildcard(t *testing.T, pattern string, expected bool) {
	result := HasWildcard(pattern)
	if result != expected {
		t.Errorf("HasWildcard(%v) returned %v but expected %v", pattern, result, expected)
	}
}

func TestHasWildcard(t *testing.T) {

	testWildcard(t, "aaaa*bbbb", true)
	testWildcard(t, "aaaa\\*bbbb", false)

	testWildcard(t, "aaaa?bbbb", true)
	testWildcard(t, "aaaa\\?bbbb", false)

	testWildcard(t, "aaaa[^bcd]", true)
	testWildcard(t, "aaaa\\[b", false)

	// invalid, but returns true so when we try to Match it we fail
	testWildcard(t, "aaaa\\", true)
	testWildcard(t, "aaaa[", true)
}
