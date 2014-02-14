package fileutil

import (
	"testing"
)

func testWildcard(t *testing.T, pattern string, expected bool) {
	result := HasWildcard(pattern)
	if result {
		if !expected {
			t.Errorf("HasWildcard(%v) returned true but expected false", pattern)
		}
	} else {
		if expected {
			t.Errorf("HasWildcard(%v) returned false but expected true", pattern)
		}
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
