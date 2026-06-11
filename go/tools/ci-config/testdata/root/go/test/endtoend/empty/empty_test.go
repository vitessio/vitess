package empty

import "testing"

// TestMain with *testing.M does not count as a test function, so this
// package has tests files but no runnable tests.
func TestMain(m *testing.M) {}
