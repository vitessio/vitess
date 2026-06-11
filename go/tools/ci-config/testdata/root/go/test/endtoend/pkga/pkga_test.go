package pkga

import "testing"

func TestFoo(t *testing.T) {}

func TestBar(t *testing.T) {}

// TestMain with *testing.M is the package's test entrypoint, not a test.
func TestMain(m *testing.M) {}

// testHelper does not have the Test prefix.
func testHelper(t *testing.T) {}

// TestWrongSig has more than one parameter.
func TestWrongSig(t *testing.T, x int) {}

// TestNoParams has no parameters.
func TestNoParams() {}

// TestWithResult returns a value.
func TestWithResult(t *testing.T) error { return nil }

func BenchmarkFoo(b *testing.B) {}

type thing struct{}

// TestMethod has a receiver.
func (th *thing) TestMethod(t *testing.T) {}
