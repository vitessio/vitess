package simpleacl

import (
	"testing"

	"vitess.io/vitess/go/vt/tableacl/testlib"
)

func TestSimpleAcl(t *testing.T) {
	testlib.TestSuite(t, &Factory{})
}
