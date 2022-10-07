package testlib

import (
	"os"
	"testing"

	"vitess.io/vitess/go/internal/flag"
)

func TestMain(m *testing.M) {
	flag.ParseFlagsForTest()
	os.Exit(m.Run())
}
