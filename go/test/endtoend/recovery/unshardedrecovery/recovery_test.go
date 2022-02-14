package unshardedrecovery

import (
	"testing"

	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
)

func TestMain(m *testing.M) {
	TestMainImpl(m)
}

func TestRecovery(t *testing.T) {
	TestRecoveryImpl(t)
}
