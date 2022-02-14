package xtrabackup

import (
	"testing"

	"vitess.io/vitess/go/test/endtoend/recovery"
	"vitess.io/vitess/go/test/endtoend/recovery/unshardedrecovery"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
)

func TestMain(m *testing.M) {
	recovery.UseXb = true
	unshardedrecovery.TestMainImpl(m)
}

func TestRecovery(t *testing.T) {
	unshardedrecovery.TestRecoveryImpl(t)
}
