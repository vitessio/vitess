package mysqlctld

import (
	"testing"

	"vitess.io/vitess/go/test/endtoend/backup/transform"
)

func TestMain(m *testing.M) {
	transform.TestMainSetup(m, true)
}

func TestBackupTransform(t *testing.T) {
	transform.TestBackupTransformImpl(t)
}
func TestBackupTransformError(t *testing.T) {
	transform.TestBackupTransformErrorImpl(t)
}
