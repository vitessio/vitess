package transform

import "testing"

func TestMain(m *testing.M) {
	TestMainSetup(m, false)
}

func TestBackupTransform(t *testing.T) {
	TestBackupTransformImpl(t)
}
func TestBackupTransformError(t *testing.T) {
	TestBackupTransformErrorImpl(t)
}
