package vtctlbackup

import (
	"testing"
)

// TestBackupMain - main tests backup using vtctl commands
func TestBackupMain(t *testing.T) {
	TestBackup(t, Backup, "", 0)
}
