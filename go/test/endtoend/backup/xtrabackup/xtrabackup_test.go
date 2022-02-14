package vtctlbackup

import (
	"testing"

	backup "vitess.io/vitess/go/test/endtoend/backup/vtctlbackup"
)

// TestXtraBackup - tests the backup using xtrabackup
func TestXtrabackup(t *testing.T) {
	backup.TestBackup(t, backup.XtraBackup, "tar", 0)
}
