package vtctlbackup

import (
	"testing"

	backup "vitess.io/vitess/go/test/endtoend/backup/vtctlbackup"
)

// TestXtrabackupStream - tests the backup using xtrabackup with xbstream mode
func TestXtrabackupStream(t *testing.T) {
	backup.TestBackup(t, backup.XtraBackup, "xbstream", 8)
}
