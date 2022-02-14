package mysqlctld

import (
	"testing"

	backup "vitess.io/vitess/go/test/endtoend/backup/vtctlbackup"
)

// TestBackupMysqlctld - tests the backup using mysqlctld.
func TestBackupMysqlctld(t *testing.T) {
	backup.TestBackup(t, backup.Mysqlctld, "", 0)
}
