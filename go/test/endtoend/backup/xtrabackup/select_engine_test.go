package vtctlbackup

import (
	"testing"

	backup "vitess.io/vitess/go/test/endtoend/backup/vtctlbackup"
)

func TestBackupEngineSelector(t *testing.T) {
	defer setDefaultCompressionFlag()
	backup.TestBackupEngineSelector(t)
}

func TestRestoreAllowedBackupEngines(t *testing.T) {
	defer setDefaultCompressionFlag()
	backup.TestRestoreAllowedBackupEngines(t)
}
