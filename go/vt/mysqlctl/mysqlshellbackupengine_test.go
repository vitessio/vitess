package mysqlctl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

func TestMySQLShellBackupRestorePreCheck(t *testing.T) {
	original := *mysqlShellLoadFlags
	defer func() { *mysqlShellLoadFlags = original }()

	engine := MySQLShellBackupEngine{}
	tests := []struct {
		name  string
		flags string
		err   error
	}{
		{
			"empty load flags",
			`{}`,
			MySQLShellPreCheckError,
		},
		{
			"only updateGtidSet",
			`{"updateGtidSet": "replace"}`,
			MySQLShellPreCheckError,
		},
		{
			"only progressFile",
			`{"progressFile": ""}`,
			MySQLShellPreCheckError,
		},
		{
			"both values but unsupported values",
			`{"updateGtidSet": "append", "progressFile": "/tmp/test1"}`,
			MySQLShellPreCheckError,
		},
		{
			"supported values",
			`{"updateGtidSet": "replace", "progressFile": ""}`,
			nil,
		},
	}

	for _, tt := range tests {
		*mysqlShellLoadFlags = tt.flags
		assert.ErrorIs(t, engine.restorePreCheck(), tt.err)
	}

}

func TestShouldDrainForBackupMySQLShell(t *testing.T) {
	original := *mysqlShellBackupShouldDrain
	defer func() { *mysqlShellBackupShouldDrain = original }()

	engine := MySQLShellBackupEngine{}

	*mysqlShellBackupShouldDrain = false

	assert.False(t, engine.ShouldDrainForBackup(nil))
	assert.False(t, engine.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{}))

	*mysqlShellBackupShouldDrain = true

	assert.True(t, engine.ShouldDrainForBackup(nil))
	assert.True(t, engine.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{}))
}
