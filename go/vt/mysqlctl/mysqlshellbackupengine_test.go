package mysqlctl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

func TestMySQLShellBackupBackupPreCheck(t *testing.T) {
	originalLocation := mysqlShellBackupLocation
	originalFlags := mysqlShellFlags
	defer func() {
		mysqlShellBackupLocation = originalLocation
		mysqlShellFlags = originalFlags
	}()

	engine := MySQLShellBackupEngine{}
	tests := []struct {
		name     string
		location string
		flags    string
		err      error
	}{
		{
			"empty flags",
			"",
			`{}`,
			MySQLShellPreCheckError,
		},
		{
			"only location",
			"/dev/null",
			"",
			MySQLShellPreCheckError,
		},
		{
			"only flags",
			"",
			"--js",
			MySQLShellPreCheckError,
		},
		{
			"both values present but without --js",
			"",
			"-h localhost",
			MySQLShellPreCheckError,
		},
		{
			"supported values",
			"/tmp/backup/",
			"--js -h localhost",
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			mysqlShellBackupLocation = tt.location
			mysqlShellFlags = tt.flags
			assert.ErrorIs(t, engine.backupPreCheck(), tt.err)
		})
	}

}

func TestMySQLShellBackupRestorePreCheck(t *testing.T) {
	original := mysqlShellLoadFlags
	defer func() { mysqlShellLoadFlags = original }()

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
		t.Run(tt.name, func(t *testing.T) {
			mysqlShellLoadFlags = tt.flags
			assert.ErrorIs(t, engine.restorePreCheck(context.Background(), RestoreParams{}), tt.err)
		})
	}

}

func TestMySQLShellBackupRestorePreCheckDisableRedolog(t *testing.T) {
	original := mysqlShellSpeedUpRestore
	defer func() { mysqlShellSpeedUpRestore = original }()

	mysqlShellSpeedUpRestore = true
	engine := MySQLShellBackupEngine{}

	fakeMysqld := NewFakeMysqlDaemon(fakesqldb.New(t)) // defaults to 8.0.32
	params := RestoreParams{
		Mysqld: fakeMysqld,
	}

	// this should work as it is supported since 8.0.21
	require.NoError(t, engine.restorePreCheck(context.Background(), params))

	// it should error out if we change to an older version
	fakeMysqld.Version = "8.0.20"

	err := engine.restorePreCheck(context.Background(), params)
	require.ErrorIs(t, err, MySQLShellPreCheckError)
	require.ErrorContains(t, err, "doesn't support disabling the redo log")
}

func TestShouldDrainForBackupMySQLShell(t *testing.T) {
	original := mysqlShellBackupShouldDrain
	defer func() { mysqlShellBackupShouldDrain = original }()

	engine := MySQLShellBackupEngine{}

	mysqlShellBackupShouldDrain = false

	assert.False(t, engine.ShouldDrainForBackup(nil))
	assert.False(t, engine.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{}))

	mysqlShellBackupShouldDrain = true

	assert.True(t, engine.ShouldDrainForBackup(nil))
	assert.True(t, engine.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{}))
}
