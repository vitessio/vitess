/*
Copyright 2024 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysqlctl

import (
	"context"
	"fmt"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/logutil"
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
			t.TempDir(),
			"--js -h localhost",
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			mysqlShellBackupLocation = tt.location
			mysqlShellFlags = tt.flags
			assert.ErrorIs(t, engine.backupPreCheck(path.Join(mysqlShellBackupLocation, "test")), tt.err)
		})
	}

}

func TestMySQLShellBackupRestorePreCheck(t *testing.T) {
	original := mysqlShellLoadFlags
	defer func() { mysqlShellLoadFlags = original }()

	engine := MySQLShellBackupEngine{}
	tests := []struct {
		name              string
		flags             string
		err               error
		shouldDeleteUsers bool
	}{
		{
			"empty load flags",
			`{}`,
			MySQLShellPreCheckError,
			false,
		},
		{
			"only updateGtidSet",
			`{"updateGtidSet": "replace"}`,
			MySQLShellPreCheckError,
			false,
		},
		{
			"only progressFile",
			`{"progressFile": ""}`,
			MySQLShellPreCheckError,
			false,
		},
		{
			"both values but unsupported values",
			`{"updateGtidSet": "append", "progressFile": "/tmp/test1"}`,
			MySQLShellPreCheckError,
			false,
		},
		{
			"supported values",
			`{"updateGtidSet": "replace", "progressFile": "", "skipBinlog": true, "loadUsers": false}`,
			nil,
			false,
		},
		{
			"should delete users",
			`{"updateGtidSet": "replace", "progressFile": "", "skipBinlog": true, "loadUsers": true}`,
			nil,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mysqlShellLoadFlags = tt.flags
			shouldDeleteUsers, err := engine.restorePreCheck(context.Background(), RestoreParams{})
			assert.ErrorIs(t, err, tt.err)
			assert.Equal(t, tt.shouldDeleteUsers, shouldDeleteUsers)
		})
	}

}

func TestMySQLShellBackupRestorePreCheckDisableRedolog(t *testing.T) {
	original := mysqlShellSpeedUpRestore
	defer func() { mysqlShellSpeedUpRestore = original }()

	mysqlShellSpeedUpRestore = true
	engine := MySQLShellBackupEngine{}

	fakedb := fakesqldb.New(t)
	defer fakedb.Close()
	fakeMysqld := NewFakeMysqlDaemon(fakedb) // defaults to 8.0.32
	defer fakeMysqld.Close()

	params := RestoreParams{
		Mysqld: fakeMysqld,
	}

	// this should work as it is supported since 8.0.21
	_, err := engine.restorePreCheck(context.Background(), params)
	require.NoError(t, err, params)

	// it should error out if we change to an older version
	fakeMysqld.Version = "8.0.20"

	_, err = engine.restorePreCheck(context.Background(), params)
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

func TestCleanupMySQL(t *testing.T) {
	type userRecord struct {
		user, host string
	}

	tests := []struct {
		name              string
		existingDBs       []string
		expectedDropDBs   []string
		currentUser       string
		existingUsers     []userRecord
		expectedDropUsers []string
		shouldDeleteUsers bool
	}{
		{
			name:            "testing only specific DBs",
			existingDBs:     []string{"_vt", "vt_test"},
			expectedDropDBs: []string{"_vt", "vt_test"},
		},
		{
			name:            "testing with internal dbs",
			existingDBs:     []string{"_vt", "mysql", "vt_test", "performance_schema"},
			expectedDropDBs: []string{"_vt", "vt_test"},
		},
		{
			name:            "with users but without delete",
			existingDBs:     []string{"_vt", "mysql", "vt_test", "performance_schema"},
			expectedDropDBs: []string{"_vt", "vt_test"},
			existingUsers: []userRecord{
				{"test", "localhost"},
				{"app", "10.0.0.1"},
			},
			expectedDropUsers: []string{},
			shouldDeleteUsers: false,
		},
		{
			name:            "with users and delete",
			existingDBs:     []string{"_vt", "mysql", "vt_test", "performance_schema"},
			expectedDropDBs: []string{"_vt", "vt_test"},
			existingUsers: []userRecord{
				{"test", "localhost"},
				{"app", "10.0.0.1"},
			},
			expectedDropUsers: []string{"'test'@'localhost'", "'app'@'10.0.0.1'"},
			shouldDeleteUsers: true,
		},
		{
			name:            "with reserved users",
			existingDBs:     []string{"_vt", "mysql", "vt_test", "performance_schema"},
			expectedDropDBs: []string{"_vt", "vt_test"},
			existingUsers: []userRecord{
				{"mysql.sys", "localhost"},
				{"mysql.infoschema", "localhost"},
				{"mysql.session", "localhost"},
				{"test", "localhost"},
				{"app", "10.0.0.1"},
			},
			expectedDropUsers: []string{"'test'@'localhost'", "'app'@'10.0.0.1'"},
			shouldDeleteUsers: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakedb := fakesqldb.New(t)
			defer fakedb.Close()
			mysql := NewFakeMysqlDaemon(fakedb)
			defer mysql.Close()

			databases := [][]sqltypes.Value{}
			for _, db := range tt.existingDBs {
				databases = append(databases, []sqltypes.Value{sqltypes.NewVarChar(db)})
			}

			users := [][]sqltypes.Value{}
			for _, record := range tt.existingUsers {
				users = append(users, []sqltypes.Value{sqltypes.NewVarChar(record.user), sqltypes.NewVarChar(record.host)})
			}

			mysql.FetchSuperQueryMap = map[string]*sqltypes.Result{
				"SHOW DATABASES":                    {Rows: databases},
				"SELECT user()":                     {Rows: [][]sqltypes.Value{{sqltypes.NewVarChar(tt.currentUser)}}},
				"SELECT user, host FROM mysql.user": {Rows: users},
			}

			for _, drop := range tt.expectedDropDBs {
				mysql.ExpectedExecuteSuperQueryList = append(mysql.ExpectedExecuteSuperQueryList,
					fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", drop),
				)
			}

			if tt.shouldDeleteUsers {
				for _, drop := range tt.expectedDropUsers {
					mysql.ExpectedExecuteSuperQueryList = append(mysql.ExpectedExecuteSuperQueryList,
						fmt.Sprintf("DROP USER %s", drop),
					)
				}
			}

			params := RestoreParams{
				Mysqld: mysql,
				Logger: logutil.NewMemoryLogger(),
			}

			err := cleanupMySQL(context.Background(), params, tt.shouldDeleteUsers)
			require.NoError(t, err)

			require.Equal(t, len(tt.expectedDropDBs)+len(tt.expectedDropUsers), mysql.ExpectedExecuteSuperQueryCurrent,
				"unexpected number of queries executed")
		})
	}

}
