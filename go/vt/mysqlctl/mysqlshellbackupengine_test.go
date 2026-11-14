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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/ioutil"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/netutil"
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
			ErrMySQLShellPreCheck,
		},
		{
			"only location",
			"/dev/null",
			"",
			ErrMySQLShellPreCheck,
		},
		{
			"only flags",
			"",
			"--js",
			ErrMySQLShellPreCheck,
		},
		{
			"both values present but without --js",
			"",
			"-h localhost",
			ErrMySQLShellPreCheck,
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
			ErrMySQLShellPreCheck,
			false,
		},
		{
			"only updateGtidSet",
			`{"updateGtidSet": "replace"}`,
			ErrMySQLShellPreCheck,
			false,
		},
		{
			"only progressFile",
			`{"progressFile": ""}`,
			ErrMySQLShellPreCheck,
			false,
		},
		{
			"both values but unsupported values",
			`{"updateGtidSet": "append", "progressFile": "/tmp/test1"}`,
			ErrMySQLShellPreCheck,
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

	tests := []struct {
		version string
		err     error
	}{
		{
			version: "mysqld  Ver 5.7.27-0ubuntu0.19.04.1 for Linux on x86_64 ((Ubuntu))",
			err:     ErrMySQLShellPreCheck,
		},
		{
			version: "mysqld  Ver 5.7.26 for linux-glibc2.12 on x86_64 (MySQL Community Server (GPL))",
			err:     ErrMySQLShellPreCheck,
		},
		{
			version: "mysqld  Ver 5.7.26-29 for Linux on x86_64 (Percona Server (GPL), Release 29, Revision 11ad961)",
			err:     ErrMySQLShellPreCheck,
		},
		{
			version: "mysqld  Ver 8.0.16 for linux-glibc2.12 on x86_64 (MySQL Community Server - GPL)",
			err:     ErrMySQLShellPreCheck,
		},
		{
			version: "mysqld  Ver 8.0.15-6 for Linux on x86_64 (Percona Server (GPL), Release 6, Revision 63abd08)",
			err:     ErrMySQLShellPreCheck,
		},
		{
			version: "mysqld  Ver 8.0.42-0ubuntu0.22.04.1 for Linux on x86_64 ((Ubuntu))",
			err:     nil,
		},
		{
			version: "mysqld  Ver 8.0.42-33 for Linux on x86_64 (Percona Server (GPL), Release '33', Revision '9dc49998')",
			err:     nil,
		},
	}

	fakedb := fakesqldb.New(t)
	defer fakedb.Close()
	fakeMysqld := NewFakeMysqlDaemon(fakedb)
	defer fakeMysqld.Close()

	params := RestoreParams{
		Mysqld: fakeMysqld,
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			fakeMysqld.Version = tt.version

			_, err := engine.restorePreCheck(context.Background(), params)
			require.ErrorIs(t, err, tt.err)
		})
	}
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
						"DROP USER "+drop,
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

// this is a helper to write files in a temporary directory
func generateTestFile(t *testing.T, name, contents string) {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o700)
	require.NoError(t, err)
	defer f.Close()
	_, err = f.WriteString(contents)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

// This tests if we are properly releasing the global read lock we acquire
// during ExecuteBackup(), even if the backup didn't succeed.
func TestMySQLShellBackupEngine_ExecuteBackup_ReleaseLock(t *testing.T) {
	originalLocation := mysqlShellBackupLocation
	mysqlShellBackupLocation = "logical"

	defer func() { // restore the original value.
		mysqlShellBackupLocation = originalLocation
	}()

	logger := logutil.NewMemoryLogger()
	fakedb := fakesqldb.New(t)
	defer fakedb.Close()
	mysql := NewFakeMysqlDaemon(fakedb)
	defer mysql.Close()

	params := BackupParams{
		TabletAlias: "test",
		Logger:      logger,
		Mysqld:      mysql,
	}
	bs := FakeBackupStorage{
		StartBackupReturn: FakeBackupStorageStartBackupReturn{},
	}

	t.Run("lock released if we see the mysqlsh lock being acquired", func(t *testing.T) {
		be := &MySQLShellBackupEngine{binaryName: path.Join(t.TempDir(), "mysqlsh.sh")}
		logger.Clear()
		manifestBuffer := ioutil.NewBytesBufferWriter()
		bs.StartBackupReturn.BackupHandle = &FakeBackupHandle{
			Dir:           t.TempDir(),
			AddFileReturn: FakeBackupHandleAddFileReturn{WriteCloser: manifestBuffer},
		}

		// this simulates mysql shell completing without any issues.
		generateTestFile(t, be.binaryName, fmt.Sprintf(
			"#!/bin/bash\n>&2 echo %s; echo \"backup completed\"; sleep 0.01", mysqlShellLockMessage))

		bh, err := bs.StartBackup(context.Background(), t.TempDir(), t.Name())
		require.NoError(t, err)

		_, err = be.ExecuteBackup(context.Background(), params, bh)
		require.NoError(t, err)
		require.False(t, mysql.GlobalReadLock) // lock must be released.

		// check the manifest is valid.
		var manifest MySQLShellBackupManifest
		err = json.Unmarshal(manifestBuffer.Bytes(), &manifest)
		require.NoError(t, err)

		require.Equal(t, mysqlShellBackupEngineName, manifest.BackupMethod)

		if hostname, err := netutil.FullyQualifiedHostname(); err == nil {
			require.Equal(t, hostname, manifest.Hostname)
		}

		// did we notice the lock was release and did we release it ours as well?
		require.Contains(t, logger.String(), "global read lock released after",
			"failed to release the global lock after mysqlsh")
	})

	t.Run("lock released if we don't see mysqlsh release it", func(t *testing.T) {
		be := &MySQLShellBackupEngine{binaryName: path.Join(t.TempDir(), "mysqlsh.sh")}
		mysql.GlobalReadLock = false // clear lock status.
		logger.Clear()
		manifestBuffer := ioutil.NewBytesBufferWriter()
		bs.StartBackupReturn.BackupHandle = &FakeBackupHandle{
			Dir:           t.TempDir(),
			AddFileReturn: FakeBackupHandleAddFileReturn{WriteCloser: manifestBuffer},
		}

		// this simulates mysqlshell completing, but we don't see the message that is released its lock.
		generateTestFile(t, be.binaryName, "#!/bin/bash\nexit 0")

		bh, err := bs.StartBackup(context.Background(), t.TempDir(), t.Name())
		require.NoError(t, err)

		// in this case the backup was successful, but even if we didn't see mysqlsh release its lock
		// we make sure it is released at the end.
		_, err = be.ExecuteBackup(context.Background(), params, bh)
		require.NoError(t, err)
		require.False(t, mysql.GlobalReadLock) // lock must be released.

		// make sure we are at least logging the lock wasn't able to be released earlier.
		require.Contains(t, logger.String(), "could not release global lock earlier",
			"failed to log error message when unable to release lock during backup")
	})

	t.Run("lock released when backup fails", func(t *testing.T) {
		be := &MySQLShellBackupEngine{binaryName: path.Join(t.TempDir(), "mysqlsh.sh")}
		mysql.GlobalReadLock = false // clear lock status.
		logger.Clear()
		manifestBuffer := ioutil.NewBytesBufferWriter()
		bs.StartBackupReturn.BackupHandle = &FakeBackupHandle{
			Dir:           t.TempDir(),
			AddFileReturn: FakeBackupHandleAddFileReturn{WriteCloser: manifestBuffer},
		}

		// this simulates the backup process failing.
		generateTestFile(t, be.binaryName, "#!/bin/bash\nexit 1")

		bh, err := bs.StartBackup(context.Background(), t.TempDir(), t.Name())
		require.NoError(t, err)

		_, err = be.ExecuteBackup(context.Background(), params, bh)
		require.ErrorContains(t, err, "mysqlshell failed")
		require.False(t, mysql.GlobalReadLock) // lock must be released.
	})
}

// TestMySQLShellLogOmitsFlags checks that the mysqlsh command line that a
// backup and a restore log leaves out the operator's --mysql-shell-flags and
// the dump or load flags, which can hold passwords and tokens in forms that are
// hard to parse, keeps the rest of the "-e" call that Vitess adds, and that
// mysqlsh still gets everything as given.
func TestMySQLShellLogOmitsFlags(t *testing.T) {
	const secret = "mysqlsh_secret_5d2b"
	originalFlags, originalLocation, originalDumpFlags, originalLoadFlags := mysqlShellFlags, mysqlShellBackupLocation, mysqlShellDumpFlags, mysqlShellLoadFlags
	t.Cleanup(func() {
		mysqlShellFlags, mysqlShellBackupLocation, mysqlShellDumpFlags, mysqlShellLoadFlags = originalFlags, originalLocation, originalDumpFlags, originalLoadFlags
	})
	withSecret := func(s string) string { return strings.ReplaceAll(s, "{pw}", secret) }
	mysqlShellFlags = withSecret("--defaults-file=/dev/null --js -h localhost -uroot --password={pw} " +
		"--password2 -{pw} -mc root:p@{pw}@host:3306 mysql://root:pa/{pw}@host:3306")
	// Compact JSON, as mysqlsh gets it.
	const credentials = `"s3EndpointOverride":"https://user:{pw}@minio:9000","azureStorageSasToken":"{pw}"`
	mysqlShellDumpFlags = withSecret(`{"threads":4,` + credentials + `}`)

	newMysqld := func(t *testing.T) *FakeMysqlDaemon {
		fakedb := fakesqldb.New(t)
		t.Cleanup(fakedb.Close)
		mysqld := NewFakeMysqlDaemon(fakedb)
		t.Cleanup(mysqld.Close)
		return mysqld
	}

	// fakeMySQLShell writes a mysqlsh that records its arguments, one per line,
	// and fails.
	fakeMySQLShell := func(t *testing.T, binary string) (argsFile string) {
		argsFile = path.Join(t.TempDir(), "args")
		generateTestFile(t, binary, "#!/bin/bash\nprintf '%s\\n' \"$@\" > '"+argsFile+"'\nexit 1")
		return argsFile
	}
	// assertRanAndLogged checks that mysqlsh got the flags and the "-e" call
	// with the JSON flags as given, and that the logged command line is that
	// call with placeholders for the flags and the JSON.
	assertRanAndLogged := func(t *testing.T, logger *logutil.MemoryLogger, binary, argsFile, function, jsonFlags, placeholder string) {
		recorded, err := os.ReadFile(argsFile)
		require.NoError(t, err)
		args := strings.Split(strings.TrimSuffix(string(recorded), "\n"), "\n")
		require.GreaterOrEqual(t, len(args), 2)
		call := args[len(args)-1]
		assert.Equal(t, mysqlShellFlags+" -e", strings.Join(args[:len(args)-1], " "))
		require.True(t, strings.HasPrefix(call, "util."+function+"(\""), call)
		require.True(t, strings.HasSuffix(call, "\", "+jsonFlags+")"), call)

		var running []string
		for _, event := range logger.LogEvents() {
			if strings.HasPrefix(event.Value, "running ") {
				running = append(running, event.Value)
			}
		}
		assert.Equal(t, []string{"running " + binary + " <--mysql-shell-flags> -e " + strings.TrimSuffix(call, jsonFlags+")") + placeholder + ")"}, running)
		assert.NotContains(t, logger.String(), secret)
	}

	t.Run("backup", func(t *testing.T) {
		mysqlShellBackupLocation = t.TempDir()
		be := &MySQLShellBackupEngine{binaryName: path.Join(t.TempDir(), "mysqlsh.sh")}
		argsFile := fakeMySQLShell(t, be.binaryName)
		logger := logutil.NewMemoryLogger()

		_, err := be.ExecuteBackup(t.Context(), BackupParams{
			TabletAlias: "test",
			Logger:      logger,
			Mysqld:      newMysqld(t),
		}, &FakeBackupHandle{Dir: t.TempDir(), NameV: t.Name()})
		require.ErrorContains(t, err, "mysqlshell failed")
		assertRanAndLogged(t, logger, be.binaryName, argsFile, "dumpInstance", mysqlShellDumpFlags, "<--mysql-shell-dump-flags>")
	})

	t.Run("restore", func(t *testing.T) {
		mysqlShellBackupLocation = t.TempDir()
		mysqlShellLoadFlags = withSecret(`{"updateGtidSet":"replace","skipBinlog":true,"progressFile":"",` + credentials + `}`)
		bin := t.TempDir()
		argsFile := fakeMySQLShell(t, path.Join(bin, "mysqlsh"))
		t.Setenv("PATH", bin)
		manifest, err := json.Marshal(MySQLShellBackupManifest{BackupLocation: path.Join(mysqlShellBackupLocation, "backup")})
		require.NoError(t, err)
		mysqld := newMysqld(t)
		mysqld.FetchSuperQueryMap = map[string]*sqltypes.Result{"SHOW DATABASES": {}}
		mysqld.ExpectedExecuteSuperQueryList = []string{"FAKE RESET ALL REPLICATION", "SET GLOBAL LOCAL_INFILE=1"}
		logger := logutil.NewMemoryLogger()

		_, err = (&MySQLShellBackupEngine{}).ExecuteRestore(t.Context(), RestoreParams{
			Cnf:    &Mycnf{DataDir: path.Join(t.TempDir(), "data")},
			Logger: logger,
			Mysqld: mysqld,
		}, &FakeBackupHandle{ReadFileReturnF: func(context.Context, string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(manifest)), nil
		}})
		require.ErrorContains(t, err, "mysqlshell failed")
		assertRanAndLogged(t, logger, path.Join(bin, "mysqlsh"), argsFile, "loadDump", mysqlShellLoadFlags, "<--mysql-shell-load-flags>")
	})
}
