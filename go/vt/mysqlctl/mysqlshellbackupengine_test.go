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
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/ioutil"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/vterrors"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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
			shouldDeleteUsers, err := engine.restorePreCheck(t.Context(), RestoreParams{})
			require.ErrorIs(t, err, tt.err)
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

			_, err := engine.restorePreCheck(t.Context(), params)
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

			err := cleanupMySQL(t.Context(), params, tt.shouldDeleteUsers)
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
	pos, err := replication.DecodePosition("MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3:1-10")
	require.NoError(t, err)
	mysql.CurrentPrimaryPosition = pos

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

		bh, err := bs.StartBackup(t.Context(), t.TempDir(), t.Name())
		require.NoError(t, err)

		var verified []replication.Position
		verifyingParams := params
		verifyingParams.VerifyPosition = func(ctx context.Context, p replication.Position) error {
			verified = append(verified, p)
			return nil
		}
		_, err = be.ExecuteBackup(t.Context(), verifyingParams, bh)
		require.NoError(t, err)
		require.False(t, mysql.GlobalReadLock) // lock must be released.

		// check the manifest is valid.
		var manifest MySQLShellBackupManifest
		err = json.Unmarshal(manifestBuffer.Bytes(), &manifest)
		require.NoError(t, err)

		require.Equal(t, mysqlShellBackupEngineName, manifest.BackupMethod)

		// The engine verified the position its MANIFEST records.
		require.Len(t, verified, 1)
		assert.True(t, pos.Equal(verified[0]), "verified %v, want %v", verified[0], pos)
		assert.True(t, manifest.Position.Equal(verified[0]))

		if hostname, err := netutil.FullyQualifiedHostname(); err == nil {
			require.Equal(t, hostname, manifest.Hostname)
		}

		// did we notice the lock was release and did we release it ours as well?
		require.Contains(t, logger.String(), "global read lock released duration=",
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

		bh, err := bs.StartBackup(t.Context(), t.TempDir(), t.Name())
		require.NoError(t, err)

		// in this case the backup was successful, but even if we didn't see mysqlsh release its lock
		// we make sure it is released at the end.
		_, err = be.ExecuteBackup(t.Context(), params, bh)
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

		bh, err := bs.StartBackup(t.Context(), t.TempDir(), t.Name())
		require.NoError(t, err)

		_, err = be.ExecuteBackup(t.Context(), params, bh)
		require.ErrorContains(t, err, "mysqlshell failed")
		require.False(t, mysql.GlobalReadLock) // lock must be released.
	})

	t.Run("lock released and no MANIFEST written when the position check fails", func(t *testing.T) {
		be := &MySQLShellBackupEngine{binaryName: path.Join(t.TempDir(), "mysqlsh.sh")}
		mysql.GlobalReadLock = false // clear lock status.
		logger.Clear()
		handle := &FakeBackupHandle{
			Dir:           t.TempDir(),
			AddFileReturn: FakeBackupHandleAddFileReturn{WriteCloser: ioutil.NewBytesBufferWriter()},
		}
		bs.StartBackupReturn.BackupHandle = handle

		// this simulates mysql shell completing without any issues.
		generateTestFile(t, be.binaryName, fmt.Sprintf(
			"#!/bin/bash\n>&2 echo %s; echo \"backup completed\"; sleep 0.01", mysqlShellLockMessage))

		bh, err := bs.StartBackup(t.Context(), t.TempDir(), t.Name())
		require.NoError(t, err)

		failingParams := params
		failingParams.VerifyPosition = func(context.Context, replication.Position) error {
			return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "errant GTIDs")
		}
		result, err := be.ExecuteBackup(t.Context(), failingParams, bh)
		require.ErrorContains(t, err, "errant GTIDs")
		assert.Equal(t, BackupUnusable, result)
		assert.Empty(t, handle.AddFileCalls, "a failed check must not write the MANIFEST")
		require.False(t, mysql.GlobalReadLock) // lock must be released.
	})
}

// TestMySQLShellLogsMaskPasswords checks that the mysqlsh command line that a
// backup and a restore log has each password from --mysql-shell-flags masked,
// in every form mysqlsh takes one, and the other arguments as given, and that
// mysqlsh still gets the passwords.
func TestMySQLShellLogsMaskPasswords(t *testing.T) {
	const secret = "mysqlsh_secret_5d2b"
	originalFlags, originalLocation, originalLoadFlags := mysqlShellFlags, mysqlShellBackupLocation, mysqlShellLoadFlags
	t.Cleanup(func() {
		mysqlShellFlags, mysqlShellBackupLocation, mysqlShellLoadFlags = originalFlags, originalLocation, originalLoadFlags
	})
	mysqlShellFlags = strings.ReplaceAll("--defaults-file=/dev/null --js -h localhost -uroot --user root "+
		"--password={pw} --password {pw} -p{pw} -p {pw} -Ep{pw} --password1={pw} --password2 {pw} --password3={pw} "+
		"--dbpassword={pw} --dbPassword={pw} --db-password={pw} --loose-password={pw} "+
		"--uri=root:{pw}@localhost --uri root:{pw}@localhost mysql://root:{pw}@localhost:3306 --ssh=me:{pw}@bastion "+
		"--uri=root:{pw}@host/db?ssl-ca=file:/x "+
		"-C root:{pw}@localhost -S root:{pw}@localhost -i root:{pw}@localhost -mc root:{pw}@host:3306", "{pw}", secret)
	wantArgs := "--defaults-file=/dev/null --js -h localhost -uroot --user root " +
		"--password=**** --password **** -p**** -p **** -Ep**** --password1=**** --password2 **** --password3=**** " +
		"--dbpassword=**** --dbPassword=**** --db-password=**** --loose-password=**** " +
		"--uri=root:****@localhost --uri root:****@localhost mysql://root:****@localhost:3306 --ssh=me:****@bastion " +
		"--uri=root:****@host/db?ssl-ca=file:/x " +
		"-C root:****@localhost -S root:****@localhost -i root:****@localhost -mc root:****@host:3306 -e util."

	newMysqld := func(t *testing.T) *FakeMysqlDaemon {
		fakedb := fakesqldb.New(t)
		t.Cleanup(fakedb.Close)
		mysqld := NewFakeMysqlDaemon(fakedb)
		t.Cleanup(mysqld.Close)
		return mysqld
	}
	assertMasked := func(t *testing.T, logs *logtest.Logs, stream *logutil.MemoryLogger, binary string) {
		record := logs.Record(t, "running mysql shell")
		assert.Equal(t, "INFO", record["level"])
		assert.Contains(t, record["exec_cmd"], binary+" "+wantArgs)
		assert.NotContains(t, logs.String(), secret)
		assert.NotContains(t, stream.String(), secret)
	}

	// fakeMySQLShell writes a mysqlsh that records its arguments, one per line,
	// and fails. assertRanUnmasked checks that it got the flags as given.
	fakeMySQLShell := func(t *testing.T, binary string) (argsFile string) {
		argsFile = path.Join(t.TempDir(), "args")
		generateTestFile(t, binary, "#!/bin/bash\nprintf '%s\\n' \"$@\" > '"+argsFile+"'\nexit 1")
		return argsFile
	}
	assertRanUnmasked := func(t *testing.T, argsFile string) {
		args, err := os.ReadFile(argsFile)
		require.NoError(t, err)
		assert.Contains(t, strings.ReplaceAll(string(args), "\n", " "), mysqlShellFlags+" -e util.")
	}

	t.Run("backup", func(t *testing.T) {
		mysqlShellBackupLocation = "logical"
		be := &MySQLShellBackupEngine{binaryName: path.Join(t.TempDir(), "mysqlsh.sh")}
		argsFile := fakeMySQLShell(t, be.binaryName)
		logs := logtest.Capture(t, slog.LevelInfo)
		stream := logutil.NewMemoryLogger()

		_, err := be.ExecuteBackup(t.Context(), BackupParams{
			TabletAlias: "test",
			Logger:      logutil.NewTeeLogger(logutil.NewConsoleLogger(), stream),
			Mysqld:      newMysqld(t),
		}, &FakeBackupHandle{Dir: t.TempDir(), NameV: t.Name()})
		require.ErrorContains(t, err, "mysqlshell failed")
		assertMasked(t, logs, stream, be.binaryName)
		assertRanUnmasked(t, argsFile)
	})

	t.Run("restore", func(t *testing.T) {
		mysqlShellBackupLocation = t.TempDir()
		mysqlShellLoadFlags = `{"updateGtidSet": "replace", "skipBinlog": true, "progressFile": ""}`
		bin := t.TempDir()
		argsFile := fakeMySQLShell(t, path.Join(bin, "mysqlsh"))
		t.Setenv("PATH", bin)
		manifest, err := json.Marshal(MySQLShellBackupManifest{BackupLocation: path.Join(mysqlShellBackupLocation, "backup")})
		require.NoError(t, err)
		mysqld := newMysqld(t)
		mysqld.FetchSuperQueryMap = map[string]*sqltypes.Result{"SHOW DATABASES": {}}
		mysqld.ExpectedExecuteSuperQueryList = []string{"FAKE RESET ALL REPLICATION", "SET GLOBAL LOCAL_INFILE=1"}
		logs := logtest.Capture(t, slog.LevelInfo)
		stream := logutil.NewMemoryLogger()

		_, err = (&MySQLShellBackupEngine{}).ExecuteRestore(t.Context(), RestoreParams{
			Cnf:    &Mycnf{DataDir: path.Join(t.TempDir(), "data")},
			Logger: logutil.NewTeeLogger(logutil.NewConsoleLogger(), stream),
			Mysqld: mysqld,
		}, &FakeBackupHandle{ReadFileReturnF: func(context.Context, string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(manifest)), nil
		}})
		require.ErrorContains(t, err, "mysqlshell failed")
		assertMasked(t, logs, stream, path.Join(bin, "mysqlsh"))
		assertRanUnmasked(t, argsFile)
	})
}

// TestMaskURIPassword probes maskURIPassword across the connection-string
// shapes mysqlsh accepts, since a wrong parse logs a password in clear.
func TestMaskURIPassword(t *testing.T) {
	cases := []struct {
		name string
		uri  string
		want string
	}{
		{"no scheme", "root:secret@localhost", "root:****@localhost"},
		{"with scheme", "mysql://root:secret@localhost:3306", "mysql://root:****@localhost:3306"},
		{"ssh user info", "me:secret@bastion", "me:****@bastion"},
		// mysqlsh's parser takes the first ":/" anywhere as the end of the
		// scheme, so it rejects a schemeless string with a ":/" after the "@"
		// (it reads everything up to that ":/" as the scheme). The password is
		// real, so mask it.
		{"colon-slash after host", "root:secret@host/db?ssl-ca=file:/x", "root:****@host/db?ssl-ca=file:/x"},
		{"colon-slash after host with scheme", "mysql://root:secret@host/db?ssl-ca=file:/x", "mysql://root:****@host/db?ssl-ca=file:/x"},
		{"percent-encoded at in password", "root:p%40ss@host", "root:****@host"},
		{"colon in password", "user:pa:ss@host", "user:****@host"},
		{"colon-run password", "user:::@host", "user:****@host"},
		{"IPv6 host", "root:secret@[::1]:3306", "root:****@[::1]:3306"},
		{"IPv6 host with scheme", "mysql://root:secret@[fe80::1]:3306", "mysql://root:****@[fe80::1]:3306"},
		{"no password", "root@localhost", "root@localhost"},
		{"no at sign", "localhost:3306", "localhost:3306"},
		{"empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := maskURIPassword(tc.uri)
			assert.Equal(t, tc.want, got)
			assert.NotContains(t, got, "secret")
		})
	}
}

// shellLoadDecision is how MySQL Shell 8.0.43 util.loadDump treats the URL
// Vitess passes it. A non-empty s3BucketName, osBucketName, or azureContainerName
// selects that container and refuses a URL that has a scheme; any other string
// is the object prefix, used literally. With no container option, an empty
// scheme or file:// is a local directory. loadUsers runs that dump's account SQL.
type shellLoadDecision struct {
	prefix  string
	local   bool
	users   bool
	refused bool
}

func shellLoadDecisionFromArgs(args []string) (shellLoadDecision, error) {
	var execute string
	for i, arg := range args {
		if arg == "-e" && i+1 < len(args) {
			execute = args[i+1]
		}
	}
	location, options, err := parseLoadDump(execute)
	if err != nil {
		return shellLoadDecision{}, err
	}
	return decideShellLoad(location, options), nil
}

func parseLoadDump(execute string) (string, map[string]any, error) {
	const prefix = "util.loadDump("
	if !strings.HasPrefix(execute, prefix) || !strings.HasSuffix(execute, ")") {
		return "", nil, fmt.Errorf("not a util.loadDump call: %q", execute)
	}
	body := execute[len(prefix) : len(execute)-1]
	if len(body) == 0 || body[0] != '"' {
		return "", nil, fmt.Errorf("loadDump location is not a quoted string: %q", body)
	}
	end := 1
	for end < len(body) {
		if body[end] == '\\' {
			end += 2
			continue
		}
		if body[end] == '"' {
			break
		}
		end++
	}
	if end >= len(body) {
		return "", nil, fmt.Errorf("unterminated loadDump location: %q", body)
	}
	location, err := strconv.Unquote(body[:end+1])
	if err != nil {
		return "", nil, err
	}
	rest := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(body[end+1:]), ","))
	var options map[string]any
	if err := json.Unmarshal([]byte(rest), &options); err != nil {
		return "", nil, err
	}
	return location, options, nil
}

// mysqlShellStubMain is the mysqlsh stand-in. The test binary is re-executed
// with MYSQLSH_STUB=1 so the exit status follows decideShellLoad.
func mysqlShellStubMain() int {
	if argsFile := os.Getenv("MYSQLSH_ARGS_FILE"); argsFile != "" {
		_ = os.WriteFile(argsFile, []byte(strings.Join(os.Args[1:], "\n")+"\n"), 0o600)
	}
	decision, err := shellLoadDecisionFromArgs(os.Args[1:])
	if err != nil || decision.refused {
		return 1
	}
	return 0
}

func decideShellLoad(location string, options map[string]any) shellLoadDecision {
	bucket := false
	for _, key := range []string{"s3BucketName", "osBucketName", "azureContainerName"} {
		if value, ok := options[key].(string); ok && value != "" {
			bucket = true
		}
	}
	users, _ := options["loadUsers"].(bool)
	scheme := ""
	if i := strings.Index(location, "://"); i > 0 {
		scheme = location[:i]
	}
	if bucket {
		if scheme != "" {
			return shellLoadDecision{refused: true}
		}
		return shellLoadDecision{prefix: location, users: users}
	}
	if scheme == "" || strings.EqualFold(scheme, "file") {
		return shellLoadDecision{prefix: location, local: true, users: users}
	}
	return shellLoadDecision{refused: true}
}

// TestMySQLShellRestoreManifestLocation checks that an object-storage restore
// refuses a MANIFEST BackupLocation that is not under --mysql-shell-backup-location
// before it creates the restore state file, issues cleanup SQL, or starts mysqlsh.
// A location under that root is the one mysqlsh loads, including when a later
// read of the same MANIFEST returns a different location. Load options come from
// --mysql-shell-load-flags, not from the manifest Params field.
func TestMySQLShellRestoreManifestLocation(t *testing.T) {
	const backupRoot = "vitess/shell-backups"
	const allowedLocation = backupRoot + "/ks/backup-1"
	// Params is stored in the manifest and must not affect the load.
	const manifestParams = `{"loadUsers":false,"s3BucketName":"attacker-bucket","updateGtidSet":"off","skipBinlog":false,"progressFile":"/tmp/mysqlsh-progress"}`

	originalFlags, originalLocation, originalLoadFlags := mysqlShellFlags, mysqlShellBackupLocation, mysqlShellLoadFlags
	t.Cleanup(func() {
		mysqlShellFlags, mysqlShellBackupLocation, mysqlShellLoadFlags = originalFlags, originalLocation, originalLoadFlags
	})
	mysqlShellFlags = "--defaults-file=/dev/null --js -h localhost"
	mysqlShellBackupLocation = backupRoot

	loadFlags := func(option, container string) string {
		return fmt.Sprintf(`{"threads":4,"loadUsers":true,"updateGtidSet":"replace","skipBinlog":true,"progressFile":"","%s":%q}`, option, container)
	}

	type observation struct {
		err           error
		argv          []string
		fetches       []string
		executed      int
		stateExists   bool
		sentinel      string
		manifestReads int
	}

	run := func(t *testing.T, flags, first, second string) observation {
		t.Helper()
		mysqlShellLoadFlags = flags

		bin := t.TempDir()
		argsFile := filepath.Join(t.TempDir(), "mysqlsh-args")
		testBin, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		generateTestFile(t, filepath.Join(bin, "mysqlsh"), fmt.Sprintf("#!/bin/bash\nexport MYSQLSH_STUB=1\nexport MYSQLSH_ARGS_FILE=%q\nexec %q \"$@\"\n", argsFile, testBin))
		t.Setenv("PATH", bin)

		tabletDir := t.TempDir()
		sentinel := filepath.Join(tabletDir, "sentinel")
		require.NoError(t, os.WriteFile(sentinel, []byte("keep"), 0o600))
		stateFile := filepath.Join(tabletDir, RestoreState)

		var fetched []string
		fakedb := fakesqldb.New(t)
		t.Cleanup(fakedb.Close)
		mysqld := NewFakeMysqlDaemon(fakedb)
		t.Cleanup(mysqld.Close)
		mysqld.FetchSuperQueryCallback = func(query string) (*sqltypes.Result, error) {
			fetched = append(fetched, query)
			switch query {
			case "SHOW DATABASES":
				return &sqltypes.Result{Rows: [][]sqltypes.Value{{sqltypes.NewVarChar("vt_app")}}}, nil
			case "SELECT user()":
				return &sqltypes.Result{Rows: [][]sqltypes.Value{{sqltypes.NewVarChar("vitess@localhost")}}}, nil
			case "SELECT user, host FROM mysql.user":
				return &sqltypes.Result{Rows: [][]sqltypes.Value{
					{sqltypes.NewVarChar("vitess"), sqltypes.NewVarChar("localhost")},
					{sqltypes.NewVarChar("app"), sqltypes.NewVarChar("%")},
				}}, nil
			default:
				return nil, fmt.Errorf("unexpected query: %s", query)
			}
		}
		mysqld.ExpectedExecuteSuperQueryList = []string{
			"DROP DATABASE IF EXISTS `vt_app`",
			"DROP USER 'app'@'%'",
			"FAKE RESET ALL REPLICATION",
			"SET GLOBAL LOCAL_INFILE=1",
			"SET GLOBAL LOCAL_INFILE=0",
		}

		reads := 0
		handle := &FakeBackupHandle{ReadFileReturnF: func(context.Context, string) (io.ReadCloser, error) {
			reads++
			location := first
			if reads > 1 {
				location = second
			}
			raw, err := json.Marshal(MySQLShellBackupManifest{
				BackupManifest: BackupManifest{BackupMethod: mysqlShellBackupEngineName},
				BackupLocation: location,
				Params:         manifestParams,
			})
			if err != nil {
				return nil, err
			}
			return io.NopCloser(bytes.NewReader(raw)), nil
		}}

		_, err = (&MySQLShellBackupEngine{}).ExecuteRestore(t.Context(), RestoreParams{
			Cnf:    &Mycnf{DataDir: filepath.Join(tabletDir, "data")},
			Logger: logutil.NewMemoryLogger(),
			Mysqld: mysqld,
		}, handle)

		argvText, readErr := os.ReadFile(argsFile)
		if readErr != nil && !os.IsNotExist(readErr) {
			t.Fatal(readErr)
		}
		var argv []string
		if readErr == nil {
			argv = strings.Split(strings.TrimSuffix(string(argvText), "\n"), "\n")
		}
		_, statErr := os.Stat(stateFile)
		sentinelText, sentinelErr := os.ReadFile(sentinel)
		require.NoError(t, sentinelErr)
		return observation{
			err:           err,
			argv:          argv,
			fetches:       fetched,
			executed:      mysqld.ExpectedExecuteSuperQueryCurrent,
			stateExists:   statErr == nil,
			sentinel:      string(sentinelText),
			manifestReads: len(handle.ReadFileCalls),
		}
	}

	outside := []struct {
		name     string
		option   string
		location string
	}{
		{name: "s3 foreign prefix", option: "s3BucketName", location: "attacker/dump"},
		{name: "s3 absolute path", option: "s3BucketName", location: "/tmp/attacker-mysqlsh-dump"},
		{name: "s3 scheme url", option: "s3BucketName", location: "https://attacker.example/dump"},
		{name: "s3 prefix boundary", option: "s3BucketName", location: backupRoot + "-evil/dump"},
		{name: "s3 dotdot", option: "s3BucketName", location: backupRoot + "/../../attacker/dump"},
		{name: "oci foreign prefix", option: "osBucketName", location: "attacker/dump"},
		{name: "azure foreign prefix", option: "azureContainerName", location: "attacker/dump"},
	}
	for _, tc := range outside {
		t.Run(tc.name, func(t *testing.T) {
			got := run(t, loadFlags(tc.option, "vitess-bucket"), tc.location, "attacker/second-read")
			if len(got.argv) > 0 {
				decision, decErr := shellLoadDecisionFromArgs(got.argv)
				require.NoError(t, decErr)
				t.Errorf("mysqlsh was started for manifest location %q: prefix=%q local=%v users=%v refused=%v", tc.location, decision.prefix, decision.local, decision.users, decision.refused)
			}
			assert.Error(t, got.err)
			assert.Empty(t, got.argv)
			assert.False(t, got.stateExists, "restore state file was created")
			assert.Empty(t, got.fetches, "cleanup queries: %q", got.fetches)
			assert.Zero(t, got.executed)
			assert.Equal(t, "keep", got.sentinel)
			assert.Equal(t, 1, got.manifestReads)
		})
	}

	t.Run("allowed prefix", func(t *testing.T) {
		const second = "attacker/second-read"
		flags := loadFlags("s3BucketName", "vitess-bucket")
		got := run(t, flags, allowedLocation, second)
		require.NoError(t, got.err)
		require.Equal(t, 1, got.manifestReads)
		decision, err := shellLoadDecisionFromArgs(got.argv)
		require.NoError(t, err)
		assert.Equal(t, allowedLocation, decision.prefix)
		assert.True(t, decision.users)
		assert.False(t, decision.local)
		assert.False(t, decision.refused)
		execute := ""
		for i, arg := range got.argv {
			if arg == "-e" && i+1 < len(got.argv) {
				execute = got.argv[i+1]
			}
		}
		assert.NotContains(t, execute, second)
		assert.NotContains(t, execute, "attacker-bucket")
		assert.NotContains(t, execute, `"loadUsers":false`)
		assert.Contains(t, execute, `"loadUsers":true`)
		require.Equal(t, 5, got.executed)
		require.Equal(t, "keep", got.sentinel)
	})
}
