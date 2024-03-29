/*
Copyright 2019 The Vitess Authors.

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
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testcase struct {
	versionString string
	version       ServerVersion
	flavor        MySQLFlavor
}

func TestParseVersionString(t *testing.T) {

	var testcases = []testcase{

		{
			versionString: "mysqld  Ver 5.7.27-0ubuntu0.19.04.1 for Linux on x86_64 ((Ubuntu))",
			version:       ServerVersion{5, 7, 27},
			flavor:        FlavorMySQL,
		},
		{
			versionString: "mysqld  Ver 5.6.43 for linux-glibc2.12 on x86_64 (MySQL Community Server (GPL))",
			version:       ServerVersion{5, 6, 43},
			flavor:        FlavorMySQL,
		},
		{
			versionString: "mysqld  Ver 5.7.26 for linux-glibc2.12 on x86_64 (MySQL Community Server (GPL))",
			version:       ServerVersion{5, 7, 26},
			flavor:        FlavorMySQL,
		},
		{
			versionString: "mysqld  Ver 8.0.16 for linux-glibc2.12 on x86_64 (MySQL Community Server - GPL)",
			version:       ServerVersion{8, 0, 16},
			flavor:        FlavorMySQL,
		},
		{
			versionString: "mysqld  Ver 5.7.26-29 for Linux on x86_64 (Percona Server (GPL), Release 29, Revision 11ad961)",
			version:       ServerVersion{5, 7, 26},
			flavor:        FlavorPercona,
		},
		{
			versionString: "mysqld  Ver 10.0.38-MariaDB for Linux on x86_64 (MariaDB Server)",
			version:       ServerVersion{10, 0, 38},
			flavor:        FlavorMariaDB,
		},
		{
			versionString: "mysqld  Ver 10.1.40-MariaDB for Linux on x86_64 (MariaDB Server)",
			version:       ServerVersion{10, 1, 40},
			flavor:        FlavorMariaDB,
		},
		{
			versionString: "mysqld  Ver 10.2.25-MariaDB for Linux on x86_64 (MariaDB Server)",
			version:       ServerVersion{10, 2, 25},
			flavor:        FlavorMariaDB,
		},
		{
			versionString: "mysqld  Ver 10.3.16-MariaDB for Linux on x86_64 (MariaDB Server)",
			version:       ServerVersion{10, 3, 16},
			flavor:        FlavorMariaDB,
		},
		{
			versionString: "mysqld  Ver 10.4.6-MariaDB for Linux on x86_64 (MariaDB Server)",
			version:       ServerVersion{10, 4, 6},
			flavor:        FlavorMariaDB,
		},
		{
			versionString: "mysqld  Ver 5.6.42 for linux-glibc2.12 on x86_64 (MySQL Community Server (GPL))",
			version:       ServerVersion{5, 6, 42},
			flavor:        FlavorMySQL,
		},
		{
			versionString: "mysqld  Ver 5.6.44-86.0 for Linux on x86_64 (Percona Server (GPL), Release 86.0, Revision eba1b3f)",
			version:       ServerVersion{5, 6, 44},
			flavor:        FlavorPercona,
		},
		{
			versionString: "mysqld  Ver 8.0.15-6 for Linux on x86_64 (Percona Server (GPL), Release 6, Revision 63abd08)",
			version:       ServerVersion{8, 0, 15},
			flavor:        FlavorPercona,
		},
	}

	for _, testcase := range testcases {
		f, v, err := ParseVersionString(testcase.versionString)
		if v != testcase.version || f != testcase.flavor || err != nil {
			t.Errorf("ParseVersionString failed for: %#v, Got: %#v, %#v Expected: %#v, %#v", testcase.versionString, v, f, testcase.version, testcase.flavor)
		}
	}

}

func TestRegexps(t *testing.T) {
	{
		submatch := binlogEntryTimestampGTIDRegexp.FindStringSubmatch(`#230608 13:14:31 server id 484362839  end_log_pos 259 CRC32 0xc07510d0 	GTID	last_committed=0	sequence_number=1	rbr_only=yes`)
		require.NotEmpty(t, submatch)
		assert.Equal(t, "230608 13:14:31", submatch[1])
		_, err := ParseBinlogTimestamp(submatch[1])
		assert.NoError(t, err)
	}
	{
		submatch := binlogEntryTimestampGTIDRegexp.FindStringSubmatch(`#230608 13:14:31 server id 484362839  end_log_pos 322 CRC32 0x651af842 	Query	thread_id=62	exec_time=0	error_code=0`)
		assert.Empty(t, submatch)
	}

	{
		submatch := binlogEntryCommittedTimestampRegex.FindStringSubmatch(`#230605 16:06:34 server id 22233  end_log_pos 1037 CRC32 0xa4707c5b 	GTID	last_committed=4	sequence_number=5	rbr_only=no	original_committed_timestamp=1685970394031366	immediate_commit_timestamp=1685970394032458	transaction_length=186`)
		require.NotEmpty(t, submatch)
		assert.Equal(t, "1685970394031366", submatch[1])
	}
	{
		submatch := binlogEntryCommittedTimestampRegex.FindStringSubmatch(`#230608 13:14:31 server id 484362839  end_log_pos 322 CRC32 0x651af842 	Query	thread_id=62	exec_time=0	error_code=0`)
		assert.Empty(t, submatch)
	}

}

func TestParseBinlogEntryTimestamp(t *testing.T) {
	tcases := []struct {
		name  string
		entry string
		tm    time.Time
	}{
		{
			name:  "empty",
			entry: "",
		},
		{
			name:  "irrelevant",
			entry: "/*!80001 SET @@session.original_commit_timestamp=1685970394031366*//*!*/;",
		},
		{
			name:  "irrelevant 2",
			entry: "#230605 16:06:34 server id 22233  end_log_pos 1139 CRC32 0x9fa6f3c8 	Query	thread_id=21	exec_time=0	error_code=0",
		},
		{
			name:  "mysql80",
			entry: "#230605 16:06:34 server id 22233  end_log_pos 1037 CRC32 0xa4707c5b 	GTID	last_committed=4	sequence_number=5	rbr_only=no	original_committed_timestamp=1685970394031366	immediate_commit_timestamp=1685970394032458	transaction_length=186",
			tm:    time.UnixMicro(1685970394031366),
		},
		{
			name:  "mysql57",
			entry: "#230608 13:14:31 server id 484362839  end_log_pos 259 CRC32 0xc07510d0 	GTID	last_committed=0	sequence_number=1	rbr_only=yes",
			tm:    time.Date(2023, time.June, 8, 13, 14, 31, 0, time.UTC),
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			tm, err := parseBinlogEntryTimestamp(tcase.entry)
			assert.NoError(t, err)
			assert.Equal(t, tcase.tm, tm)
		})
	}
}

func TestCleanupLockfile(t *testing.T) {
	t.Cleanup(func() {
		os.Remove("mysql.sock.lock")
	})
	ts := "prefix"
	// All good if no lockfile exists
	assert.NoError(t, cleanupLockfile("mysql.sock", ts))

	// If lockfile exists, but the process is not found, we clean it up.
	os.WriteFile("mysql.sock.lock", []byte("123456789"), 0o600)
	assert.NoError(t, cleanupLockfile("mysql.sock", ts))
	assert.NoFileExists(t, "mysql.sock.lock")

	// If lockfile exists, but the process is not found, we clean it up.
	os.WriteFile("mysql.sock.lock", []byte("123456789\n"), 0o600)
	assert.NoError(t, cleanupLockfile("mysql.sock", ts))
	assert.NoFileExists(t, "mysql.sock.lock")

	// If the lockfile exists, and the process is found, but it's for ourselves,
	// we clean it up.
	os.WriteFile("mysql.sock.lock", []byte(strconv.Itoa(os.Getpid())), 0o600)
	assert.NoError(t, cleanupLockfile("mysql.sock", ts))
	assert.NoFileExists(t, "mysql.sock.lock")

	// If the lockfile exists, and the process is found, we don't clean it up.
	os.WriteFile("mysql.sock.lock", []byte(strconv.Itoa(os.Getppid())), 0o600)
	assert.Error(t, cleanupLockfile("mysql.sock", ts))
	assert.FileExists(t, "mysql.sock.lock")
}
