/*
Copyright 2022 The Vitess Authors.

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

package mysqlctld

import (
	"testing"

	backup "vitess.io/vitess/go/test/endtoend/backup/vtctlbackup"
)

// TestIncrementalBackupAndRestoreToPos
func TestIncrementalBackupAndRestoreToPos(t *testing.T) {
	tcase := &backup.PITRTestCase{
		Name:           "BuiltinBackup",
		SetupType:      backup.BuiltinBackup,
		ComprssDetails: nil,
	}
	backup.ExecTestIncrementalBackupAndRestoreToPos(t, tcase)
}

// TestIncrementalBackupAndRestoreToTimestamp - tests incremental backups and restores.
// The general outline of the test:
//   - Generate some schema with data
//   - Take a full backup
//   - Proceed to take a series of inremental backups. In between, inject data (insert rows), and keep record
//     of which data (number of rows) is present in each backup, and at which timestamp.
//   - Expect backups success/failure per scenario
//   - Next up, we start testing restores. Randomly pick recorded timestamps and restore to those points in time.
//   - In each restore, excpect to find the data (number of rows) recorded for said timestamp
//   - Some restores should fail because the timestamp exceeds the last binlog
//   - Do so for all recorded tiemstamps.
//   - Then, a 2nd round where some backups are purged -- this tests to see that we're still able to find a restore path
//     (of course we only delete backups that still leave us with valid restore paths).
//
// All of the above is done for BuiltinBackup, XtraBackup, Mysqlctld (which is technically builtin)
func TestIncrementalBackupAndRestoreToTimestamp(t *testing.T) {
	tcase := &backup.PITRTestCase{
		Name:           "BuiltinBackup",
		SetupType:      backup.BuiltinBackup,
		ComprssDetails: nil,
	}
	backup.ExecTestIncrementalBackupAndRestoreToTimestamp(t, tcase)
}
