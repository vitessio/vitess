/*
Copyright 2023 The Vitess Authors.

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

// Package mysqlctl_test is the blackbox tests for package mysqlctl.
package mysqlctl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

func TestGetIncrementalFromPosGTIDSet(t *testing.T) {
	tcases := []struct {
		incrementalFromPos string
		gtidSet            string
		expctError         bool
	}{
		{
			"MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615",
			"16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615",
			false,
		},
		{
			"16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615",
			"16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615",
			false,
		},
		{
			"MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3",
			"",
			true,
		},
		{
			"MySQL56/invalid",
			"",
			true,
		},
		{
			"16b1039f-22b6-11ed-b765-0a43f95f28a3",
			"",
			true,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.incrementalFromPos, func(t *testing.T) {
			gtidSet, err := getIncrementalFromPosGTIDSet(tcase.incrementalFromPos)
			if tcase.expctError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tcase.gtidSet, gtidSet.String())
			}
		})
	}
}

func TestFileEntryFullPath(t *testing.T) {
	cnf := &Mycnf{
		DataDir:               "/vt/data",
		InnodbDataHomeDir:     "/vt/innodb-data",
		InnodbLogGroupHomeDir: "/vt/innodb-log",
		BinLogPath:            "/vt/binlogs/mysql-bin",
	}

	tests := []struct {
		name      string
		entry     FileEntry
		wantPath  string
		wantError string
	}{
		{
			name:     "valid relative path in DataDir",
			entry:    FileEntry{Base: backupData, Name: "mydb/table1.ibd"},
			wantPath: "/vt/data/mydb/table1.ibd",
		},
		{
			name:     "valid relative path in InnodbDataHomeDir",
			entry:    FileEntry{Base: backupInnodbDataHomeDir, Name: "ibdata1"},
			wantPath: "/vt/innodb-data/ibdata1",
		},
		{
			name:     "valid relative path in InnodbLogGroupHomeDir",
			entry:    FileEntry{Base: backupInnodbLogGroupHomeDir, Name: "ib_logfile0"},
			wantPath: "/vt/innodb-log/ib_logfile0",
		},
		{
			name:     "valid relative path in BinlogDir",
			entry:    FileEntry{Base: backupBinlogDir, Name: "mysql-bin.000001"},
			wantPath: "/vt/binlogs/mysql-bin.000001",
		},
		{
			name:     "valid path with ParentPath",
			entry:    FileEntry{Base: backupData, Name: "mydb/table1.ibd", ParentPath: "/tmp/restore"},
			wantPath: "/tmp/restore/vt/data/mydb/table1.ibd",
		},
		{
			name:      "path traversal escapes base directory",
			entry:     FileEntry{Base: backupData, Name: "../../etc/passwd"},
			wantError: "path traversal not allowed",
		},
		{
			name:      "path traversal with deeper nesting",
			entry:     FileEntry{Base: backupData, Name: "mydb/../../../etc/shadow"},
			wantError: "path traversal not allowed",
		},
		{
			name:      "path traversal to root",
			entry:     FileEntry{Base: backupData, Name: "../../../../../etc/crontab"},
			wantError: "path traversal not allowed",
		},
		{
			name:      "path traversal escapes ParentPath",
			entry:     FileEntry{Base: backupData, Name: "../../../../etc/passwd", ParentPath: "/tmp/restore"},
			wantError: "path traversal not allowed",
		},
		{
			name:     "relative path with dot-dot that stays within base",
			entry:    FileEntry{Base: backupData, Name: "mydb/../mydb/table1.ibd"},
			wantPath: "/vt/data/mydb/table1.ibd",
		},
		{
			name:      "unknown base",
			entry:     FileEntry{Base: "unknown", Name: "file"},
			wantError: "unknown base",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.entry.fullPath(cnf)
			if tt.wantError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantError)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantPath, got)
			}
		})
	}
}

func TestShouldDrainForBackupBuiltIn(t *testing.T) {
	be := &BuiltinBackupEngine{}

	assert.True(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{}))
	assert.False(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{IncrementalFromPos: "auto"}))
	assert.False(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{IncrementalFromPos: "99ca8ed4-399c-11ee-861b-0a43f95f28a3:1-197"}))
	assert.False(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{IncrementalFromPos: "MySQL56/99ca8ed4-399c-11ee-861b-0a43f95f28a3:1-197"}))
}
