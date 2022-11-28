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

package vtctlbackup

import (
	"testing"

	"vitess.io/vitess/go/vt/mysqlctl"

	backup "vitess.io/vitess/go/test/endtoend/backup/vtctlbackup"
)

// TestXtrabackupStream - tests the backup using xtrabackup with xbstream mode
func TestXtrabackupStream(t *testing.T) {
	backup.TestBackup(t, backup.XtraBackup, "xbstream", 8, nil, nil)
}

func TestXtrabackupStreamWithlz4Compression(t *testing.T) {
	defer setDefaultCompressionFlag()
	cDetails := &backup.CompressionDetails{
		CompressorEngineName: "lz4",
	}

	backup.TestBackup(t, backup.XtraBackup, "xbstream", 8, cDetails, []string{"primaryReplicaSameBackupModifiedCompressionEngine"})
}

func setDefaultCompressionFlag() {
	mysqlctl.CompressionEngineName = "pgzip"
	mysqlctl.ExternalCompressorCmd = ""
	mysqlctl.ExternalCompressorExt = ""
	mysqlctl.ExternalDecompressorCmd = ""
}
