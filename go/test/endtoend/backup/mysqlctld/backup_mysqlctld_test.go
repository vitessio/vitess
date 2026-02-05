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

package mysqlctld

import (
	"testing"

	"vitess.io/vitess/go/vt/mysqlctl"

	backup "vitess.io/vitess/go/test/endtoend/backup/vtctlbackup"
)

// TestBackupMysqlctld - tests the backup using mysqlctld.
func TestBackupMysqlctld(t *testing.T) {
	backup.TestBackup(t, backup.Mysqlctld, "xbstream", 0, nil, nil)
}

func TestBackupMysqlctldWithlz4Compression(t *testing.T) {
	defer setDefaultCompressionFlag()
	cDetails := &backup.CompressionDetails{
		CompressorEngineName: "lz4",
	}

	backup.TestBackup(t, backup.Mysqlctld, "xbstream", 0, cDetails, []string{"TestReplicaBackup", "TestPrimaryBackup"})
}

func setDefaultCompressionFlag() {
	mysqlctl.CompressionEngineName = "pgzip"
	mysqlctl.ExternalCompressorCmd = ""
	mysqlctl.ExternalCompressorExt = ""
	mysqlctl.ExternalDecompressorCmd = ""
}
