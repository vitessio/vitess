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

	"github.com/stretchr/testify/require"

	backup "vitess.io/vitess/go/test/endtoend/backup/vtctlbackup"
)

// TestXtraBackup - tests the backup using xtrabackup
func TestXtrabackup(t *testing.T) {
	backup.TestBackup(t, backup.XtraBackup, "tar", 0, nil, nil)
}

func TestBackupMainWithlz4Compression(t *testing.T) {
	cDetails := &backup.CompressionDetails{
		BuiltinCompressor: "lz4",
	}

	backup.TestBackup(t, backup.XtraBackup, "tar", 0, cDetails, []string{"TestReplicaBackup"})
}

func TestBackupMainWithZstdCompression(t *testing.T) {
	cDetails := &backup.CompressionDetails{
		ExternalCompressorCmd:   "zstd",
		ExternalCompressorExt:   ".zst",
		ExternalDecompressorCmd: "zstd -d",
	}

	backup.TestBackup(t, backup.XtraBackup, "tar", 0, cDetails, []string{"TestReplicaBackup"})
}

func TestBackupMainWithError(t *testing.T) {
	cDetails := &backup.CompressionDetails{
		BuiltinCompressor:   "pargzip",
		BuiltinDecompressor: "lz4",
	}

	err := backup.TestBackup(t, backup.XtraBackup, "tar", 0, cDetails, []string{"TestPrimaryBackup"})
	require.EqualError(t, err, "test failure: TestReplicaBackup")
}
