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
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/mysqlctl"
)

// TestBuiltinBackup - main tests backup using vtctl commands
func TestBuiltinBackup(t *testing.T) {
	TestBackup(t, BuiltinBackup, "xbstream", 0, nil, nil)
}

func TestBuiltinBackupWithZstdCompression(t *testing.T) {
	defer setDefaultCompressionFlag()
	defer setDefaultCommonArgs()
	cDetails := &CompressionDetails{
		CompressorEngineName:    "zstd",
		ExternalCompressorCmd:   "zstd",
		ExternalCompressorExt:   ".zst",
		ExternalDecompressorCmd: "zstd -d",
	}

	TestBackup(t, BuiltinBackup, "xbstream", 0, cDetails, []string{"TestReplicaBackup", "TestPrimaryBackup"})
}

func TestBuiltinBackupWithExternalZstdCompression(t *testing.T) {
	defer setDefaultCompressionFlag()
	defer setDefaultCommonArgs()
	cDetails := &CompressionDetails{
		CompressorEngineName:    "external",
		ExternalCompressorCmd:   "zstd",
		ExternalCompressorExt:   ".zst",
		ExternalDecompressorCmd: "zstd -d",
	}

	TestBackup(t, BuiltinBackup, "xbstream", 0, cDetails, []string{"TestReplicaBackup", "TestPrimaryBackup"})
}

func TestBuiltinBackupWithExternalZstdCompressionAndManifestedDecompressor(t *testing.T) {
	defer setDefaultCompressionFlag()
	defer setDefaultCommonArgs()
	cDetails := &CompressionDetails{
		CompressorEngineName:            "external",
		ExternalCompressorCmd:           "zstd",
		ExternalCompressorExt:           ".zst",
		ExternalDecompressorUseManifest: true,
		ManifestExternalDecompressorCmd: "zstd -d",
	}

	TestBackup(t, BuiltinBackup, "xbstream", 0, cDetails, []string{"TestReplicaBackup", "TestPrimaryBackup"})
}

// TestBuiltinBackupChunked uses a low chunk threshold and chunk size (4MiB each) to force
// InnoDB files (ibdata1, undo tablespaces) to be split into multiple chunks during backup.
// This verifies that MySQL can start successfully and read back rows after a chunked restore.
func TestBuiltinBackupChunked(t *testing.T) {
	defer setDefaultCommonArgs()
	const chunkSizeBytes = 4194304 // 4MiB
	commonTabletArg = append(
		getDefaultCommonArgs(),
		"--builtinbackup-file-chunk-threshold", strconv.Itoa(chunkSizeBytes),
		"--builtinbackup-file-chunk-size", strconv.Itoa(chunkSizeBytes),
	)

	code, err := LaunchCluster(BuiltinBackup, "xbstream", 0, nil)
	require.NoErrorf(t, err, "setup failed with status code %d", code)
	defer TearDownCluster()

	t.Run("TestChunkedBackup", chunkedBackup)
}

// TestBuiltinBackupNonChunked verifies that with the default threshold of 0, no chunking
// occurs and the MANIFEST remains compatible with older Vitess versions.
func TestBuiltinBackupNonChunked(t *testing.T) {
	defer setDefaultCommonArgs()
	code, err := LaunchCluster(BuiltinBackup, "xbstream", 0, nil)
	require.NoErrorf(t, err, "setup failed with status code %d", code)
	defer TearDownCluster()

	t.Run("TestNonChunkedBackup", nonChunkedBackup)
}

// TestBuiltinBackupChunkedRestoreNonChunked verifies that a tablet configured with
// chunking can restore a non-chunked backup (forward compatibility).
func TestBuiltinBackupChunkedRestoreNonChunked(t *testing.T) {
	defer setDefaultCommonArgs()
	code, err := LaunchCluster(BuiltinBackup, "xbstream", 0, nil)
	require.NoErrorf(t, err, "setup failed with status code %d", code)
	defer TearDownCluster()

	t.Run("TestChunkedRestoreNonChunked", chunkedRestoreNonChunkedBackup)
}

func setDefaultCompressionFlag() {
	mysqlctl.CompressionEngineName = "pgzip"
	mysqlctl.ExternalCompressorCmd = ""
	mysqlctl.ExternalCompressorExt = ""
	mysqlctl.ExternalDecompressorCmd = ""
	mysqlctl.ManifestExternalDecompressorCmd = ""
}
