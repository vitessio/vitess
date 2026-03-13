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
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/backup/s3"
	"vitess.io/vitess/go/test/endtoend/cluster"
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

func setDefaultCompressionFlag() {
	mysqlctl.CompressionEngineName = "pgzip"
	mysqlctl.ExternalCompressorCmd = ""
	mysqlctl.ExternalCompressorExt = ""
	mysqlctl.ExternalDecompressorCmd = ""
	mysqlctl.ManifestExternalDecompressorCmd = ""
}

func TestBackupRestoreClusterS3MicroCeph(t *testing.T) {
	cfg := s3.SkipIfMicroCephUnavailable(t)
	if cfg == nil {
		return
	}
	os.Setenv("AWS_ACCESS_KEY_ID", cfg.AccessKey)
	os.Setenv("AWS_SECRET_ACCESS_KEY", cfg.SecretKey)
	t.Cleanup(func() {
		os.Unsetenv("AWS_ACCESS_KEY_ID")
		os.Unsetenv("AWS_SECRET_ACCESS_KEY")
	})

	s3Config := &cluster.S3BackupConfig{
		Endpoint:       cfg.Endpoint,
		Bucket:         cfg.Bucket,
		Region:         cfg.Region,
		ForcePathStyle: true,
	}
	code, err := LaunchCluster(BuiltinBackup, "xbstream", 0, nil, s3Config)
	require.NoError(t, err)
	require.Equal(t, 0, code)
	defer TearDownCluster()

	localCluster.DisableVTOrcRecoveries(t)
	defer func() {
		localCluster.EnableVTOrcRecoveries(t)
	}()
	verifyInitialReplication(t)

	err = localCluster.VtctldClientProcess.ExecuteCommand("Backup", "--allow-primary", primary.Alias)
	require.NoError(t, err)

	backups := localCluster.VerifyBackupCount(t, shardKsName, 1)
	require.GreaterOrEqual(t, len(backups), 1)

	err = localCluster.VtctldClientProcess.ExecuteCommand("RestoreFromBackup", replica2.Alias)
	require.NoError(t, err)

	err = replica2.VttabletProcess.WaitForTabletStatusesForTimeout([]string{"SERVING"}, timeout)
	require.NoError(t, err)
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 1)
}
