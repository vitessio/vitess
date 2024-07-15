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
	"encoding/json"
	"io"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/mysqlctl"
)

// TestBuiltinBackup - main tests backup using vtctl commands
func TestBuiltinBackup(t *testing.T) {
	TestBackup(t, BuiltinBackup, "xbstream", 0, nil, nil)
}

func TestBuiltinBackupWithZstdCompression(t *testing.T) {
	defer setDefaultCompressionFlag()
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
	cDetails := &CompressionDetails{
		CompressorEngineName:            "external",
		ExternalCompressorCmd:           "zstd",
		ExternalCompressorExt:           ".zst",
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

func TestBackupEngineSelector(t *testing.T) {
	defer setDefaultCompressionFlag()
	defer cluster.PanicHandler(t)

	// launch the custer with xtrabackup as the default engine
	code, err := LaunchCluster(XtraBackup, "xbstream", 0, &CompressionDetails{CompressorEngineName: "pgzip"})
	require.Nilf(t, err, "setup failed with status code %d", code)

	defer TearDownCluster()

	localCluster.DisableVTOrcRecoveries(t)
	defer func() {
		localCluster.EnableVTOrcRecoveries(t)
	}()
	verifyInitialReplication(t)

	// first try to backup with an alternative engine (builtin)
	err = localCluster.VtctldClientProcess.ExecuteCommand("Backup", "--allow-primary", "--backup-engine=builtin", primary.Alias)
	require.NoError(t, err)
	engineUsed := getBackupEngineOfLastBackup(t)
	require.Equal(t, "builtin", engineUsed)

	// then try to backup specifying the xtrabackup engine
	err = localCluster.VtctldClientProcess.ExecuteCommand("Backup", "--allow-primary", "--backup-engine=xtrabackup", primary.Alias)
	require.NoError(t, err)
	engineUsed = getBackupEngineOfLastBackup(t)
	require.Equal(t, "xtrabackup", engineUsed)

	// check that by default we still use the xtrabackup engine if not specified
	err = localCluster.VtctldClientProcess.ExecuteCommand("Backup", "--allow-primary", primary.Alias)
	require.NoError(t, err)
	engineUsed = getBackupEngineOfLastBackup(t)
	require.Equal(t, "xtrabackup", engineUsed)
}

// fetch the backup engine used on the last backup triggered by the end-to-end tests.
func getBackupEngineOfLastBackup(t *testing.T) string {
	output, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput("GetBackups", shardKsName)

	// split the backups response into a slice of strings
	backups := strings.Split(strings.TrimSpace(output), "\n")

	// open the Manifest and retrieve the backup engine that was used
	f, err := os.Open(path.Join(localCluster.CurrentVTDATAROOT,
		"backups", keyspaceName, shardName,
		backups[len(backups)-1], "MANIFEST",
	))
	require.NoError(t, err)
	defer f.Close()

	data, err := io.ReadAll(f)
	require.NoError(t, err)

	var manifest mysqlctl.BackupManifest
	err = json.Unmarshal(data, &manifest)
	require.NoError(t, err)

	return manifest.BackupMethod
}
