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
	lastBackup := getLastBackup(t)

	// open the Manifest and retrieve the backup engine that was used
	f, err := os.Open(path.Join(localCluster.CurrentVTDATAROOT,
		"backups", keyspaceName, shardName,
		lastBackup, "MANIFEST",
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

func getLastBackup(t *testing.T) string {
	output, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput("GetBackups", shardKsName)
	require.NoError(t, err)

	// split the backups response into a slice of strings
	backups := strings.Split(strings.TrimSpace(output), "\n")

	return backups[len(backups)-1]
}

func TestRestoreIgnoreBackups(t *testing.T) {
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

	// lets take two backups, each using a different backup engine
	err = localCluster.VtctldClientProcess.ExecuteCommand("Backup", "--allow-primary", "--backup-engine=builtin", primary.Alias)
	require.NoError(t, err)
	// firstBackup := getLastBackup(t)

	err = localCluster.VtctldClientProcess.ExecuteCommand("Backup", "--allow-primary", "--backup-engine=xtrabackup", primary.Alias)
	require.NoError(t, err)

	//  insert more data on the primary
	_, err = primary.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	require.NoError(t, err)

	// now bring up the other replica, letting it restore from backup.
	restoreWaitForBackup(t, "replica", nil, true)
	err = replica2.VttabletProcess.WaitForTabletStatusesForTimeout([]string{"SERVING"}, timeout)
	require.NoError(t, err)

	// check the new replica has the data
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 2)

	// now lets break the last backup in the shard
	err = os.Remove(path.Join(localCluster.CurrentVTDATAROOT,
		"backups", keyspaceName, shardName,
		getLastBackup(t), "backup.xbstream.gz"))
	require.NoError(t, err)

	// and try to restore from it
	err = localCluster.VtctldClientProcess.ExecuteCommand("RestoreFromBackup", replica2.Alias)
	require.ErrorContains(t, err, "exit status 1") // this should fail

	// now we retry but trying the earlier backup
	err = localCluster.VtctldClientProcess.ExecuteCommand("RestoreFromBackup", "--ignored-backup-engines=xtrabackup", replica2.Alias)
	require.NoError(t, err) // this should succeed

	// make sure we are replicating after the restore is done
	err = replica2.VttabletProcess.WaitForTabletStatusesForTimeout([]string{"SERVING"}, timeout)
	require.NoError(t, err)
	cluster.VerifyRowsInTablet(t, replica2, keyspaceName, 2)
}
