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
)

// TestBuiltinBackup - main tests backup using vtctl commands
func TestBuiltinBackup(t *testing.T) {
	TestBackup(t, BuiltinBackup, "xbstream", 0, nil, nil)
}

// compressionVariantTests are the tests that the compression variants below
// run. TestReplicaBackup takes a backup through the variant's compressor and
// restores replica2 from it, and vtctlBackup checks the restore stats to
// confirm that a decompressor processed the data. What TestPrimaryBackup adds
// on top (allow-primary gating, restoring to a backup timestamp, init SQL
// queries, PRS, tablet deletion) does not depend on compression and stays
// covered by TestBuiltinBackup under the default engine.
var compressionVariantTests = []string{"TestReplicaBackup"}

func TestBuiltinBackupWithZstdCompression(t *testing.T) {
	defer setDefaultCompressionFlag()
	defer setDefaultCommonArgs()
	cDetails := &CompressionDetails{
		CompressorEngineName:    "zstd",
		ExternalCompressorCmd:   "zstd",
		ExternalCompressorExt:   ".zst",
		ExternalDecompressorCmd: "zstd -d",
	}

	TestBackup(t, BuiltinBackup, "xbstream", 0, cDetails, compressionVariantTests)
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

	TestBackup(t, BuiltinBackup, "xbstream", 0, cDetails, compressionVariantTests)
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

	TestBackup(t, BuiltinBackup, "xbstream", 0, cDetails, compressionVariantTests)
}

func setDefaultCompressionFlag() {
	mysqlctl.CompressionEngineName = "pgzip"
	mysqlctl.ExternalCompressorCmd = ""
	mysqlctl.ExternalCompressorExt = ""
	mysqlctl.ExternalDecompressorCmd = ""
	mysqlctl.ManifestExternalDecompressorCmd = ""
}
