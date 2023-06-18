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
