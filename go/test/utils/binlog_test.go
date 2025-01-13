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

package utils

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSetBinlogRowImageOptions tests the SetBinlogRowImageOptions function.
func TestUtils(t *testing.T) {
	tmpDir := "/tmp"
	cnfFile := fmt.Sprintf("%s/%s", tmpDir, BinlogRowImageCnf)

	// Test that setting the mode will create the cnf file and add it to the EXTRA_MY_CNF env var.
	require.NoError(t, SetBinlogRowImageOptions("noblob", false, tmpDir))
	data, err := os.ReadFile(cnfFile)
	require.NoError(t, err)
	require.Contains(t, string(data), "binlog_row_image=noblob")
	require.Contains(t, os.Getenv(ExtraCnf), BinlogRowImageCnf)

	// Test that setting the mode and passing true for includePartialJSON will set both options
	// as expected.
	if CIDBPlatformIsMySQL8orLater() {
		require.NoError(t, SetBinlogRowImageOptions("noblob", true, tmpDir))
		data, err = os.ReadFile(cnfFile)
		require.NoError(t, err)
		require.Contains(t, string(data), "binlog_row_image=noblob")
		require.Contains(t, string(data), "binlog_row_value_options=PARTIAL_JSON")
		require.Contains(t, os.Getenv(ExtraCnf), BinlogRowImageCnf)
	} else {
		require.Error(t, SetBinlogRowImageOptions("noblob", true, tmpDir))
	}

	// Test that clearing the mode will remove the cnf file and the cnf from the EXTRA_MY_CNF env var.
	require.NoError(t, SetBinlogRowImageOptions("", false, tmpDir))
	require.NotContains(t, os.Getenv(ExtraCnf), BinlogRowImageCnf)
	_, err = os.Stat(cnfFile)
	require.True(t, os.IsNotExist(err))
}
