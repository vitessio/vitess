/*
Copyright 2024 The Vitess Authors.

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
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

const (
	DebugDelayCommitShard = "VT_DELAY_COMMIT_SHARD"
	DebugDelayCommitTime  = "VT_DELAY_COMMIT_TIME"
)

// ClearOutTable deletes everything from a table. Sometimes the table might have more rows than allowed in a single delete query,
// so we have to do the deletions iteratively.
func ClearOutTable(t *testing.T, vtParams mysql.ConnParams, tableName string) {
	ctx := context.Background()
	for {
		conn, err := mysql.Connect(ctx, &vtParams)
		if err != nil {
			fmt.Printf("Error in connection - %v\n", err)
			continue
		}

		res, err := conn.ExecuteFetch(fmt.Sprintf("SELECT count(*) FROM %v", tableName), 1, false)
		if err != nil {
			fmt.Printf("Error in selecting - %v\n", err)
			conn.Close()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		require.Len(t, res.Rows, 1)
		require.Len(t, res.Rows[0], 1)
		rowCount, err := res.Rows[0][0].ToInt()
		require.NoError(t, err)
		if rowCount == 0 {
			conn.Close()
			return
		}
		_, err = conn.ExecuteFetch(fmt.Sprintf("DELETE FROM %v LIMIT 10000", tableName), 10000, false)
		conn.Close()
		if err != nil {
			fmt.Printf("Error in cleanup deletion - %v\n", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
	}
}

// WriteTestCommunicationFile writes the content to the file with the given name.
// We use these files to coordinate with the vttablets running in the debug mode.
func WriteTestCommunicationFile(t *testing.T, fileName string, content string) {
	err := os.WriteFile(path.Join(os.Getenv("VTDATAROOT"), fileName), []byte(content), 0644)
	require.NoError(t, err)
}

// DeleteFile deletes the file specified.
func DeleteFile(fileName string) {
	_ = os.Remove(path.Join(os.Getenv("VTDATAROOT"), fileName))
}
