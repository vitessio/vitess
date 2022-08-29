// Package mysqlctl_test is the blackbox tests for package mysqlctl.
// Tests that need to use fakemysqldaemon must be written as blackbox tests;
// since fakemysqldaemon imports mysqlctl, importing fakemysqldaemon in
// a `package mysqlctl` test would cause a circular import.
package mysqlctl

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

func TestBinlogsToBackup(t *testing.T) {
	binlogs := []string{
		"vt-bin.000001",
		"vt-bin.000002",
		"vt-bin.000003",
		"vt-bin.000004",
		"vt-bin.000005",
		"vt-bin.000006",
	}
	basePreviousGTIDs := map[string]string{
		"vt-bin.000001": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-50",
		"vt-bin.000002": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-60",
		"vt-bin.000003": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-60",
		"vt-bin.000004": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-78",
		"vt-bin.000005": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-243",
		"vt-bin.000006": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-331",
	}
	tt := []struct {
		previousGTIDs map[string]string
		backupPos     string
		expectBinlogs []string
		expectError   string
	}{
		{
			previousGTIDs: basePreviousGTIDs,
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-78",
			expectBinlogs: []string{"vt-bin.000004", "vt-bin.000005"},
		},
		{
			previousGTIDs: basePreviousGTIDs,
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-60",
			expectBinlogs: []string{"vt-bin.000003", "vt-bin.000004", "vt-bin.000005"},
		},
		{
			previousGTIDs: basePreviousGTIDs,
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-63",
			expectBinlogs: []string{"vt-bin.000003", "vt-bin.000004", "vt-bin.000005"},
		},
		{
			previousGTIDs: basePreviousGTIDs,
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-243",
			expectBinlogs: []string{"vt-bin.000005"},
		},
		{
			previousGTIDs: basePreviousGTIDs,
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-331",
			expectError:   "no binary logs to backup",
		},
		{
			previousGTIDs: basePreviousGTIDs,
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-630000",
			expectError:   "no binary logs to backup",
		},
		{
			previousGTIDs: basePreviousGTIDs,
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f0000:1-63",
			expectError:   "There are GTID entries that are missing",
		},
		{
			previousGTIDs: map[string]string{
				"vt-bin.000001": "",
				"vt-bin.000002": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-60",
				"vt-bin.000003": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-60",
				"vt-bin.000004": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-78",
				"vt-bin.000005": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-243",
				"vt-bin.000006": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-331",
			},
			backupPos:   "16b1039f-22b6-11ed-b765-0a43f95f0000:1-63",
			expectError: "neither contains requested GTID",
		},
	}
	for i, tc := range tt {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			backupPos, err := mysql.ParsePosition(mysql.Mysql56FlavorID, tc.backupPos)
			require.NoError(t, err)
			require.NoError(t, err)
			binlogsToBackup, fromGTID, toGTID, err := ChooseBinlogsForIncrementalBackup(
				context.Background(),
				backupPos.GTIDSet,
				binlogs,
				func(ctx context.Context, binlog string) (gtids string, err error) {
					gtids, ok := tc.previousGTIDs[binlog]
					if !ok {
						return "", fmt.Errorf("previous gtids not found for binary log %v", binlog)
					}
					return gtids, nil
				},
			)
			if tc.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectError)
				return
			}
			require.NotEmpty(t, binlogsToBackup)
			assert.Equal(t, tc.expectBinlogs, binlogsToBackup)
			assert.Equal(t, tc.previousGTIDs[binlogsToBackup[0]], fromGTID)
			assert.Equal(t, tc.previousGTIDs[binlogs[len(binlogs)-1]], toGTID)
			assert.NotEqual(t, fromGTID, toGTID)
		})
	}
}
