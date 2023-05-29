/*
Copyright 2022 The Vitess Authors.

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

package mysqlctl

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

func TestChooseBinlogsForIncrementalBackup(t *testing.T) {
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
		name          string
		previousGTIDs map[string]string
		backupPos     string
		gtidPurged    string
		expectBinlogs []string
		expectError   string
	}{
		{
			name:          "exact match",
			previousGTIDs: basePreviousGTIDs,
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-78",
			expectBinlogs: []string{"vt-bin.000004", "vt-bin.000005"},
		},
		{
			name:          "exact match, two binlogs with same previous GTIDs",
			previousGTIDs: basePreviousGTIDs,
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-60",
			expectBinlogs: []string{"vt-bin.000003", "vt-bin.000004", "vt-bin.000005"},
		},
		{
			name:          "inexact match",
			previousGTIDs: basePreviousGTIDs,
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-63",
			expectBinlogs: []string{"vt-bin.000003", "vt-bin.000004", "vt-bin.000005"},
		},
		{
			name:          "one binlog match",
			previousGTIDs: basePreviousGTIDs,
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-243",
			expectBinlogs: []string{"vt-bin.000005"},
		},
		{
			name:          "last binlog excluded, no binlogs found",
			previousGTIDs: basePreviousGTIDs,
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-331",
			expectError:   "no binary logs to backup",
		},
		{
			name:          "backup pos beyond all binlogs",
			previousGTIDs: basePreviousGTIDs,
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-630000",
			expectError:   "no binary logs to backup",
		},
		{
			name:          "missing GTID entries",
			previousGTIDs: basePreviousGTIDs,
			backupPos:     "16b1039f-0000-0000-0000-000000000000:1-63",
			expectError:   "Required entries have been purged",
		},
		{
			name: "empty previous GTIDs in first binlog",
			previousGTIDs: map[string]string{
				"vt-bin.000001": "",
				"vt-bin.000002": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-60",
				"vt-bin.000003": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-60",
				"vt-bin.000004": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-78",
				"vt-bin.000005": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-243",
				"vt-bin.000006": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-331",
			},
			backupPos:   "16b1039f-0000-0000-0000-000000000000:1-63",
			expectError: "Mismatching GTID entries",
		},
		{
			name: "empty previous GTIDs in first binlog covering backup pos",
			previousGTIDs: map[string]string{
				"vt-bin.000001": "",
				"vt-bin.000002": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-60",
				"vt-bin.000003": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-60",
				"vt-bin.000004": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-78",
				"vt-bin.000005": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-243",
				"vt-bin.000006": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-331",
			},
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-30",
			expectBinlogs: []string{"vt-bin.000001", "vt-bin.000002", "vt-bin.000003", "vt-bin.000004", "vt-bin.000005"},
		},
		{
			name: "empty previous GTIDs in first binlog not covering backup pos",
			previousGTIDs: map[string]string{
				"vt-bin.000001": "",
				"vt-bin.000002": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-60",
				"vt-bin.000003": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-60",
				"vt-bin.000004": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-78",
				"vt-bin.000005": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-243",
				"vt-bin.000006": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-331",
			},
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-65",
			expectBinlogs: []string{"vt-bin.000003", "vt-bin.000004", "vt-bin.000005"},
		},
		{
			name: "empty previous GTIDs in first binlog not covering backup pos, 2",
			previousGTIDs: map[string]string{
				"vt-bin.000001": "",
				"vt-bin.000002": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-60",
				"vt-bin.000003": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-60",
				"vt-bin.000004": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-78",
				"vt-bin.000005": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-243",
				"vt-bin.000006": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-331",
			},
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-100",
			expectBinlogs: []string{"vt-bin.000004", "vt-bin.000005"},
		},
		{
			name: "match with non strictly monotonic sequence",
			previousGTIDs: map[string]string{
				"vt-bin.000001": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-50",
				"vt-bin.000002": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-60",
				"vt-bin.000003": "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-60",
				"vt-bin.000004": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-78",
				"vt-bin.000005": "16b1039f-22b6-11ed-b765-0a43f95f28a3:20-243",
				"vt-bin.000006": "16b1039f-22b6-11ed-b765-0a43f95f28a3:200-331",
			},
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-63",
			expectBinlogs: []string{"vt-bin.000003", "vt-bin.000004", "vt-bin.000005"},
		},
		{
			name: "exact, gitd_purged",
			previousGTIDs: map[string]string{
				"vt-bin.000001": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-78",
				"vt-bin.000002": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-90",
				"vt-bin.000003": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-90",
				"vt-bin.000004": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-100",
				"vt-bin.000005": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-110",
				"vt-bin.000006": "16b1039f-22b6-11ed-b765-0a43f95f28a3:2-300",
			},
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-78",
			gtidPurged:    "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-78",
			expectBinlogs: []string{"vt-bin.000001", "vt-bin.000002", "vt-bin.000003", "vt-bin.000004", "vt-bin.000005"},
		},
		{
			name:          "exact, gitd_purged 2",
			previousGTIDs: basePreviousGTIDs,
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-50",
			gtidPurged:    "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-50",
			expectBinlogs: []string{"vt-bin.000001", "vt-bin.000002", "vt-bin.000003", "vt-bin.000004", "vt-bin.000005"},
		},
		{
			name: "inexact, gitd_purged, missing",
			previousGTIDs: map[string]string{
				"vt-bin.000001": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-78",
				"vt-bin.000002": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-90",
				"vt-bin.000003": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-90",
				"vt-bin.000004": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-100",
				"vt-bin.000005": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-110",
				"vt-bin.000006": "16b1039f-22b6-11ed-b765-0a43f95f28a3:2-300",
			},
			backupPos:   "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-63",
			gtidPurged:  "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-2",
			expectError: "Required entries have been purged",
		},
		{
			name: "inexact, gitd_purged, missing 2",
			previousGTIDs: map[string]string{
				"vt-bin.000001": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-78",
				"vt-bin.000002": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-90",
				"vt-bin.000003": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-90",
				"vt-bin.000004": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-100",
				"vt-bin.000005": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-110",
				"vt-bin.000006": "16b1039f-22b6-11ed-b765-0a43f95f28a3:2-300",
			},
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-80",
			gtidPurged:    "16b1039f-22b6-11ed-b765-0a43f95f28a3:1",
			expectBinlogs: []string{"vt-bin.000001", "vt-bin.000002", "vt-bin.000003", "vt-bin.000004", "vt-bin.000005"},
			expectError:   "Mismatching GTID entries",
		},
		{
			name: "inexact, gitd_purged, found",
			previousGTIDs: map[string]string{
				"vt-bin.000001": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-78",
				"vt-bin.000002": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-90",
				"vt-bin.000003": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-90",
				"vt-bin.000004": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-100",
				"vt-bin.000005": "16b1039f-22b6-11ed-b765-0a43f95f28a3:3-110",
				"vt-bin.000006": "16b1039f-22b6-11ed-b765-0a43f95f28a3:2-300",
			},
			backupPos:     "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-84",
			gtidPurged:    "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-2",
			expectBinlogs: []string{"vt-bin.000001", "vt-bin.000002", "vt-bin.000003", "vt-bin.000004", "vt-bin.000005"},
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			backupPos, err := mysql.ParsePosition(mysql.Mysql56FlavorID, tc.backupPos)
			require.NoError(t, err)
			gtidPurged, err := mysql.ParsePosition(mysql.Mysql56FlavorID, tc.gtidPurged)
			require.NoError(t, err)
			binlogsToBackup, fromGTID, toGTID, err := ChooseBinlogsForIncrementalBackup(
				context.Background(),
				backupPos.GTIDSet,
				gtidPurged.GTIDSet,
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
			require.NoError(t, err)
			require.NotEmpty(t, binlogsToBackup)
			assert.Equal(t, tc.expectBinlogs, binlogsToBackup)
			if tc.previousGTIDs[binlogsToBackup[0]] != "" {
				assert.Equal(t, tc.previousGTIDs[binlogsToBackup[0]], fromGTID)
			}
			assert.Equal(t, tc.previousGTIDs[binlogs[len(binlogs)-1]], toGTID)
			assert.NotEqual(t, fromGTID, toGTID)
		})
	}
}

func TestIsValidIncrementalBakcup(t *testing.T) {
	incrementalManifest := func(backupPos string, backupFromPos string) *BackupManifest {
		return &BackupManifest{
			Position:     mysql.MustParsePosition(mysql.Mysql56FlavorID, fmt.Sprintf("16b1039f-22b6-11ed-b765-0a43f95f28a3:%s", backupPos)),
			FromPosition: mysql.MustParsePosition(mysql.Mysql56FlavorID, fmt.Sprintf("16b1039f-22b6-11ed-b765-0a43f95f28a3:%s", backupFromPos)),
			Incremental:  true,
		}
	}
	tt := []struct {
		baseGTID      string
		purgedGTID    string
		backupFromPos string
		backupPos     string
		expectIsValid bool
	}{
		{
			baseGTID:      "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-58",
			backupFromPos: "1-58",
			backupPos:     "1-70",
			expectIsValid: true,
		},
		{
			baseGTID:      "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-58",
			backupFromPos: "1-51",
			backupPos:     "1-70",
			expectIsValid: true,
		},
		{
			baseGTID:      "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-58",
			backupFromPos: "1-51",
			backupPos:     "1-58",
			expectIsValid: false,
		},
		{
			baseGTID:      "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-58",
			backupFromPos: "1-58",
			backupPos:     "1-58",
			expectIsValid: false,
		},
		{
			baseGTID:      "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-58",
			backupFromPos: "1-51",
			backupPos:     "1-55",
			expectIsValid: false,
		},
		{
			baseGTID:      "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-58",
			backupFromPos: "1-59",
			backupPos:     "1-70",
			expectIsValid: false,
		},
		{
			baseGTID:      "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-58",
			backupFromPos: "1-60",
			backupPos:     "1-70",
			expectIsValid: false,
		},
		{
			baseGTID:      "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-58",
			backupFromPos: "3-51",
			backupPos:     "3-70",
			expectIsValid: false,
		},
		{
			baseGTID:      "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-58",
			purgedGTID:    "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-2",
			backupFromPos: "3-51",
			backupPos:     "3-70",
			expectIsValid: true,
		},
		{
			baseGTID:      "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-58",
			purgedGTID:    "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-2",
			backupFromPos: "4-51",
			backupPos:     "4-70",
			expectIsValid: false,
		},
	}
	for i, tc := range tt {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			basePos, err := mysql.ParsePosition(mysql.Mysql56FlavorID, tc.baseGTID)
			require.NoError(t, err)
			purgedPos, err := mysql.ParsePosition(mysql.Mysql56FlavorID, tc.purgedGTID)
			require.NoError(t, err)
			isValid := IsValidIncrementalBakcup(basePos.GTIDSet, purgedPos.GTIDSet, incrementalManifest(tc.backupPos, tc.backupFromPos))
			assert.Equal(t, tc.expectIsValid, isValid)
		})
	}
}

func TestFindPITRPath(t *testing.T) {
	generatePosition := func(posRange string) mysql.Position {
		return mysql.MustParsePosition(mysql.Mysql56FlavorID, fmt.Sprintf("16b1039f-22b6-11ed-b765-0a43f95f28a3:%s", posRange))
	}
	fullManifest := func(backupPos string) *BackupManifest {
		return &BackupManifest{
			Position: generatePosition(backupPos),
		}
	}
	incrementalManifest := func(backupPos string, backupFromPos string) *BackupManifest {
		return &BackupManifest{
			Position:     generatePosition(backupPos),
			FromPosition: generatePosition(backupFromPos),
			Incremental:  true,
		}
	}
	fullBackups := []*BackupManifest{
		fullManifest("1-50"),
		fullManifest("1-5"),
		fullManifest("1-80"),
		fullManifest("1-70"),
		fullManifest("1-70"),
	}
	incrementalBackups := []*BackupManifest{
		incrementalManifest("1-34", "1-5"),
		incrementalManifest("1-38", "1-34"),
		incrementalManifest("1-52", "1-35"),
		incrementalManifest("1-60", "1-50"),
		incrementalManifest("1-70", "1-60"),
		incrementalManifest("1-82", "1-70"),
		incrementalManifest("1-92", "1-79"),
		incrementalManifest("1-95", "1-89"),
	}
	tt := []struct {
		name                       string
		restoreGTID                string
		purgedGTID                 string
		incrementalBackups         []*BackupManifest
		expectFullManifest         *BackupManifest
		expectIncrementalManifests []*BackupManifest
		expectError                string
	}{
		{
			name:               "1-58",
			restoreGTID:        "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-58",
			expectFullManifest: fullManifest("1-50"),
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifest("1-52", "1-35"),
				incrementalManifest("1-60", "1-50"),
			},
		},
		{
			name:               "1-50",
			restoreGTID:        "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-50",
			expectFullManifest: fullManifest("1-50"),
		},
		{
			name:               "1-78",
			restoreGTID:        "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-78",
			expectFullManifest: fullManifest("1-70"),
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifest("1-82", "1-70"),
			},
		},
		{
			name:               "1-45",
			restoreGTID:        "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-45",
			expectFullManifest: fullManifest("1-5"),
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifest("1-34", "1-5"),
				incrementalManifest("1-38", "1-34"),
				incrementalManifest("1-52", "1-35"),
			},
		},
		{
			name:               "1-28",
			restoreGTID:        "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-28",
			expectFullManifest: fullManifest("1-5"),
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifest("1-34", "1-5"),
			},
		},
		{
			name:               "1-88",
			restoreGTID:        "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-88",
			expectFullManifest: fullManifest("1-80"),
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifest("1-82", "1-70"),
				incrementalManifest("1-92", "1-79"),
			},
		},
		{
			name:        "fail 1-2",
			restoreGTID: "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-2",
			expectError: "no full backup",
		},
		{
			name:        "fail unknown UUID",
			restoreGTID: "00000000-0000-0000-0000-0a43f95f28a3:1-50",
			expectError: "no full backup",
		},
		{
			name:        "fail 1-99",
			restoreGTID: "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-99",
			expectError: "no path found",
		},
		{
			name:               "1-94",
			restoreGTID:        "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-94",
			expectFullManifest: fullManifest("1-80"),
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifest("1-82", "1-70"),
				incrementalManifest("1-92", "1-79"),
				incrementalManifest("1-95", "1-89"),
			},
		},
		{
			name:               "1-95",
			restoreGTID:        "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-95",
			expectFullManifest: fullManifest("1-80"),
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifest("1-82", "1-70"),
				incrementalManifest("1-92", "1-79"),
				incrementalManifest("1-95", "1-89"),
			},
		},
		{
			name:        "fail 1-88 with gaps",
			restoreGTID: "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-88",
			incrementalBackups: []*BackupManifest{
				incrementalManifest("1-34", "1-5"),
				incrementalManifest("1-38", "1-34"),
				incrementalManifest("1-52", "1-35"),
				incrementalManifest("1-60", "1-50"),
				incrementalManifest("1-70", "1-60"),
				incrementalManifest("1-82", "1-70"),
				incrementalManifest("1-92", "1-84"),
				incrementalManifest("1-95", "1-89"),
			},
			expectError: "no path found",
		},
		{
			name:        "1-45 first solution even when shorter exists",
			restoreGTID: "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-45",
			incrementalBackups: append(
				incrementalBackups,
				incrementalManifest("1-99", "1-5"),
			),
			expectFullManifest: fullManifest("1-5"),
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifest("1-34", "1-5"),
				incrementalManifest("1-38", "1-34"),
				incrementalManifest("1-52", "1-35"),
			},
		},
		{
			name:        "fail incomplete binlog previous GTIDs",
			restoreGTID: "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-92",
			incrementalBackups: []*BackupManifest{
				incrementalManifest("3-90", "3-75"),
				incrementalManifest("3-95", "3-90"),
			},
			expectFullManifest: fullManifest("1-80"),
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifest("3-90", "3-75"),
				incrementalManifest("3-95", "3-90"),
			},
			expectError: "no path found",
		},
		{
			name:        "incomplete binlog previous GTIDs",
			restoreGTID: "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-92",
			purgedGTID:  "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-2",
			incrementalBackups: []*BackupManifest{
				incrementalManifest("3-90", "3-75"),
				incrementalManifest("3-95", "3-90"),
			},
			expectFullManifest: fullManifest("1-80"),
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifest("3-90", "3-75"),
				incrementalManifest("3-95", "3-90"),
			},
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			if tc.incrementalBackups == nil {
				tc.incrementalBackups = incrementalBackups
			}
			for i := range fullBackups {
				var err error
				fullBackup := fullBackups[i]
				fullBackup.PurgedPosition, err = mysql.ParsePosition(mysql.Mysql56FlavorID, tc.purgedGTID)
				require.NoError(t, err)
				defer func() {
					fullBackup.PurgedPosition = mysql.Position{}
				}()
			}
			var manifests []*BackupManifest
			manifests = append(manifests, fullBackups...)
			manifests = append(manifests, tc.incrementalBackups...)

			restorePos, err := mysql.ParsePosition(mysql.Mysql56FlavorID, tc.restoreGTID)
			require.NoErrorf(t, err, "%v", err)
			path, err := FindPITRPath(restorePos.GTIDSet, manifests)
			if tc.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectError)
				return
			}
			require.NoErrorf(t, err, "%v", err)
			require.NotEmpty(t, path)
			// the path always consists of one full backup and zero or more incremental backups
			fullBackup := path[0]
			require.False(t, fullBackup.Incremental)
			for _, manifest := range path[1:] {
				require.True(t, manifest.Incremental)
			}
			assert.Equal(t, tc.expectFullManifest.Position.GTIDSet, fullBackup.Position.GTIDSet)
			if tc.expectIncrementalManifests == nil {
				tc.expectIncrementalManifests = []*BackupManifest{}
			}
			expected := BackupManifestPath(tc.expectIncrementalManifests)
			got := BackupManifestPath(path[1:])
			assert.Equal(t, expected, got, "expected: %s, got: %s", expected.String(), got.String())
		})
	}
}
