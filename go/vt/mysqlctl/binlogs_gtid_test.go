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

// Package mysqlctl_test is the blackbox tests for package mysqlctl.
// Tests that need to use fakemysqldaemon must be written as blackbox tests;
// since fakemysqldaemon imports mysqlctl, importing fakemysqldaemon in
// a `package mysqlctl` test would cause a circular import.
package mysqlctl

import (
	"context"
	"fmt"
	"testing"
	"time"

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

func TestFindPITRToTimePath(t *testing.T) {
	generatePosition := func(posRange string) mysql.Position {
		return mysql.MustParsePosition(mysql.Mysql56FlavorID, fmt.Sprintf("16b1039f-22b6-11ed-b765-0a43f95f28a3:%s", posRange))
	}
	fullManifest := func(backupPos string, timeStr string) *BackupManifest {
		_, err := ParseRFC3339(timeStr)
		require.NoError(t, err)
		return &BackupManifest{
			BackupMethod: builtinBackupEngineName,
			Position:     generatePosition(backupPos),
			BackupTime:   timeStr,
			FinishedTime: timeStr,
		}
	}
	incrementalManifest := func(backupPos string, backupFromPos string, firstTimestampStr string, lastTimestampStr string) *BackupManifest {
		firstTimestamp, err := ParseRFC3339(firstTimestampStr)
		require.NoError(t, err)
		lastTimestamp, err := ParseRFC3339(lastTimestampStr)
		require.NoError(t, err)

		return &BackupManifest{
			Position:     generatePosition(backupPos),
			FromPosition: generatePosition(backupFromPos),
			Incremental:  true,
			IncrementalDetails: &IncrementalBackupDetails{
				FirstTimestamp: FormatRFC3339(firstTimestamp),
				LastTimestamp:  FormatRFC3339(lastTimestamp),
			},
		}
	}

	fullManifests := map[string]*BackupManifest{
		"1-50":  fullManifest("1-50", "2020-02-02T02:20:20.000000Z"),
		"1-5":   fullManifest("1-5", "2020-02-02T02:01:20.000000Z"),
		"1-80":  fullManifest("1-80", "2020-02-02T03:31:00.000000Z"),
		"1-70":  fullManifest("1-70", "2020-02-02T03:10:01.000000Z"),
		"1-70b": fullManifest("1-70", "2020-02-02T03:10:11.000000Z"),
	}
	fullBackups := []*BackupManifest{
		fullManifests["1-50"],
		fullManifests["1-5"],
		fullManifests["1-80"],
		fullManifests["1-70"],
		fullManifests["1-70b"],
	}
	incrementalManifests := map[string]*BackupManifest{
		"1-34:1-5":  incrementalManifest("1-34", "1-5", "2020-02-02T02:01:44.000000Z", "2020-02-02T02:17:00.000000Z"),
		"1-38:1-34": incrementalManifest("1-38", "1-34", "2020-02-02T02:17:05.000000Z", "2020-02-02T02:18:00.000000Z"),
		"1-52:1-35": incrementalManifest("1-52", "1-35", "2020-02-02T02:17:59.000000Z", "2020-02-02T02:22:00.000000Z"),
		"1-60:1-50": incrementalManifest("1-60", "1-50", "2020-02-02T02:20:21.000000Z", "2020-02-02T02:47:20.000000Z"),
		"1-70:1-60": incrementalManifest("1-70", "1-60", "2020-02-02T02:47:20.000000Z", "2020-02-02T03:10:00.700000Z"),
		"1-82:1-70": incrementalManifest("1-82", "1-70", "2020-02-02T03:10:11.000000Z", "2020-02-02T03:39:09.000000Z"),
		"1-92:1-79": incrementalManifest("1-92", "1-79", "2020-02-02T03:37:07.000000Z", "2020-02-02T04:04:04.000000Z"),
		"1-95:1-89": incrementalManifest("1-95", "1-89", "2020-02-02T03:59:05.000000Z", "2020-02-02T04:15:00.000000Z"),
	}
	incrementalBackups := []*BackupManifest{
		incrementalManifests["1-34:1-5"],
		incrementalManifests["1-38:1-34"],
		incrementalManifests["1-52:1-35"],
		incrementalManifests["1-60:1-50"],
		incrementalManifests["1-70:1-60"],
		incrementalManifests["1-82:1-70"],
		incrementalManifests["1-92:1-79"],
		incrementalManifests["1-95:1-89"],
	}
	incrementalBackupName := func(manifest *BackupManifest) string {
		for k, v := range incrementalManifests {
			if v == manifest {
				return k
			}
		}
		return "unknown"
	}
	tt := []struct {
		name                       string
		restoreToTimestamp         string
		purgedGTID                 string
		incrementalBackups         []*BackupManifest
		expectFullManifest         *BackupManifest
		expectIncrementalManifests []*BackupManifest
		expectError                string
	}{
		{
			name:                       "full is enough",
			restoreToTimestamp:         "2020-02-02T02:01:20.000000Z",
			expectFullManifest:         fullManifests["1-5"],
			expectIncrementalManifests: []*BackupManifest{},
		},
		{
			name:                       "full is still enough",
			restoreToTimestamp:         "2020-02-02T02:01:41.000000Z",
			expectFullManifest:         fullManifests["1-5"],
			expectIncrementalManifests: []*BackupManifest{},
		},
		{
			name:               "full is just not enough",
			restoreToTimestamp: "2020-02-02T02:01:44.000000Z",
			expectFullManifest: fullManifests["1-5"],
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifests["1-34:1-5"],
			},
		},
		{
			name:               "just one",
			restoreToTimestamp: "2020-02-02T02:20:21.000000Z",
			expectFullManifest: fullManifests["1-50"],
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifests["1-52:1-35"],
			},
		},
		{
			name:               "two",
			restoreToTimestamp: "2020-02-02T02:23:23.000000Z",
			expectFullManifest: fullManifests["1-50"],
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifests["1-52:1-35"],
				incrementalManifests["1-60:1-50"],
			},
		},
		{
			name:               "three",
			restoreToTimestamp: "2020-02-02T02:55:55.000000Z",
			expectFullManifest: fullManifests["1-50"],
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifests["1-52:1-35"],
				incrementalManifests["1-60:1-50"],
				incrementalManifests["1-70:1-60"],
			},
		},
		{
			name:               "still three",
			restoreToTimestamp: "2020-02-02T03:10:00.600000Z",
			expectFullManifest: fullManifests["1-50"],
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifests["1-52:1-35"],
				incrementalManifests["1-60:1-50"],
				incrementalManifests["1-70:1-60"],
			},
		},
		{
			name:               "and still three",
			restoreToTimestamp: "2020-02-02T03:10:00.700000Z",
			expectFullManifest: fullManifests["1-50"],
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifests["1-52:1-35"],
				incrementalManifests["1-60:1-50"],
				incrementalManifests["1-70:1-60"],
			},
		},
		{
			name:               "and still three, exceeding binlog last timestamp",
			restoreToTimestamp: "2020-02-02T03:10:00.800000Z",
			expectFullManifest: fullManifests["1-50"],
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifests["1-52:1-35"],
				incrementalManifests["1-60:1-50"],
				incrementalManifests["1-70:1-60"],
			},
		},
		{
			name:                       "next backup 1-70",
			restoreToTimestamp:         "2020-02-02T03:10:01.000000Z",
			expectFullManifest:         fullManifests["1-70"],
			expectIncrementalManifests: []*BackupManifest{},
		},
		{
			name:               "next backup 1-70 with one binlog",
			restoreToTimestamp: "2020-02-02T03:10:13.000000Z",
			expectFullManifest: fullManifests["1-70"],
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifests["1-82:1-70"],
			},
		},
		{
			name:               "next backup 1-70b, included first binlog",
			restoreToTimestamp: "2020-02-02T03:10:11.000000Z",
			expectFullManifest: fullManifests["1-70b"],
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifests["1-82:1-70"],
			},
		},
		{
			name:               "next backup 1-70b, still included first binlog",
			restoreToTimestamp: "2020-02-02T03:20:11.000000Z",
			expectFullManifest: fullManifests["1-70b"],
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifests["1-82:1-70"],
			},
		},
		{
			name:               "1-80 and two binlogs",
			restoreToTimestamp: "2020-02-02T04:00:00.000000Z",
			expectFullManifest: fullManifests["1-80"],
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifests["1-82:1-70"],
				incrementalManifests["1-92:1-79"],
			},
		},
		{
			name:               "1-80 and all remaining binlogs",
			restoreToTimestamp: "2020-02-02T04:10:00.000000Z",
			expectFullManifest: fullManifests["1-80"],
			expectIncrementalManifests: []*BackupManifest{
				incrementalManifests["1-82:1-70"],
				incrementalManifests["1-92:1-79"],
				incrementalManifests["1-95:1-89"],
			},
		},
		{
			name:               "no incremental backup reaches this timestamp",
			restoreToTimestamp: "2020-02-02T07:07:07.000000Z",
			expectError:        "no path found",
		},
		{
			name:               "sooner than any full backup",
			restoreToTimestamp: "2020-02-02T01:59:59.000000Z",
			expectError:        "no full backup",
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

			restoreToTime, err := ParseRFC3339(tc.restoreToTimestamp)
			require.NoError(t, err)
			require.False(t, restoreToTime.IsZero())

			path, err := FindPITRToTimePath(restoreToTime, manifests)
			if tc.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectError)
				return
			}
			require.NoError(t, err)
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
			gotNames := []string{}
			for _, manifest := range got {
				gotNames = append(gotNames, incrementalBackupName(manifest))
			}
			assert.Equal(t, expected, got, "got names: %v", gotNames)
		})
	}
	t.Run("iterate all valid timestamps", func(t *testing.T) {
		var manifests []*BackupManifest
		manifests = append(manifests, fullBackups...)
		manifests = append(manifests, incrementalBackups...)

		firstTimestamp, err := ParseRFC3339(fullManifests["1-5"].BackupTime)
		require.NoError(t, err)
		lastTimestamp, err := ParseRFC3339(incrementalManifests["1-95:1-89"].IncrementalDetails.LastTimestamp)
		require.NoError(t, err)

		for restoreToTime := firstTimestamp; !restoreToTime.After(lastTimestamp); restoreToTime = restoreToTime.Add(10 * time.Second) {
			testName := fmt.Sprintf("restore to %v", restoreToTime)
			t.Run(testName, func(t *testing.T) {
				path, err := FindPITRToTimePath(restoreToTime, manifests)
				require.NoError(t, err)
				require.NotEmpty(t, path)
				fullBackup := path[0]
				require.False(t, fullBackup.Incremental)
				for _, manifest := range path[1:] {
					require.True(t, manifest.Incremental)
				}
			})
		}
	})
}
