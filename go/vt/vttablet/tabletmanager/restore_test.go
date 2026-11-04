/*
Copyright 2026 The Vitess Authors.

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

package tabletmanager

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func newTestTabletManager(t *testing.T) *TabletManager {
	t.Helper()
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  100,
		},
		Keyspace: "testkeyspace",
		Shard:    "-80",
	}
	tm := &TabletManager{
		BatchCtx:    t.Context(),
		tabletAlias: tablet.Alias,
	}
	tm.tmState = newTMState(tm, tablet)
	return tm
}

// setupHookDir creates a temporary VTROOT with a hook script that dumps
// environment variables into the given output file.
func setupHookDir(t *testing.T) (outputFile string) {
	t.Helper()

	vtroot := t.TempDir()
	t.Setenv("VTROOT", vtroot)

	hookDir := filepath.Join(vtroot, "vthook")
	require.NoError(t, os.MkdirAll(hookDir, 0o755))

	outputFile = filepath.Join(vtroot, "hook_env_output")
	hookScript := filepath.Join(hookDir, "vttablet_restore_done")
	require.NoError(t, os.WriteFile(hookScript, []byte("#!/bin/bash\nenv > "+outputFile+"\n"), 0o755))

	return outputFile
}

func readHookOutput(t *testing.T, outputFile string) string {
	t.Helper()
	data, err := os.ReadFile(outputFile)
	require.NoError(t, err, "hook output file should exist after synchronous execution")
	return string(data)
}

func TestInvokeRestoreDoneHook_BackupEngine(t *testing.T) {
	outputFile := setupHookDir(t)
	tm := newTestTabletManager(t)

	startTime := time.Now().Add(-10 * time.Second)
	tm.invokeRestoreDoneHook(startTime, nil, "xtrabackup")

	content := readHookOutput(t, outputFile)

	assert.Contains(t, content, "TM_RESTORE_DATA_BACKUP_ENGINE=xtrabackup")
	assert.Contains(t, content, "TM_RESTORE_DATA_START_TS=")
	assert.Contains(t, content, "TM_RESTORE_DATA_STOP_TS=")
	assert.Contains(t, content, "TM_RESTORE_DATA_DURATION=")
	assert.Contains(t, content, "TABLET_ALIAS=zone1-0000000100")
	assert.Contains(t, content, "KEYSPACE=testkeyspace")
	assert.Contains(t, content, "SHARD=-80")
	assert.NotContains(t, content, "TM_RESTORE_DATA_ERROR=")
}

func TestInvokeRestoreDoneHook_EmptyBackupEngine(t *testing.T) {
	outputFile := setupHookDir(t)
	tm := newTestTabletManager(t)

	tm.invokeRestoreDoneHook(time.Now(), nil, "")

	content := readHookOutput(t, outputFile)

	assert.NotContains(t, content, "TM_RESTORE_DATA_BACKUP_ENGINE=")
}

func TestInvokeRestoreDoneHook_WithError(t *testing.T) {
	outputFile := setupHookDir(t)
	tm := newTestTabletManager(t)

	restoreErr := errors.New("restore failed: connection refused")
	tm.invokeRestoreDoneHook(time.Now(), restoreErr, "builtin")

	content := readHookOutput(t, outputFile)

	assert.Contains(t, content, "TM_RESTORE_DATA_BACKUP_ENGINE=builtin")
	assert.Contains(t, content, "TM_RESTORE_DATA_ERROR=restore failed: connection refused")
}

func TestInvokeRestoreDoneHook_ErrorWithoutBackupEngine(t *testing.T) {
	outputFile := setupHookDir(t)
	tm := newTestTabletManager(t)

	restoreErr := errors.New("no backup found")
	tm.invokeRestoreDoneHook(time.Now(), restoreErr, "")

	content := readHookOutput(t, outputFile)

	assert.Contains(t, content, "TM_RESTORE_DATA_ERROR=no backup found")
	assert.NotContains(t, content, "TM_RESTORE_DATA_BACKUP_ENGINE=")
}

func TestInvokeRestoreDoneHook_Timestamps(t *testing.T) {
	outputFile := setupHookDir(t)
	tm := newTestTabletManager(t)

	startTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	tm.invokeRestoreDoneHook(startTime, nil, "xtrabackup")

	content := readHookOutput(t, outputFile)

	assert.Contains(t, content, "TM_RESTORE_DATA_START_TS=2024-01-15T10:30:00Z")
	// Verify the duration is present and contains a non-zero value.
	for line := range strings.SplitSeq(content, "\n") {
		if duration, ok := strings.CutPrefix(line, "TM_RESTORE_DATA_DURATION="); ok {
			assert.NotEmpty(t, duration)
		}
	}
}

func TestStartReplicationKeepsRestoredBinlogs(t *testing.T) {
	ts := memorytopo.NewServer(t.Context(), "cell1")
	t.Cleanup(ts.Close)
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	t.Cleanup(tm.Stop)
	fake := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	pos, err := replication.DecodePosition("MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3:1-10")
	require.NoError(t, err)

	// The shard has no primary, so initializeReplication returns before reparenting.
	fake.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "FAKE RESET REPLICA ALL"}
	fake.ExpectedExecuteSuperQueryCurrent = 0
	require.NoError(t, tm.startReplication(t.Context(), pos, topodatapb.TabletType_REPLICA, true))
	assert.Equal(t, len(fake.ExpectedExecuteSuperQueryList), fake.ExpectedExecuteSuperQueryCurrent)

	// Without restored binlogs, the position is still set (RESET + gtid_purged).
	fake.SetReplicationPositionPos = pos
	fake.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "FAKE RESET REPLICA ALL", "FAKE RESET BINARY LOGS AND GTIDS", "FAKE SET GLOBAL gtid_purged"}
	fake.ExpectedExecuteSuperQueryCurrent = 0
	require.NoError(t, tm.startReplication(t.Context(), pos, topodatapb.TabletType_REPLICA, false))
	assert.Equal(t, len(fake.ExpectedExecuteSuperQueryList), fake.ExpectedExecuteSuperQueryCurrent)
}

// TestRestoreManifestAttrs checks that "Restore: got a restore manifest" logs
// the manifest's fields as attributes instead of printing the struct, leaves
// out empty positions and a nil error, and logs the error and its code when
// there is one.
func TestRestoreManifestAttrs(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	pos, err := replication.DecodePosition("MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-23")
	require.NoError(t, err)
	m := &mysqlctl.BackupManifest{
		BackupName:      "2026-11-04.150000.zone1-0000000101",
		BackupMethod:    "builtin",
		Position:        pos,
		BackupTime:      "2026-11-04T15:00:00Z",
		FinishedTime:    "2026-11-04T15:01:00Z",
		ServerUUID:      "3e11fa47-71ca-11e1-9e33-c80aa9429562",
		TabletAlias:     "zone1-0000000101",
		Hostname:        "host1",
		Keyspace:        "ks",
		Shard:           "-80",
		MySQLVersion:    "8.4.0",
		UpgradeSafe:     true,
		IncludesBinlogs: true,
		IncrementalDetails: &mysqlctl.IncrementalBackupDetails{
			FirstTimestamp: "2026-11-04T14:00:00Z",
		},
	}

	logutil.NewConsoleLogger().Info("Restore: got a restore manifest", restoreManifestAttrs(m, nil, time.Minute)...)
	r := logs.Record(t, "Restore: got a restore manifest")
	assert.Equal(t, map[string]any{
		"time":                   r["time"],
		"level":                  "INFO",
		"msg":                    "Restore: got a restore manifest",
		"backup_name":            "2026-11-04.150000.zone1-0000000101",
		"backup_engine":          "builtin",
		"position":               "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-23",
		"incremental":            false,
		"manifest_backup_time":   "2026-11-04T15:00:00Z",
		"manifest_finished_time": "2026-11-04T15:01:00Z",
		"server_uuid":            "3e11fa47-71ca-11e1-9e33-c80aa9429562",
		"backup_tablet":          "zone1-0000000101",
		"hostname":               "host1",
		"keyspace":               "ks",
		"shard":                  "-80",
		"backup_mysql_version":   "8.4.0",
		"upgrade_safe":           true,
		"includes_binlogs":       true,
		"retry_delay":            float64(time.Minute),
	}, r)

	logs.Reset()
	logutil.NewConsoleLogger().Info("Restore: got a restore manifest", restoreManifestAttrs(nil, mysqlctl.ErrNoBackup, 0)...)
	r = logs.Record(t, "Restore: got a restore manifest")
	assert.Equal(t, mysqlctl.ErrNoBackup.Error(), r["error"])
	assert.Equal(t, "UNKNOWN", r["error_code"])
	assert.NotContains(t, r, "backup_name")
}
