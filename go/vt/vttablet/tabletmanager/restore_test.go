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
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
		BatchCtx:    context.Background(),
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
	require.NoError(t, os.MkdirAll(hookDir, 0755))

	outputFile = filepath.Join(vtroot, "hook_env_output")
	hookScript := filepath.Join(hookDir, "vttablet_restore_done")
	require.NoError(t, os.WriteFile(hookScript, []byte("#!/bin/bash\nenv > "+outputFile+"\n"), 0755))

	return outputFile
}

func waitForHookOutput(t *testing.T, outputFile string) string {
	t.Helper()
	var content string
	assert.Eventually(t, func() bool {
		data, err := os.ReadFile(outputFile)
		if err != nil {
			return false
		}
		content = string(data)
		return len(content) > 0
	}, 5*time.Second, 50*time.Millisecond)
	return content
}

func TestInvokeRestoreDoneHook_BackupEngine(t *testing.T) {
	outputFile := setupHookDir(t)
	tm := newTestTabletManager(t)

	startTime := time.Now().Add(-10 * time.Second)
	tm.invokeRestoreDoneHook(startTime, nil, "xtrabackup")

	content := waitForHookOutput(t, outputFile)

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

	content := waitForHookOutput(t, outputFile)

	assert.NotContains(t, content, "TM_RESTORE_DATA_BACKUP_ENGINE=")
}

func TestInvokeRestoreDoneHook_WithError(t *testing.T) {
	outputFile := setupHookDir(t)
	tm := newTestTabletManager(t)

	restoreErr := errors.New("restore failed: connection refused")
	tm.invokeRestoreDoneHook(time.Now(), restoreErr, "builtin")

	content := waitForHookOutput(t, outputFile)

	assert.Contains(t, content, "TM_RESTORE_DATA_BACKUP_ENGINE=builtin")
	assert.Contains(t, content, "TM_RESTORE_DATA_ERROR=restore failed: connection refused")
}

func TestInvokeRestoreDoneHook_ErrorWithoutBackupEngine(t *testing.T) {
	outputFile := setupHookDir(t)
	tm := newTestTabletManager(t)

	restoreErr := errors.New("no backup found")
	tm.invokeRestoreDoneHook(time.Now(), restoreErr, "")

	content := waitForHookOutput(t, outputFile)

	assert.Contains(t, content, "TM_RESTORE_DATA_ERROR=no backup found")
	assert.NotContains(t, content, "TM_RESTORE_DATA_BACKUP_ENGINE=")
}

func TestInvokeRestoreDoneHook_Timestamps(t *testing.T) {
	outputFile := setupHookDir(t)
	tm := newTestTabletManager(t)

	startTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	tm.invokeRestoreDoneHook(startTime, nil, "xtrabackup")

	content := waitForHookOutput(t, outputFile)

	assert.Contains(t, content, "TM_RESTORE_DATA_START_TS=2024-01-15T10:30:00Z")
	// Verify the duration is present and contains a non-zero value.
	for line := range strings.SplitSeq(content, "\n") {
		if duration, ok := strings.CutPrefix(line, "TM_RESTORE_DATA_DURATION="); ok {
			assert.NotEmpty(t, duration)
		}
	}
}
