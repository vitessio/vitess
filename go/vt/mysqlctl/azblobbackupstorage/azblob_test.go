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

package azblobbackupstorage

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
)

// TestMergeCancelCancelsOnOther guards the fix that makes an Azure upload honor
// the per-file context passed to AddFile in addition to the handle abort
// context. Cancelling the "other" (per-file) context must cancel the merged
// context, even though the merged context is derived from the parent (handle)
// context.
func TestMergeCancelCancelsOnOther(t *testing.T) {
	parent := t.Context()
	other, cancelOther := context.WithCancel(t.Context())

	ctx, cleanup := mergeCancel(parent, other)
	defer cleanup()

	require.NoError(t, ctx.Err())

	cancelOther()

	assert.Eventually(t, func() bool {
		return ctx.Err() != nil
	}, 30*time.Second, 10*time.Millisecond)
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
}

// TestMergeCancelCancelsOnParent verifies the merged context still honors the
// parent (handle abort) context, which is what AbortBackup cancels.
func TestMergeCancelCancelsOnParent(t *testing.T) {
	parent, cancelParent := context.WithCancel(t.Context())
	other := t.Context()

	ctx, cleanup := mergeCancel(parent, other)
	defer cleanup()

	require.NoError(t, ctx.Err())

	cancelParent()

	assert.Eventually(t, func() bool {
		return ctx.Err() != nil
	}, 30*time.Second, 10*time.Millisecond)
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
}

// TestMergeCancelCleanupCancels verifies the cleanup function cancels the
// merged context so a completed upload doesn't leak it.
func TestMergeCancelCleanupCancels(t *testing.T) {
	ctx, cleanup := mergeCancel(t.Context(), t.Context())
	require.NoError(t, ctx.Err())

	cleanup()

	assert.ErrorIs(t, ctx.Err(), context.Canceled)
}

// TestListBackupsLogsContainerName checks that the log carries the configured
// container name, not the formatted viperutil value.
func TestListBackupsLogsContainerName(t *testing.T) {
	containerName.Set("vt-backups")
	t.Cleanup(func() { containerName.Set("") })
	t.Setenv("VT_AZBLOB_ACCOUNT_KEY", "")
	logs := logtest.Capture(t, slog.LevelInfo)

	bs := &AZBlobBackupStorage{}
	_, err := bs.ListBackups(t.Context(), "ks/0")
	require.Error(t, err)

	record := logs.Record(t, "ListBackups: [azblob]")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, "vt-backups", record["container"])
	assert.Equal(t, "ks/0/", record["search_prefix"])
}
