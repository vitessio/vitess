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

package memorytopo

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/topo"
)

// TestLeaderParticipationUnlockFailureLogs checks that a failed unlock on
// Stop logs the election path and the error as attributes.
func TestLeaderParticipationUnlockFailureLogs(t *testing.T) {
	ctx := t.Context()
	ts, f := NewServerAndFactory(ctx, "zone1")
	defer ts.Close()
	conn, err := f.Create(topo.GlobalCell, "", "")
	require.NoError(t, err)
	mp, err := conn.NewLeaderParticipation("zq7election", "id1")
	require.NoError(t, err)
	_, err = mp.WaitForLeadership()
	require.NoError(t, err)
	logs := logtest.Capture(t, slog.LevelInfo)

	conn.Close()
	mp.Stop()

	record := logs.Record(t, "failed to unlock LockDescriptor")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "elections/zq7election", record["path"])
	assert.Equal(t, ErrConnectionClosed.Error(), record["error"])
}
