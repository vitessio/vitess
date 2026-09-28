//go:build linux

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

package diskhealthmonitor

import (
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
)

// TestDiskHealthMonitor_StallAndRecover exercises the full integration path
// of the stalled disk monitor: probe write -> timeout -> IsDiskStalled() ->
// StateManager flips the tablet to NOT_SERVING. We stall the FUSE-backed
// --disk-write-dir by sending SIGUSR1 to the helper (see fuse_helper's signal
// protocol), then SIGHUP to clear and verify the tablet returns to SERVING.
func TestDiskHealthMonitor_StallAndRecover(t *testing.T) {
	require.NotNil(t, primaryTablet, "primary tablet not initialized in TestMain")
	require.NotNil(t, fuseHelperCmd, "fuse helper not initialized in TestMain")
	assertHelperAlive(t)

	require.NoError(
		t,
		primaryTablet.VttabletProcess.WaitForTabletStatusesForTimeout(
			[]string{"SERVING"}, tabletStatusTimeout,
		),
		"primary tablet did not reach SERVING before stall",
	)

	require.NoError(
		t,
		syscall.Kill(fuseHelperCmd.Process.Pid, syscall.SIGUSR1),
		"failed to send SIGUSR1 (stall) to fuse helper",
	)

	// Pin the cause before asserting the integration outcome: NOT_SERVING is
	// the AND of several state-manager predicates, so a green test must also
	// prove IsDiskStalled() flipped (via FullStatus, which short-circuits on
	// the same signal — no FUSE I/O involved in the RPC).
	assertEventuallyDiskStalled(t, true)

	require.NoError(
		t,
		primaryTablet.VttabletProcess.WaitForTabletStatusesForTimeout(
			[]string{"NOT_SERVING"}, tabletStatusTimeout,
		),
		"primary tablet did not transition to NOT_SERVING after disk stall",
	)

	require.NoError(
		t,
		syscall.Kill(fuseHelperCmd.Process.Pid, syscall.SIGHUP),
		"failed to send SIGHUP (clear) to fuse helper",
	)

	assertEventuallyDiskStalled(t, false)

	require.NoError(
		t,
		primaryTablet.VttabletProcess.WaitForTabletStatusesForTimeout(
			[]string{"SERVING"}, tabletStatusTimeout,
		),
		"primary tablet did not recover to SERVING after disk unstall",
	)

	// Catch the wrong-reason failure mode: if the helper crashed mid-test,
	// mysqld I/O would surface EIO and the tablet might flip on its own. Fail
	// loudly so we don't trust a green test that wasn't actually testing the
	// monitor.
	assertHelperAlive(t)
}

// assertHelperAlive fails the test immediately if the fuse_helper subprocess
// has exited. helperDied is closed by the watcher in TestMain, so this is a
// non-blocking, race-free check that any number of callers can perform.
func assertHelperAlive(t *testing.T) {
	t.Helper()
	select {
	case <-helperDied:
		require.Failf(t, "fuse_helper exited unexpectedly", "wait error: %v", helperWaitErr)
	default:
	}
}

// assertEventuallyDiskStalled polls the primary tablet's FullStatus until
// DiskStalled matches want, or fails the test on timeout. RPC/parse errors
// are treated as "not yet" so the predicate keeps polling through transient
// failures during the transition.
func assertEventuallyDiskStalled(t *testing.T, want bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		out, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("GetFullStatus", primaryTablet.Alias)
		if err != nil {
			return false
		}
		status := &replicationdatapb.FullStatus{}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal([]byte(out), status); err != nil {
			return false
		}
		return status.DiskStalled == want
	}, tabletStatusTimeout, 200*time.Millisecond,
		"FullStatus.DiskStalled did not become %v within %s", want, tabletStatusTimeout)
}
