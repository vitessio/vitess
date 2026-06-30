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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/tabletmanager/disk_health_monitor/testfs"
)

// TestDiskHealthMonitor_StallAndRecover exercises the full integration path
// of the stalled disk monitor: probe write -> timeout -> IsDiskStalled() ->
// StateManager flips the tablet to NOT_SERVING. We stall the FUSE-backed
// --disk-write-dir through testfs, then clear it and verify the tablet
// returns to SERVING.
func TestDiskHealthMonitor_StallAndRecover(t *testing.T) {
	require.NotNil(t, primaryTablet, "primary tablet not initialized in TestMain")
	require.NotNil(t, testfsCmd, "testfs not initialized in TestMain")
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
		testfs.SetStalled(testfsCmd.Process.Pid),
		"failed to stall testfs",
	)
	t.Cleanup(func() {
		_ = testfs.Clear(testfsCmd.Process.Pid)
	})

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
		testfs.Clear(testfsCmd.Process.Pid),
		"failed to clear testfs",
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

func TestDiskHealthMonitor_FullAndRecover(t *testing.T) {
	require.NotNil(t, primaryTablet, "primary tablet not initialized in TestMain")
	require.NotNil(t, testfsCmd, "testfs not initialized in TestMain")
	assertHelperAlive(t)

	require.NoError(
		t,
		primaryTablet.VttabletProcess.WaitForTabletStatusesForTimeout(
			[]string{"SERVING"}, tabletStatusTimeout,
		),
		"primary tablet did not reach SERVING before disk full",
	)

	require.NoError(t, testfs.SetFull(testfsCmd.Process.Pid), "failed to set testfs full")
	t.Cleanup(func() {
		_ = testfs.Clear(testfsCmd.Process.Pid)
	})
	assertEventuallyDiskFull(t, true)

	require.NoError(
		t,
		primaryTablet.VttabletProcess.WaitForTabletStatusesForTimeout(
			[]string{"NOT_SERVING"}, tabletStatusTimeout,
		),
		"primary tablet did not transition to NOT_SERVING after disk full",
	)

	require.NoError(t, testfs.Clear(testfsCmd.Process.Pid), "failed to clear testfs")
	assertEventuallyDiskFull(t, false)

	require.NoError(
		t,
		primaryTablet.VttabletProcess.WaitForTabletStatusesForTimeout(
			[]string{"SERVING"}, tabletStatusTimeout,
		),
		"primary tablet did not recover to SERVING after disk full cleared",
	)
	assertHelperAlive(t)
}

// assertHelperAlive fails the test immediately if the testfs subprocess
// has exited. helperDied is closed by the watcher in TestMain, so this is a
// non-blocking, race-free check that any number of callers can perform.
func assertHelperAlive(t *testing.T) {
	t.Helper()
	select {
	case <-helperDied:
		require.Failf(t, "testfs exited unexpectedly", "wait error: %v", helperWaitErr)
	default:
	}
}

// assertEventuallyDiskStalled polls the primary tablet's FullStatus until
// DiskStalled matches want, or fails the test on timeout. Pins the
// mutual-exclusion invariant from both sides: a stalled disk must never be
// reported as full.
func assertEventuallyDiskStalled(t *testing.T, want bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		status := cluster.FullStatus(t, primaryTablet, clusterInstance.Hostname)
		return status.DiskStalled == want && !status.DiskFull
	}, tabletStatusTimeout, 200*time.Millisecond,
		"FullStatus.DiskStalled did not become %v within %s", want, tabletStatusTimeout)
}

func assertEventuallyDiskFull(t *testing.T, want bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		status := cluster.FullStatus(t, primaryTablet, clusterInstance.Hostname)
		return status.DiskFull == want && !status.DiskStalled
	}, tabletStatusTimeout, 200*time.Millisecond,
		"FullStatus.DiskFull did not become %v within %s", want, tabletStatusTimeout)
}
