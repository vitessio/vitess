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

	"github.com/stretchr/testify/require"
)

// TestDiskHealthMonitor_StallAndRecover exercises the full integration path
// of the stalled disk monitor: probe write -> timeout -> IsDiskStalled() ->
// StateManager flips the tablet to NOT_SERVING. We stall the FUSE-backed
// data directory by sending SIGUSR1 to the helper (see fuse_helper's signal
// protocol), then SIGHUP to clear and verify the tablet returns to SERVING.
func TestDiskHealthMonitor_StallAndRecover(t *testing.T) {
	require.NotNil(t, primaryTablet, "primary tablet not initialized in TestMain")
	require.NotNil(t, fuseHelperCmd, "fuse helper not initialized in TestMain")

	require.NoError(t,
		primaryTablet.VttabletProcess.WaitForTabletStatusesForTimeout(
			[]string{"SERVING"}, tabletStatusTimeout),
		"primary tablet did not reach SERVING before stall",
	)

	require.NoError(t,
		syscall.Kill(fuseHelperCmd.Process.Pid, syscall.SIGUSR1),
		"failed to send SIGUSR1 (stall) to fuse helper",
	)

	require.NoError(t,
		primaryTablet.VttabletProcess.WaitForTabletStatusesForTimeout(
			[]string{"NOT_SERVING"}, tabletStatusTimeout),
		"primary tablet did not transition to NOT_SERVING after disk stall",
	)

	require.NoError(t,
		syscall.Kill(fuseHelperCmd.Process.Pid, syscall.SIGHUP),
		"failed to send SIGHUP (clear) to fuse helper",
	)

	require.NoError(t,
		primaryTablet.VttabletProcess.WaitForTabletStatusesForTimeout(
			[]string{"SERVING"}, tabletStatusTimeout),
		"primary tablet did not recover to SERVING after disk unstall",
	)
}
