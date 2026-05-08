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

package vttest

import (
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWaitTerminateAfterPrematureExit verifies that WaitTerminate returns
// promptly when the underlying process has already exited during WaitStart.
//
// Regression test: previously, a premature exit during WaitStart drained the
// exit channel but left vtp.proc non-nil, so WaitTerminate would send SIGTERM
// to a dead pid, time out after 10s, and then block forever on a second read
// from the now-empty exit channel — surfacing as a 10-minute test timeout in
// CI whenever vtcombo failed to start (e.g. transient port collisions).
func TestWaitTerminateAfterPrematureExit(t *testing.T) {
	falsePath, err := exec.LookPath("false")
	if err != nil {
		t.Skipf("`false` command not available: %v", err)
	}

	vtp := &VtProcess{
		Name:        "exits-immediately",
		Binary:      falsePath,
		BindAddress: "127.0.0.1",
		Port:        1,
	}

	startErr := vtp.WaitStart()
	require.ErrorContains(t, startErr, "exited prematurely")

	done := make(chan error, 1)
	go func() {
		done <- vtp.WaitTerminate()
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		require.Fail(t, "WaitTerminate hung after a premature exit during WaitStart")
	}
}
