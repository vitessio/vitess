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

package cluster

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// receive returns the next value or close on ch, failing the test after 30s.
func receive(t *testing.T, ch chan error) (error, bool) {
	t.Helper()
	select {
	case err, ok := <-ch:
		return err, ok
	case <-time.After(30 * time.Second):
		require.FailNow(t, "no exit result within 30s")
		return nil, false
	}
}

// TestVttabletSetupExitWaiterKeepsItsGeneration starts a second generation while the
// first one still runs. Each generation's exit result and close must go to that
// generation's own channel, not to the one the struct holds when the process exits.
func TestVttabletSetupExitWaiterKeepsItsGeneration(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-vttablet")
	require.NoError(t, os.WriteFile(bin, []byte("#!/bin/sh\n[ -n \"$FAKE_EXIT\" ] && exit \"$FAKE_EXIT\"\nexec sleep 3600\n"), 0o755))
	vp := &VttabletProcess{Binary: bin, LogDir: dir, TabletPath: "zone1-0000000100"}

	require.NoError(t, vp.Setup())
	gen1, gen1Exit := vp.proc, vp.exit
	t.Cleanup(func() { _ = gen1.Process.Kill() })

	// The second generation exits on its own.
	t.Setenv("FAKE_EXIT", "3")
	require.NoError(t, vp.Setup())
	gen2Exit := vp.exit
	err, ok := receive(t, gen2Exit)
	require.True(t, ok)
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, 3, exitErr.ExitCode())
	_, ok = receive(t, gen2Exit)
	require.False(t, ok, "the second generation's channel is closed after its result")

	require.NoError(t, gen1.Process.Kill())
	err, ok = receive(t, gen1Exit)
	require.True(t, ok)
	require.ErrorContains(t, err, "killed")
	_, ok = receive(t, gen1Exit)
	require.False(t, ok, "the first generation's channel is closed after its result")

	// An ordinary start and stop of a third generation.
	t.Setenv("FAKE_EXIT", "")
	require.NoError(t, vp.Setup())
	require.NoError(t, vp.TearDownWithTimeout(30*time.Second))
	require.Nil(t, vp.proc)
}
