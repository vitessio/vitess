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

package utils

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

// stopped reports whether pid has exited: it is gone, or a zombie waiting to be reaped.
// Only a confirmed state counts; a failed observation is an error.
func stopped(pid int) (bool, error) {
	if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	out, err := exec.Command("ps", "-o", "stat=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		// ps also fails when pid exited after the check above.
		if errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
			return true, nil
		}
		return false, fmt.Errorf("ps -p %d: %w", pid, err)
	}
	stat := strings.TrimSpace(string(out))
	if stat == "" {
		return false, fmt.Errorf("ps -p %d printed no state", pid)
	}
	return strings.HasPrefix(stat, "Z"), nil
}

// requireStopped waits for pid to exit, failing on a failed observation.
func requireStopped(t *testing.T, pid int, msg string) {
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ok, err := stopped(pid)
		require.NoError(c, err)
		assert.True(c, ok)
	}, 30*time.Second, 50*time.Millisecond, msg)
}

// TestStopMySQLInits starts a fake mysqlctl that starts a child, as mysqlctl starts mysqld,
// and writes no pid file. Whatever state the fake is in when stopMySQLInits runs, the fake
// must be reaped and its child must be gone.
func TestStopMySQLInits(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-mysqlctl")
	require.NoError(t, os.WriteFile(bin, []byte("#!/bin/sh\nsleep 3600 &\necho $! > \"$FAKE_CHILD_PID\"\n[ -n \"$FAKE_EXIT\" ] && exit \"$FAKE_EXIT\"\nwait\n"), 0o755))

	for _, tc := range []struct {
		name string
		// exit is the fake's exit status; empty keeps it running.
		exit string
		// reaped waits for the fake before stopMySQLInits, as setup does for each initializer
		// it reaches; otherwise an exited fake is left as a zombie.
		reaped bool
	}{
		{name: "running"},
		{name: "failed and reaped", exit: "1", reaped: true},
		{name: "failed and not reaped", exit: "1"},
		{name: "succeeded and reaped", exit: "0", reaped: true},
		{name: "succeeded and not reaped", exit: "0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			childPidFile := filepath.Join(t.TempDir(), "child.pid")
			t.Setenv("FAKE_CHILD_PID", childPidFile)
			t.Setenv("FAKE_EXIT", tc.exit)
			proc, err := (&cluster.MysqlctlProcess{Binary: bin, ProcessGroup: true}).StartProcess()
			require.NoError(t, err)
			t.Cleanup(func() { _ = syscall.Kill(-proc.Process.Pid, syscall.SIGKILL) })
			var child int
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				b, err := os.ReadFile(childPidFile)
				require.NoError(c, err)
				child, err = strconv.Atoi(strings.TrimSpace(string(b)))
				require.NoError(c, err)
			}, 30*time.Second, 50*time.Millisecond, "the fake did not start its child")
			switch {
			case tc.reaped:
				_ = proc.Wait()
			case tc.exit != "":
				requireStopped(t, proc.Process.Pid, "the fake did not exit")
			}

			stopMySQLInits([]*exec.Cmd{proc})
			require.NotNil(t, proc.ProcessState, "the fake was not reaped")
			requireStopped(t, child, "the fake's child is still running")
		})
	}
}
