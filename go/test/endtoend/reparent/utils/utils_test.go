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

// TestFinishMySQLInitsKillsStalledInitializer starts a fake mysqlctl that starts a child, as
// mysqlctl starts mysqld, writes no pid file and never finishes. At the deadline both the
// fake and its child must be gone.
func TestFinishMySQLInitsKillsStalledInitializer(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-mysqlctl")
	require.NoError(t, os.WriteFile(bin, []byte("#!/bin/sh\nsleep 3600 &\necho $! > \"$FAKE_CHILD_PID\"\nwait\n"), 0o755))
	childPidFile := filepath.Join(dir, "child.pid")
	t.Setenv("FAKE_CHILD_PID", childPidFile)

	proc, err := (&cluster.MysqlctlProcess{Binary: bin, ProcessGroup: true}).StartProcess()
	require.NoError(t, err)
	t.Cleanup(func() { _ = syscall.Kill(-proc.Process.Pid, syscall.SIGKILL) })
	var child int
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		b, err := os.ReadFile(childPidFile)
		require.NoError(c, err)
		child, err = strconv.Atoi(strings.TrimSpace(string(b)))
		require.NoError(c, err)
	}, 30*time.Second, 50*time.Millisecond, "the fake initializer did not start its child")

	start := time.Now()
	finishMySQLInits([]*exec.Cmd{proc}, start.Add(500*time.Millisecond))
	require.GreaterOrEqual(t, time.Since(start), 500*time.Millisecond, "returned before the deadline")
	require.NotNil(t, proc.ProcessState, "the initializer was not reaped")
	require.Eventually(t, func() bool {
		// Gone, or a zombie waiting for init to reap it.
		out, _ := exec.Command("ps", "-o", "stat=", "-p", strconv.Itoa(child)).Output()
		s := strings.TrimSpace(string(out))
		return s == "" || strings.HasPrefix(s, "Z")
	}, 30*time.Second, 50*time.Millisecond, "the initializer's child is still running")
}
