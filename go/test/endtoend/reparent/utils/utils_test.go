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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
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

// shardedSetupFault returns a customizer that fails the first tablet of the second shard:
// its MySQL initialization, or its vttablet.
func shardedSetupFault(fault string) any {
	// Each shard has 3 tablets, so the second shard's first tablet is the 4th one started.
	n := 0
	switch fault {
	case "mysql-init":
		return func(mysqlctl *cluster.MysqlctlProcess) {
			if n++; n == 4 {
				mysqlctl.ExtraArgs = append(mysqlctl.ExtraArgs, "--injected-fault")
			}
		}
	case "vttablet":
		return func(vttablet *cluster.VttabletProcess) {
			if n++; n == 4 {
				vttablet.ExtraArgs = append(vttablet.ExtraArgs, "--injected-fault")
			}
		}
	}
	panic("unknown fault " + fault)
}

// ownedProcesses returns this user's processes that name root in their environment or arguments.
// The cluster's processes inherit VTDATAROOT, which is under root, except mysqld_safe and mysqld,
// whose arguments name their data directory. A failed observation is an error.
func ownedProcesses(root string) ([]string, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	var owned []string
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		dir := filepath.Join("/proc", entry.Name())
		info, err := os.Stat(dir)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, err
		}
		if info.Sys().(*syscall.Stat_t).Uid != uint32(os.Getuid()) {
			continue
		}
		env, err := os.ReadFile(filepath.Join(dir, "environ"))
		if errors.Is(err, fs.ErrPermission) {
			// A non-dumpable process, such as systemd's sd-pam, hides its environment. The
			// processes found by environment, mysqlctl and the Vitess binaries, are dumpable.
			env, err = nil, nil
		}
		var args []byte
		if err == nil {
			args, err = os.ReadFile(filepath.Join(dir, "cmdline"))
		}
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
			continue
		} else if err != nil {
			return nil, err
		}
		if bytes.Contains(env, []byte(root)) || bytes.Contains(args, []byte(root)) {
			owned = append(owned, entry.Name()+" "+strings.ReplaceAll(string(args), "\x00", " "))
		}
	}
	return owned, nil
}

// TestOwnedProcesses checks that ownedProcesses finds a process by its environment alone, as
// it must find mysqlctl, and by its arguments alone, as it must find mysqld.
func TestOwnedProcesses(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("reads /proc")
	}
	root, err := os.MkdirTemp("", "*.owned")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	byEnv := exec.Command("sleep", "60")
	byEnv.Env = append(os.Environ(), "VTDATAROOT="+root)
	// No shell: a shell would start sleep as a descendant that the cleanup below misses.
	byArgs := exec.Command("sleep", "60")
	byArgs.Args[0] = root
	for _, cmd := range []*exec.Cmd{byEnv, byArgs} {
		require.NoError(t, cmd.Start())
		t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	}

	owned, err := ownedProcesses(root)
	require.NoError(t, err)
	assert.Len(t, owned, 2, "owned: %v", owned)
}

// TestShardedSetupFailureCleanup fails SetupShardedReparentCluster in its second shard, in a
// child test process. The child must leave no process behind, and must remove its data
// directory on CI only.
func TestShardedSetupFailureCleanup(t *testing.T) {
	if fault := os.Getenv("SHARDED_SETUP_FAULT"); fault != "" {
		setupShardedCluster(t, policy.DurabilitySemiSync, nil, shardedSetupFault(fault))
		t.Fatal("the setup did not fail")
	}
	if runtime.GOOS != "linux" {
		t.Skip("reads /proc")
	}

	for fault, want := range map[string]string{
		"mysql-init": "unable to start mysql process",
		"vttablet":   "error starting vttablet",
	} {
		for _, ci := range []string{"true", "false"} {
			t.Run(fault+"/CI="+ci, func(t *testing.T) {
				// Not t.TempDir: MySQL socket paths must stay short. The suffix keeps one
				// case's root from being a prefix of another's.
				root, err := os.MkdirTemp(os.Getenv("VTDATAROOT"), "*.fault")
				require.NoError(t, err)
				t.Cleanup(func() { _ = os.RemoveAll(root) })
				// Whatever the child does, kill what it left before removing its files.
				t.Cleanup(func() {
					require.EventuallyWithT(t, func(c *assert.CollectT) {
						owned, err := ownedProcesses(root)
						require.NoError(c, err)
						for _, process := range owned {
							pid, _ := strconv.Atoi(strings.Fields(process)[0])
							_ = syscall.Kill(pid, syscall.SIGKILL)
						}
						assert.Empty(c, owned)
					}, 30*time.Second, 500*time.Millisecond, "could not kill the child's processes")
				})

				// The child times itself out at 90s. A case then takes at most 100s, the WaitDelay
				// and two 30s waits: 170s, which test/config.json's 15m package timeout covers four times.
				ctx, cancel := context.WithTimeout(t.Context(), 100*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestShardedSetupFailureCleanup$", "-test.v", "-test.timeout=90s")
				cmd.Env = append(os.Environ(), "SHARDED_SETUP_FAULT="+fault, "CI="+ci, "VTDATAROOT="+root)
				cmd.WaitDelay = 10 * time.Second
				out, err := cmd.CombinedOutput()
				require.Error(t, err, "the child passed:\n%s", out)
				// The setup logs each shard it starts, and why it failed.
				require.Regexp(t, "(?s)Starting shard: 40-80.*"+want, string(out), "the child did not fail in the second shard:\n%s", out)

				require.EventuallyWithT(t, func(c *assert.CollectT) {
					owned, err := ownedProcesses(root)
					require.NoError(c, err)
					assert.Empty(c, owned, "processes left running")
				}, 30*time.Second, time.Second)

				dirs, err := filepath.Glob(filepath.Join(root, "vtroot_*"))
				require.NoError(t, err)
				if ci == "true" {
					assert.Empty(t, dirs, "the data directory was not removed on CI")
				} else {
					assert.Len(t, dirs, 1, "the data directory was not kept locally")
				}
			})
		}
	}
}
