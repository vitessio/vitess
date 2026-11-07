/*
Copyright 2024 The Vitess Authors.

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

package hook

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vtenv "vitess.io/vitess/go/vt/env"
	"vitess.io/vitess/go/vt/log/logtest"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestExecuteContext(t *testing.T) {
	vtroot, err := vtenv.VtRoot()
	require.NoError(t, err)

	sleep, err := exec.LookPath("sleep")
	require.NoError(t, err)

	sleepHookPath := path.Join(vtroot, "vthook", "sleep")

	if _, err := os.Lstat(sleepHookPath); err == nil {
		require.NoError(t, os.Remove(sleepHookPath))
	}

	require.NoError(t, os.Symlink(sleep, sleepHookPath))
	defer func() {
		require.NoError(t, os.Remove(sleepHookPath))
	}()

	h := NewHook("sleep", []string{"5"})
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond*10)
	defer cancel()

	hr := h.ExecuteContext(ctx)
	assert.Equal(t, HOOK_TIMEOUT_ERROR, hr.ExitStatus)

	h.Parameters = []string{"0.1"}
	hr = h.Execute()
	assert.Equal(t, HOOK_SUCCESS, hr.ExitStatus)
}

// processStart returns the start time of process pid, or "" if there is no
// such process. A PID with a different start time is another process.
func processStart(pid int) string {
	out, err := exec.Command("ps", "-o", "lstart=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// recordProcess is the shell code that writes "<pid> <start time>" for the
// process whose PID it is given.
func recordProcess(pid string) string {
	return `echo "` + pid + ` $(ps -o lstart= -p ` + pid + `)"`
}

// recordedProcess returns the PID and start time that recordProcess wrote in
// line.
func recordedProcess(line string) (int, string, bool) {
	pid, start, ok := strings.Cut(strings.TrimSpace(line), " ")
	n, err := strconv.Atoi(pid)
	return n, strings.TrimSpace(start), ok && err == nil
}

// killChildren kills the hook children that the hook installed with marker
// recorded, and that are still running.
func killChildren(marker string) {
	data, _ := os.ReadFile(path.Join(path.Dir(marker), "pids"))
	for line := range strings.Lines(string(data)) {
		// A PID can be reused once its process ends: kill it only if it is
		// still the recorded child.
		if pid, start, ok := recordedProcess(line); ok && start != "" && processStart(pid) == start {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
}

// installHook writes a shell hook to $VTROOT/vthook, in a VTROOT of its own,
// with $MARKER and $PIDS in script replaced by a marker file to touch and a
// file to record the hook's children in, with `echo $! >> $PIDS`. Children
// that are still running are killed on cleanup.
func installHook(t *testing.T, name, script string) (marker string) {
	vtroot := t.TempDir()
	t.Setenv("VTROOT", vtroot)
	require.NoError(t, os.Mkdir(path.Join(vtroot, "vthook"), 0o700))
	dir := t.TempDir()
	marker, pids := path.Join(dir, "marker"), path.Join(dir, "pids")
	t.Cleanup(func() { killChildren(marker) })
	script = strings.ReplaceAll(script, "echo $! >> $PIDS", recordProcess("$!")+" >> $PIDS")
	script = strings.NewReplacer("$MARKER", marker, "$PIDS", pids).Replace(script)
	require.NoError(t, os.WriteFile(path.Join(vtroot, "vthook", name), []byte("#!/bin/sh\n"+script), 0o755))
	return marker
}

// TestExecuteOptionalContext runs real hook processes.
func TestExecuteOptionalContext(t *testing.T) {
	waitForFile := func(t *testing.T, file string) {
		require.Eventually(t, func() bool {
			_, err := os.Stat(file)
			return err == nil
		}, 30*time.Second, 10*time.Millisecond, "the hook did not write %s", file)
	}
	// runAsync runs the hook in the background and returns its error.
	runAsync := func(ctx context.Context, name string) chan error {
		returned := make(chan error, 1)
		go func() { returned <- NewSimpleHook(name).ExecuteOptionalContext(ctx) }()
		return returned
	}
	requireReturns := func(t *testing.T, returned chan error) (err error) {
		t.Helper()
		require.Eventually(t, func() bool {
			select {
			case err = <-returned:
				return true
			default:
				return false
			}
		}, 30*time.Second, 10*time.Millisecond, "the hook call did not return")
		return err
	}

	t.Run("canceled running hook", func(t *testing.T) {
		marker := installHook(t, "running_hook", "echo diagnostics >&2\ntouch $MARKER\nexec sleep 60\n")
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		returned := runAsync(ctx, "running_hook")
		waitForFile(t, marker)
		cancel()
		err := requireReturns(t, returned)
		require.ErrorContains(t, err, "failed(-7)")
		assert.Equal(t, vtrpcpb.Code_CANCELED, vterrors.Code(err))
		// Killing the hook closed its output, which is complete.
		require.ErrorContains(t, err, "diagnostics")
		assert.NotContains(t, err.Error(), "the output is incomplete")
	})

	t.Run("running hook past its deadline", func(t *testing.T) {
		installHook(t, "slow_hook", "exec sleep 60\n")
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		t.Cleanup(cancel)
		err := NewSimpleHook("slow_hook").ExecuteOptionalContext(ctx)
		require.ErrorContains(t, err, "failed(-7)")
		assert.Equal(t, vtrpcpb.Code_DEADLINE_EXCEEDED, vterrors.Code(err))
	})

	t.Run("canceled hook with a child holding its output", func(t *testing.T) {
		marker := installHook(t, "background_child", "sleep 60 &\necho $! >> $PIDS\ntouch $MARKER\nwait\n")
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		returned := runAsync(ctx, "background_child")
		waitForFile(t, marker)
		cancel()
		require.ErrorContains(t, requireReturns(t, returned), "failed(-7)")
	})

	t.Run("successful hook with a child holding its output", func(t *testing.T) {
		installHook(t, "exiting_parent", "sleep 60 &\necho $! >> $PIDS\nexit 0\n")
		start := time.Now()
		require.NoError(t, NewSimpleHook("exiting_parent").ExecuteOptionalContext(t.Context()))
		assert.Less(t, time.Since(start), 30*time.Second, "the hook call waited for the child")
		logs := logtest.Capture(t, slog.LevelInfo)
		hr := NewSimpleHook("exiting_parent").execute(t.Context(), hookWaitDelay)
		assert.Equal(t, HOOK_SUCCESS, hr.ExitStatus)
		assert.Contains(t, hr.Stderr, "the output is incomplete")
		incomplete := logs.Record(t, "hook: the output is incomplete: the hook's children kept it open for more than the wait delay after the hook ended")
		assert.Equal(t, "WARN", incomplete["level"])
		assert.Equal(t, "exiting_parent", incomplete["hook"])
		assert.Equal(t, float64(hookWaitDelay), incomplete["wait_delay"])
	})

	t.Run("failed hook with a child holding its output", func(t *testing.T) {
		installHook(t, "failing_parent", "sleep 60 &\necho $! >> $PIDS\necho diagnostics >&2\nexit 17\n")
		hr := NewSimpleHook("failing_parent").execute(t.Context(), hookWaitDelay)
		assert.Equal(t, 17, hr.ExitStatus)
		assert.Contains(t, hr.Stderr, "diagnostics")
		assert.Contains(t, hr.Stderr, "the output is incomplete", "a failed hook's output was cut off without a warning")
	})

	// The context ends after the hook ended, while a child holds its output:
	// the hook keeps its result.
	for _, tc := range []struct {
		name    string
		end     string
		wantErr string
	}{
		{name: "hook that exited 0", end: "exit 0"},
		{name: "hook that exited 17", end: "exit 17", wantErr: "failed(17)"},
		{name: "hook killed by a signal", end: "kill -TERM $$", wantErr: "failed(-6)"},
	} {
		t.Run(tc.name+" before the cancellation", func(t *testing.T) {
			marker := installHook(t, "exited_hook", fmt.Sprintf("sleep 60 &\necho $! >> $PIDS\n%s > $MARKER\n%s\n", recordProcess("$$"), tc.end))
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			// The output wait is long enough that only the child's exit ends it.
			returned := make(chan error, 1)
			go func() {
				h := NewSimpleHook("exited_hook")
				returned <- h.optionalError(h.execute(ctx, time.Minute))
			}()
			waitForFile(t, marker)
			require.Eventually(t, func() bool {
				data, err := os.ReadFile(marker)
				if err != nil {
					return false
				}
				pid, start, ok := recordedProcess(string(data))
				// The hook has exited and been reaped.
				return ok && start != "" && processStart(pid) != start
			}, 30*time.Second, time.Millisecond)
			select {
			case err := <-returned:
				require.FailNow(t, "the hook call returned before the cancellation", "error: %v", err)
			default:
			}
			cancel()
			killChildren(marker)
			err := requireReturns(t, returned)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantErr)
				// The hook finished on its own: its result is not a cancellation.
				assert.NotEqual(t, vtrpcpb.Code_CANCELED, vterrors.Code(err))
			}
		})
	}

	t.Run("hook killed by a signal", func(t *testing.T) {
		installHook(t, "signaled_hook", "kill -TERM $$\n")
		require.ErrorContains(t, NewSimpleHook("signaled_hook").ExecuteOptionalContext(t.Context()), "failed(-6)")
	})

	t.Run("missing hook", func(t *testing.T) {
		require.NoError(t, NewSimpleHook("missing_hook").ExecuteOptionalContext(t.Context()))
	})
}

// TestExecuteContextWaitsForChildOutput checks that ExecuteContext still waits
// for output a child of the hook writes after the hook exits.
func TestExecuteContextWaitsForChildOutput(t *testing.T) {
	installHook(t, "late_writer", "(sleep 2; echo late) &\necho $! >> $PIDS\nexit 0\n")

	hr := NewSimpleHook("late_writer").ExecuteContext(t.Context())
	assert.Equal(t, HOOK_SUCCESS, hr.ExitStatus)
	assert.Equal(t, "late\n", hr.Stdout)
	assert.Empty(t, hr.Stderr)
}

func TestExecuteOptional(t *testing.T) {
	vtroot, err := vtenv.VtRoot()
	require.NoError(t, err)

	echo, err := exec.LookPath("echo")
	require.NoError(t, err)

	echoHookPath := path.Join(vtroot, "vthook", "echo")

	if _, err := os.Lstat(echoHookPath); err == nil {
		require.NoError(t, os.Remove(echoHookPath))
	}

	require.NoError(t, os.Symlink(echo, echoHookPath))
	t.Cleanup(func() {
		require.NoError(t, os.Remove(echoHookPath))
	})
	tt := []struct {
		name          string
		hookName      string
		parameters    []string
		expectedError string
	}{
		{
			name:       "HookSuccess",
			hookName:   "echo",
			parameters: []string{"test", "args"},
		},
		{
			name:       "HookDoesNotExist",
			hookName:   "nonexistent-hook",
			parameters: []string{"test"},
		},
	}

	logs := logtest.Capture(t, slog.LevelInfo)
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHook(tc.hookName, tc.parameters)
			err := h.ExecuteOptional()
			if tc.expectedError == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.expectedError)
			}
		})
	}

	executing := logs.Record(t, "hook: executing hook")
	assert.Equal(t, "INFO", executing["level"])
	assert.Equal(t, echoHookPath, executing["path"])
	assert.Equal(t, []any{"test", "args"}, executing["args"])

	result := logs.Record(t, "hook: result")
	assert.Equal(t, "INFO", result["level"])
	assert.Equal(t, float64(HOOK_SUCCESS), result["exit_status"])
	assert.Equal(t, "test args\n", result["stdout"])
	assert.NotContains(t, result, "stderr")

	missing := logs.Record(t, "hook doesn't exist")
	assert.Equal(t, "INFO", missing["level"])
	assert.Equal(t, "nonexistent-hook", missing["hook"])

	logs.Reset()
	require.NoError(t, NewSimpleHook("echo").ExecuteOptional())
	noArgs := logs.Record(t, "hook: executing hook")
	assert.Equal(t, []any{}, noArgs["args"])
}

func TestNewHook(t *testing.T) {
	h := NewHook("test-hook", []string{"arg1", "arg2"})
	assert.Equal(t, "test-hook", h.Name)
	assert.Equal(t, []string{"arg1", "arg2"}, h.Parameters)
}

func TestNewSimpleHook(t *testing.T) {
	h := NewSimpleHook("simple-hook")
	assert.Equal(t, "simple-hook", h.Name)
	assert.Empty(t, h.Parameters)
}

func TestNewHookWithEnv(t *testing.T) {
	h := NewHookWithEnv("env-hook", []string{"arg1", "arg2"}, map[string]string{"KEY": "VALUE"})
	assert.Equal(t, "env-hook", h.Name)
	assert.Equal(t, []string{"arg1", "arg2"}, h.Parameters)
	assert.Equal(t, map[string]string{"KEY": "VALUE"}, h.ExtraEnv)
}

func TestString(t *testing.T) {
	tt := []struct {
		name     string
		input    HookResult
		expected string
	}{
		{
			name:     "HOOK_SUCCESS",
			input:    HookResult{ExitStatus: HOOK_SUCCESS, Stdout: "output"},
			expected: "result: HOOK_SUCCESS\nstdout:\noutput",
		},
		{
			name:     "HOOK_DOES_NOT_EXIST",
			input:    HookResult{ExitStatus: HOOK_DOES_NOT_EXIST},
			expected: "result: HOOK_DOES_NOT_EXIST",
		},
		{
			name:     "HOOK_STAT_FAILED",
			input:    HookResult{ExitStatus: HOOK_STAT_FAILED},
			expected: "result: HOOK_STAT_FAILED",
		},
		{
			name:     "HOOK_CANNOT_GET_EXIT_STATUS",
			input:    HookResult{ExitStatus: HOOK_CANNOT_GET_EXIT_STATUS},
			expected: "result: HOOK_CANNOT_GET_EXIT_STATUS",
		},
		{
			name:     "HOOK_INVALID_NAME",
			input:    HookResult{ExitStatus: HOOK_INVALID_NAME},
			expected: "result: HOOK_INVALID_NAME",
		},
		{
			name:     "HOOK_VTROOT_ERROR",
			input:    HookResult{ExitStatus: HOOK_VTROOT_ERROR},
			expected: "result: HOOK_VTROOT_ERROR",
		},
		{
			name:     "case default",
			input:    HookResult{ExitStatus: 42},
			expected: "result: exit(42)",
		},
		{
			name:     "WithStderr",
			input:    HookResult{ExitStatus: HOOK_SUCCESS, Stderr: "error"},
			expected: "result: HOOK_SUCCESS\nstderr:\nerror",
		},
		{
			name:     "WithStderr",
			input:    HookResult{ExitStatus: HOOK_SUCCESS, Stderr: "error"},
			expected: "result: HOOK_SUCCESS\nstderr:\nerror",
		},
		{
			name:     "WithStdoutAndStderr",
			input:    HookResult{ExitStatus: HOOK_SUCCESS, Stdout: "output", Stderr: "error"},
			expected: "result: HOOK_SUCCESS\nstdout:\noutput\nstderr:\nerror",
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.input.String()
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestExecuteAsReadPipe(t *testing.T) {
	vtroot, err := vtenv.VtRoot()
	require.NoError(t, err)

	cat, err := exec.LookPath("cat")
	require.NoError(t, err)

	catHookPath := path.Join(vtroot, "vthook", "cat")

	if _, err := os.Lstat(catHookPath); err == nil {
		require.NoError(t, os.Remove(catHookPath))
	}

	require.NoError(t, os.Symlink(cat, catHookPath))
	defer func() {
		require.NoError(t, os.Remove(catHookPath))
	}()

	h := NewHook("cat", nil)
	reader, waitFunc, status, err := h.ExecuteAsReadPipe(strings.NewReader("Hello, World!\n"))
	require.NoError(t, err)
	defer reader.(io.Closer).Close()

	output, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "Hello, World!\n", string(output))

	stderr, waitErr := waitFunc()
	assert.Empty(t, stderr)
	require.NoError(t, waitErr)
	assert.Equal(t, HOOK_SUCCESS, status)
}

func TestExecuteAsReadPipeErrorFindingHook(t *testing.T) {
	h := NewHook("nonexistent-hook", nil)
	reader, waitFunc, status, err := h.ExecuteAsReadPipe(strings.NewReader("Hello, World!\n"))
	require.Error(t, err)
	assert.Nil(t, reader)
	assert.Nil(t, waitFunc)
	assert.Equal(t, HOOK_DOES_NOT_EXIST, status)
}

func TestExecuteAsWritePipe(t *testing.T) {
	var writer strings.Builder
	var writerMutex sync.Mutex

	vtroot, err := vtenv.VtRoot()
	require.NoError(t, err)

	echo, err := exec.LookPath("echo")
	require.NoError(t, err)

	echoHookPath := path.Join(vtroot, "vthook", "echo")

	if _, err := os.Lstat(echoHookPath); err == nil {
		require.NoError(t, os.Remove(echoHookPath))
	}

	require.NoError(t, os.Symlink(echo, echoHookPath))
	defer func() {
		require.NoError(t, os.Remove(echoHookPath))
	}()

	h := NewHook("echo", nil)

	writerMutex.Lock()
	var writerTemp strings.Builder
	_, waitFunc, status, err := h.ExecuteAsWritePipe(&writerTemp)
	writerMutex.Unlock()

	require.NoError(t, err)
	defer func() {
		writerMutex.Lock()
		writer.Reset()
		writerMutex.Unlock()
	}()

	writerMutex.Lock()
	_, err = writer.WriteString("Hello, World!\n")
	writerMutex.Unlock()
	require.NoError(t, err)

	stderr, waitErr := waitFunc()
	assert.Empty(t, stderr)
	require.NoError(t, waitErr)
	assert.Equal(t, HOOK_SUCCESS, status)
}

func TestExecuteAsWritePipeErrorFindingHook(t *testing.T) {
	h := NewHook("nonexistent-hook", nil)
	var writer strings.Builder
	writerPtr := &writer
	_, _, status, err := h.ExecuteAsWritePipe(writerPtr)
	require.Error(t, err)
	assert.Equal(t, HOOK_DOES_NOT_EXIST, status)
}
