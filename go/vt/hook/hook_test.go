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
	"errors"
	"fmt"
	"io"
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

// installHook writes a shell hook to $VTROOT/vthook, with $MARKER and $PIDS in
// script replaced by a marker file to touch and a file to append the PIDs of
// the hook's children to. The children are killed on cleanup.
func installHook(t *testing.T, name, script string) (marker string) {
	vtroot, err := vtenv.VtRoot()
	require.NoError(t, err)
	dir := t.TempDir()
	marker, pids := path.Join(dir, "marker"), path.Join(dir, "pids")
	t.Cleanup(func() {
		data, _ := os.ReadFile(pids)
		for field := range strings.FieldsSeq(string(data)) {
			if pid, err := strconv.Atoi(field); err == nil {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	})
	hookPath := path.Join(vtroot, "vthook", name)
	script = strings.NewReplacer("$MARKER", marker, "$PIDS", pids).Replace(script)
	require.NoError(t, os.WriteFile(hookPath, []byte("#!/bin/sh\n"+script), 0o755))
	t.Cleanup(func() { require.NoError(t, os.Remove(hookPath)) })
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
	requireReturns := func(t *testing.T, returned chan error) error {
		t.Helper()
		select {
		case err := <-returned:
			return err
		case <-time.After(30 * time.Second):
			require.FailNow(t, "the hook call did not return")
			return nil
		}
	}

	t.Run("canceled running hook", func(t *testing.T) {
		marker := installHook(t, "running_hook", "touch $MARKER\nexec sleep 60\n")
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		returned := runAsync(ctx, "running_hook")
		waitForFile(t, marker)
		cancel()
		require.ErrorContains(t, requireReturns(t, returned), "failed(-7)")
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
		hr := NewSimpleHook("exiting_parent").execute(t.Context(), hookWaitDelay)
		assert.Equal(t, HOOK_SUCCESS, hr.ExitStatus)
		assert.Contains(t, hr.Stderr, "the output may be incomplete")
	})

	// The context ends after the hook exited, while a child holds its output.
	for _, tc := range []struct {
		exitCode int
		wantErr  string
	}{
		{exitCode: 0},
		{exitCode: 17, wantErr: "failed(17)"},
	} {
		t.Run(fmt.Sprintf("hook that exited %d before the cancellation", tc.exitCode), func(t *testing.T) {
			marker := installHook(t, "exited_hook", fmt.Sprintf("sleep 60 &\necho $! >> $PIDS\necho $$ > $MARKER\nexit %d\n", tc.exitCode))
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			returned := runAsync(ctx, "exited_hook")
			waitForFile(t, marker)
			require.Eventually(t, func() bool {
				data, err := os.ReadFile(marker)
				if err != nil {
					return false
				}
				pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
				// The hook has exited and been reaped.
				return err == nil && errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
			}, 30*time.Second, time.Millisecond)
			select {
			case err := <-returned:
				require.FailNow(t, "the hook call returned before the cancellation", "error: %v", err)
			default:
			}
			cancel()
			err := requireReturns(t, returned)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantErr)
			}
		})
	}

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
	defer func() {
		require.NoError(t, os.Remove(echoHookPath))
	}()
	tt := []struct {
		name          string
		hookName      string
		parameters    []string
		expectedError string
	}{
		{
			name:       "HookSuccess",
			hookName:   "echo",
			parameters: []string{"test"},
		},
		{
			name:       "HookDoesNotExist",
			hookName:   "nonexistent-hook",
			parameters: []string{"test"},
		},
	}

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
