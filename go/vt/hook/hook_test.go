package hook

import (
	"context"
	"os"
	"os/exec"
	"path"
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
	require.NoError(t, os.Symlink(sleep, sleepHookPath))
	defer func() {
		require.NoError(t, os.Remove(sleepHookPath))
	}()

	h := NewHook("sleep", []string{"5"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	defer cancel()

	hr := h.ExecuteContext(ctx)
	assert.Equal(t, HOOK_TIMEOUT_ERROR, hr.ExitStatus)

	h.Parameters = []string{"0.1"}
	hr = h.Execute()
	assert.Equal(t, HOOK_SUCCESS, hr.ExitStatus)
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
