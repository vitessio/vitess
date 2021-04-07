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
