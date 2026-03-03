//go:build !windows

/*
Copyright 2025 The Vitess Authors.

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
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerifyProcessDead_AlreadyDead(t *testing.T) {
	// Use a PID that doesn't exist.
	err := verifyProcessDead(99999999, 1*time.Second)
	assert.NoError(t, err)
}

func TestVerifyProcessDead_LiveProcess(t *testing.T) {
	cmd := exec.Command("sleep", "60")
	require.NoError(t, cmd.Start())
	defer cmd.Process.Kill()

	err := verifyProcessDead(cmd.Process.Pid, 500*time.Millisecond)
	assert.ErrorContains(t, err, "still alive")
}

func TestMysqlForceShutdown_NoPIDFile(t *testing.T) {
	t.Setenv("VTDATAROOT", t.TempDir())

	err := mysqlForceShutdown(99999999)
	assert.NoError(t, err)
}

func TestMysqlForceShutdown_InvalidPIDFile(t *testing.T) {
	t.Setenv("VTDATAROOT", t.TempDir())

	tabletUID := 88888888
	pidDir := path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d", tabletUID))
	require.NoError(t, os.MkdirAll(pidDir, 0o755))

	pidFile := path.Join(pidDir, "mysql.pid")
	require.NoError(t, os.WriteFile(pidFile, []byte("not-a-number\n"), 0o644))

	err := mysqlForceShutdown(tabletUID)
	assert.ErrorContains(t, err, "failed to parse PID")
}

func TestMysqlForceShutdown_ProcessAlreadyDead(t *testing.T) {
	t.Setenv("VTDATAROOT", t.TempDir())

	// Start a process, get its PID, kill it, then write that PID to a PID file.
	cmd := exec.Command("sleep", "60")
	require.NoError(t, cmd.Start())
	pid := cmd.Process.Pid
	require.NoError(t, cmd.Process.Kill())
	cmd.Wait()

	tabletUID := 77777777
	pidDir := path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d", tabletUID))
	require.NoError(t, os.MkdirAll(pidDir, 0o755))

	pidFile := path.Join(pidDir, "mysql.pid")
	require.NoError(t, os.WriteFile(pidFile, fmt.Appendf(nil, "%d\n", pid), 0o644))

	err := mysqlForceShutdown(tabletUID)
	assert.NoError(t, err)
}
