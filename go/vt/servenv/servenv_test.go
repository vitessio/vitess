/*
Copyright 2019 The Vitess Authors.

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

package servenv

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/vt/log/logtest"
)

func TestFireOnTermSyncHooksFinished(t *testing.T) {
	onTermSyncHooks = event.Hooks{}

	triggered1 := false
	OnTermSync(func() {
		triggered1 = true
	})
	triggered2 := false
	OnTermSync(func() {
		triggered2 = true
	})

	finished, want := fireOnTermSyncHooks(1*time.Second), true
	assert.Equalf(t, want, finished, "finished = %v, want %v", finished, want)
	want1 := true
	assert.Equalf(t, want1, triggered1, "triggered1 = %v, want %v", triggered1, want1)
	want2 := true
	assert.Equalf(t, want2, triggered2, "triggered1 = %v, want %v", triggered2, want2)
}

func TestFireOnTermSyncHooksTimeout(t *testing.T) {
	onTermSyncHooks = event.Hooks{}

	OnTermSync(func() {
		time.Sleep(1 * time.Second)
	})

	finished, want := fireOnTermSyncHooks(1*time.Nanosecond), false
	assert.Equalf(t, want, finished, "finished = %v, want %v", finished, want)
}

func TestFireOnCloseHooksTimeout(t *testing.T) {
	onCloseHooks = event.Hooks{}

	OnClose(func() {
		time.Sleep(1 * time.Second)
	})

	finished, want := fireOnCloseHooks(1*time.Nanosecond), false
	assert.Equalf(t, want, finished, "finished = %v, want %v", finished, want)
}

func TestMkprofileLogsUnlessQuiet(t *testing.T) {
	for _, quiet := range []bool{false, true} {
		t.Run(fmt.Sprintf("quiet=%v", quiet), func(t *testing.T) {
			logs := logtest.Capture(t, slog.LevelInfo)
			prof := &profile{mode: profileMutex, path: t.TempDir(), quiet: quiet}
			require.NoError(t, prof.mkprofile().Close())
			if quiet {
				assert.Empty(t, logs.Records(t))
				return
			}
			record := logs.Record(t, "pprof: profiling enabled")
			assert.Equal(t, "mutex", record["profile_mode"])
			assert.Equal(t, filepath.Join(prof.path, "mutex.pprof"), record["path"])
		})
	}
}

// TestParseFlagsRejectsPositionalArgs runs ParseFlags in a child process,
// since it exits.
func TestParseFlagsRejectsPositionalArgs(t *testing.T) {
	if os.Getenv("TEST_SERVENV_POSITIONAL_ARGS") == "1" {
		os.Args = []string{"vtfoo", "a", "b"}
		ParseFlags("vtfoo")
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestParseFlagsRejectsPositionalArgs$")
	cmd.Env = append(os.Environ(), "TEST_SERVENV_POSITIONAL_ARGS=1")
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "output:\n%s", out)
	assert.Equal(t, 1, exitErr.ExitCode())
	// Under test the logger writes colored text, not JSON.
	logged := regexp.MustCompile(`\x1b\[[0-9;]*m`).ReplaceAllString(string(out), "")
	assert.Regexp(t, `ERR \S+ Command doesn't take any positional arguments command=vtfoo args="\[a b\]"\n`, logged)
}
