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

package mysqlctlclient

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

func TestRegisterFactoryTwice(t *testing.T) {
	if os.Getenv("TEST_REGISTER_FACTORY") == "1" {
		log.SwapLogger(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
		factory := func(context.Context, string, string) (MysqlctlClient, error) { return nil, nil }
		RegisterFactory("test-protocol", factory)
		RegisterFactory("test-protocol", factory)
		return
	}

	// RegisterFactory exits the process, so run it in a child process.
	cmd := exec.Command(os.Args[0], "-test.run=^TestRegisterFactoryTwice$")
	cmd.Env = append(os.Environ(), "TEST_REGISTER_FACTORY=1")
	stderr, err := cmd.Output()
	if exitErr := (*exec.ExitError)(nil); errors.As(err, &exitErr) {
		stderr = exitErr.Stderr
		assert.Equal(t, 1, exitErr.ExitCode())
	} else {
		require.Fail(t, "unexpected process result", "process ran with err %v, want exit status 1", err)
	}

	var record map[string]any
	require.NoError(t, json.Unmarshal(stderr, &record), "stderr: %s", stderr)
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "RegisterFactory already exists", record["msg"])
	assert.Equal(t, "test-protocol", record["protocol"])
}
