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

package binlogplayer

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

func TestRegisterClientFactoryTwiceLogsProtocol(t *testing.T) {
	if os.Getenv("TEST_BINLOGPLAYER_EXIT") == "1" {
		log.SwapLogger(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
		RegisterClientFactory("twice", nil)
		RegisterClientFactory("twice", nil)
		return
	}

	// RegisterClientFactory calls os.Exit, so run it in a subprocess.
	cmd := exec.Command(os.Args[0], "-test.run=^TestRegisterClientFactoryTwiceLogsProtocol$")
	cmd.Env = append(os.Environ(), "TEST_BINLOGPLAYER_EXIT=1")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()

	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	assert.Equal(t, 1, exitErr.ExitCode())

	lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
	var record map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &record), stderr.String())
	assert.Equal(t, "ClientFactory already exists", record["msg"])
	assert.Equal(t, "twice", record["protocol"])
}
