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

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

// TestExitLogs runs vtadmin in a child process, as it exits the process.
func TestExitLogs(t *testing.T) {
	const rbacPath = "testdata/no-such-rbac-config.yaml"
	mode := os.Getenv("VTADMIN_EXIT_LOGS")
	if mode != "" {
		log.SwapLogger(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	}
	switch mode {
	case "rbac", "tracer":
		if mode == "tracer" {
			traceCloser = errCloser{}
		}
		enableDynamicClusters = true
		enableRBAC = true
		rbacConfigPath = rbacPath
		rootCmd.SetContext(t.Context())
		run(rootCmd, nil)
		return
	case "flag":
		rootCmd.SetArgs([]string{"--no-such-flag"})
		main()
		return
	}

	runExit := func(t *testing.T, mode string, msg string) map[string]any {
		cmd := exec.Command(os.Args[0], "-test.run=^TestExitLogs$")
		cmd.Env = append(os.Environ(), "VTADMIN_EXIT_LOGS="+mode)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		err := cmd.Run()
		var exitErr *exec.ExitError
		require.ErrorAs(t, err, &exitErr, "stderr:\n%s", stderr.String())
		assert.Equal(t, 1, exitErr.ExitCode())

		for line := range strings.SplitSeq(stderr.String(), "\n") {
			var record map[string]any
			if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == msg {
				return record
			}
		}
		require.FailNow(t, "record not logged", "%s\nstderr:\n%s", msg, stderr.String())
		return nil
	}

	record := runExit(t, "rbac", "failed to load RBAC config")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, rbacPath, record["path"])
	assert.Contains(t, record["error"], rbacPath)

	record = runExit(t, "flag", "command failed")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "unknown flag: --no-such-flag", record["error"])

	record = runExit(t, "tracer", "Failed to close tracer")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, errTracerClose.Error(), record["error"])
}

var errTracerClose = errors.New("tracer close failed")

type errCloser struct{}

func (errCloser) Close() error { return errTracerClose }

type countingCloser struct{ closed int }

func (c *countingCloser) Close() error {
	c.closed++
	return nil
}

func TestPostRunClosesTracer(t *testing.T) {
	old := traceCloser
	t.Cleanup(func() { traceCloser = old })
	closer := &countingCloser{}
	traceCloser = closer

	rootCmd.PostRun(rootCmd, nil)
	assert.Equal(t, 1, closer.closed)
}

func TestMainFlagRegistration(t *testing.T) {
	registerFlags()

	// Test that the TLS flags are properly registered
	t.Run("grpc tls flags are registered", func(t *testing.T) {
		certFlag := rootCmd.Flags().Lookup("vtctld-grpc-cert")
		require.NotNil(t, certFlag, "vtctld-grpc-cert flag should be registered")
		assert.Empty(t, certFlag.DefValue)
		assert.Equal(t, "the cert to use to connect", certFlag.Usage)

		keyFlag := rootCmd.Flags().Lookup("vtctld-grpc-key")
		require.NotNil(t, keyFlag, "vtctld-grpc-key flag should be registered")
		assert.Empty(t, keyFlag.DefValue)

		caFlag := rootCmd.Flags().Lookup("vtctld-grpc-ca")
		require.NotNil(t, caFlag, "vtctld-grpc-ca flag should be registered")
		assert.Empty(t, caFlag.DefValue)

		crlFlag := rootCmd.Flags().Lookup("vtctld-grpc-crl")
		require.NotNil(t, crlFlag, "vtctld-grpc-crl flag should be registered")
		assert.Empty(t, crlFlag.DefValue)

		serverNameFlag := rootCmd.Flags().Lookup("vtctld-grpc-server-name")
		require.NotNil(t, serverNameFlag, "vtctld-grpc-server-name flag should be registered")
		assert.Empty(t, serverNameFlag.DefValue)
	})
}
