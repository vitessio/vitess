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

package log

import (
	"log/slog"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInitWithoutRegisterFlags checks that Init succeeds when the logging flags are not registered.
func TestInitWithoutRegisterFlags(t *testing.T) {
	previous := SwapLogger(nil)
	t.Cleanup(func() { SwapLogger(previous) })

	require.NoError(t, Init())
	assert.True(t, Enabled(slog.LevelInfo))
	assert.False(t, Enabled(slog.LevelDebug))
}

// TestRemovedFlagsHaveNoEffect checks that the logging flags removed without a v24 warning still parse, stay out of
// the help output, and do not stop Init.
func TestRemovedFlagsHaveNoEffect(t *testing.T) {
	previous := SwapLogger(nil)
	t.Cleanup(func() { SwapLogger(previous) })

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	RegisterFlags(fs)
	RegisterRemovedClientFlags(fs)

	err := fs.Parse([]string{
		"--log-structured",
		"--log-rotate-max-size=1024",
		"--keep-logs=1h",
		"--keep-logs-by-mtime=1h",
		"--purge-logs-interval=1h",
		"--logtostderr",
		"--alsologtostderr",
	})
	require.NoError(t, err)

	for _, name := range []string{"log-structured", "log-rotate-max-size", "keep-logs", "keep-logs-by-mtime", "purge-logs-interval", "logtostderr", "alsologtostderr"} {
		assert.NotEmpty(t, fs.Lookup(name).Deprecated, name)
	}

	require.NoError(t, Init())
}

// TestInitRejectsLogStructuredFalse checks that Init fails when --log-structured=false asks for glog log files.
func TestInitRejectsLogStructuredFalse(t *testing.T) {
	previous := SwapLogger(nil)
	t.Cleanup(func() { SwapLogger(previous) })

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	RegisterFlags(fs)
	t.Cleanup(func() { logStructured = true })

	require.NoError(t, fs.Parse([]string{"--log-structured=false"}))
	assert.ErrorContains(t, Init(), "--log-structured=false")
}
