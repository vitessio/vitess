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

package buffer

import (
	"log/slog"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
)

func TestVerifyFlags(t *testing.T) {
	parse := func(args []string) {
		fs := pflag.NewFlagSet("vtgate_buffer_test", pflag.ContinueOnError)
		registerFlags(fs)

		err := fs.Parse(args)
		assert.NoErrorf(t, err, "failed to parse args %v", args)
	}
	resetFlagsForTesting := func() {
		// Set all flags to their default value.
		parse([]string{})
	}

	// Verify that the non-allowed (non-trivial) flag combinations are caught.
	defer resetFlagsForTesting()

	parse([]string{"--buffer-keyspace-shards", "ks1/0"})
	require.ErrorContains(t, verifyFlags(), "also requires that")

	resetFlagsForTesting()

	parse([]string{
		"--enable-buffer",
		"--enable-buffer-dry-run",
	})
	require.ErrorContains(t, verifyFlags(), "To avoid ambiguity")

	resetFlagsForTesting()

	parse([]string{
		"--enable-buffer",
		"--buffer-keyspace-shards", "ks1//0",
	})
	require.ErrorContains(t, verifyFlags(), "invalid shard path")

	resetFlagsForTesting()

	parse([]string{
		"--enable-buffer",
		"--buffer-keyspace-shards", "ks1,ks1/0",
	})
	require.ErrorContains(t, verifyFlags(), "has overlapping entries")
}

func TestNewConfigFromFlagsLogsLimitedKeyspacesAndShards(t *testing.T) {
	parse := func(args []string) {
		fs := pflag.NewFlagSet("vtgate_buffer_test", pflag.ContinueOnError)
		registerFlags(fs)
		require.NoError(t, fs.Parse(args))
	}
	t.Cleanup(func() { parse([]string{}) })
	logs := logtest.Capture(t, slog.LevelInfo)

	parse([]string{"--enable-buffer", "--buffer-keyspace-shards", "ks2,ks1,ks3/-80"})
	NewConfigFromFlags()
	record := logs.Record(t, "Buffering limited to configured keyspaces and shards.")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, []any{"ks1", "ks2"}, record["keyspaces"])
	assert.Equal(t, []any{"ks3/-80"}, record["shards"])

	logs.Reset()
	parse([]string{"--enable-buffer", "--enable-buffer-dry-run", "--buffer-keyspace-shards", "ks3/-80"})
	NewConfigFromFlags()
	record = logs.Record(t, "Buffering limited to configured keyspaces and shards. Dry-run mode is overridden for these entries and actual buffering will take place.")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, []any{}, record["keyspaces"])
	assert.Equal(t, []any{"ks3/-80"}, record["shards"])
}
