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
	"strings"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestVerifyFlags(t *testing.T) {
	parse := func(args []string) {
		fs := pflag.NewFlagSet("vtgate_buffer_test", pflag.ContinueOnError)
		registerFlags(fs)

		if err := fs.Parse(args); err != nil {
			t.Errorf("failed to parse args %v: %s", args, err)
		}
	}
	resetFlagsForTesting := func() {
		// Set all flags to their default value.
		parse([]string{})
	}

	// Verify that the non-allowed (non-trivial) flag combinations are caught.
	defer resetFlagsForTesting()

	parse([]string{"--buffer-keyspace-shards", "ks1/0"})
	if err := verifyFlags(); err == nil || !strings.Contains(err.Error(), "also requires that") {
		require.NoError(t, err)
	}

	resetFlagsForTesting()

	parse([]string{
		"--enable-buffer",
		"--enable-buffer-dry-run",
	})
	if err := verifyFlags(); err == nil || !strings.Contains(err.Error(), "To avoid ambiguity") {
		require.NoError(t, err)
	}

	resetFlagsForTesting()

	parse([]string{
		"--enable-buffer",
		"--buffer-keyspace-shards", "ks1//0",
	})
	if err := verifyFlags(); err == nil || !strings.Contains(err.Error(), "invalid shard path") {
		require.NoError(t, err)
	}

	resetFlagsForTesting()

	parse([]string{
		"--enable-buffer",
		"--buffer-keyspace-shards", "ks1,ks1/0",
	})
	if err := verifyFlags(); err == nil || !strings.Contains(err.Error(), "has overlapping entries") {
		require.NoError(t, err)
	}
}
