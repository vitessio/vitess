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

package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVtgateExtraArgsIncludesEnvironmentArgs(t *testing.T) {
	t.Setenv(vtgateExtraArgsEnv, `--mysql-server-use-streaming --mysql-server-version "8.0.45-vitess"`)

	vtgate := &VtgateProcess{
		ExtraArgs: []string{"--enable-views"},
	}
	args, err := vtgate.extraArgs()
	require.NoError(t, err)

	assert.Equal(t, []string{
		"--enable-views",
		"--mysql-server-use-streaming",
		"--mysql-server-version",
		"8.0.45-vitess",
	}, args)
}

func TestVtgateExtraArgsReportsInvalidEnvironmentArgs(t *testing.T) {
	t.Setenv(vtgateExtraArgsEnv, `"unterminated`)

	_, err := (&VtgateProcess{}).extraArgs()
	require.ErrorContains(t, err, "parse "+vtgateExtraArgsEnv)
}
