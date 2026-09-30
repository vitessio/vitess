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
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterFlags(t *testing.T) {
	fs := pflag.NewFlagSet("logging", pflag.ContinueOnError)

	RegisterFlags(fs)

	require.NotNil(t, fs.Lookup("log-level"))
	require.NotNil(t, fs.Lookup("log-format"))
	assert.Nil(t, fs.Lookup("log-structured"))
	assert.Nil(t, fs.Lookup("log-rotate-max-size"))
}
