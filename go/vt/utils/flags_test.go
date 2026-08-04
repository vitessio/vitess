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

package utils

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testFlagVar is a generic helper to test both flag setters for various data types.
func testFlagVar[T any](t *testing.T, name string, def T, usage string, setter func(fs *pflag.FlagSet, p *T, name string, def T, usage string)) {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	var value T
	setter(fs, &value, name, def, usage)

	flag := fs.Lookup(name)
	require.NotNilf(t, flag, "Expected flag %q to be registered", name)
	assert.Equal(t, usage, flag.Usage)
	assert.Falsef(t, flag.Hidden, "Flag %q should not be hidden", name)
}

func TestSetFlagIntVar(t *testing.T) {
	testFlagVar(t, "int-flag", 42, "an integer flag", SetFlagIntVar)
}

func TestSetFlagBoolVar(t *testing.T) {
	testFlagVar(t, "bool-flag", true, "a boolean flag", SetFlagBoolVar)
}
