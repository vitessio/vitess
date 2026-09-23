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

package flagutil

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sets"
)

func TestStringSetFlag(t *testing.T) {
	strSetFlag := StringSetFlag{}
	set := strSetFlag.ToSet()
	require.Empty(t, set)

	set = set.Insert("mango", "apple", "mango")
	strSetFlag.set = set

	require.Equal(t, "StringSetFlag", strSetFlag.Type())
	require.Equal(t, "apple, mango", strSetFlag.String())

	err := strSetFlag.Set("guvava")
	require.NoError(t, err)
	require.Equal(t, "apple, guvava, mango", strSetFlag.String())

	require.NotEmpty(t, strSetFlag.ToSet())
}

func TestStringSetFlagWithEmptySet(t *testing.T) {
	strSetFlag := StringSetFlag{}
	require.Empty(t, strSetFlag.String())

	err := strSetFlag.Set("tmp")
	require.NoError(t, err)
	require.Equal(t, "tmp", strSetFlag.String())

	err = strSetFlag.Set("guvava")
	require.NoError(t, err)
	require.Equal(t, "guvava, tmp", strSetFlag.String())
}

// A repeated flag must collect every occurrence, including the first one.
func TestStringSetFlagRepeatedOnCommandLine(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		expected []string
	}{
		{
			name:     "single occurrence",
			args:     []string{"--foo", "x"},
			expected: []string{"x"},
		},
		{
			name:     "two occurrences",
			args:     []string{"--foo", "x", "--foo", "y"},
			expected: []string{"x", "y"},
		},
		{
			name:     "repeated value",
			args:     []string{"--foo", "x", "--foo", "y", "--foo", "x"},
			expected: []string{"x", "y"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var strSetFlag StringSetFlag

			fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
			fs.Var(&strSetFlag, "foo", "")
			require.NoError(t, fs.Parse(tt.args))

			require.Equal(t, tt.expected, sets.List(strSetFlag.ToSet()))
		})
	}
}
