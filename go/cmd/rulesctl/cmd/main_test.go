/*
Copyright 2024 The Vitess Authors.

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

package cmd

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMainFunction(t *testing.T) {
	rootCmd := Main()
	require.NotNil(t, rootCmd)
	require.Equal(t, "rulesctl", rootCmd.Name())

	originalStdOut := os.Stdout
	defer func() {
		os.Stdout = originalStdOut
	}()
	// Redirect stdout to a buffer
	r, w, _ := os.Pipe()
	os.Stdout = w

	args := os.Args
	t.Cleanup(func() { os.Args = args })
	os.Args = []string{"rulesctl", "-f=testdata/rules.json", "list"}
	err := rootCmd.Execute()
	require.NoError(t, err)

	err = w.Close()
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)

	expected := `[
  {
    "Description": "Some value",
    "Name": "Name",
    "Action": "FAIL"
  }
]
`
	require.EqualValues(t, expected, string(got))
}
