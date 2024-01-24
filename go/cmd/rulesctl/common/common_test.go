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

package common

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetRules(t *testing.T) {
	rules := GetRules("testdata/rules.json")
	require.NotEmpty(t, rules)
}

func TestMustPrintJSON(t *testing.T) {
	jsonFile, err := os.ReadFile("testdata/rules.json")
	require.NoError(t, err)

	// Redirect stdout to a buffer
	rescueStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	MustPrintJSON(jsonFile)

	w.Close()
	got, _ := io.ReadAll(r)
	os.Stdout = rescueStdout

	want := `"WwogICAgewogICAgICAgICJEZXNjcmlwdGlvbiI6ICJTb21lIHZhbHVlIiwKICAgICAgICAiTmFtZSI6ICJOYW1lIgogICAgfQpdCg=="` + "\n"
	require.Equal(t, want, string(got))
}

func TestMustWriteJSON(t *testing.T) {
	jsonFile, err := os.ReadFile("testdata/rules.json")
	require.NoError(t, err)

	tmpFile := "testdata/temp.json"
	MustWriteJSON(jsonFile, tmpFile)

	_, err = os.ReadFile(tmpFile)
	require.NoError(t, err)

	err = os.Remove(tmpFile)
	require.NoError(t, err)
}
