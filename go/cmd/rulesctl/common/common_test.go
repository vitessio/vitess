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
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetRules(t *testing.T) {
	rules := GetRules("testdata/rules.json")
	require.NotEmpty(t, rules)
}

type testStruct struct {
	StringField  string  `yaml:"stringfield"`
	IntField     int     `yaml:"intfield"`
	BoolField    bool    `yaml:"boolfield"`
	Float64Field float64 `yaml:"float64field"`
}

var testData = testStruct{
	"tricky text to test text",
	32,
	true,
	3.141,
}

func TestMustPrintJSON(t *testing.T) {
	originalStdOut := os.Stdout
	defer func() {
		os.Stdout = originalStdOut
	}()

	// Redirect stdout to a buffer
	r, w, _ := os.Pipe()
	os.Stdout = w
	MustPrintJSON(testData)

	err := w.Close()
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, `{
  "StringField": "tricky text to test text",
  "IntField": 32,
  "BoolField": true,
  "Float64Field": 3.141
}
`, string(got))
}

func TestMustWriteJSON(t *testing.T) {
	tmpFile := path.Join(t.TempDir(), "temp.json")
	MustWriteJSON(testData, tmpFile)

	res, err := os.ReadFile(tmpFile)
	require.NoError(t, err)

	require.EqualValues(t, `{
  "StringField": "tricky text to test text",
  "IntField": 32,
  "BoolField": true,
  "Float64Field": 3.141
}`, string(res))
}
