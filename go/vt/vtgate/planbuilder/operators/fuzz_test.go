//go:build gofuzz
// +build gofuzz

/*
Copyright 2021 The Vitess Authors.

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

package operators

import (
	"os"
	"path"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFuzzAnalyze(t *testing.T) {
	directoryName := "fuzzdata"
	files, err := os.ReadDir(directoryName)
	require.NoError(t, err)
	for _, file := range files {
		t.Run(file.Name(), func(t *testing.T) {
			defer func() {
				r := recover()
				if r != nil {
					t.Error(r)
					t.Fatal(string(debug.Stack()))
				}
			}()
			testcase, err := os.ReadFile(path.Join(directoryName, file.Name()))
			require.NoError(t, err)
			FuzzAnalyse(testcase)
		})
	}
}
