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

package codegen

import (
	"testing"

	"github.com/dave/jennifer/jen"
	"github.com/stretchr/testify/require"
)

func TestFormatGenFile(t *testing.T) {
	tests := []struct {
		name        string
		file        *jen.File
		expectedErr string
	}{
		{
			name:        "some-file",
			file:        jen.NewFile("some-file"),
			expectedErr: "Error 1:13: expected ';', found '-' while formatting source",
		},
		{
			name: "random",
			file: jen.NewFile("random"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := FormatJenFile(tt.file)
			if tt.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.expectedErr)
			}
		})
	}
}

func TestGoImports(t *testing.T) {
	err := GoImports("")
	require.ErrorContains(t, err, "exit status 2")
}

func TestSaveJenFile(t *testing.T) {
	tests := []struct {
		name        string
		filePath    string
		file        *jen.File
		expectedErr string
	}{
		{
			name:        "Empty file path",
			filePath:    "",
			file:        jen.NewFile("random"),
			expectedErr: "open : no such file or directory",
		},
		{
			name:     "Non empty file path",
			filePath: "random",
			file:     jen.NewFile("random"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := SaveJenFile(tt.filePath, tt.file)
			if tt.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.expectedErr)
			}
		})
	}
}
