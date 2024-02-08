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

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetDirs(t *testing.T) {
	tests := []struct {
		name        string
		currentDir  dir
		expectedErr string
	}{
		{
			name:        "Empty dir",
			currentDir:  dir{},
			expectedErr: "open : no such file or directory",
		},
		{
			name: "Non empty dir",
			currentDir: dir{
				Path: "./",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := getDirs(tt.currentDir)
			if tt.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.expectedErr)
			}
		})
	}
}

func TestExecReadMeTemplateWithDir(t *testing.T) {
	tests := []struct {
		name        string
		template    string
		currentDir  dir
		expectedErr string
	}{
		{
			name:        "Empty dir and empty template",
			currentDir:  dir{},
			template:    "",
			expectedErr: "",
		},
		{
			name: "Invaild directory path",
			currentDir: dir{
				Path: `\./`,
			},
			template:    "",
			expectedErr: `open \./README.md: no such file or directory`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := execReadMeTemplateWithDir(tt.currentDir, tt.template)
			if tt.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.expectedErr)
			}
		})
	}
}
