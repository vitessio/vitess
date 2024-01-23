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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/packages"
)

func TestCheckErrors(t *testing.T) {
	tests := []struct {
		name        string
		loaded      []*packages.Package
		expectedErr string
	}{
		{
			name:        "Empty packages",
			loaded:      []*packages.Package{},
			expectedErr: "",
		},
		{
			name: "Non-empty packages",
			loaded: []*packages.Package{
				{
					Errors: []packages.Error{
						{
							Pos:  "",
							Msg:  "New error",
							Kind: 7,
						},
						{
							Pos:  "1:7",
							Msg:  "New error",
							Kind: 7,
						},
					},
				},
			},
			expectedErr: "found 2 error(s) when loading Go packages:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckErrors(tt.loaded, GeneratedInSqlparser)
			if tt.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.expectedErr)
			}
		})
	}
}

func TestGeneratedInSqlParser(t *testing.T) {
	tests := []struct {
		name           string
		fileName       string
		expectedOutput bool
	}{
		{
			name:           "Empty file name",
			fileName:       "",
			expectedOutput: false,
		},
		{
			name:           "Random file name",
			fileName:       "random",
			expectedOutput: false,
		},
		{
			name:           "ast_format_fast.go",
			fileName:       "ast_format_fast.go",
			expectedOutput: true,
		},
		{
			name:           "ast_equals.go",
			fileName:       "ast_equals.go",
			expectedOutput: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectedOutput, GeneratedInSqlparser(tt.fileName))
		})
	}
}
