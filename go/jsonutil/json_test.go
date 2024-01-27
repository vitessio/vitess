/*
Copyright 2020 The Vitess Authors.

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

package jsonutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarshalNoEscape(t *testing.T) {
	tests := []struct {
		name        string
		obj         any
		expectedStr string
		expectedErr string
	}{
		{
			name: "valid json",
			obj: struct {
				User string
				Pass string
			}{
				User: "new-user",
				Pass: "password",
			},
			expectedStr: "{\"User\":\"new-user\",\"Pass\":\"password\"}\n",
		},
		{
			name:        "invalid json",
			obj:         func() {},
			expectedStr: "",
			expectedErr: "json: unsupported type: func()",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			json, err := MarshalNoEscape(tt.obj)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				require.Equal(t, tt.expectedStr, string(json))
			} else {
				require.ErrorContains(t, err, tt.expectedErr)
			}
		})
	}
}

func TestMarshalIndentNoEscape(t *testing.T) {
	tests := []struct {
		name        string
		obj         any
		prefix      string
		ident       string
		expectedStr string
		expectedErr string
	}{
		{
			name: "valid json",
			obj: struct {
				User string
				Pass string
			}{
				User: "new-user",
				Pass: "password",
			},
			prefix:      "test",
			ident:       "\t",
			expectedStr: "{\ntest\t\"User\": \"new-user\",\ntest\t\"Pass\": \"password\"\ntest}\n",
		},
		{
			name:        "invalid json",
			obj:         func() {},
			expectedStr: "",
			expectedErr: "json: unsupported type: func()",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			json, err := MarshalIndentNoEscape(tt.obj, tt.prefix, tt.ident)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				require.Equal(t, tt.expectedStr, string(json))
			} else {
				require.ErrorContains(t, err, tt.expectedErr)
			}
		})
	}
}
