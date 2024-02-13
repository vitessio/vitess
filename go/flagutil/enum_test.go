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

package flagutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStringEnum(t *testing.T) {
	tests := []struct {
		name            string
		initialValue    string
		choices         []string
		caseInsensitive bool
		secondValue     string
		expectedErr     string
	}{
		{
			name:            "valid set call",
			initialValue:    "mango",
			choices:         []string{"apple", "mango", "kiwi"},
			caseInsensitive: true,
			secondValue:     "kiwi",
		},
		{
			name:            "invalid set call",
			initialValue:    "apple",
			choices:         []string{"apple", "mango", "kiwi"},
			caseInsensitive: false,
			secondValue:     "banana",
			expectedErr:     "invalid choice for enum (valid choices: [apple kiwi mango])",
		},
		{
			name:            "invalid set call case insensitive",
			initialValue:    "apple",
			choices:         []string{"apple", "kiwi"},
			caseInsensitive: true,
			secondValue:     "banana",
			expectedErr:     "invalid choice for enum (valid choices: [apple kiwi] [case insensitive])",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var enum *StringEnum
			if tt.caseInsensitive {
				enum = NewCaseInsensitiveStringEnum(tt.name, tt.initialValue, tt.choices)
			} else {
				enum = NewStringEnum(tt.name, tt.initialValue, tt.choices)
			}

			require.Equal(t, "string", enum.Type())
			err := enum.Set(tt.secondValue)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				require.Equal(t, tt.secondValue, enum.String())
			} else {
				require.ErrorContains(t, err, tt.expectedErr)
				require.Equal(t, tt.initialValue, enum.String())
			}
		})
	}
}
