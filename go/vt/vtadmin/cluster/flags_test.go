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

package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMergeFlagsByImpl(t *testing.T) {
	t.Parallel()

	var NilMap map[string]map[string]string

	tests := []struct {
		name     string
		base     map[string]map[string]string
		in       map[string]map[string]string
		expected map[string]map[string]string
	}{
		{
			name:     "nil",
			base:     nil,
			in:       nil,
			expected: map[string]map[string]string{},
		},
		{
			name:     "wrapped nil",
			base:     NilMap,
			in:       NilMap,
			expected: map[string]map[string]string{},
		},
		{
			name: "all overrides",
			base: nil,
			in: map[string]map[string]string{
				"consul": {
					"flag1": "value1",
				},
			},
			expected: map[string]map[string]string{
				"consul": {
					"flag1": "value1",
				},
			},
		},
		{
			name: "all defaults",
			base: map[string]map[string]string{
				"consul": {
					"flag1": "value1",
				},
			},
			in: nil,
			expected: map[string]map[string]string{
				"consul": {
					"flag1": "value1",
				},
			},
		},
		{
			name: "mixed",
			base: map[string]map[string]string{
				"consul": {
					"flag1": "value1",
					"flag2": "value2",
				},
				"other": {},
			},
			in: map[string]map[string]string{
				"consul": {
					"flag1": "othervalue",
				},
			},
			expected: map[string]map[string]string{
				"consul": {
					"flag1": "othervalue",
					"flag2": "value2",
				},
				"other": {},
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			flags := FlagsByImpl(tt.base)
			flags.Merge(tt.in)
			assert.Equal(t, FlagsByImpl(tt.expected), flags)
		})
	}
}
