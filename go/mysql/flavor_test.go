/*
Copyright 2019 The Vitess Authors.

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

package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServerVersionAtLeast(t *testing.T) {
	testcases := []struct {
		version     string
		parts       []int
		expect      bool
		expectError bool
	}{
		{
			version: "8.0.14",
			parts:   []int{8, 0, 14},
			expect:  true,
		},
		{
			version: "8.0.14-log",
			parts:   []int{8, 0, 14},
			expect:  true,
		},
		{
			version: "8.0.14",
			parts:   []int{8, 0, 13},
			expect:  true,
		},
		{
			version: "8.0.14",
			parts:   []int{7, 5, 20},
			expect:  true,
		},
		{
			version: "8.0.14-log",
			parts:   []int{7, 5, 20},
			expect:  true,
		},
		{
			version: "8.0.14",
			parts:   []int{8, 1, 2},
			expect:  false,
		},
		{
			version: "8.0.14",
			parts:   []int{10, 1, 2},
			expect:  false,
		},
		{
			version: "8.0",
			parts:   []int{8, 0, 14},
			expect:  false,
		},
		{
			version:     "8.0.x",
			parts:       []int{8, 0, 14},
			expectError: true,
		},
	}
	for _, tc := range testcases {
		result, err := serverVersionAtLeast(tc.version, tc.parts...)
		if tc.expectError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tc.expect, result)
		}
	}
}
