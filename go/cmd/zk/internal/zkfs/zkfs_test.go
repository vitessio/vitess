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

package zkfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/z-division/go-zookeeper/zk"
)

func TestIsFile(t *testing.T) {
	assert.True(t, IsFile("/zk/somepath"))
	assert.False(t, IsFile("/nonzk/somepath"))
	assert.False(t, IsFile("nonzkpath"))
}

func TestParsePermMode(t *testing.T) {
	assert.Equal(t, int32(0), ParsePermMode("zk"))
	assert.Equal(t, int32(zk.PermRead|zk.PermWrite), ParsePermMode("zkrw"))
	assert.Equal(t, int32(zk.PermRead|zk.PermWrite|zk.PermAdmin), ParsePermMode("zkrwa"))
	assert.PanicsWithValue(t, "invalid mode", func() {
		ParsePermMode("")
	})
	assert.PanicsWithValue(t, "invalid mode", func() {
		ParsePermMode("z")
	})
}

func TestFormatACL(t *testing.T) {
	testCases := []struct {
		name     string
		acl      zk.ACL
		expected string
	}{
		{
			name:     "Full Permissions",
			acl:      zk.ACL{Perms: zk.PermAll},
			expected: "rwdca",
		},
		{
			name:     "Read and Write Permissions",
			acl:      zk.ACL{Perms: zk.PermRead | zk.PermWrite},
			expected: "rw---",
		},
		{
			name:     "No Permissions",
			acl:      zk.ACL{Perms: 0},
			expected: "-----",
		},
		{
			name:     "Create and Admin Permissions",
			acl:      zk.ACL{Perms: zk.PermAdmin | zk.PermCreate},
			expected: "---ca",
		},
		{
			name:     "Mixed Permissions",
			acl:      zk.ACL{Perms: zk.PermRead | zk.PermDelete | zk.PermAdmin},
			expected: "r-d-a",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, FormatACL(tc.acl))
		})
	}
}
