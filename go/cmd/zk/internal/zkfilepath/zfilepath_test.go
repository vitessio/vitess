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

package zkfilepath

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/z-division/go-zookeeper/zk"
)

func TestClean(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"/path/to/some/dir/", "/path/to/some/dir"},
		{"/", "/"},
		{"", "."},
		{"/root", "/root"},
		{"no/slash/at/the/end", "no/slash/at/the/end"},
	}

	for _, test := range tests {
		result := Clean(test.input)
		assert.Equal(t, test.expected, result)
	}
}

func TestFormat(t *testing.T) {
	testTime := time.Now()
	stat := &zk.Stat{
		DataLength:     100,
		NumChildren:    1,
		Mtime:          testTime.UnixMilli(),
		EphemeralOwner: 1,
	}

	tests := []struct {
		stat         *zk.Stat
		zkPath       string
		showFullPath bool
		longListing  bool
		expected     string
	}{
		// Checking the effect of showFullPath without longListing
		{stat, "/path/to/node", true, false, "/path/to/node\n"},
		{stat, "/path/to/node", false, false, "node\n"},

		// Checking the effect of showFullPath with longListing
		{stat, "/path/to/node", true, true, "drwxrwxrwx zk zk      100  " + testTime.Format(TimeFmt) + " /path/to/node\n"},
		{stat, "/path/to/node", false, true, "drwxrwxrwx zk zk      100  " + testTime.Format(TimeFmt) + " node\n"},
	}

	for _, test := range tests {
		result := Format(test.stat, test.zkPath, test.showFullPath, test.longListing)
		assert.Equal(t, test.expected, result)
	}
}

func TestGetPermissions(t *testing.T) {
	tests := []struct {
		numChildren    int32
		dataLength     int32
		ephemeralOwner int64
		expected       string
	}{
		// Children, Data, Ephemeral, Expected
		{0, 0, 0, "-rw-rw-rw-"},
		{1, 1, 0, "drwxrwxrwx"},
		{1, 0, 123, "drwxrwxrwx"},
		{0, 1, 1, "erw-rw-rw-"},
		{1, 1, 0, "drwxrwxrwx"},
		{0, 0, 1, "erw-rw-rw-"},
		{0, 0, 0, "-rw-rw-rw-"},
	}

	for _, test := range tests {
		result := getPermissions(test.numChildren, test.dataLength, test.ephemeralOwner)
		assert.Equal(t, test.expected, result)
	}
}
