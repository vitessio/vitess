/*
Copyright 2025 The Vitess Authors.

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

package mysqltopo

import (
	"context"
	"fmt"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/test"
)

func TestMatchDirectoryPattern(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple path",
			input:    "/path/to/key",
			expected: "/path/to/key%",
		},
		{
			name:     "simple path",
			input:    "/path/to/key/",
			expected: "/path/to/key/%",
		},
		{
			name:     "path with underscore",
			input:    "/path/to/key_with_underscore",
			expected: "/path/to/key\\_with\\_underscore%",
		},
		{
			name:     "path with percent",
			input:    "/path/to/key%with%percent",
			expected: "/path/to/key\\%with\\%percent%",
		},
		{
			name:     "path with both underscore and percent",
			input:    "/path/to/key_%mixed",
			expected: "/path/to/key\\_\\%mixed%",
		},
		{
			name:     "empty path",
			input:    "",
			expected: "%",
		},
		{
			name:     "root path",
			input:    "/",
			expected: "/%",
		},
		{
			name:     "path with trailing slash",
			input:    "/path/to/key/",
			expected: "/path/to/key/%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchDirectory(tt.input)
			if result != tt.expected {
				t.Errorf("matchDirectory(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestMatchDirectoryMatching(t *testing.T) {
	tests := []struct {
		name           string
		lockPath       string
		shouldMatch    []string
		shouldNotMatch []string
	}{
		{
			name:     "directory lock matching",
			lockPath: "/path/to/foo",
			shouldMatch: []string{
				"/path/to/foo/subdir/file2",
				"/path/to/foo/a",
				"/path/to/foo",
				"/path/to/foot",          // required by vitess/go/vt/topo/test/file.go:checkList()
				"/path/to/foot/otherkey", // required by vitess/go/vt/topo/test/file.go:checkList()
			},
			shouldNotMatch: []string{
				"/other/path",       // completely different
				"/path/to/fo_other", // underscore variant
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pattern := matchDirectory(tt.lockPath)
			for _, shouldMatch := range tt.shouldMatch {
				// Basic prefix check (the % at the end means it should start with the prefix)
				prefix := pattern[:len(pattern)-1] // Remove the trailing %
				if !hasPrefix(shouldMatch, prefix) {
					t.Errorf("Pattern %q should match %q, but prefix check failed", pattern, shouldMatch)
				}
			}

			// Test that non-matches don't have the right prefix
			for _, shouldNotMatch := range tt.shouldNotMatch {
				prefix := pattern[:len(pattern)-1] // Remove the trailing %
				if hasPrefix(shouldNotMatch, prefix) {
					t.Errorf("Pattern %q should NOT match %q, but prefix check passed", pattern, shouldNotMatch)
				}
			}
		})
	}
}

// hasPrefix checks if s has the given prefix, handling escaped characters
func hasPrefix(s, prefix string) bool {
	// This is a simplified version - real SQL LIKE would handle escaping properly
	// For our test purposes, we'll do a basic check
	if len(s) < len(prefix) {
		return false
	}

	// Simple character-by-character comparison, handling basic escapes
	si, pi := 0, 0
	for pi < len(prefix) && si < len(s) {
		if prefix[pi] == '\\' && pi+1 < len(prefix) {
			// Escaped character
			pi++ // Skip the backslash
			if prefix[pi] != s[si] {
				return false
			}
			pi++
			si++
		} else if prefix[pi] == s[si] {
			pi++
			si++
		} else {
			return false
		}
	}

	return pi == len(prefix)
}

func TestRelativePath(t *testing.T) {
	// Create a server instance for testing
	server := &Server{root: "/vitess/global"}

	tests := []struct {
		name        string
		filePath    string
		fullDirPath string
		expected    string
	}{
		{
			name:        "basic path extraction",
			filePath:    "/vitess/global/cells/zone1/CellInfo",
			fullDirPath: "/vitess/global/cells",
			expected:    "zone1/CellInfo",
		},
		{
			name:        "single level extraction",
			filePath:    "/vitess/global/cells/zone1",
			fullDirPath: "/vitess/global/cells",
			expected:    "zone1",
		},
		{
			name:        "exact match returns empty",
			filePath:    "/vitess/global/cells",
			fullDirPath: "/vitess/global/cells",
			expected:    "",
		},
		{
			name:        "no common prefix",
			filePath:    "/other/path/file",
			fullDirPath: "/vitess/global/cells",
			expected:    "other/path/file", // always trims slash
		},
		{
			name:        "partial prefix match",
			filePath:    "/vitess/global/cellsother/file",
			fullDirPath: "/vitess/global/cells",
			expected:    "other/file",
		},
		{
			name:        "empty file path",
			filePath:    "",
			fullDirPath: "/vitess/global/cells",
			expected:    "",
		},
		{
			name:        "empty dir path",
			filePath:    "/vitess/global/cells/zone1",
			fullDirPath: "",
			expected:    "vitess/global/cells/zone1",
		},
		{
			name:        "both empty",
			filePath:    "",
			fullDirPath: "",
			expected:    "",
		},
		{
			name:        "file path shorter than dir path",
			filePath:    "/vitess",
			fullDirPath: "/vitess/global/cells",
			expected:    "vitess", // always trims slash
		},
		{
			name:        "multiple leading slashes after trim",
			filePath:    "/vitess/global/cells//zone1/CellInfo",
			fullDirPath: "/vitess/global/cells/",
			expected:    "zone1/CellInfo",
		},
		{
			name:        "trailing slash in dir path",
			filePath:    "/vitess/global/cells/zone1/CellInfo",
			fullDirPath: "/vitess/global/cells/",
			expected:    "zone1/CellInfo",
		},
		{
			name:        "keyspace path example",
			filePath:    "/vitess/global/keyspaces/commerce/shards/0/Shard",
			fullDirPath: "/vitess/global/keyspaces/commerce/shards",
			expected:    "0/Shard",
		},
		{
			name:        "lock path example",
			filePath:    "/vitess/global/locks/keyspaces/commerce/lock",
			fullDirPath: "/vitess/global/locks/keyspaces",
			expected:    "commerce/lock",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := server.relativePath(tt.filePath, tt.fullDirPath)
			if result != tt.expected {
				t.Errorf("relativePath(%q, %q) = %q, want %q", tt.filePath, tt.fullDirPath, result, tt.expected)
			}
		})
	}
}

func TestRelativePathEdgeCases(t *testing.T) {
	server := &Server{root: "/vitess/global"}

	// Test the specific bug case that was fixed
	t.Run("bug case - leading slash removal", func(t *testing.T) {
		// This was the problematic case: after TrimPrefix, we had "/zone1/CellInfo"
		// which when split by "/" gave ["", "zone1", "CellInfo"]
		// The fix ensures we trim the leading slash to get "zone1/CellInfo"
		filePath := "/vitess/global/cells/zone1/CellInfo"
		fullDirPath := "/vitess/global/cells"
		expected := "zone1/CellInfo"

		result := server.relativePath(filePath, fullDirPath)
		if result != expected {
			t.Errorf("Bug case failed: relativePath(%q, %q) = %q, want %q", filePath, fullDirPath, result, expected)
		}

		// Verify that splitting this result by "/" gives the expected parts
		parts := strings.Split(result, "/")
		expectedParts := []string{"zone1", "CellInfo"}
		if len(parts) != len(expectedParts) {
			t.Errorf("Split result has wrong length: got %d parts %v, want %d parts %v", len(parts), parts, len(expectedParts), expectedParts)
		}
		for i, part := range parts {
			if i < len(expectedParts) && part != expectedParts[i] {
				t.Errorf("Split part %d: got %q, want %q", i, part, expectedParts[i])
			}
		}
	})

	// Test with different server roots
	t.Run("different server roots", func(t *testing.T) {
		testCases := []struct {
			serverRoot  string
			filePath    string
			fullDirPath string
			expected    string
		}{
			{
				serverRoot:  "",
				filePath:    "/cells/zone1/CellInfo",
				fullDirPath: "/cells",
				expected:    "zone1/CellInfo",
			},
			{
				serverRoot:  "/",
				filePath:    "/cells/zone1/CellInfo",
				fullDirPath: "/cells",
				expected:    "zone1/CellInfo",
			},
			{
				serverRoot:  "/custom/root",
				filePath:    "/custom/root/cells/zone1/CellInfo",
				fullDirPath: "/custom/root/cells",
				expected:    "zone1/CellInfo",
			},
		}

		for _, tc := range testCases {
			server := &Server{root: tc.serverRoot}
			result := server.relativePath(tc.filePath, tc.fullDirPath)
			if result != tc.expected {
				t.Errorf("With root %q: relativePath(%q, %q) = %q, want %q",
					tc.serverRoot, tc.filePath, tc.fullDirPath, result, tc.expected)
			}
		}
	})
}

func TestMySQLTopo(t *testing.T) {
	testIndex := 0
	newServer := func() *topo.Server {
		// Each test will use its own sub-directories and schema.
		testRoot := fmt.Sprintf("/test-%v", testIndex)
		testIndex++

		// Create a test server with a unique schema
		server, schemaName, cleanup := createTestServer(t, "")
		t.Cleanup(cleanup)

		// Create the topo.Server using the test server's connection string
		ts, err := topo.OpenServer("mysql", server.serverAddr, path.Join(testRoot, topo.GlobalCell))
		require.NoError(t, err, "OpenServer failed")

		// Create the CellInfo.
		err = ts.CreateCellInfo(context.Background(), test.LocalCellName, &topodatapb.CellInfo{
			ServerAddress: server.serverAddr,
			Root:          path.Join(testRoot, test.LocalCellName),
		})
		require.NoError(t, err, "CreateCellInfo failed")

		t.Logf("Created test server with schema: %s", schemaName)
		return ts
	}

	// Run the TopoServerTestSuite tests.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	test.TopoServerTestSuite(t, ctx, func() *topo.Server {
		return newServer()
	}, []string{})
}
