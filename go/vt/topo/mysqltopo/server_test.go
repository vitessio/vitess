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
	"testing"
)

func TestCreateLikePattern(t *testing.T) {
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
			result := createLikePattern(tt.input)
			if result != tt.expected {
				t.Errorf("createLikePattern(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestCreateLikePatternDirectoryMatching(t *testing.T) {
	// Test the specific case that was causing the bug
	tests := []struct {
		name           string
		lockPath       string
		shouldMatch    []string
		shouldNotMatch []string
	}{
		{
			name:     "directory lock matching",
			lockPath: "/path/to/key/",
			shouldMatch: []string{
				"/path/to/key/file1",
				"/path/to/key/subdir/file2",
				"/path/to/key/a",
			},
			shouldNotMatch: []string{
				"/path/to/key",          // the directory itself
				"/path/to/keysandother", // similar prefix but not under directory
				"/path/to/key_other",    // underscore variant
				"/other/path",           // completely different
			},
		},
		{
			name:     "underscore in path",
			lockPath: "/path_with_underscore/",
			shouldMatch: []string{
				"/path_with_underscore/file1",
				"/path_with_underscore/sub/file2",
			},
			shouldNotMatch: []string{
				"/path_with_underscore",       // the directory itself
				"/pathXwithXunderscore/file",  // X instead of underscore
				"/path_with_underscoreX/file", // extra char after
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pattern := createLikePattern(tt.lockPath)

			// Test that expected matches would match the pattern
			// Note: This is a simplified test - in reality we'd need to test against actual SQL LIKE
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
