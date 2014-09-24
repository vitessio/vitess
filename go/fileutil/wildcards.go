// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package fileutil contains utility functions related to files and paths.
package fileutil

// HasWildcard checks if a string has a wildcard in it. In the cases
// where we detect a bad pattern, we return 'true', and let the path.Match
// function find it.
func HasWildcard(path string) bool {
	for i := 0; i < len(path); i++ {
		switch path[i] {
		case '\\':
			if i+1 >= len(path) {
				return true
			} else {
				i++
			}
		case '*', '?', '[':
			return true
		}
	}
	return false
}
