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
			}
			i++
		case '*', '?', '[':
			return true
		}
	}
	return false
}
