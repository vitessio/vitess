/*
Copyright 2021 The Vitess Authors.

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

package debug

import "time"

// Debuggable is an optional interface that different vtadmin subcomponents can
// implement to provide information to debug endpoints.
type Debuggable interface {
	Debug() map[string]any
}

const sanitized = "********"

// SanitizeString returns a sanitized version of the input string. If the input
// is empty, an empty string is returned. Otherwise a constant-length string
// (irrespective of the input length) is returned.
func SanitizeString(s string) string {
	if len(s) == 0 {
		return s
	}

	return sanitized
}

// TimeToString returns a time in RFC3339-formatted string, so all debug-related
// times are in the same format.
func TimeToString(t time.Time) string { return t.Format(time.RFC3339) }
