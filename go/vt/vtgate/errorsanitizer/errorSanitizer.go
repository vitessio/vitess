/*
Copyright 2022 The Vitess Authors.

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

package errorsanitizer

import (
	"regexp"
)

const (
	// Truncate errors longer than this
	maxErrorLength = 256
)

var (
	// Remove any BindVars or Sql, and anything after it
	reTruncateError = regexp.MustCompile(`[,:] BindVars:|[,:] Sql:`)

	// Keep code = foo, Duplicate entry, or syntax error, but remove anything after it
	reTruncateErrorAfter = regexp.MustCompile(`code = \S+|Duplicate entry|syntax error at position \d+`)
)

/* Error messages often have PII in them. That can come from vttablet or from mysql, so by the time it gets to vtgate,
it's an opaque string. To avoid leaking PII into, we normalize error messages before sending them out.
We remove five different sorts of strings:
*/

// * Anything after /code = \S+/
// * Anything after /Duplicate entry/
// * Anything after /syntax error at position \d+/
// * [,:] BindVars: .*/
// * [,:] Sql: '.*/'

/* We also truncate the error message at a maximum length of 256 bytes, if it's somehow longer than that after normalizing.*/

func NormalizeError(str string) string {
	if idx := reTruncateError.FindStringIndex(str); idx != nil {
		str = str[0:idx[0]]
	}
	if idx := reTruncateErrorAfter.FindStringIndex(str); idx != nil {
		str = str[0:idx[1]]
	}
	if len(str) > maxErrorLength {
		return str[0:maxErrorLength]
	}
	return str
}
