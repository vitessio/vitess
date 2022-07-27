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

package sqlescape

import (
	"strings"
)

// EscapeID returns a backticked identifier given an input string.
func EscapeID(in string) string {
	var buf strings.Builder
	WriteEscapeID(&buf, in)
	return buf.String()
}

// WriteEscapeID writes a backticked identifier from an input string into buf.
func WriteEscapeID(buf *strings.Builder, in string) {
	// growing by 4 more than the length, gives us room
	// for guaranteed escaping with backticks on each end,
	// plus a small amount of room just in case there are
	// backticks within the symbol that needs to be double
	// escaped. This is an unlikely edge case.
	buf.Grow(4 + len(in))

	buf.WriteByte('`')
	for i := 0; i < len(in); i++ {
		buf.WriteByte(in[i])
		if in[i] == '`' {
			buf.WriteByte('`')
		}
	}
	buf.WriteByte('`')
}

// EscapeIDs runs sqlescape.EscapeID() for all entries in the slice.
func EscapeIDs(identifiers []string) []string {
	result := make([]string, len(identifiers))
	for i := range identifiers {
		result[i] = EscapeID(identifiers[i])
	}
	return result
}

// UnescapeID reverses any backticking in the input string.
func UnescapeID(in string) string {
	l := len(in)
	if l >= 2 && in[0] == '`' && in[l-1] == '`' {
		return in[1 : l-1]
	}
	return in
}
