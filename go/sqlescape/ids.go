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
	"bytes"
	"strings"
)

// EscapeID returns a backticked identifier given an input string.
func EscapeID(in string) string {
	var buf bytes.Buffer
	WriteEscapeID(&buf, in)
	return buf.String()
}

// WriteEscapeID writes a backticked identifier from an input string into buf.
func WriteEscapeID(buf *bytes.Buffer, in string) {
	buf.WriteByte('`')
	for _, c := range in {
		buf.WriteRune(c)
		if c == '`' {
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

	// can't be escaped if only 2 chars
	if l <= 2 {
		return in
	}

	// not escaped if the first and last chars aren't backticks
	if !(in[0] == '`' && in[l-1] == '`') {
		return in
	}

	// truncate first and last backticks
	in = in[1 : l-1]
	l -= 2

	// Quickly determine if there are any double backticks within
	// the string that need to be unescaped
	newLen := l
	for i := 0; i < l-1; i++ {
		// double backticks are collapsed to single backticks,
		// so decrement the new len, and skip the next char
		if in[i] == '`' && in[i+1] == '`' {
			newLen--
			i++
		}
	}

	// nothing changed, so we can return the string as-is
	if newLen == l {
		return in
	}

	// fill up a new string with the backticks collapsed
	var b strings.Builder
	b.Grow(newLen)

	for i := 0; i < l; i++ {
		if in[i] == '`' && in[i+1] == '`' {
			i++
		}
		b.WriteByte(in[i])
	}

	return b.String()
}
