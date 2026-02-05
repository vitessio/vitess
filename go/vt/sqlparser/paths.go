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

package sqlparser

import (
	"fmt"
	"strings"
)

// ASTPath is stored as a string.
// Each 2 bytes => one step (big-endian).
// Some steps (e.g., referencing a slice) consume *additional* bytes for an index
type ASTPath string

// nextPathOffset is an implementation of binary.Uvarint that works directly on
// the ASTPath without having to cast and allocate a byte slice
func (path ASTPath) nextPathOffset() (uint64, int) {
	var x uint64
	var s uint
	for i := 0; i < len(path); i++ {
		b := path[i]
		if b < 0x80 {
			return x | uint64(b)<<s, i + 1
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, 0
}

func (path ASTPath) nextPathStep() ASTStep {
	_ = path[1] // bounds check hint to compiler; see golang.org/issue/14808
	return ASTStep(uint16(path[1]) | uint16(path[0])<<8)
}

func (path ASTPath) DebugString() string {
	var sb strings.Builder
	stepCount := 0

	for len(path) >= 2 {
		// If this isn't the very first step in the path, prepend a separator
		if stepCount > 0 {
			sb.WriteString("->")
		}
		stepCount++

		// Read the step code (2 bytes)
		step := path.nextPathStep()
		path = path[2:]

		// Write the step name
		stepStr := step.DebugString()
		sb.WriteString(stepStr)

		// Check suffix to see if we need to read an offset
		switch {
		case strings.HasSuffix(stepStr, "Offset"):
			if len(path) < 1 {
				sb.WriteString("(ERR-no-offset-byte)")
				return sb.String()
			}
			offset, readBytes := path.nextPathOffset()
			path = path[readBytes:]
			sb.WriteString(fmt.Sprintf("(%d)", offset))
		}
	}

	// If there's leftover data that doesn't fit into 2 (or more) bytes, you could note it:
	if len(path) != 0 {
		sb.WriteString("->(ERR-unaligned-extra-bytes)")
	}

	return sb.String()
}
