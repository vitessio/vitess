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
	"encoding/binary"
	"fmt"
	"strings"
)

// ASTPath is stored as a string.
// Each 2 bytes => one step (big-endian).
// Some steps (e.g., referencing a slice) consume *additional* bytes for an index
type ASTPath string

// AddStep appends a single step (2 bytes) to path.
func AddStep(path ASTPath, step ASTStep) ASTPath {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(step))
	return path + ASTPath(b)
}

func AddStepWithSliceIndex(path ASTPath, step ASTStep, idx int) ASTPath {
	if idx < 255 {
		// 2 bytes for step code + 1 byte for index
		b := make([]byte, 3)
		binary.BigEndian.PutUint16(b[:2], uint16(step))
		b[2] = byte(idx)
		return path + ASTPath(b)
	}

	// 2 bytes for step code + 4 byte for index
	b := make([]byte, 6)
	longStep := step + 1
	binary.BigEndian.PutUint16(b[:2], uint16(longStep))
	binary.BigEndian.PutUint32(b[2:], uint32(idx))
	return path + ASTPath(b)
}

func (path ASTPath) DebugString() string {
	var sb strings.Builder

	remaining := []byte(path)
	stepCount := 0

	for len(remaining) >= 2 {
		// Read the step code (2 bytes)
		stepVal := binary.BigEndian.Uint16(remaining[:2])
		remaining = remaining[2:]

		step := ASTStep(stepVal)
		stepStr := step.DebugString() // e.g. "CaseExprWhens8" or "CaseExprWhens32"

		// If this isn't the very first step in the path, prepend a separator
		if stepCount > 0 {
			sb.WriteString("->")
		}
		stepCount++

		// Write the step name
		sb.WriteString(stepStr)

		// Check suffix to see if we need to read an offset
		switch {
		case strings.HasSuffix(stepStr, "Offset"):
			if len(remaining) < 1 {
				sb.WriteString("(ERR-no-offset-byte)")
				return sb.String()
			}
			offset, readBytes := binary.Varint(remaining)
			remaining = remaining[readBytes:]
			sb.WriteString(fmt.Sprintf("(%d)", offset))
		}
	}

	// If there's leftover data that doesn't fit into 2 (or more) bytes, you could note it:
	if len(remaining) != 0 {
		sb.WriteString("->(ERR-unaligned-extra-bytes)")
	}

	return sb.String()
}
