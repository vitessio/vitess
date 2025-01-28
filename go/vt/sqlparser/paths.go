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

import "encoding/binary"

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

// func (p ASTPath) DebugString() string {
// }
