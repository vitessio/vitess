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

package pathbuilder

import (
	"encoding/binary"
)

// ASTPathBuilder is used to build
// paths for an AST. The steps are uints.
type ASTPathBuilder struct {
	path    []byte
	pathLen int
	sizes   []int
}

const initialCap = 16

// NewASTPathBuilder creates a new ASTPathBuilder with an optional initial capacity.
func NewASTPathBuilder() *ASTPathBuilder {
	return &ASTPathBuilder{
		path: make([]byte, initialCap),
	}
}

func (apb *ASTPathBuilder) sanityCheck() {
	var total int
	for _, size := range apb.sizes {
		total += size
	}
	if total != apb.pathLen {
		panic("path length mismatch")
	}
}

// growIfNeeded expands the slice capacity if we don't have enough room for n more bytes.
func (apb *ASTPathBuilder) growIfNeeded(n int) {
	needed := apb.pathLen + n
	i := cap(apb.path)
	if needed <= i {
		return
	}
	newCap := cap(apb.path) * 2
	for newCap < needed {
		newCap *= 2
	}
	newPath := make([]byte, apb.pathLen, newCap)
	copy(newPath, apb.path[:apb.pathLen])
	apb.path = newPath
}

// ToPath returns the current path as a string of the used bytes.
func (apb *ASTPathBuilder) ToPath() string {
	return string(apb.path[:apb.pathLen])
}

// AddStep appends a single step (2 bytes) to path.
func (apb *ASTPathBuilder) AddStep(step uint16) {
	apb.growIfNeeded(2)
	binary.BigEndian.PutUint16(apb.path[apb.pathLen:apb.pathLen+2], step)
	apb.pathLen += 2
	apb.sizes = append(apb.sizes, 2)
	apb.sanityCheck()
}

// AddStepWithOffset appends a single step (2 bytes) + varint offset.
func (apb *ASTPathBuilder) AddStepWithOffset(step uint16, offset int) {
	// Step
	apb.growIfNeeded(2)
	binary.BigEndian.PutUint16(apb.path[apb.pathLen:apb.pathLen+2], step)
	apb.pathLen += 2

	// Offset
	var buf [8]byte
	bytesWritten := binary.PutVarint(buf[:], int64(offset))
	apb.growIfNeeded(bytesWritten)
	copy(apb.path[apb.pathLen:apb.pathLen+bytesWritten], buf[:bytesWritten])
	apb.pathLen += bytesWritten

	apb.sizes = append(apb.sizes, 2+bytesWritten)
	apb.sanityCheck()
}

// ChangeOffset modifies the offset of the last step (which must have included an offset).
func (apb *ASTPathBuilder) ChangeOffset(newOffset int) {
	lastIndex := len(apb.sizes) - 1
	oldSize := apb.sizes[lastIndex]
	offsetSize := oldSize - 2 // remove the step portion

	// Move pathLen back to overwrite the old offset
	apb.pathLen -= offsetSize

	// Encode new offset
	var buf [8]byte
	bytesWritten := binary.PutVarint(buf[:], int64(newOffset))
	apb.growIfNeeded(bytesWritten)
	copy(apb.path[apb.pathLen:apb.pathLen+bytesWritten], buf[:bytesWritten])
	apb.pathLen += bytesWritten

	// Update the size
	apb.sizes[lastIndex] = 2 + bytesWritten
	apb.sanityCheck()
}

// Pop removes the last step (including offset, if any) from the path.
func (apb *ASTPathBuilder) Pop() {
	if len(apb.sizes) == 0 {
		return
	}
	n := len(apb.sizes) - 1
	lastSize := apb.sizes[n]
	apb.pathLen -= lastSize
	apb.sizes = apb.sizes[:n]
	apb.sanityCheck()
}
