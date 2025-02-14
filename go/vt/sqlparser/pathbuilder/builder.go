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
	path  []byte
	sizes []int
}

const initialCap = 16

// NewASTPathBuilder creates a new ASTPathBuilder
func NewASTPathBuilder() *ASTPathBuilder {
	return &ASTPathBuilder{
		path: make([]byte, 0, initialCap),
	}
}

// ToPath returns the current path as a string of the used bytes.
func (apb *ASTPathBuilder) ToPath() string {
	return string(apb.path)
}

// AddStep appends a single step (2 bytes) to path.
func (apb *ASTPathBuilder) AddStep(step uint16) {
	apb.sizes = append(apb.sizes, len(apb.path))
	apb.path = binary.BigEndian.AppendUint16(apb.path, step)
}

// AddStepWithOffset appends a single step (2 bytes) + varint offset with value 0 to path.
func (apb *ASTPathBuilder) AddStepWithOffset(step uint16) {
	apb.sizes = append(apb.sizes, len(apb.path))
	apb.path = binary.BigEndian.AppendUint16(apb.path, step)
	apb.path = binary.AppendUvarint(apb.path, 0) // 0 offset
}

// ChangeOffset modifies the offset of the last step (which must have included an offset).
func (apb *ASTPathBuilder) ChangeOffset(newOffset int) {
	pos := apb.sizes[len(apb.sizes)-1] + 2
	apb.path = binary.AppendUvarint(apb.path[:pos], uint64(newOffset))
}

// Pop removes the last step (including offset, if any) from the path.
func (apb *ASTPathBuilder) Pop() {
	if len(apb.sizes) == 0 {
		return
	}
	n := len(apb.sizes) - 1
	apb.path = apb.path[:apb.sizes[n]]
	apb.sizes = apb.sizes[:n]
}
