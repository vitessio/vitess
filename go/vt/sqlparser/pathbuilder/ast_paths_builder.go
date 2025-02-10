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

import "encoding/binary"

// ASTPathBuilder is used to build
// paths for an AST. The steps are uints.
type ASTPathBuilder struct {
	path  []byte
	sizes []int
}

// NewASTPathBuilder creates a new ASTPathBuilder.
func NewASTPathBuilder() *ASTPathBuilder {
	return &ASTPathBuilder{}
}

// ToPath converts the ASTPathBuilder to a string.
func (apb *ASTPathBuilder) ToPath() string {
	return string(apb.path)
}

// AddStep appends a single step (2 bytes) to path.
func (apb *ASTPathBuilder) AddStep(step uint16) {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, step)
	apb.path = append(apb.path, b...)
	apb.sizes = append(apb.sizes, 2)
}

// AddStepWithOffset appends a single step (2 bytes) to path
func (apb *ASTPathBuilder) AddStepWithOffset(step uint16, offset int) {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, step)
	apb.path = append(apb.path, b...)

	b = make([]byte, 8)
	bytesWritten := binary.PutVarint(b, int64(offset))
	apb.path = append(apb.path, b[:bytesWritten]...)
	apb.sizes = append(apb.sizes, 2+bytesWritten)
}

// ChangeOffset changes the offset of the last step
func (apb *ASTPathBuilder) ChangeOffset(newOffset int) {
	previousOffsetSize := apb.sizes[len(apb.sizes)-1] - 2

	b := make([]byte, 8)
	bytesWritten := binary.PutVarint(b, int64(newOffset))

	// Remove the previous offset
	apb.path = apb.path[:len(apb.path)-previousOffsetSize]
	// Add the new offset
	apb.path = append(apb.path, b[:bytesWritten]...)
	apb.sizes[len(apb.sizes)-1] = 2 + bytesWritten
}

// Pop removes the last step from the path.
func (apb *ASTPathBuilder) Pop() {
	if len(apb.sizes) == 0 {
		return
	}

	lastSize := apb.sizes[len(apb.sizes)-1]
	apb.path = apb.path[:len(apb.path)-lastSize]
	apb.sizes = apb.sizes[:len(apb.sizes)-1]
}
