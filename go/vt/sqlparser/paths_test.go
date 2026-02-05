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
	"testing"

	"github.com/stretchr/testify/assert"
)

// AddStep appends a single step (2 bytes) to path.
func AddStep(path ASTPath, step ASTStep) ASTPath {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(step))
	return path + ASTPath(b)
}

func AddStepWithOffset(path ASTPath, step ASTStep, offset int) ASTPath {
	var buf [10]byte
	binary.BigEndian.PutUint16(buf[0:], uint16(step))
	n := binary.PutUvarint(buf[2:], uint64(offset))

	return path + ASTPath(buf[:2+n])
}

func TestASTPathDebugString(t *testing.T) {
	// select * from t where func(1,2,x) = 10
	// path to X
	p := ASTPath("")
	p = AddStep(p, RefOfSelectWhere)
	p = AddStep(p, RefOfWhereExpr)
	p = AddStep(p, RefOfBinaryExprLeft)
	p = AddStepWithOffset(p, RefOfFuncExprExprsOffset, 2)
	expected := "(*Select).Where->(*Where).Expr->(*BinaryExpr).Left->(*FuncExpr).ExprsOffset(2)"
	assert.Equal(t, expected, p.DebugString())
}
