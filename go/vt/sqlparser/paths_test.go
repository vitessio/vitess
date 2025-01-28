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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestASTPathDebugString(t *testing.T) {
	p := ASTPath("")
	p = AddStep(p, RefOfSelectWhere)
	p = AddStep(p, RefOfWhereExpr)
	p = AddStep(p, RefOfBinaryExprLeft)
	p = AddStep(p, RefOfFuncExprExprs)
	expected := "(*Select).Where->(*Where).Expr->(*BinaryExpr).Left->(*FuncExpr).Exprs->(Exprs).Offset8(10)"
	assert.Equal(t, expected, p.DebugString())
}
