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
package sqlparser

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimple(t *testing.T) {
	statement, err := Parse("select 1 + 2")
	assert.NoError(t, err)

	result := Walk(statement, func(node SQLNode) (SQLNode, bool) {

		a, ok := node.(*BinaryExpr)
		if ok {
			tmp := a.Left
			a.Left = a.Right
			a.Right = tmp
			return a, true
		}

		return node, true
	})

	expected, err := Parse("select 2 + 1")
	assert.NoError(t, err)

	buf := NewTrackedBuffer(nil)
	result.Format(buf)
	fmt.Println(buf.String())
	assert.Equal(t, expected, result)
}
