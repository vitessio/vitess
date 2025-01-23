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

	"github.com/stretchr/testify/require"
)

func TestOnLeaveFunctionality(t *testing.T) {
	ast, err := NewTestParser().Parse("select * from test_table where a = 1")
	require.NoError(t, err)

	var stack []SQLNode

	Rewrite(ast, func(cursor *Cursor) bool {
		stack = append(stack, cursor.Node())
		cursor.OnLeave(func(node SQLNode) {
			// pop the last element from the stack
			last := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			require.Equal(t, last, node)
		})
		return true
	}, nil)
}
