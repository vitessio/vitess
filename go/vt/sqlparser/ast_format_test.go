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

func TestColNamesInSlice(t *testing.T) {
	c1 := NewColName("a1")
	c2 := NewColName("a2")
	exprs := []Expr{
		c1,
		c2,
	}

	result := SliceString(exprs)
	assert.Equal(t, "a1, a2", result)
}
