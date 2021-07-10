/*
Copyright 2020 The Vitess Authors.

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

package semantics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	_ TableSet = 1 << iota
	F1
	F2
	F3
)

func TestTableSet_IsOverlapping(t *testing.T) {
	assert.True(t, (F1 | F2).IsOverlapping(F1|F2))
	assert.True(t, F1.IsOverlapping(F1|F2))
	assert.True(t, (F1 | F2).IsOverlapping(F1))
	assert.False(t, F3.IsOverlapping(F1|F2))
	assert.False(t, (F1 | F2).IsOverlapping(F3))
}

func TestTableSet_IsSolvedBy(t *testing.T) {
	assert.True(t, F1.IsSolvedBy(F1|F2))
	assert.False(t, (F1 | F2).IsSolvedBy(F1))
	assert.False(t, F3.IsSolvedBy(F1|F2))
	assert.False(t, (F1 | F2).IsSolvedBy(F3))
}

func TestTableSet_Constituents(t *testing.T) {
	assert.Equal(t, []TableSet{F1, F2, F3}, (F1 | F2 | F3).Constituents())
	assert.Equal(t, []TableSet{F1, F2}, (F1 | F2).Constituents())
	assert.Equal(t, []TableSet{F1, F3}, (F1 | F3).Constituents())
	assert.Equal(t, []TableSet{F2, F3}, (F2 | F3).Constituents())
	assert.Empty(t, TableSet(0).Constituents())
}
