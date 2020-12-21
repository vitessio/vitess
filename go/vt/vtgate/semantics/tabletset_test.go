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

func TestTableSet(t *testing.T) {
	assert.True(t, IsOverlapping(F1|F2, F1|F2))
	assert.True(t, IsOverlapping(F1|F2, F1))
	assert.True(t, IsOverlapping(F1, F1|F2))
	assert.False(t, IsOverlapping(F1|F2, F3))
	assert.False(t, IsOverlapping(F3, F1|F2))

	assert.False(t, IsContainedBy(F1|F2, F1))
	assert.True(t, IsContainedBy(F1, F1|F2))
	assert.False(t, IsContainedBy(F1|F2, F3))
	assert.False(t, IsContainedBy(F3, F1|F2))
}
