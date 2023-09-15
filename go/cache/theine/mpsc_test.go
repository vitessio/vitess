/*
Copyright 2023 The Vitess Authors.
Copyright 2023 Yiling-J

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

package theine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue_PushPop(t *testing.T) {
	q := NewQueue[int]()

	q.Push(1)
	q.Push(2)
	v, ok := q.Pop()
	assert.True(t, ok)
	assert.Equal(t, 1, v)
	v, ok = q.Pop()
	assert.True(t, ok)
	assert.Equal(t, 2, v)
	_, ok = q.Pop()
	assert.False(t, ok)
}

func TestQueue_Empty(t *testing.T) {
	q := NewQueue[int]()
	assert.True(t, q.Empty())
	q.Push(1)
	assert.False(t, q.Empty())
}
