/*
Copyright 2026 The Vitess Authors.

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

package smartconnpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testPooled struct {
	Connection
}

func TestStackPop(t *testing.T) {
	s := &connStack[testPooled]{}

	first := &Pooled[testPooled]{}
	s.Push(first)

	second := &Pooled[testPooled]{}
	s.Push(second)

	c, ok := s.Pop()
	assert.True(t, ok)

	assert.Nil(t, c.next.Load())
}

func TestStackPopAll(t *testing.T) {
	s := &connStack[testPooled]{}

	first := &Pooled[testPooled]{}
	s.Push(first)

	second := &Pooled[testPooled]{}
	s.Push(second)

	c, ok := s.PopAll()
	assert.True(t, ok)
	assert.Same(t, second, c)
	assert.Same(t, first, c.next.Load())

	c, ok = s.Pop()
	assert.False(t, ok)
	assert.Nil(t, c)
}
