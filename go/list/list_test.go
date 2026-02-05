/*
Copyright 2024 The Vitess Authors.
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

package list

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitEmptyList(t *testing.T) {
	l := New[int]()
	assert.Equal(t, 0, l.Len())
	assert.Nil(t, l.Front())
	assert.Nil(t, l.Back())
}

func TestInsertFront(t *testing.T) {
	l := New[int]()
	e := l.PushFront(1)
	assert.Equal(t, 1, l.Len())
	assert.Equal(t, e, l.Front())
	assert.Equal(t, e, l.Back())
}

func TestInsertBack(t *testing.T) {
	l := New[int]()
	e := l.PushBack(1)
	assert.Equal(t, 1, l.Len())
	assert.Equal(t, e, l.Front())
	assert.Equal(t, e, l.Back())
}

func TestInsertFrontEmptyList(t *testing.T) {
	l := New[int]()
	e := l.PushFront(1)
	assert.Equal(t, 1, l.Len())
	assert.Equal(t, e, l.Front())
	assert.Equal(t, e, l.Back())
}

func TestInsertBackEmptyList(t *testing.T) {
	l := New[int]()
	e := l.PushBack(1)
	assert.Equal(t, 1, l.Len())
	assert.Equal(t, e, l.Front())
	assert.Equal(t, e, l.Back())
}

func TestRemoveOnlyElement(t *testing.T) {
	l := New[int]()
	e := l.PushFront(1)
	l.Remove(e)
	assert.Equal(t, 0, l.Len())
	assert.Nil(t, l.Front())
	assert.Nil(t, l.Back())
}

func TestRemoveFromWrongList(t *testing.T) {
	l1 := New[int]()
	l2 := New[int]()
	e := l1.PushFront(1)
	assert.Panics(t, func() { l2.Remove(e) })
}

func TestGetFirstElement(t *testing.T) {
	l := New[int]()
	e := l.PushFront(1)
	assert.Equal(t, e, l.Front())
}

func TestGetLastElement(t *testing.T) {
	l := New[int]()
	e := l.PushBack(1)
	assert.Equal(t, e, l.Back())
}

func TestGetNextElement(t *testing.T) {
	l := New[int]()
	e := l.PushBack(1)
	assert.Nil(t, e.Next())
	f := l.PushBack(2)
	assert.Equal(t, f, e.Next())
}

func TestGetPrevElement(t *testing.T) {
	l := New[int]()
	e := l.PushBack(1)
	assert.Nil(t, e.Prev())
	f := l.PushBack(2)
	assert.Equal(t, e, f.Prev())
}

func TestMoveElement(t *testing.T) {
	l := New[int]()
	e := l.PushBack(1)
	l.move(e, e)
	assert.Equal(t, e, l.Front())
	f := l.PushBack(2)
	l.move(e, f)
	assert.Equal(t, f, l.Front())
	assert.Equal(t, e, l.Back())
	assert.Equal(t, e, f.next)
}

func TestPushBackValue(t *testing.T) {
	l := New[int]()
	m := New[int]()
	a := m.PushBack(5)
	e := l.PushBack(1)
	l.PushBackValue(a)
	assert.Equal(t, a, l.Back())
	assert.Equal(t, a, e.next)
}

func TestPushFrontValue(t *testing.T) {
	l := New[int]()
	m := New[int]()
	a := m.PushBack(5)
	e := l.PushBack(1)
	l.PushFrontValue(a)
	assert.Equal(t, a, l.Front())
	assert.Equal(t, a, e.prev)
}
