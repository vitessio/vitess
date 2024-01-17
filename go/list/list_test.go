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
)

// Can initialize an empty list
func TestInitEmptyList(t *testing.T) {
        l := New[int]()
        if l.Len() != 0 {
                t.Errorf("Expected length of 0, got %d", l.Len())
        }
        if l.Front() != nil {
                t.Error("Expected Front() to be nil")
        }
        if l.Back() != nil {
                t.Error("Expected Back() to be nil")
        }
}

// Can insert an element at the front of the list
func TestInsertFront(t *testing.T) {
        l := New[int]()
        e := l.PushFront(1)
        if l.Len() != 1 {
                t.Errorf("Expected length of 1, got %d", l.Len())
        }
        if l.Front() != e {
                t.Error("Expected Front() to be the inserted element")
        }
        if l.Back() != e {
                t.Error("Expected Back() to be the inserted element")
        }
}

// Can insert an element at the back of the list
func TestInsertBack(t *testing.T) {
        l := New[int]()
        e := l.PushBack(1)
        if l.Len() != 1 {
                t.Errorf("Expected length of 1, got %d", l.Len())
        }
        if l.Front() != e {
                t.Error("Expected Front() to be the inserted element")
        }
        if l.Back() != e {
                t.Error("Expected Back() to be the inserted element")
        }
}

// Can insert an element at the front of an empty list
func TestInsertFrontEmptyList(t *testing.T) {
        l := New[int]()
        e := l.PushFront(1)
        if l.Len() != 1 {
                t.Errorf("Expected length of 1, got %d", l.Len())
        }
        if l.Front() != e {
                t.Error("Expected Front() to be the inserted element")
        }
        if l.Back() != e {
                t.Error("Expected Back() to be the inserted element")
        }
}

// Can insert an element at the back of an empty list
func TestInsertBackEmptyList(t *testing.T) {
        l := New[int]()
        e := l.PushBack(1)
        if l.Len() != 1 {
                t.Errorf("Expected length of 1, got %d", l.Len())
        }
        if l.Front() != e {
                t.Error("Expected Front() to be the inserted element")
        }
        if l.Back() != e {
                t.Error("Expected Back() to be the inserted element")
        }
}

// Can remove the only element from the list
func TestRemoveOnlyElement(t *testing.T) {
        l := New[int]()
        e := l.PushFront(1)
        l.Remove(e)
        if l.Len() != 0 {
                t.Errorf("Expected length of 0, got %d", l.Len())
        }
        if l.Front() != nil {
                t.Error("Expected Front() to be nil")
        }
        if l.Back() != nil {
                t.Error("Expected Back() to be nil")
        }
}

func TestGetFirstElement(t *testing.T) {
        l := New[int]()
        e := l.PushFront(1)
        if l.Front() != e {
                t.Error("Expected Front() to be the first element")
        }
}

func TestGetLastElement(t *testing.T) {
        l := New[int]()
        e := l.PushBack(1)
        if l.Back() != e {
                t.Error("Expected Back() to be the last element")
        }
}

func TestGetNextElement(t *testing.T) {
        l := New[int]()
        e := l.PushBack(1)
        if e.Next() != nil {
                t.Error("Expected Next() to be nil")
        }
        f := l.PushBack(2)
        if e.Next() != f {
                t.Error("Expected Next() to be the next element")
        }
}

func TestGetPrevElement(t *testing.T) {
        l := New[int]()
        e := l.PushBack(1)
        if e.Prev() != nil {
                t.Error("Expected Prev() to be nil")
        }
        f := l.PushBack(2)
        if f.Prev() != e {
                t.Error("Expected Prev() to be the previous element")
        }
}

func TestMoveElement(t *testing.T) {
        l := New[int]()
        e := l.PushBack(1)
        l.move(e, e)
        if l.Front() != e {
                t.Error("Expected Front() to be the first element")
        }
        f := l.PushBack(2)
        l.move(e, f)
        if l.Front() != f {
                t.Error("Expected Front() to be the second element")
        }
        if l.Back() != e {
                t.Error("Expected Back() to be the first element")
        }
        if f.next != e {
                t.Error("Expected next element to be e")
        }
}

func TestPushBackValue(t *testing.T) {
        l := New[int]()
        m := New[int]()
        a := m.PushBack(5)
        e := l.PushBack(1)
        l.PushBackValue(a)
        if l.Back() != a {
                t.Error("Expected Back() to be the last element")
        }
        if e.next != a {
                t.Error("Expected next element to be a")
        }
}

func TestPushFrontValue(t *testing.T) {
        l := New[int]()
        m := New[int]()
        a := m.PushBack(5)
        e := l.PushBack(1)
        l.PushFrontValue(a)
        if l.Front() != a {
                t.Error("Expected Front() to be the first element")
        }
        if e.prev != a {
                t.Error("Expected prev element to be a")
        }
}