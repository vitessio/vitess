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

package theine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewEntry(t *testing.T) {
	key := StringKey("test-key")
	value := "test-value"
	cost := int64(10)

	entry := NewEntry(key, value, cost)

	assert.Equal(t, key, entry.key)
	assert.Equal(t, value, entry.value)
	assert.Equal(t, cost, entry.cost.Load())
	assert.Equal(t, int32(0), entry.frequency.Load())
	assert.Equal(t, uint32(0), entry.epoch.Load())
	assert.False(t, entry.removed)
	assert.False(t, entry.deque)
	assert.False(t, entry.root)
	assert.Equal(t, uint8(0), entry.list)
}

func TestEntryLinkedListMethods(t *testing.T) {
	entry1 := NewEntry(StringKey("key1"), "value1", 1)
	entry2 := NewEntry(StringKey("key2"), "value2", 2)
	entry3 := NewEntry(StringKey("key3"), "value3", 3)

	root := &Entry[StringKey, string]{root: true}

	root.setNext(entry1)
	entry1.setPrev(root)

	entry1.setNext(entry2)
	entry2.setPrev(entry1)

	entry2.setNext(entry3)
	entry3.setPrev(entry2)

	entry3.setNext(root)
	root.setPrev(entry3)

	assert.Equal(t, root, entry1.prev())
	assert.Equal(t, entry2, entry1.next())

	assert.Equal(t, entry1, entry2.prev())
	assert.Equal(t, entry3, entry2.next())

	assert.Equal(t, entry2, entry3.prev())
	assert.Equal(t, root, entry3.next())

	assert.Nil(t, entry1.Prev())
	assert.Equal(t, entry2, entry1.Next())

	assert.Equal(t, entry1, entry2.Prev())
	assert.Equal(t, entry3, entry2.Next())

	assert.Equal(t, entry2, entry3.Prev())
	assert.Nil(t, entry3.Next())
}
