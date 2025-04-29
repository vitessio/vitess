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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSlru(t *testing.T) {
	sizes := []uint{10, 100, 1000}

	for _, size := range sizes {
		slru := NewSlru[StringKey, string](size)

		assert.Equal(t, size, slru.maxsize)
		assert.Equal(t, size, slru.probation.capacity)
		assert.Equal(t, uint(float32(size)*0.8), slru.protected.capacity)
		assert.Equal(t, 0, slru.probation.Len())
		assert.Equal(t, 0, slru.protected.Len())
		assert.Equal(t, LIST_PROBATION, slru.probation.listType)
		assert.Equal(t, LIST_PROTECTED, slru.protected.listType)
	}
}

func TestSlruInsert(t *testing.T) {
	slru := NewSlru[StringKey, string](5)

	for i := 0; i < 5; i++ {
		entry := NewEntry(StringKey(fmt.Sprintf("key%d", i)), fmt.Sprintf("value%d", i), 1)
		evicted := slru.insert(entry)
		assert.Nil(t, evicted, "No entry should be evicted when SLRU is not full")
		assert.Equal(t, LIST_PROBATION, entry.list, "Entry should be in probation list")
	}

	assert.Equal(t, 5, slru.probation.Len())
	assert.Equal(t, 0, slru.protected.Len())

	newEntry := NewEntry(StringKey("keyNew"), "valueNew", 1)
	evicted := slru.insert(newEntry)

	assert.NotNil(t, evicted)
	assert.Equal(t, StringKey("key0"), evicted.key)
	assert.Equal(t, 5, slru.probation.Len())
	assert.Equal(t, 0, slru.protected.Len())
}

func TestSlruVictim(t *testing.T) {
	slru := NewSlru[StringKey, string](5)

	victim := slru.victim()
	assert.Nil(t, victim)

	entries := make([]*Entry[StringKey, string], 5)
	for i := 0; i < 5; i++ {
		entries[i] = NewEntry(StringKey(fmt.Sprintf("key%d", i)), fmt.Sprintf("value%d", i), 1)
		slru.insert(entries[i])
	}

	victim = slru.victim()
	assert.NotNil(t, victim)
	assert.Equal(t, StringKey("key0"), victim.key)

	assert.Equal(t, 5, slru.probation.Len())
}

func TestSlruAccess(t *testing.T) {
	slru := NewSlru[StringKey, string](10)

	entries := make([]*Entry[StringKey, string], 5)
	for i := 0; i < 5; i++ {
		entries[i] = NewEntry(StringKey(fmt.Sprintf("key%d", i)), fmt.Sprintf("value%d", i), 1)
		slru.insert(entries[i])
	}

	slru.access(entries[2])

	assert.Equal(t, 4, slru.probation.Len())
	assert.Equal(t, 1, slru.protected.Len())
	assert.Equal(t, LIST_PROTECTED, entries[2].list)

	slru.access(entries[2])

	assert.Equal(t, 4, slru.probation.Len())
	assert.Equal(t, 1, slru.protected.Len())
	assert.Equal(t, LIST_PROTECTED, entries[2].list)

	protectedEntries := make([]*Entry[StringKey, string], 8)
	for i := 0; i < 8; i++ {
		protectedEntries[i] = NewEntry(StringKey(fmt.Sprintf("protected%d", i)), fmt.Sprintf("value%d", i), 1)
		slru.insert(protectedEntries[i])
		slru.access(protectedEntries[i])
	}

	assert.Equal(t, uint(float32(10)*0.8), slru.protected.capacity)
	assert.LessOrEqual(t, slru.protected.Len(), int(slru.protected.capacity))
}

func TestSlruRemove(t *testing.T) {
	slru := NewSlru[StringKey, string](5)

	entries := make([]*Entry[StringKey, string], 5)
	for i := 0; i < 5; i++ {
		entries[i] = NewEntry(StringKey(fmt.Sprintf("key%d", i)), fmt.Sprintf("value%d", i), 1)
		slru.insert(entries[i])
	}

	slru.access(entries[1])
	slru.access(entries[3])

	assert.Equal(t, 3, slru.probation.Len())
	assert.Equal(t, 2, slru.protected.Len())

	slru.remove(entries[0])
	assert.Equal(t, 2, slru.probation.Len())
	assert.Equal(t, 2, slru.protected.Len())

	slru.remove(entries[1])
	assert.Equal(t, 2, slru.probation.Len())
	assert.Equal(t, 1, slru.protected.Len())
}

func TestSlruUpdateCost(t *testing.T) {
	slru := NewSlru[StringKey, string](10)

	entries := make([]*Entry[StringKey, string], 5)
	for i := 0; i < 5; i++ {
		entries[i] = NewEntry(StringKey(fmt.Sprintf("key%d", i)), fmt.Sprintf("value%d", i), 1)
		slru.insert(entries[i])
	}

	slru.access(entries[1])
	slru.access(entries[3])

	assert.Equal(t, 3, slru.probation.Len())
	assert.Equal(t, 2, slru.protected.Len())

	slru.updateCost(entries[0], 2)
	assert.Equal(t, 5, slru.probation.Len())
	assert.Equal(t, 2, slru.protected.Len())

	slru.updateCost(entries[1], 3)
	assert.Equal(t, 5, slru.probation.Len())
	assert.Equal(t, 5, slru.protected.Len())

	slru.updateCost(entries[0], -1)
	assert.Equal(t, 4, slru.probation.Len())
	assert.Equal(t, 5, slru.protected.Len())
}

func TestSlruIntegration(t *testing.T) {
	slru := NewSlru[StringKey, string](10)

	var evicted *Entry[StringKey, string]
	entries := make([]*Entry[StringKey, string], 15)

	for i := 0; i < 10; i++ {
		entries[i] = NewEntry(StringKey(fmt.Sprintf("key%d", i)), fmt.Sprintf("value%d", i), 1)
		evicted = slru.insert(entries[i])
		assert.Nil(t, evicted)
	}

	for i := 0; i < 5; i++ {
		slru.access(entries[i])
	}

	assert.Equal(t, 5, slru.probation.Len())
	assert.Equal(t, 5, slru.protected.Len())

	for i := 10; i < 15; i++ {
		entries[i] = NewEntry(StringKey(fmt.Sprintf("key%d", i)), fmt.Sprintf("value%d", i), 1)
		evicted = slru.insert(entries[i])
		assert.NotNil(t, evicted)
		assert.Equal(t, StringKey(fmt.Sprintf("key%d", i-5)), evicted.key)
	}

	assert.Equal(t, slru.probation.Len()+slru.protected.Len(), 10)
}
