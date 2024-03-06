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

	"github.com/stretchr/testify/require"
)

func TestList(t *testing.T) {
	l := NewList[StringKey, string](5, LIST_PROBATION)
	require.Equal(t, uint(5), l.capacity)
	require.Equal(t, LIST_PROBATION, l.listType)
	for i := range 5 {
		evicted := l.PushFront(NewEntry(StringKey(fmt.Sprintf("%d", i)), "", 1))
		require.Nil(t, evicted)
	}
	require.Equal(t, 5, l.len)
	require.Equal(t, "4/3/2/1/0", l.display())
	require.Equal(t, "0/1/2/3/4", l.displayReverse())

	evicted := l.PushFront(NewEntry(StringKey("5"), "", 1))
	require.Equal(t, StringKey("0"), evicted.key)
	require.Equal(t, 5, l.len)
	require.Equal(t, "5/4/3/2/1", l.display())
	require.Equal(t, "1/2/3/4/5", l.displayReverse())

	for i := range 5 {
		entry := l.PopTail()
		require.Equal(t, StringKey(fmt.Sprintf("%d", i+1)), entry.key)
	}
	entry := l.PopTail()
	require.Nil(t, entry)

	var entries []*Entry[StringKey, string]
	for i := range 5 {
		new := NewEntry(StringKey(fmt.Sprintf("%d", i)), "", 1)
		evicted := l.PushFront(new)
		entries = append(entries, new)
		require.Nil(t, evicted)
	}
	require.Equal(t, "4/3/2/1/0", l.display())
	l.MoveToBack(entries[2])
	require.Equal(t, "4/3/1/0/2", l.display())
	require.Equal(t, "2/0/1/3/4", l.displayReverse())
	l.MoveBefore(entries[1], entries[3])
	require.Equal(t, "4/1/3/0/2", l.display())
	require.Equal(t, "2/0/3/1/4", l.displayReverse())
	l.MoveAfter(entries[2], entries[4])
	require.Equal(t, "4/2/1/3/0", l.display())
	require.Equal(t, "0/3/1/2/4", l.displayReverse())
	l.Remove(entries[1])
	require.Equal(t, "4/2/3/0", l.display())
	require.Equal(t, "0/3/2/4", l.displayReverse())

}

func TestListCountCost(t *testing.T) {
	l := NewList[StringKey, string](100, LIST_PROBATION)
	require.Equal(t, uint(100), l.capacity)
	require.Equal(t, LIST_PROBATION, l.listType)
	for i := range 5 {
		evicted := l.PushFront(NewEntry(StringKey(fmt.Sprintf("%d", i)), "", 20))
		require.Nil(t, evicted)
	}
	require.Equal(t, 100, l.len)
	require.Equal(t, 5, l.count)
	for range 3 {
		entry := l.PopTail()
		require.NotNil(t, entry)
	}
	require.Equal(t, 40, l.len)
	require.Equal(t, 2, l.count)
}
