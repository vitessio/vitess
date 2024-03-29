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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTlfu(t *testing.T) {
	tlfu := NewTinyLfu[StringKey, string](1000)
	require.Equal(t, uint(1000), tlfu.slru.probation.capacity)
	require.Equal(t, uint(800), tlfu.slru.protected.capacity)
	require.Equal(t, 0, tlfu.slru.probation.len)
	require.Equal(t, 0, tlfu.slru.protected.len)

	var entries []*Entry[StringKey, string]
	for i := range 200 {
		e := NewEntry(StringKey(fmt.Sprintf("%d", i)), "", 1)
		evicted := tlfu.Set(e)
		entries = append(entries, e)
		require.Nil(t, evicted)
	}

	require.Equal(t, 200, tlfu.slru.probation.len)
	require.Equal(t, 0, tlfu.slru.protected.len)

	// probation -> protected
	tlfu.Access(ReadBufItem[StringKey, string]{entry: entries[11]})
	require.Equal(t, 199, tlfu.slru.probation.len)
	require.Equal(t, 1, tlfu.slru.protected.len)
	tlfu.Access(ReadBufItem[StringKey, string]{entry: entries[11]})
	require.Equal(t, 199, tlfu.slru.probation.len)
	require.Equal(t, 1, tlfu.slru.protected.len)

	for i := 200; i < 1000; i++ {
		e := NewEntry(StringKey(fmt.Sprintf("%d", i)), "", 1)
		entries = append(entries, e)
		evicted := tlfu.Set(e)
		require.Nil(t, evicted)
	}
	// access protected
	tlfu.Access(ReadBufItem[StringKey, string]{entry: entries[11]})
	require.Equal(t, 999, tlfu.slru.probation.len)
	require.Equal(t, 1, tlfu.slru.protected.len)

	evicted := tlfu.Set(NewEntry(StringKey("0a"), "", 1))
	require.Equal(t, StringKey("0a"), evicted.key)
	require.Equal(t, 999, tlfu.slru.probation.len)
	require.Equal(t, 1, tlfu.slru.protected.len)

	victim := tlfu.slru.victim()
	require.Equal(t, StringKey("0"), victim.key)
	tlfu.Access(ReadBufItem[StringKey, string]{entry: entries[991]})
	tlfu.Access(ReadBufItem[StringKey, string]{entry: entries[991]})
	tlfu.Access(ReadBufItem[StringKey, string]{entry: entries[991]})
	tlfu.Access(ReadBufItem[StringKey, string]{entry: entries[991]})
	evicted = tlfu.Set(NewEntry(StringKey("1a"), "", 1))
	require.Equal(t, StringKey("1a"), evicted.key)
	require.Equal(t, 998, tlfu.slru.probation.len)

	var entries2 []*Entry[StringKey, string]
	for i := range 1000 {
		e := NewEntry(StringKey(fmt.Sprintf("%d*", i)), "", 1)
		tlfu.Set(e)
		entries2 = append(entries2, e)
	}
	require.Equal(t, 998, tlfu.slru.probation.len)
	require.Equal(t, 2, tlfu.slru.protected.len)

	for _, i := range []int{997, 998, 999} {
		tlfu.Remove(entries2[i])
		tlfu.slru.probation.display()
		tlfu.slru.probation.displayReverse()
		tlfu.slru.protected.display()
		tlfu.slru.protected.displayReverse()
	}

}

func TestEvictEntries(t *testing.T) {
	tlfu := NewTinyLfu[StringKey, string](500)
	require.Equal(t, uint(500), tlfu.slru.probation.capacity)
	require.Equal(t, uint(400), tlfu.slru.protected.capacity)
	require.Equal(t, 0, tlfu.slru.probation.len)
	require.Equal(t, 0, tlfu.slru.protected.len)

	for i := range 500 {
		tlfu.Set(NewEntry(StringKey(fmt.Sprintf("%d:1", i)), "", 1))
	}
	require.Equal(t, 500, tlfu.slru.probation.len)
	require.Equal(t, 0, tlfu.slru.protected.len)
	new := NewEntry(StringKey("l:10"), "", 10)
	new.frequency.Store(10)
	tlfu.Set(new)
	require.Equal(t, 509, tlfu.slru.probation.len)
	require.Equal(t, 0, tlfu.slru.protected.len)
	//  2. probation length is 509, so remove 9 entries from probation
	removed := tlfu.EvictEntries()
	for _, rm := range removed {
		require.True(t, strings.HasSuffix(string(rm.key), ":1"))
	}
	require.Equal(t, 9, len(removed))
	require.Equal(t, 500, tlfu.slru.probation.len)
	require.Equal(t, 0, tlfu.slru.protected.len)

	// put l:450 to probation, this will remove 1 entry, probation len is 949 now
	// remove 449 entries from probation
	new = NewEntry(StringKey("l:450"), "", 450)
	new.frequency.Store(10)
	tlfu.Set(new)
	removed = tlfu.EvictEntries()
	require.Equal(t, 449, len(removed))
	require.Equal(t, 500, tlfu.slru.probation.len)
	require.Equal(t, 0, tlfu.slru.protected.len)

	// put l:460 to probation, this will remove 1 entry, probation len is 959 now
	// remove all entries except the new l:460 one
	new = NewEntry(StringKey("l:460"), "", 460)
	new.frequency.Store(10)
	tlfu.Set(new)
	removed = tlfu.EvictEntries()
	require.Equal(t, 41, len(removed))
	require.Equal(t, 460, tlfu.slru.probation.len)
	require.Equal(t, 0, tlfu.slru.protected.len)

	// access
	tlfu.Access(ReadBufItem[StringKey, string]{entry: new})
	require.Equal(t, 0, tlfu.slru.probation.len)
	require.Equal(t, 460, tlfu.slru.protected.len)
	new.cost.Store(600)
	tlfu.UpdateCost(new, 140)
	removed = tlfu.EvictEntries()
	require.Equal(t, 1, len(removed))
	require.Equal(t, 0, tlfu.slru.probation.len)
	require.Equal(t, 0, tlfu.slru.protected.len)

}
