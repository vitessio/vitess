/*
Copyright 2019 The Vitess Authors.

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

package sync2

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
)

func TestAddWaiterCount(t *testing.T) {
	con := NewConsolidator()
	sql := "select * from SomeTable"
	pr, _ := con.Create(sql)
	var wgAdd sync.WaitGroup
	var wgSub sync.WaitGroup

	concurrent := 1000

	for range concurrent {
		wgAdd.Add(1)
		wgSub.Add(1)
		go func() {
			defer wgAdd.Done()
			pr.AddWaiterCounter(1)
		}()
		go func() {
			defer wgSub.Done()
			pr.AddWaiterCounter(-1)
		}()
	}

	wgAdd.Wait()
	wgSub.Wait()

	require.Equal(t, int64(0), *pr.AddWaiterCounter(0), "expected 0 totalWaiterCount")
}

func TestConsolidator(t *testing.T) {
	con := NewConsolidator()
	sql := "select * from SomeTable"

	want := []ConsolidatorCacheItem{}
	require.Equal(t, want, con.Items(), "expected consolidator to have no items")

	orig, added := con.Create(sql)
	require.True(t, added, "expected consolidator to register a new entry")

	require.Equal(t, want, con.Items(), "expected consolidator to still have no items")

	dup, added := con.Create(sql)
	require.False(t, added, "did not expect consolidator to register a new entry")

	result := &sqltypes.Result{}
	go func() {
		orig.SetResult(result)
		orig.Broadcast()
	}()
	dup.Wait()

	assert.Equal(t, result, orig.Result(), "failed to pass result")
	require.Equal(t, orig.Result(), dup.Result(), "failed to share the result")

	want = []ConsolidatorCacheItem{{Query: sql, Count: 1}}
	require.Equal(t, want, con.Items(), "expected consolidator to have one item")

	// Running the query again should add a new entry since the original
	// query execution completed
	second, added := con.Create(sql)
	require.True(t, added, "expected consolidator to register a new entry")

	go func() {
		second.SetResult(result)
		second.Broadcast()
	}()
	dup.Wait()

	want = []ConsolidatorCacheItem{{Query: sql, Count: 2}}
	require.Equal(t, want, con.Items(), "expected consolidator to have two items")
}
