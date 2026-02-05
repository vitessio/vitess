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
	"reflect"
	"sync"
	"testing"

	"vitess.io/vitess/go/sqltypes"
)

func TestAddWaiterCount(t *testing.T) {
	con := NewConsolidator()
	sql := "select * from SomeTable"
	pr, _ := con.Create(sql)
	var wgAdd sync.WaitGroup
	var wgSub sync.WaitGroup

	var concurrent = 1000

	for i := 0; i < concurrent; i++ {
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

	if *pr.AddWaiterCounter(0) != 0 {
		t.Fatalf("Expect 0 totalWaiterCount but got: %v", *pr.AddWaiterCounter(0))
	}
}

func TestConsolidator(t *testing.T) {
	con := NewConsolidator()
	sql := "select * from SomeTable"

	want := []ConsolidatorCacheItem{}
	if !reflect.DeepEqual(con.Items(), want) {
		t.Fatalf("expected consolidator to have no items")
	}

	orig, added := con.Create(sql)
	if !added {
		t.Fatalf("expected consolidator to register a new entry")
	}

	if !reflect.DeepEqual(con.Items(), want) {
		t.Fatalf("expected consolidator to still have no items")
	}

	dup, added := con.Create(sql)
	if added {
		t.Fatalf("did not expect consolidator to register a new entry")
	}

	result := &sqltypes.Result{}
	go func() {
		orig.SetResult(result)
		orig.Broadcast()
	}()
	dup.Wait()

	if orig.Result() != result {
		t.Errorf("failed to pass result")
	}
	if orig.Result() != dup.Result() {
		t.Fatalf("failed to share the result")
	}

	want = []ConsolidatorCacheItem{{Query: sql, Count: 1}}
	if !reflect.DeepEqual(con.Items(), want) {
		t.Fatalf("expected consolidator to have one items %v", con.Items())
	}

	// Running the query again should add a new entry since the original
	// query execution completed
	second, added := con.Create(sql)
	if !added {
		t.Fatalf("expected consolidator to register a new entry")
	}

	go func() {
		second.SetResult(result)
		second.Broadcast()
	}()
	dup.Wait()

	want = []ConsolidatorCacheItem{{Query: sql, Count: 2}}
	if !reflect.DeepEqual(con.Items(), want) {
		t.Fatalf("expected consolidator to have two items %v", con.Items())
	}

}
