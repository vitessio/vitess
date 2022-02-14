package sync2

import (
	"reflect"
	"testing"
)

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

	result := 1
	go func() {
		orig.Result = &result
		orig.Broadcast()
	}()
	dup.Wait()

	if *orig.Result.(*int) != result {
		t.Errorf("failed to pass result")
	}
	if *orig.Result.(*int) != *dup.Result.(*int) {
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
		second.Result = &result
		second.Broadcast()
	}()
	dup.Wait()

	want = []ConsolidatorCacheItem{{Query: sql, Count: 2}}
	if !reflect.DeepEqual(con.Items(), want) {
		t.Fatalf("expected consolidator to have two items %v", con.Items())
	}

}
