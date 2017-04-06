package sync2

import "testing"

func TestConsolidator(t *testing.T) {
	con := NewConsolidator()
	sql := "select * from SomeTable"

	orig, added := con.Create(sql)
	if !added {
		t.Fatalf("expected consolidator to register a new entry")
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

	// Running the query again should add a new entry since the original
	// query execution completed
	_, added = con.Create(sql)
	if !added {
		t.Fatalf("expected consolidator to register a new entry")
	}
}
