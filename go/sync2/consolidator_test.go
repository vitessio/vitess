/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
