// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package client2

import (
	"database/sql"
	"fmt"
	"testing"
)

// This is more of an example code than a real test
func TestExec(t *testing.T) {
	sql.Register("vtocc", &Driver{})
	db, err := sql.Open("vtocc", "localhost:6510/testdb")
	if err != nil {
		t.Fatalf("client error: %v", err)
	}
	r, err := db.Query("select id, name from mytable")
	if err != nil {
		t.Fatalf("client error: %v", err)
	}
	cols, err := r.Columns()
	if err != nil {
		t.Fatalf("client error: %v", err)
	}
	fmt.Printf("%v\n", cols)
	for r.Next() {
		var id int
		var name string
		err = r.Scan(&id, &name)
		fmt.Printf("%d %s\n", id, name)
	}
}
