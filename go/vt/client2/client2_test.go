// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package client2

import (
	"testing"
)

// // This is more of an example code than a real test
// func TestExec(t *testing.T) {
// 	sql.Register("vtocc", &Driver{})
// 	db, err := sql.Open("vtocc", "localhost:6510/testdb")
// 	if err != nil {
// 		t.Fatalf("client error: %v", err)
// 	}
// 	r, err := db.Query("select id, name from mytable")
// 	if err != nil {
// 		t.Fatalf("client error: %v", err)
// 	}
// 	cols, err := r.Columns()
// 	if err != nil {
// 		t.Fatalf("client error: %v", err)
// 	}
// 	fmt.Printf("%v\n", cols)
// 	for r.Next() {
// 		var id int
// 		var name string
// 		err = r.Scan(&id, &name)
// 		fmt.Printf("%d %s\n", id, name)
// 	}
// }

func TestParseDataSourceName(t *testing.T) {
	if url, dbname, _, _, useAuth, err := parseDataSourceName("localhost:6520/testdb"); err == nil {
		if useAuth {
			t.Errorf("localhost:6520/testdb does not contain auth information")
		}
		if expected := "localhost:6520"; url != expected {
			t.Errorf("wrong url: %v, expected %v", url, expected)
		}

		if expected := "testdb"; dbname != expected {
			t.Errorf("wrong db name: %v, expected %v", dbname, expected)
		}
	} else {
		t.Errorf("unexpected error: %v", err.Error())
	}

	if url, dbname, user, password, useAuth, err := parseDataSourceName("user:password@localhost:6520/testdb"); err == nil {
		if !useAuth {
			t.Errorf("user:password@localhost:6520/testdb does contain auth information")
		}
		if expected := "localhost:6520"; url != expected {
			t.Errorf("wrong url: %v, expected %v", url, expected)
		}

		if expected := "testdb"; dbname != expected {
			t.Errorf("wrong db name: %v, expected %v", dbname, expected)
		}

		if expected := "user"; user != expected {
			t.Errorf("wrong user name: %v, expected %v", user, expected)
		}

		if expected := "password"; password != expected {
			t.Errorf("wrong password: %v, expected %v", password, expected)
		}
	} else {
		t.Errorf("unexpected error: %v", err.Error())
	}

	if _, _, _, _, _, err := parseDataSourceName("%$%/@#$%:::"); err == nil {
		t.Errorf("no expected error parsing junk string")
	}
}
