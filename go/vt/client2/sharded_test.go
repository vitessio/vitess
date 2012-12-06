// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package client2

// import (
// 	_ "database/sql"
// 	_ "fmt"
// 	"testing"
// 	_ "time"

// 	_ "code.google.com/p/vitess/go/zk"
// )

// // This is more of an example code than a real test
// func TestShardExec(t *testing.T) {
// 	zconn := zk.NewMetaConn(false)
// 	sconn, err := Dial(zconn, "/zk/local/vt/keyspaces/test_keyspace", "master", false, 5*time.Second)
// 	if err != nil {
// 		t.Fatalf("dial error: %v", err)
// 	}
// 	rows, err := sconn.QueryBind("select id, name from mytable", nil)
// 	if err != nil {
// 		t.Fatalf("exec error: %v", err)
// 	}
// 	cols, err := r.Columns()
// 	if err != nil {
// 		t.Fatalf("columns error: %v", err)
// 	}
// 	fmt.Printf("%v\n", cols)
// 	for r.Next() {
// 		var id int
// 		var name string
// 		err = r.Scan(&id, &name)
// 		fmt.Printf("%d %s\n", id, name)
// 	}
// }
