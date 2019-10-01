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

package endtoend

import (
	"context"
	"fmt"
	"testing"

	"vitess.io/vitess/go/mysql"
)

func TestJoins(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	exec(t, conn, "insert into user(id, name) values(1,'name1'), (2,'name2'), (3,'name3'), (4,'name4')")
	exec(t, conn, "insert into user_details(user_id, email) values(1,'name1@email'), (2,'name2@email'), (3,'name3@email'), (4,'name4@email')")

	qr := exec(t, conn, "select u.id, u.name, ud.email from user u inner join user_details ud on u.id = ud.user_id order by u.id limit 1")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("name1") VARCHAR("name1@email")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}
