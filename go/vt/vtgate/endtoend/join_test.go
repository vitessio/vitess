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

	exec(t, conn, "insert into user(id, name) values(1,'name1')")
	exec(t, conn, "insert into user_details(user_id, email) values(1,'name1@email'), (2,'name2@email')")
	exec(t, conn, "insert into user_info(name, info) values('name1','info1')")

	// Test inner join
	qr := exec(t, conn, "select u.id, u.name, ud.user_id, ud.email from user u inner join user_details ud on u.id = ud.user_id")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("name1") INT64(1) VARCHAR("name1@email")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Test join with increments
	qr = exec(t, conn, "select u.id, u.name, ud.user_id, ud.email from user u join user_details ud where u.id + 1 = ud.user_id")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("name1") INT64(2) VARCHAR("name2@email")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Test left join
	qr = exec(t, conn, "select u.id, u.name, ud.user_id, ud.email from user u left join user_details ud on u.id + 1 = ud.user_id")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("name1") INT64(2) VARCHAR("name2@email")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Test left join with no values on joiner table
	qr = exec(t, conn, "select u.id, u.name, ud.user_id, ud.email from user u left join user_details ud on u.id + 2 = ud.user_id")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("name1") NULL NULL]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Test left join with no matching rows
	qr = exec(t, conn, "select u.id, u.name, ud.user_id, ud.email from user u left join user_details ud on u.id + 2 = ud.user_id where u.id = 2")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Test left join with only one matching row
	qr = exec(t, conn, "select u.id, u.name, ui.info from user u join user_info ui on u.name = ui.name where u.id = 1")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("name1") VARCHAR("info1")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	exec(t, conn, "insert into user_details(user_id, email) values(3,'name1@email'), (4,'name2@email')")
	exec(t, conn, "insert into user(id, name) values(4,'name4')")

	// Test cross shard query
	qr = exec(t, conn, "select id, name from user where id in (select user_id from user_details) order by id")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("name1")] [INT64(4) VARCHAR("name4")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}
