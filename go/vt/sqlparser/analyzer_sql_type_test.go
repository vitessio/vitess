/*
Copyright 2021 The Vitess Authors.

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
package sqlparser

import (
	"testing"
)

func TestIsSelectForUpdateStatement(t *testing.T) {
	testcases := []struct {
		sql  string
		want bool
	}{
		{"select 1 from t for update", true},
		{"select /* for update */ 1 from t for update", true},
		{"select * from t lock in share mode", true},
		{"select /* for update */ 1 from t", false},
		{"select /* for update */ 1 from t union all select * from t2 for update", true},
		{"select /* for update */ 1 from t union all select * from t2 lock in share mode", true},
		{"select /* for update, lock in share mode */ 1 from t union all select * from t2", false},
		{"/* select 1 from t for update */", false},
		{"create database d1", false},
	}
	for _, tcase := range testcases {
		tree, err := Parse(tcase.sql)
		if err != nil {
			t.Error(err)
			continue
		}
		if got := IsSelectForUpdateStatement(tree); got != tcase.want {
			t.Errorf("IsSelectForUpdateStatement(%s): %v, want %v", tcase.sql, got, tcase.want)
		}
	}
}

func TestIsDDLStatement(t *testing.T) {
	testcases := []struct {
		sql  string
		want bool
	}{
		{"create database d1", true},
		{"drop database d1", true},
		{"create table t1 (c1 int)", true},
		{"drop table t1", true},
		{"alter table t1 add column c2 int", true},
		{"alter table t1 drop column c2", true},
		{"/* alter table t1 drop column c2 */", false},
		{"CREATE VIEW my_view AS SELECT id, col FROM user", true},
	}
	for _, tcase := range testcases {
		tree, err := Parse(tcase.sql)
		if err != nil {
			t.Error(err)
			continue
		}
		if got := IsDDLStatement(tree); got != tcase.want {
			t.Errorf("IsDDLStatement(%s): %v, want %v", tcase.sql, got, tcase.want)
		}
	}
}

func TestIsPureSelectStatement(t *testing.T) {
	testcases := []struct {
		sql  string
		want bool
	}{
		{"select 1 from t for update", false},
		{"select /* for update */ 1 from t for update", false},
		{"select * from t lock in share mode", false},
		{"select /* for update */ 1 from t", true},
		{"select /* for update */ 1 from t union all select * from t2 for update", false},
		{"select /* for update */ 1 from t union all select * from t2 lock in share mode", false},
		{"select /* for update, lock in share mode */ 1 from t union all select * from t2", true},
		{"/* select 1 from t for update */", false},
		{"create database d1", false},
	}
	for _, tcase := range testcases {
		tree, err := Parse(tcase.sql)
		if err != nil {
			t.Error(err)
			continue
		}
		if got := IsPureSelectStatement(tree); got != tcase.want {
			t.Errorf("IsPureSelectStatement(%s): %v, want %v", tcase.sql, got, tcase.want)
		}
	}
}

func TestIsSelectLastInsertIDStatement(t *testing.T) {
	testcases := []struct {
		sql  string
		want bool
	}{
		{"select last_insert_id()", true},
		{"select last_insert_id() union all select last_insert_id()", true},
		{"select /* select last_insert_id() */ 1", false},
		{"select database()", false},
	}
	for _, tcase := range testcases {
		tree, err := Parse(tcase.sql)
		if err != nil {
			t.Error(err)
			continue
		}
		if got := ContainsLastInsertIDStatement(tree); got != tcase.want {
			t.Errorf("ContainsLastInsertIDStatement(%s): %v, want %v", tcase.sql, got, tcase.want)
		}
	}
}

func _TestIsKillStatement(t *testing.T) {
	testcases := []struct {
		sql  string
		want bool
	}{
		{"kill 1", true},
		{"kill connection 1", true},
		{"kill query 1", true},
	}
	for _, tcase := range testcases {
		tree, err := Parse(tcase.sql)
		if err != nil {
			t.Error(err)
			continue
		}
		if got := IsKillStatement(tree); got != tcase.want {
			t.Errorf("IsKillStatement(%s): %v, want %v", tcase.sql, got, tcase.want)
		}
	}
}

func TestContainsLockStatement(t *testing.T) {
	testcases := []struct {
		sql  string
		want bool
	}{
		{"select get_lock('aaa', 1000)", true},
		{"select get_lock('aaa', 1000) union all select get_lock('bbb', 1000) ", true},
		{"select release_lock('aaa')", true},
		{"select IS_USED_LOCK('aaa')", true},
		{"select IS_FREE_LOCK('aaa')", true},
		{"select RELEASE_ALL_LOCKS()", true},
		{"select /* get_lock('aaa', 1000) */ 1", false},
		{"select database()", false},
	}
	for _, tcase := range testcases {
		tree, err := Parse(tcase.sql)
		if err != nil {
			t.Error(err)
			continue
		}
		if got := ContainsLockStatement(tree); got != tcase.want {
			t.Errorf("ContainsLockStatement(%s): %v, want %v", tcase.sql, got, tcase.want)
		}
	}
}
