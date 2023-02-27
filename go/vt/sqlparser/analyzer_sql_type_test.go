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
