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

package rules

import (
	"bytes"
	"encoding/json"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestQueryRules(t *testing.T) {
	qrs := New()
	qr1 := NewQueryRule("rule 1", "r1", QRFail)
	qr2 := NewQueryRule("rule 2", "r2", QRFail)
	qrs.Add(qr1)
	qrs.Add(qr2)

	qrf := qrs.Find("r1")
	if qrf != qr1 {
		t.Errorf("want:\n%#v\ngot:\n%#v", qr1, qrf)
	}

	qrf = qrs.Find("r2")
	if qrf != qr2 {
		t.Errorf("want:\n%#v\ngot:\n%#v", qr2, qrf)
	}

	qrf = qrs.Find("unknown_rule")
	if qrf != nil {
		t.Fatalf("rule: unknown_rule does not exist, should get nil")
	}

	if qrs.rules[0] != qr1 {
		t.Errorf("want:\n%#v\ngot:\n%#v", qr1, qrs.rules[0])
	}

	qrf = qrs.Delete("r1")
	if qrf != qr1 {
		t.Errorf("want:\n%#v\ngot:\n%#v", qr1, qrf)
	}

	if len(qrs.rules) != 1 {
		t.Errorf("want 1, got %d", len(qrs.rules))
	}

	if qrs.rules[0] != qr2 {
		t.Errorf("want:\n%#v\ngot:\n%#v", qr2, qrf)
	}

	qrf = qrs.Delete("unknown_rule")
	if qrf != nil {
		t.Fatalf("delete an unknown_rule, should return nil")
	}
}

// TestCopy tests for deep copy
func TestCopy(t *testing.T) {
	qrs1 := New()
	qr1 := NewQueryRule("rule 1", "r1", QRFail)
	qr1.AddPlanCond(planbuilder.PlanSelect)
	qr1.AddTableCond("aa")
	qr1.AddBindVarCond("a", true, false, QRNoOp, nil)

	qr2 := NewQueryRule("rule 2", "r2", QRFail)
	qrs1.Add(qr1)
	qrs1.Add(qr2)

	qrs2 := qrs1.Copy()
	if !reflect.DeepEqual(qrs2, qrs1) {
		t.Errorf("qrs1: %+v, not equal to %+v", qrs2, qrs1)
	}

	qrs1 = New()
	qrs2 = qrs1.Copy()
	if !reflect.DeepEqual(qrs2, qrs1) {
		t.Errorf("qrs1: %+v, not equal to %+v", qrs2, qrs1)
	}
}

func TestFilterByPlan(t *testing.T) {
	qrs := New()

	qr1 := NewQueryRule("rule 1", "r1", QRFail)
	qr1.SetIPCond("123")
	qr1.SetQueryCond("select")
	qr1.AddPlanCond(planbuilder.PlanSelect)
	qr1.AddBindVarCond("a", true, false, QRNoOp, nil)

	qr2 := NewQueryRule("rule 2", "r2", QRFail)
	qr2.AddPlanCond(planbuilder.PlanSelect)
	qr2.AddPlanCond(planbuilder.PlanSelect)
	qr2.AddBindVarCond("a", true, false, QRNoOp, nil)

	qr3 := NewQueryRule("rule 3", "r3", QRFail)
	qr3.SetQueryCond("sele.*")
	qr3.AddBindVarCond("a", true, false, QRNoOp, nil)

	qr4 := NewQueryRule("rule 4", "r4", QRFail)
	qr4.AddTableCond("b")
	qr4.AddTableCond("c")

	qrs.Add(qr1)
	qrs.Add(qr2)
	qrs.Add(qr3)
	qrs.Add(qr4)

	qrs1 := qrs.FilterByPlan("select", planbuilder.PlanSelect, "a")
	want := compacted(`[{
		"Description":"rule 1",
		"Name":"r1",
		"RequestIP":"123",
		"BindVarConds":[{
			"Name":"a",
			"OnAbsent":true,
			"Operator":""
		}],
		"Action":"FAIL"
	},{
		"Description":"rule 2",
		"Name":"r2",
		"BindVarConds":[{
			"Name":"a",
			"OnAbsent":true,
			"Operator":""
		}],
		"Action":"FAIL"
	},{
		"Description":"rule 3",
		"Name":"r3",
		"BindVarConds":[{
			"Name":"a",
			"OnAbsent":true,
			"Operator":""
		}],
		"Action":"FAIL"
	}]`)
	got := marshalled(qrs1)
	if got != want {
		t.Errorf("qrs1:\n%s, want\n%s", got, want)
	}

	qrs1 = qrs.FilterByPlan("insert", planbuilder.PlanSelect, "a")
	want = compacted(`[{
		"Description":"rule 2",
		"Name":"r2",
		"BindVarConds":[{
			"Name":"a",
			"OnAbsent":true,
			"Operator":""
		}],
		"Action":"FAIL"
	}]`)
	got = marshalled(qrs1)
	if got != want {
		t.Errorf("qrs1:\n%s, want\n%s", got, want)
	}

	qrs1 = qrs.FilterByPlan("insert", planbuilder.PlanSelect, "a")
	got = marshalled(qrs1)
	if got != want {
		t.Errorf("qrs1:\n%s, want\n%s", got, want)
	}

	qrs1 = qrs.FilterByPlan("select", planbuilder.PlanInsert, "a")
	want = compacted(`[{
		"Description":"rule 3",
		"Name":"r3",
		"BindVarConds":[{
			"Name":"a",
			"OnAbsent":true,
			"Operator":""
		}],
		"Action":"FAIL"
	}]`)
	got = marshalled(qrs1)
	if got != want {
		t.Errorf("qrs1:\n%s, want\n%s", got, want)
	}

	qrs1 = qrs.FilterByPlan("sel", planbuilder.PlanInsert, "a")
	if qrs1.rules != nil {
		t.Errorf("want nil, got non-nil")
	}

	qrs1 = qrs.FilterByPlan("table", planbuilder.PlanInsert, "b")
	want = compacted(`[{
		"Description":"rule 4",
		"Name":"r4",
		"Action":"FAIL"
	}]`)
	got = marshalled(qrs1)
	if got != want {
		t.Errorf("qrs1:\n%s, want\n%s", got, want)
	}

	qr5 := NewQueryRule("rule 5", "r5", QRFail)
	qrs.Add(qr5)

	qrs1 = qrs.FilterByPlan("sel", planbuilder.PlanInsert, "a")
	want = compacted(`[{
		"Description":"rule 5",
		"Name":"r5",
		"Action":"FAIL"
	}]`)
	got = marshalled(qrs1)
	if got != want {
		t.Errorf("qrs1:\n%s, want\n%s", got, want)
	}

	qrsnil1 := New()
	if qrsnil2 := qrsnil1.FilterByPlan("", planbuilder.PlanSelect, "a"); qrsnil2.rules != nil {
		t.Errorf("want nil, got non-nil")
	}
}

func TestQueryRule(t *testing.T) {
	qr := NewQueryRule("rule 1", "r1", QRFail)
	err := qr.SetIPCond("123")
	if err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if !qr.requestIP.MatchString("123") {
		t.Errorf("want match")
	}
	if qr.requestIP.MatchString("1234") {
		t.Errorf("want no match")
	}
	if qr.requestIP.MatchString("12") {
		t.Errorf("want no match")
	}
	err = qr.SetIPCond("[")
	if err == nil {
		t.Errorf("want error")
	}

	qr.AddPlanCond(planbuilder.PlanSelect)
	qr.AddPlanCond(planbuilder.PlanInsert)

	if qr.plans[0] != planbuilder.PlanSelect {
		t.Errorf("want PASS_SELECT, got %s", qr.plans[0].String())
	}
	if qr.plans[1] != planbuilder.PlanInsert {
		t.Errorf("want INSERT_PK, got %s", qr.plans[1].String())
	}

	qr.AddTableCond("a")
	if qr.tableNames[0] != "a" {
		t.Errorf("want a, got %s", qr.tableNames[0])
	}
}

func TestBindVarStruct(t *testing.T) {
	qr := NewQueryRule("rule 1", "r1", QRFail)

	err := qr.AddBindVarCond("b", false, true, QRNoOp, nil)
	if err != nil {
		t.Errorf("unexpected: %v", err)
	}
	err = qr.AddBindVarCond("a", true, false, QRNoOp, nil)
	if err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if qr.bindVarConds[1].name != "a" {
		t.Errorf("want a, got %s", qr.bindVarConds[1].name)
	}
	if !qr.bindVarConds[1].onAbsent {
		t.Errorf("want true, got false")
	}
	if qr.bindVarConds[1].onMismatch {
		t.Errorf("want false, got true")
	}
	if qr.bindVarConds[1].op != QRNoOp {
		t.Errorf("exepecting no-op, got %v", qr.bindVarConds[1])
	}
	if qr.bindVarConds[1].value != nil {
		t.Errorf("want nil, got %#v", qr.bindVarConds[1].value)
	}
}

type BVCreation struct {
	name       string
	onAbsent   bool
	onMismatch bool
	op         Operator
	value      interface{}
	expecterr  bool
}

var creationCases = []BVCreation{
	{"a", true, true, QREqual, uint64(1), false},
	{"a", true, true, QRNotEqual, uint64(1), false},
	{"a", true, true, QRLessThan, uint64(1), false},
	{"a", true, true, QRGreaterEqual, uint64(1), false},
	{"a", true, true, QRGreaterThan, uint64(1), false},
	{"a", true, true, QRLessEqual, uint64(1), false},

	{"a", true, true, QREqual, int64(1), false},
	{"a", true, true, QRNotEqual, int64(1), false},
	{"a", true, true, QRLessThan, int64(1), false},
	{"a", true, true, QRGreaterEqual, int64(1), false},
	{"a", true, true, QRGreaterThan, int64(1), false},
	{"a", true, true, QRLessEqual, int64(1), false},

	{"a", true, true, QREqual, "a", false},
	{"a", true, true, QRNotEqual, "a", false},
	{"a", true, true, QRLessThan, "a", false},
	{"a", true, true, QRGreaterEqual, "a", false},
	{"a", true, true, QRGreaterThan, "a", false},
	{"a", true, true, QRLessEqual, "a", false},
	{"a", true, true, QRMatch, "a", false},
	{"a", true, true, QRNoMatch, "a", false},

	{"a", true, true, QRMatch, int64(1), true},
	{"a", true, true, QRNoMatch, int64(1), true},
	{"a", true, true, QRMatch, "[", true},
	{"a", true, true, QRNoMatch, "[", true},
}

func TestBVCreation(t *testing.T) {
	qr := NewQueryRule("rule 1", "r1", QRFail)
	for i, tcase := range creationCases {
		err := qr.AddBindVarCond(tcase.name, tcase.onAbsent, tcase.onMismatch, tcase.op, tcase.value)
		haserr := (err != nil)
		if haserr != tcase.expecterr {
			t.Errorf("test %d: got %v for %#v", i, haserr, tcase)
		}
	}
}

type BindVarTestCase struct {
	bvc      BindVarCond
	bvval    *querypb.BindVariable
	expected bool
}

var bvtestcases = []BindVarTestCase{
	{BindVarCond{"b", true, true, QRNoOp, nil}, sqltypes.Int64BindVariable(1), true},
	{BindVarCond{"b", false, true, QRNoOp, nil}, sqltypes.Int64BindVariable(1), false},
	{BindVarCond{"a", true, true, QRNoOp, nil}, sqltypes.Int64BindVariable(1), false},
	{BindVarCond{"a", false, true, QRNoOp, nil}, sqltypes.Int64BindVariable(1), true},

	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, sqltypes.Int64BindVariable(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, sqltypes.Int64BindVariable(10), true},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, sqltypes.Uint64BindVariable(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, sqltypes.Uint64BindVariable(10), true},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, sqltypes.StringBindVariable("abc"), false},

	{BindVarCond{"a", true, true, QRNotEqual, bvcuint64(10)}, sqltypes.Int64BindVariable(1), true},
	{BindVarCond{"a", true, true, QRNotEqual, bvcuint64(10)}, sqltypes.Int64BindVariable(10), false},
	{BindVarCond{"a", true, true, QRNotEqual, bvcuint64(10)}, sqltypes.Int64BindVariable(11), true},
	{BindVarCond{"a", true, true, QRNotEqual, bvcuint64(10)}, sqltypes.Int64BindVariable(-1), true},

	{BindVarCond{"a", true, true, QRLessThan, bvcuint64(10)}, sqltypes.Int64BindVariable(1), true},
	{BindVarCond{"a", true, true, QRLessThan, bvcuint64(10)}, sqltypes.Int64BindVariable(10), false},
	{BindVarCond{"a", true, true, QRLessThan, bvcuint64(10)}, sqltypes.Int64BindVariable(11), false},
	{BindVarCond{"a", true, true, QRLessThan, bvcuint64(10)}, sqltypes.Int64BindVariable(-1), true},

	{BindVarCond{"a", true, true, QRGreaterEqual, bvcuint64(10)}, sqltypes.Int64BindVariable(1), false},
	{BindVarCond{"a", true, true, QRGreaterEqual, bvcuint64(10)}, sqltypes.Int64BindVariable(10), true},
	{BindVarCond{"a", true, true, QRGreaterEqual, bvcuint64(10)}, sqltypes.Int64BindVariable(11), true},
	{BindVarCond{"a", true, true, QRGreaterEqual, bvcuint64(10)}, sqltypes.Int64BindVariable(-1), false},

	{BindVarCond{"a", true, true, QRGreaterThan, bvcuint64(10)}, sqltypes.Int64BindVariable(1), false},
	{BindVarCond{"a", true, true, QRGreaterThan, bvcuint64(10)}, sqltypes.Int64BindVariable(10), false},
	{BindVarCond{"a", true, true, QRGreaterThan, bvcuint64(10)}, sqltypes.Int64BindVariable(11), true},
	{BindVarCond{"a", true, true, QRGreaterThan, bvcuint64(10)}, sqltypes.Int64BindVariable(-1), false},

	{BindVarCond{"a", true, true, QRLessEqual, bvcuint64(10)}, sqltypes.Int64BindVariable(1), true},
	{BindVarCond{"a", true, true, QRLessEqual, bvcuint64(10)}, sqltypes.Int64BindVariable(10), true},
	{BindVarCond{"a", true, true, QRLessEqual, bvcuint64(10)}, sqltypes.Int64BindVariable(11), false},
	{BindVarCond{"a", true, true, QRLessEqual, bvcuint64(10)}, sqltypes.Int64BindVariable(-1), true},

	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, sqltypes.Int64BindVariable(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, sqltypes.Int64BindVariable(10), true},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, sqltypes.Uint64BindVariable(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, sqltypes.Uint64BindVariable(0xFFFFFFFFFFFFFFFF), false},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, sqltypes.Uint64BindVariable(10), true},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, sqltypes.StringBindVariable("abc"), false},

	{BindVarCond{"a", true, true, QRNotEqual, bvcint64(10)}, sqltypes.Int64BindVariable(1), true},
	{BindVarCond{"a", true, true, QRNotEqual, bvcint64(10)}, sqltypes.Int64BindVariable(10), false},
	{BindVarCond{"a", true, true, QRNotEqual, bvcint64(10)}, sqltypes.Int64BindVariable(11), true},
	{BindVarCond{"a", true, true, QRNotEqual, bvcint64(10)}, sqltypes.Uint64BindVariable(0xFFFFFFFFFFFFFFFF), true},

	{BindVarCond{"a", true, true, QRLessThan, bvcint64(10)}, sqltypes.Int64BindVariable(1), true},
	{BindVarCond{"a", true, true, QRLessThan, bvcint64(10)}, sqltypes.Int64BindVariable(10), false},
	{BindVarCond{"a", true, true, QRLessThan, bvcint64(10)}, sqltypes.Int64BindVariable(11), false},
	{BindVarCond{"a", true, true, QRLessThan, bvcint64(10)}, sqltypes.Uint64BindVariable(0xFFFFFFFFFFFFFFFF), false},

	{BindVarCond{"a", true, true, QRGreaterEqual, bvcint64(10)}, sqltypes.Int64BindVariable(1), false},
	{BindVarCond{"a", true, true, QRGreaterEqual, bvcint64(10)}, sqltypes.Int64BindVariable(10), true},
	{BindVarCond{"a", true, true, QRGreaterEqual, bvcint64(10)}, sqltypes.Int64BindVariable(11), true},
	{BindVarCond{"a", true, true, QRGreaterEqual, bvcint64(10)}, sqltypes.Uint64BindVariable(0xFFFFFFFFFFFFFFFF), true},

	{BindVarCond{"a", true, true, QRGreaterThan, bvcint64(10)}, sqltypes.Int64BindVariable(1), false},
	{BindVarCond{"a", true, true, QRGreaterThan, bvcint64(10)}, sqltypes.Int64BindVariable(10), false},
	{BindVarCond{"a", true, true, QRGreaterThan, bvcint64(10)}, sqltypes.Int64BindVariable(11), true},
	{BindVarCond{"a", true, true, QRGreaterThan, bvcint64(10)}, sqltypes.Uint64BindVariable(0xFFFFFFFFFFFFFFFF), true},

	{BindVarCond{"a", true, true, QRLessEqual, bvcint64(10)}, sqltypes.Int64BindVariable(1), true},
	{BindVarCond{"a", true, true, QRLessEqual, bvcint64(10)}, sqltypes.Int64BindVariable(10), true},
	{BindVarCond{"a", true, true, QRLessEqual, bvcint64(10)}, sqltypes.Int64BindVariable(11), false},
	{BindVarCond{"a", true, true, QRLessEqual, bvcint64(10)}, sqltypes.Uint64BindVariable(0xFFFFFFFFFFFFFFFF), false},

	{BindVarCond{"a", true, true, QREqual, bvcstring("b")}, sqltypes.StringBindVariable("a"), false},
	{BindVarCond{"a", true, true, QREqual, bvcstring("b")}, sqltypes.StringBindVariable("b"), true},
	{BindVarCond{"a", true, true, QREqual, bvcstring("b")}, sqltypes.StringBindVariable("c"), false},
	{BindVarCond{"a", true, true, QREqual, bvcstring("b")}, sqltypes.BytesBindVariable([]byte("a")), false},
	{BindVarCond{"a", true, true, QREqual, bvcstring("b")}, sqltypes.BytesBindVariable([]byte("b")), true},
	{BindVarCond{"a", true, true, QREqual, bvcstring("b")}, sqltypes.BytesBindVariable([]byte("c")), false},
	{BindVarCond{"a", true, true, QREqual, bvcstring("b")}, sqltypes.Int64BindVariable(1), false},

	{BindVarCond{"a", true, true, QRNotEqual, bvcstring("b")}, sqltypes.StringBindVariable("a"), true},
	{BindVarCond{"a", true, true, QRNotEqual, bvcstring("b")}, sqltypes.StringBindVariable("b"), false},
	{BindVarCond{"a", true, true, QRNotEqual, bvcstring("b")}, sqltypes.StringBindVariable("c"), true},

	{BindVarCond{"a", true, true, QRLessThan, bvcstring("b")}, sqltypes.StringBindVariable("a"), true},
	{BindVarCond{"a", true, true, QRLessThan, bvcstring("b")}, sqltypes.StringBindVariable("b"), false},
	{BindVarCond{"a", true, true, QRLessThan, bvcstring("b")}, sqltypes.StringBindVariable("c"), false},

	{BindVarCond{"a", true, true, QRGreaterEqual, bvcstring("b")}, sqltypes.StringBindVariable("a"), false},
	{BindVarCond{"a", true, true, QRGreaterEqual, bvcstring("b")}, sqltypes.StringBindVariable("b"), true},
	{BindVarCond{"a", true, true, QRGreaterEqual, bvcstring("b")}, sqltypes.StringBindVariable("c"), true},

	{BindVarCond{"a", true, true, QRGreaterThan, bvcstring("b")}, sqltypes.StringBindVariable("a"), false},
	{BindVarCond{"a", true, true, QRGreaterThan, bvcstring("b")}, sqltypes.StringBindVariable("b"), false},
	{BindVarCond{"a", true, true, QRGreaterThan, bvcstring("b")}, sqltypes.StringBindVariable("c"), true},

	{BindVarCond{"a", true, true, QRLessEqual, bvcstring("b")}, sqltypes.StringBindVariable("a"), true},
	{BindVarCond{"a", true, true, QRLessEqual, bvcstring("b")}, sqltypes.StringBindVariable("b"), true},
	{BindVarCond{"a", true, true, QRLessEqual, bvcstring("b")}, sqltypes.StringBindVariable("c"), false},

	{BindVarCond{"a", true, true, QRMatch, makere("a.*")}, sqltypes.StringBindVariable("c"), false},
	{BindVarCond{"a", true, true, QRMatch, makere("a.*")}, sqltypes.StringBindVariable("a"), true},
	{BindVarCond{"a", true, true, QRMatch, makere("a.*")}, sqltypes.Int64BindVariable(1), false},

	{BindVarCond{"a", true, true, QRNoMatch, makere("a.*")}, sqltypes.StringBindVariable("c"), true},
	{BindVarCond{"a", true, true, QRNoMatch, makere("a.*")}, sqltypes.StringBindVariable("a"), false},
	{BindVarCond{"a", true, true, QRNoMatch, makere("a.*")}, sqltypes.Int64BindVariable(1), true},
}

func makere(s string) bvcre {
	re, _ := regexp.Compile(s)
	return bvcre{re}
}

func TestBVConditions(t *testing.T) {
	bv := make(map[string]*querypb.BindVariable)
	for _, tcase := range bvtestcases {
		bv["a"] = tcase.bvval
		if bvMatch(tcase.bvc, bv) != tcase.expected {
			t.Errorf("bvmatch(%+v, %v): %v, want %v", tcase.bvc, tcase.bvval, !tcase.expected, tcase.expected)
		}
	}
}

func TestAction(t *testing.T) {
	qrs := New()

	qr1 := NewQueryRule("rule 1", "r1", QRFail)
	qr1.SetIPCond("123")

	qr2 := NewQueryRule("rule 2", "r2", QRFailRetry)
	qr2.SetUserCond("user")

	qr3 := NewQueryRule("rule 3", "r3", QRFail)
	qr3.AddBindVarCond("a", true, true, QREqual, uint64(1))

	qrs.Add(qr1)
	qrs.Add(qr2)
	qrs.Add(qr3)

	bv := make(map[string]*querypb.BindVariable)
	bv["a"] = sqltypes.Uint64BindVariable(0)
	action, desc := qrs.GetAction("123", "user1", bv)
	if action != QRFail {
		t.Errorf("want fail")
	}
	if desc != "rule 1" {
		t.Errorf("want rule 1, got %s", desc)
	}
	action, desc = qrs.GetAction("1234", "user", bv)
	if action != QRFailRetry {
		t.Errorf("want fail_retry")
	}
	if desc != "rule 2" {
		t.Errorf("want rule 2, got %s", desc)
	}
	action, _ = qrs.GetAction("1234", "user1", bv)
	if action != QRContinue {
		t.Errorf("want continue")
	}
	bv["a"] = sqltypes.Uint64BindVariable(1)
	action, desc = qrs.GetAction("1234", "user1", bv)
	if action != QRFail {
		t.Errorf("want fail")
	}
	if desc != "rule 3" {
		t.Errorf("want rule 2, got %s", desc)
	}
}

func TestImport(t *testing.T) {
	var qrs = New()
	jsondata := `[{
		"Description": "desc1",
		"Name": "name1",
		"RequestIP": "123.123.123",
		"User": "user",
		"Query": "query",
		"Plans": ["Select", "Insert"],
		"TableNames":["a", "b"],
		"BindVarConds": [{
			"Name": "bvname1",
			"OnAbsent": true,
			"Operator": ""
		},{
			"Name": "bvname2",
			"OnAbsent": true,
			"OnMismatch": true,
			"Operator": "==",
			"Value": 123
		}],
		"Action": "FAIL_RETRY"
	},{
		"Description": "desc2",
		"Name": "name2",
		"Action": "FAIL"
	}]`
	err := qrs.UnmarshalJSON([]byte(jsondata))
	if err != nil {
		t.Error(err)
		return
	}
	got := marshalled(qrs)
	want := compacted(jsondata)
	if got != want {
		t.Errorf("qrs:\n%s, want\n%s", got, want)
	}
}

type ValidJSONCase struct {
	input string
	op    Operator
	typ   int
}

const (
	UINT = iota
	INT
	STR
	REGEXP
)

var validjsons = []ValidJSONCase{
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "==", "Value": 18446744073709551615}]}]`, QREqual, UINT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "!=", "Value": 18446744073709551615}]}]`, QRNotEqual, UINT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "<", "Value": 18446744073709551615}]}]`, QRLessThan, UINT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": ">=", "Value": 18446744073709551615}]}]`, QRGreaterEqual, UINT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": ">", "Value": 18446744073709551615}]}]`, QRGreaterThan, UINT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "<=", "Value": 18446744073709551615}]}]`, QRLessEqual, UINT},

	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "==", "Value": -123}]}]`, QREqual, INT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "!=", "Value": -123}]}]`, QRNotEqual, INT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "<", "Value": -123}]}]`, QRLessThan, INT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": ">=", "Value": -123}]}]`, QRGreaterEqual, INT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": ">", "Value": -123}]}]`, QRGreaterThan, INT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "<=", "Value": -123}]}]`, QRLessEqual, INT},

	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "==", "Value": "123"}]}]`, QREqual, STR},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "!=", "Value": "123"}]}]`, QRNotEqual, STR},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "<", "Value": "123"}]}]`, QRLessThan, STR},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": ">=", "Value": "123"}]}]`, QRGreaterEqual, STR},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": ">", "Value": "123"}]}]`, QRGreaterThan, STR},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "<=", "Value": "123"}]}]`, QRLessEqual, STR},

	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "MATCH", "Value": "123"}]}]`, QRMatch, REGEXP},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "NOMATCH", "Value": "123"}]}]`, QRNoMatch, REGEXP},
}

func TestValidJSON(t *testing.T) {
	for i, tcase := range validjsons {
		qrs := New()
		err := qrs.UnmarshalJSON([]byte(tcase.input))
		if err != nil {
			t.Fatalf("Unexpected error for case %d: %v", i, err)
		}
		bvc := qrs.rules[0].bindVarConds[0]
		if bvc.op != tcase.op {
			t.Errorf("want %v, got %v", tcase.op, bvc.op)
		}
		switch tcase.typ {
		case UINT:
			if bvc.value.(bvcuint64) != bvcuint64(18446744073709551615) {
				t.Errorf("want %v, got %v", uint64(18446744073709551615), bvc.value.(bvcuint64))
			}
		case INT:
			if bvc.value.(bvcint64) != -123 {
				t.Errorf("want %v, got %v", -123, bvc.value.(bvcint64))
			}
		case STR:
			if bvc.value.(bvcstring) != "123" {
				t.Errorf("want %v, got %v", "123", bvc.value.(bvcint64))
			}
		case REGEXP:
			if bvc.value.(bvcre).re == nil {
				t.Errorf("want non-nil")
			}
		}
	}
}

type InvalidJSONCase struct {
	input, err string
}

var invalidjsons = []InvalidJSONCase{
	{`[{"Name": 1 }]`, "want string for Name"},
	{`[{"Description": 1 }]`, "want string for Description"},
	{`[{"RequestIP": 1 }]`, "want string for RequestIP"},
	{`[{"User": 1 }]`, "want string for User"},
	{`[{"Query": 1 }]`, "want string for Query"},
	{`[{"Plans": 1 }]`, "want list for Plans"},
	{`[{"TableNames": 1 }]`, "want list for TableNames"},
	{`[{"BindVarConds": 1 }]`, "want list for BindVarConds"},
	{`[{"RequestIP": "[" }]`, "could not set IP condition: ["},
	{`[{"User": "[" }]`, "could not set User condition: ["},
	{`[{"Query": "[" }]`, "could not set Query condition: ["},
	{`[{"Plans": [1] }]`, "want string for Plans"},
	{`[{"Plans": ["invalid"] }]`, "invalid plan name: invalid"},
	{`[{"TableNames": [1] }]`, "want string for TableNames"},
	{`[{"BindVarConds": [1] }]`, "want json object for bind var conditions"},
	{`[{"BindVarConds": [{}] }]`, "Name missing in BindVarConds"},
	{`[{"BindVarConds": [{"Name": 1}] }]`, "want string for Name in BindVarConds"},
	{`[{"BindVarConds": [{"Name": "a"}] }]`, "OnAbsent missing in BindVarConds"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": 1}] }]`, "want bool for OnAbsent"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true}]}]`, "Operator missing in BindVarConds"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "a"}]}]`, "invalid Operator a"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "=="}]}]`, "Value missing in BindVarConds"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "==", "Value": 1.2}]}]`, "want int64/uint64: 1.2"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "==", "Value": {}}]}]`, "want string or number: map[]"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "MATCH", "Value": 1}]}]`, "want string: 1"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "NOMATCH", "Value": 1}]}]`, "want string: 1"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": 123, "Value": "1"}]}]`, "want string for Operator"},
	{`[{"Unknown": [{"Name": "a"}]}]`, "unrecognized tag Unknown"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "<=", "Value": "1"}]}]`, "OnMismatch missing in BindVarConds"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "OnMismatch": true, "Operator": "MATCH", "Value": "["}]}]`, "processing [: error parsing regexp: missing closing ]: `[$`"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "OnMismatch": true, "Operator": "NOMATCH", "Value": "["}]}]`, "processing [: error parsing regexp: missing closing ]: `[$`"},
	{`[{"Action": 1 }]`, "want string for Action"},
	{`[{"Action": "foo" }]`, "invalid Action foo"},
}

func TestInvalidJSON(t *testing.T) {
	for _, tcase := range invalidjsons {
		qrs := New()
		err := qrs.UnmarshalJSON([]byte(tcase.input))
		if err == nil {
			t.Errorf("want error for case %q", tcase.input)
			continue
		}
		recvd := strings.Replace(err.Error(), "fatal: ", "", 1)
		if recvd != tcase.err {
			t.Errorf("invalid json: %s, want '%v', got '%v'", tcase.input, tcase.err, recvd)
		}
	}
	qrs := New()
	err := qrs.UnmarshalJSON([]byte(`{`))
	if code := vterrors.Code(err); code != vtrpcpb.Code_INVALID_ARGUMENT {
		t.Errorf("qrs.UnmarshalJSON: %v, want %v", code, vtrpcpb.Code_INVALID_ARGUMENT)
	}
}

func TestBuildQueryRuleActionFail(t *testing.T) {
	var ruleInfo map[string]interface{}
	err := json.Unmarshal([]byte(`{"Action": "FAIL" }`), &ruleInfo)
	if err != nil {
		t.Fatalf("failed to unmarshal json, got error: %v", err)
	}
	qr, err := BuildQueryRule(ruleInfo)
	if err != nil {
		t.Fatalf("build query rule should succeed")
	}
	if qr.act != QRFail {
		t.Fatalf("action should fail")
	}
}

func TestBadAddBindVarCond(t *testing.T) {
	qr1 := NewQueryRule("rule 1", "r1", QRFail)
	err := qr1.AddBindVarCond("a", true, false, QRMatch, uint64(1))
	if err == nil {
		t.Fatalf("invalid op: QRMatch for value type: uint64")
	}
}

func TestOpNames(t *testing.T) {
	want := []string{
		"",
		"==",
		"!=",
		"<",
		">=",
		">",
		"<=",
		"MATCH",
		"NOMATCH",
	}
	if !reflect.DeepEqual(opnames, want) {
		t.Errorf("opnames: \n%v, want \n%v", opnames, want)
	}
}

func compacted(in string) string {
	dst := bytes.NewBuffer(nil)
	err := json.Compact(dst, []byte(in))
	if err != nil {
		panic(err)
	}
	return dst.String()
}

func marshalled(in interface{}) string {
	b, err := json.Marshal(in)
	if err != nil {
		panic(err)
	}
	return string(b)
}
