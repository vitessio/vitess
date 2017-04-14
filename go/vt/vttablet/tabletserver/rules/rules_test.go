// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rules

import (
	"bytes"
	"encoding/json"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"github.com/youtube/vitess/go/vt/vterrors"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
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
	qr1.AddPlanCond(planbuilder.PlanPassSelect)
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
	qr1.AddPlanCond(planbuilder.PlanPassSelect)
	qr1.AddBindVarCond("a", true, false, QRNoOp, nil)

	qr2 := NewQueryRule("rule 2", "r2", QRFail)
	qr2.AddPlanCond(planbuilder.PlanPassSelect)
	qr2.AddPlanCond(planbuilder.PlanSelectLock)
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

	qrs1 := qrs.FilterByPlan("select", planbuilder.PlanPassSelect, "a")
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

	qrs1 = qrs.FilterByPlan("insert", planbuilder.PlanPassSelect, "a")
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

	qrs1 = qrs.FilterByPlan("insert", planbuilder.PlanSelectLock, "a")
	got = marshalled(qrs1)
	if got != want {
		t.Errorf("qrs1:\n%s, want\n%s", got, want)
	}

	qrs1 = qrs.FilterByPlan("select", planbuilder.PlanInsertPK, "a")
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

	qrs1 = qrs.FilterByPlan("sel", planbuilder.PlanInsertPK, "a")
	if qrs1.rules != nil {
		t.Errorf("want nil, got non-nil")
	}

	qrs1 = qrs.FilterByPlan("table", planbuilder.PlanPassDML, "b")
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

	qrs1 = qrs.FilterByPlan("sel", planbuilder.PlanInsertPK, "a")
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
	if qrsnil2 := qrsnil1.FilterByPlan("", planbuilder.PlanPassSelect, "a"); qrsnil2.rules != nil {
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

	qr.AddPlanCond(planbuilder.PlanPassSelect)
	qr.AddPlanCond(planbuilder.PlanInsertPK)

	if qr.plans[0] != planbuilder.PlanPassSelect {
		t.Errorf("want PASS_SELECT, got %s", qr.plans[0].String())
	}
	if qr.plans[1] != planbuilder.PlanInsertPK {
		t.Errorf("want INSERT_PK, got %s", qr.plans[1].String())
	}

	qr.AddTableCond("a")
	if qr.tableNames[0] != "a" {
		t.Errorf("want a, got %s", qr.tableNames[0])
	}
}

func TestBindVarStruct(t *testing.T) {
	qr := NewQueryRule("rule 1", "r1", QRFail)

	var err error
	err = qr.AddBindVarCond("b", false, true, QRNoOp, nil)
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

	{"a", true, true, QRIn, &topodatapb.KeyRange{}, false},
	{"a", true, true, QRNotIn, &topodatapb.KeyRange{}, false},

	{"a", true, true, QRMatch, int64(1), true},
	{"a", true, true, QRNoMatch, int64(1), true},
	{"a", true, true, QRMatch, "[", true},
	{"a", true, true, QRNoMatch, "[", true},

	{"a", true, true, QRIn, int64(1), true},
	{"a", true, true, QRNotIn, int64(1), true},

	{"a", true, true, QREqual, int32(1), true},
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
	bvval    interface{}
	expected bool
}

var bvtestcases = []BindVarTestCase{
	{BindVarCond{"b", true, true, QRNoOp, nil}, 1, true},
	{BindVarCond{"b", false, true, QRNoOp, nil}, 1, false},
	{BindVarCond{"a", true, true, QRNoOp, nil}, 1, false},
	{BindVarCond{"a", false, true, QRNoOp, nil}, 1, true},

	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, &querypb.BindVariable{
		Type:  querypb.Type_INT24,
		Value: []byte{'1'},
	}, false},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, &querypb.BindVariable{
		Type:  querypb.Type_INT32,
		Value: []byte{'1', '0'},
	}, true},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, &querypb.BindVariable{
		Type:  querypb.Type_VARCHAR,
		Value: []byte{'1', '0'},
	}, true},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, &querypb.BindVariable{
		Type:  querypb.Type_VARCHAR,
		Value: []byte{'-', '1', '0'},
	}, false},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, int(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, int(10), true},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, int8(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, int8(10), true},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, int16(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, int32(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, int64(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, uint(10), true},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, uint8(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, uint8(10), true},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, uint16(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, uint32(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, uint64(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, int8(-1), false},
	{BindVarCond{"a", true, true, QREqual, bvcuint64(10)}, "abc", true},

	{BindVarCond{"a", true, true, QRNotEqual, bvcuint64(10)}, int8(1), true},
	{BindVarCond{"a", true, true, QRNotEqual, bvcuint64(10)}, int8(10), false},
	{BindVarCond{"a", true, true, QRNotEqual, bvcuint64(10)}, int8(11), true},
	{BindVarCond{"a", true, true, QRNotEqual, bvcuint64(10)}, int8(-1), true},

	{BindVarCond{"a", true, true, QRLessThan, bvcuint64(10)}, int8(1), true},
	{BindVarCond{"a", true, true, QRLessThan, bvcuint64(10)}, int8(10), false},
	{BindVarCond{"a", true, true, QRLessThan, bvcuint64(10)}, int8(11), false},
	{BindVarCond{"a", true, true, QRLessThan, bvcuint64(10)}, int8(-1), true},

	{BindVarCond{"a", true, true, QRGreaterEqual, bvcuint64(10)}, int8(1), false},
	{BindVarCond{"a", true, true, QRGreaterEqual, bvcuint64(10)}, int8(10), true},
	{BindVarCond{"a", true, true, QRGreaterEqual, bvcuint64(10)}, int8(11), true},
	{BindVarCond{"a", true, true, QRGreaterEqual, bvcuint64(10)}, int8(-1), false},

	{BindVarCond{"a", true, true, QRGreaterThan, bvcuint64(10)}, int8(1), false},
	{BindVarCond{"a", true, true, QRGreaterThan, bvcuint64(10)}, int8(10), false},
	{BindVarCond{"a", true, true, QRGreaterThan, bvcuint64(10)}, int8(11), true},
	{BindVarCond{"a", true, true, QRGreaterThan, bvcuint64(10)}, int8(-1), false},

	{BindVarCond{"a", true, true, QRLessEqual, bvcuint64(10)}, int8(1), true},
	{BindVarCond{"a", true, true, QRLessEqual, bvcuint64(10)}, int8(10), true},
	{BindVarCond{"a", true, true, QRLessEqual, bvcuint64(10)}, int8(11), false},
	{BindVarCond{"a", true, true, QRLessEqual, bvcuint64(10)}, int8(-1), true},

	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, &querypb.BindVariable{
		Type:  querypb.Type_INT24,
		Value: []byte{'1'},
	}, false},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, &querypb.BindVariable{
		Type:  querypb.Type_INT32,
		Value: []byte{'1', '0'},
	}, true},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, &querypb.BindVariable{
		Type:  querypb.Type_VARCHAR,
		Value: []byte{'1', '0'},
	}, true},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, &querypb.BindVariable{
		Type:  querypb.Type_VARCHAR,
		Value: []byte{'-', '1', '0'},
	}, false},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, int(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, int8(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, int8(10), true},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, int16(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, int32(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, int64(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, uint(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, uint(0xFFFFFFFFFFFFFFFF), false},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, uint8(10), true},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, uint16(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, uint32(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, uint64(1), false},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, uint64(0xFFFFFFFFFFFFFFFF), false},
	{BindVarCond{"a", true, true, QREqual, bvcint64(10)}, "abc", true},

	{BindVarCond{"a", true, true, QRNotEqual, bvcint64(10)}, int8(1), true},
	{BindVarCond{"a", true, true, QRNotEqual, bvcint64(10)}, int8(10), false},
	{BindVarCond{"a", true, true, QRNotEqual, bvcint64(10)}, int8(11), true},
	{BindVarCond{"a", true, true, QRNotEqual, bvcint64(10)}, uint64(0xFFFFFFFFFFFFFFFF), true},

	{BindVarCond{"a", true, true, QRLessThan, bvcint64(10)}, int8(1), true},
	{BindVarCond{"a", true, true, QRLessThan, bvcint64(10)}, int8(10), false},
	{BindVarCond{"a", true, true, QRLessThan, bvcint64(10)}, int8(11), false},
	{BindVarCond{"a", true, true, QRLessThan, bvcint64(10)}, uint64(0xFFFFFFFFFFFFFFFF), false},

	{BindVarCond{"a", true, true, QRGreaterEqual, bvcint64(10)}, int8(1), false},
	{BindVarCond{"a", true, true, QRGreaterEqual, bvcint64(10)}, int8(10), true},
	{BindVarCond{"a", true, true, QRGreaterEqual, bvcint64(10)}, int8(11), true},
	{BindVarCond{"a", true, true, QRGreaterEqual, bvcint64(10)}, uint64(0xFFFFFFFFFFFFFFFF), true},

	{BindVarCond{"a", true, true, QRGreaterThan, bvcint64(10)}, int8(1), false},
	{BindVarCond{"a", true, true, QRGreaterThan, bvcint64(10)}, int8(10), false},
	{BindVarCond{"a", true, true, QRGreaterThan, bvcint64(10)}, int8(11), true},
	{BindVarCond{"a", true, true, QRGreaterThan, bvcint64(10)}, uint64(0xFFFFFFFFFFFFFFFF), true},

	{BindVarCond{"a", true, true, QRLessEqual, bvcint64(10)}, int8(1), true},
	{BindVarCond{"a", true, true, QRLessEqual, bvcint64(10)}, int8(10), true},
	{BindVarCond{"a", true, true, QRLessEqual, bvcint64(10)}, int8(11), false},
	{BindVarCond{"a", true, true, QRLessEqual, bvcint64(10)}, uint64(0xFFFFFFFFFFFFFFFF), false},

	{BindVarCond{"a", true, true, QREqual, bvcstring("b")}, &querypb.BindVariable{
		Type:  querypb.Type_VARCHAR,
		Value: []byte{'a'},
	}, false},
	{BindVarCond{"a", true, true, QREqual, bvcstring("b")}, &querypb.BindVariable{
		Type:  querypb.Type_VARCHAR,
		Value: []byte{'b'},
	}, true},
	{BindVarCond{"a", true, true, QREqual, bvcstring("b")}, "a", false},
	{BindVarCond{"a", true, true, QREqual, bvcstring("b")}, "b", true},
	{BindVarCond{"a", true, true, QREqual, bvcstring("b")}, "c", false},
	{BindVarCond{"a", true, true, QREqual, bvcstring("b")}, []byte("a"), false},
	{BindVarCond{"a", true, true, QREqual, bvcstring("b")}, []byte("b"), true},
	{BindVarCond{"a", true, true, QREqual, bvcstring("b")}, []byte("c"), false},
	{BindVarCond{"a", true, true, QREqual, bvcstring("b")}, int8(1), true},

	{BindVarCond{"a", true, true, QRNotEqual, bvcstring("b")}, "a", true},
	{BindVarCond{"a", true, true, QRNotEqual, bvcstring("b")}, "b", false},
	{BindVarCond{"a", true, true, QRNotEqual, bvcstring("b")}, "c", true},

	{BindVarCond{"a", true, true, QRLessThan, bvcstring("b")}, "a", true},
	{BindVarCond{"a", true, true, QRLessThan, bvcstring("b")}, "b", false},
	{BindVarCond{"a", true, true, QRLessThan, bvcstring("b")}, "c", false},

	{BindVarCond{"a", true, true, QRGreaterEqual, bvcstring("b")}, "a", false},
	{BindVarCond{"a", true, true, QRGreaterEqual, bvcstring("b")}, "b", true},
	{BindVarCond{"a", true, true, QRGreaterEqual, bvcstring("b")}, "c", true},

	{BindVarCond{"a", true, true, QRGreaterThan, bvcstring("b")}, "a", false},
	{BindVarCond{"a", true, true, QRGreaterThan, bvcstring("b")}, "b", false},
	{BindVarCond{"a", true, true, QRGreaterThan, bvcstring("b")}, "c", true},

	{BindVarCond{"a", true, true, QRLessEqual, bvcstring("b")}, "a", true},
	{BindVarCond{"a", true, true, QRLessEqual, bvcstring("b")}, "b", true},
	{BindVarCond{"a", true, true, QRLessEqual, bvcstring("b")}, "c", false},

	{BindVarCond{"a", true, true, QRMatch, makere("a.*")}, "c", false},
	{BindVarCond{"a", true, true, QRMatch, makere("a.*")}, "a", true},
	{BindVarCond{"a", true, true, QRMatch, makere("a.*")}, int8(1), true},

	{BindVarCond{"a", true, true, QRNoMatch, makere("a.*")}, "c", true},
	{BindVarCond{"a", true, true, QRNoMatch, makere("a.*")}, "a", false},
	{BindVarCond{"a", true, true, QRNoMatch, makere("a.*")}, int8(1), true},

	{BindVarCond{"a", true, true, QRIn, numKeyRange(0x4000000000000000, 0x6000000000000000)}, uint64(0), false},
	{BindVarCond{"a", true, true, QRIn, numKeyRange(0x4000000000000000, 0x6000000000000000)}, uint64(0x5000000000000000), true},
	{BindVarCond{"a", true, true, QRIn, numKeyRange(0x4000000000000000, 0x6000000000000000)}, uint64(0x7000000000000000), false},
	{BindVarCond{"a", true, true, QRIn, numKeyRange(0x4000000000000000, 0x6000000000000000)}, int(-1), false},
	{BindVarCond{"a", true, true, QRIn, numKeyRange(0x4000000000000000, 0x6000000000000000)}, int16(-1), false},
	{BindVarCond{"a", true, true, QRIn, numKeyRange(0x4000000000000000, 0x6000000000000000)}, int32(-1), false},
	{BindVarCond{"a", true, true, QRIn, numKeyRange(0x4000000000000000, 0x6000000000000000)}, int64(-1), false},
	{BindVarCond{"a", true, true, QRIn, strKeyRange("b", "d")}, "a", false},
	{BindVarCond{"a", true, true, QRIn, strKeyRange("b", "d")}, "c", true},
	{BindVarCond{"a", true, true, QRIn, strKeyRange("b", "d")}, "e", false},
	{BindVarCond{"a", true, true, QRIn, strKeyRange("b", "d")}, float64(1.0), true},

	{BindVarCond{"a", true, true, QRNotIn, numKeyRange(0x4000000000000000, 0x6000000000000000)}, uint64(0), true},
	{BindVarCond{"a", true, true, QRNotIn, numKeyRange(0x4000000000000000, 0x6000000000000000)}, uint64(0x5000000000000000), false},
	{BindVarCond{"a", true, true, QRNotIn, numKeyRange(0x4000000000000000, 0x6000000000000000)}, uint64(0x7000000000000000), true},
	{BindVarCond{"a", true, true, QRNotIn, numKeyRange(0x4000000000000000, 0x6000000000000000)}, int(-1), true},
	{BindVarCond{"a", true, true, QRNotIn, numKeyRange(0x4000000000000000, 0x6000000000000000)}, int16(-1), true},
	{BindVarCond{"a", true, true, QRNotIn, numKeyRange(0x4000000000000000, 0x6000000000000000)}, int32(-1), true},
	{BindVarCond{"a", true, true, QRNotIn, numKeyRange(0x4000000000000000, 0x6000000000000000)}, int64(-1), true},
	{BindVarCond{"a", true, true, QRNotIn, strKeyRange("b", "d")}, "a", true},
	{BindVarCond{"a", true, true, QRNotIn, strKeyRange("b", "d")}, "c", false},
	{BindVarCond{"a", true, true, QRNotIn, strKeyRange("b", "d")}, "e", true},
	{BindVarCond{"a", true, true, QRNotIn, strKeyRange("b", "d")}, float64(1.0), true},
}

func makere(s string) bvcre {
	re, _ := regexp.Compile(s)
	return bvcre{re}
}

func numKeyRange(start, end uint64) *bvcKeyRange {
	kr := topodatapb.KeyRange{
		Start: key.Uint64Key(start).Bytes(),
		End:   key.Uint64Key(end).Bytes(),
	}
	b := bvcKeyRange(kr)
	return &b
}

func strKeyRange(start, end string) *bvcKeyRange {
	kr := topodatapb.KeyRange{
		Start: []byte(start),
		End:   []byte(end),
	}
	b := bvcKeyRange(kr)
	return &b
}

func TestBVConditions(t *testing.T) {
	bv := make(map[string]interface{})
	for i, tcase := range bvtestcases {
		bv["a"] = tcase.bvval
		if bvMatch(tcase.bvc, bv) != tcase.expected {
			t.Errorf("test %d: want %v for %#v, %#v", i, tcase.expected, tcase.bvc, tcase.bvval)
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

	bv := make(map[string]interface{})
	bv["a"] = uint64(0)
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
	action, desc = qrs.GetAction("1234", "user1", bv)
	if action != QRContinue {
		t.Errorf("want continue")
	}
	bv["a"] = uint64(1)
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
		"Plans": ["PASS_SELECT", "INSERT_PK"],
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
	KEYRANGE
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

	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "IN", "Value": {"Start": "1", "End": "2"}}]}]`, QRIn, KEYRANGE},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "NOTIN", "Value": {"Start": "1", "End": "2"}}]}]`, QRNotIn, KEYRANGE},
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
		case KEYRANGE:
			if kr := bvc.value.(*bvcKeyRange); string(kr.Start) != "1" || string(kr.End) != "2" {
				t.Errorf(`Expecting {"1", "2"}, got %v`, kr)
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

func TestBuildQueryRuleFailureModes(t *testing.T) {
	var err error
	var errStr string

	_, err = BuildQueryRule(map[string]interface{}{
		"BindVarConds": []interface{}{map[string]interface{}{"Name": "a", "OnAbsent": true, "Operator": QRIn, "Value": &topodatapb.KeyRange{}}},
	})
	if err == nil {
		t.Fatalf("should get an error")
	}
	errStr = strings.Replace(err.Error(), "fatal: ", "", 1)
	if errStr != "want string for Operator" {
		t.Fatalf("expect to get error: want string for Operator, but got: %v", err)
	}

	_, err = BuildQueryRule(map[string]interface{}{
		"BindVarConds": []interface{}{map[string]interface{}{"Name": "a", "OnAbsent": true, "Operator": "IN", "Value": 1}},
	})
	if err == nil {
		t.Fatalf("should get an error")
	}
	errStr = strings.Replace(err.Error(), "fatal: ", "", 1)
	if errStr != "want keyrange for Value" {
		t.Fatalf("expect to get error: want keyrange for Value, but got: %v", err)
	}

	_, err = BuildQueryRule(map[string]interface{}{
		"BindVarConds": []interface{}{map[string]interface{}{"Name": "a", "OnAbsent": true, "Operator": "IN", "Value": map[string]interface{}{}}},
	})
	if err == nil {
		t.Fatalf("should get an error")
	}
	errStr = strings.Replace(err.Error(), "fatal: ", "", 1)
	if errStr != "Start missing in KeyRange" {
		t.Fatalf("expect to get error: Start missing in KeyRange, but got: %v", err)
	}

	_, err = BuildQueryRule(map[string]interface{}{
		"BindVarConds": []interface{}{map[string]interface{}{"Name": "a", "OnAbsent": true, "Operator": "IN", "Value": map[string]interface{}{"Start": 1}}},
	})
	if err == nil {
		t.Fatalf("should get an error")
	}
	errStr = strings.Replace(err.Error(), "fatal: ", "", 1)
	if errStr != "want string for Start" {
		t.Fatalf("expect to get error: want string for Start, but got: %v", err)
	}

	_, err = BuildQueryRule(map[string]interface{}{
		"BindVarConds": []interface{}{map[string]interface{}{"Name": "a", "OnAbsent": true, "Operator": "IN", "Value": map[string]interface{}{"Start": "1"}}},
	})
	if err == nil {
		t.Fatalf("should get an error")
	}
	errStr = strings.Replace(err.Error(), "fatal: ", "", 1)
	if errStr != "End missing in KeyRange" {
		t.Fatalf("expect to get error: End missing in KeyRange, but got: %v", err)
	}

	_, err = BuildQueryRule(map[string]interface{}{
		"BindVarConds": []interface{}{map[string]interface{}{"Name": "a", "OnAbsent": true, "Operator": "IN", "Value": map[string]interface{}{"Start": "1", "End": 2}}},
	})
	if err == nil {
		t.Fatalf("should get an error")
	}
	errStr = strings.Replace(err.Error(), "fatal: ", "", 1)
	if errStr != "want string for End" {
		t.Fatalf("expect to get error: want string for End, but got: %v", err)
	}

	_, err = BuildQueryRule(map[string]interface{}{
		"BindVarConds": []interface{}{map[string]interface{}{"Name": "a", "OnAbsent": true, "OnMismatch": "invalid", "Operator": "IN", "Value": map[string]interface{}{"Start": "1", "End": "2"}}},
	})
	if err == nil {
		t.Fatalf("should get an error")
	}
	errStr = strings.Replace(err.Error(), "fatal: ", "", 1)
	if errStr != "want bool for OnMismatch" {
		t.Fatalf("expect to get error: want bool for OnMismatch, but got: %v", err)
	}
}

func TestBadAddBindVarCond(t *testing.T) {
	qr1 := NewQueryRule("rule 1", "r1", QRFail)
	err := qr1.AddBindVarCond("a", true, false, QRMatch, uint64(1))
	if err == nil {
		t.Fatalf("invalid op: QRMatch for value type: uint64")
	}
	err = qr1.AddBindVarCond("a", true, false, QRNotIn, "test")
	if err == nil {
		t.Fatalf("invalid op: QRNotIn for value type: string")
	}
	err = qr1.AddBindVarCond("a", true, false, QRLessThan, &topodatapb.KeyRange{})
	if err == nil {
		t.Fatalf("invalid op: QRLessThan for value type: *topodatapb.KeyRange")
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
		"IN",
		"NOTIN",
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
