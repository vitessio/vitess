// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"regexp"
	"strings"
	"testing"

	"code.google.com/p/vitess/go/vt/key"
	"code.google.com/p/vitess/go/vt/sqlparser"
)

func TestQueryRules(t *testing.T) {
	qrs := NewQueryRules()
	qr1 := NewQueryRule("rule 1", "r1", QR_FAIL_QUERY)
	qr2 := NewQueryRule("rule 2", "r2", QR_FAIL_QUERY)
	qrs.Add(qr1)
	qrs.Add(qr2)

	qrf := qrs.Find("r1")
	if qrf != qr1 {
		t.Errorf("want:\n%#v\nreceived:\n%#v", qr1, qrf)
	}

	qrf = qrs.Find("r2")
	if qrf != qr2 {
		t.Errorf("want:\n%#v\nreceived:\n%#v", qr2, qrf)
	}

	if qrs.rules[0] != qr1 {
		t.Errorf("want:\n%#v\nreceived:\n%#v", qr1, qrs.rules[0])
	}

	qrf = qrs.Delete("r1")
	if qrf != qr1 {
		t.Errorf("want:\n%#v\nreceived:\n%#v", qr1, qrf)
	}

	if len(qrs.rules) != 1 {
		t.Errorf("want 1, received %d", len(qrs.rules))
	}

	if qrs.rules[0] != qr2 {
		t.Errorf("want:\n%#v\nreceived:\n%#v", qr2, qrf)
	}
}

// TestCopy tests for deep copy
func TestCopy(t *testing.T) {
	qrs := NewQueryRules()
	qr1 := NewQueryRule("rule 1", "r1", QR_FAIL_QUERY)
	qr1.AddPlanCond(sqlparser.PLAN_PASS_SELECT)
	qr1.AddBindVarCond("a", true, false, QR_NOOP, nil)

	qr2 := NewQueryRule("rule 2", "r2", QR_FAIL_QUERY)
	qrs.Add(qr1)
	qrs.Add(qr2)

	qrs1 := qrs.Copy()
	if l := len(qrs1.rules); l != 2 {
		t.Errorf("want 2, received %d", l)
	}

	qrf1 := qrs1.Find("r1")
	if qr1 == qrf1 {
		t.Errorf("want false, got true")
	}

	qr1.plans[0] = sqlparser.PLAN_INSERT_PK
	if qr1.plans[0] == qrf1.plans[0] {
		t.Errorf("want false, got true")
	}

	if qrf1.bindVarConds[0].name != "a" {
		t.Errorf("expecting a, received %s", qrf1.bindVarConds[1].name)
	}

	qrs2 := NewQueryRules()
	if qrs3 := qrs2.Copy(); qrs3.rules != nil {
		t.Errorf("want nil, got non-nil")
	}
}

func TestFilterByPlan(t *testing.T) {
	qrs := NewQueryRules()

	qr1 := NewQueryRule("rule 1", "r1", QR_FAIL_QUERY)
	qr1.SetIPCond("123")
	qr1.SetQueryCond("select")
	qr1.AddPlanCond(sqlparser.PLAN_PASS_SELECT)
	qr1.AddBindVarCond("a", true, false, QR_NOOP, nil)

	qr2 := NewQueryRule("rule 2", "r2", QR_FAIL_QUERY)
	qr2.AddPlanCond(sqlparser.PLAN_PASS_SELECT)
	qr2.AddPlanCond(sqlparser.PLAN_SELECT_PK)
	qr2.AddBindVarCond("a", true, false, QR_NOOP, nil)

	qr3 := NewQueryRule("rule 3", "r3", QR_FAIL_QUERY)
	qr3.SetQueryCond("sele.*")
	qr3.AddBindVarCond("a", true, false, QR_NOOP, nil)

	qrs.Add(qr1)
	qrs.Add(qr2)
	qrs.Add(qr3)

	qrs1 := qrs.filterByPlan("select", sqlparser.PLAN_PASS_SELECT)
	if l := len(qrs1.rules); l != 3 {
		t.Errorf("want 3, received %d", l)
	}
	if qrs1.rules[0].Name != "r1" {
		t.Errorf("want r1, got %s", qrs1.rules[0].Name)
	}
	if qrs1.rules[0].requestIP == nil {
		t.Errorf("want non-nil, got nil")
	}
	if qrs1.rules[0].plans != nil {
		t.Errorf("want nil, got non-nil")
	}

	qrs1 = qrs.filterByPlan("insert", sqlparser.PLAN_PASS_SELECT)
	if l := len(qrs1.rules); l != 1 {
		t.Errorf("want 1, received %d", l)
	}
	if qrs1.rules[0].Name != "r2" {
		t.Errorf("want r2, got %s", qrs1.rules[0].Name)
	}

	qrs1 = qrs.filterByPlan("insert", sqlparser.PLAN_SELECT_PK)
	if l := len(qrs1.rules); l != 1 {
		t.Errorf("want 1, received %d", l)
	}
	if qrs1.rules[0].Name != "r2" {
		t.Errorf("want r2, got %s", qrs1.rules[0].Name)
	}

	qrs1 = qrs.filterByPlan("select", sqlparser.PLAN_INSERT_PK)
	if l := len(qrs1.rules); l != 1 {
		t.Errorf("want 1, received %d", l)
	}
	if qrs1.rules[0].Name != "r3" {
		t.Errorf("want r3, got %s", qrs1.rules[0].Name)
	}

	qrs1 = qrs.filterByPlan("sel", sqlparser.PLAN_INSERT_PK)
	if qrs1.rules != nil {
		t.Errorf("want nil, got non-nil")
	}

	qr4 := NewQueryRule("rule 4", "r4", QR_FAIL_QUERY)
	qrs.Add(qr4)

	qrs1 = qrs.filterByPlan("sel", sqlparser.PLAN_INSERT_PK)
	if l := len(qrs1.rules); l != 1 {
		t.Errorf("want 1, received %d", l)
	}
	if qrs1.rules[0].Name != "r4" {
		t.Errorf("want r4, got %s", qrs1.rules[0].Name)
	}

	qrsnil1 := NewQueryRules()
	if qrsnil2 := qrsnil1.filterByPlan("", sqlparser.PLAN_PASS_SELECT); qrsnil2.rules != nil {
		t.Errorf("want nil, got non-nil")
	}
}

func TestQueryRule(t *testing.T) {
	qr := NewQueryRule("rule 1", "r1", QR_FAIL_QUERY)
	err := qr.SetIPCond("123")
	if err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if !qr.requestIP.MatchString("123") {
		t.Errorf("expecting match")
	}
	if qr.requestIP.MatchString("1234") {
		t.Errorf("expecting no match")
	}
	if qr.requestIP.MatchString("12") {
		t.Errorf("expecting no match")
	}
	err = qr.SetIPCond("[")
	if err == nil {
		t.Errorf("want error")
	}

	qr.AddPlanCond(sqlparser.PLAN_PASS_SELECT)
	qr.AddPlanCond(sqlparser.PLAN_INSERT_PK)

	if qr.plans[0] != sqlparser.PLAN_PASS_SELECT {
		t.Errorf("expecting PASS_SELECT, received %s", qr.plans[0].String())
	}
	if qr.plans[1] != sqlparser.PLAN_INSERT_PK {
		t.Errorf("expecting INSERT_PK, received %s", qr.plans[1].String())
	}
}

func TestBindVarStruct(t *testing.T) {
	qr := NewQueryRule("rule 1", "r1", QR_FAIL_QUERY)

	var err error
	err = qr.AddBindVarCond("b", false, true, QR_NOOP, nil)
	err = qr.AddBindVarCond("a", true, false, QR_NOOP, nil)
	if err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if qr.bindVarConds[1].name != "a" {
		t.Errorf("expecting a, received %s", qr.bindVarConds[1].name)
	}
	if !qr.bindVarConds[1].onAbsent {
		t.Errorf("expecting true, received false")
	}
	if qr.bindVarConds[1].onMismatch {
		t.Errorf("expecting false, received true")
	}
	if qr.bindVarConds[1].op != QR_NOOP {
		t.Errorf("exepecting NOOP, received %s", qr.bindVarConds[1])
	}
	if qr.bindVarConds[1].value != nil {
		t.Errorf("expecting nil, received %#v", qr.bindVarConds[1].value)
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
	{"a", true, true, QR_EQ, uint64(1), false},
	{"a", true, true, QR_NE, uint64(1), false},
	{"a", true, true, QR_LT, uint64(1), false},
	{"a", true, true, QR_GE, uint64(1), false},
	{"a", true, true, QR_GT, uint64(1), false},
	{"a", true, true, QR_LE, uint64(1), false},

	{"a", true, true, QR_EQ, int64(1), false},
	{"a", true, true, QR_NE, int64(1), false},
	{"a", true, true, QR_LT, int64(1), false},
	{"a", true, true, QR_GE, int64(1), false},
	{"a", true, true, QR_GT, int64(1), false},
	{"a", true, true, QR_LE, int64(1), false},

	{"a", true, true, QR_EQ, "a", false},
	{"a", true, true, QR_NE, "a", false},
	{"a", true, true, QR_LT, "a", false},
	{"a", true, true, QR_GE, "a", false},
	{"a", true, true, QR_GT, "a", false},
	{"a", true, true, QR_LE, "a", false},
	{"a", true, true, QR_MATCH, "a", false},
	{"a", true, true, QR_NOMATCH, "a", false},

	{"a", true, true, QR_IN, key.KeyRange{}, false},
	{"a", true, true, QR_NOTIN, key.KeyRange{}, false},

	{"a", true, true, QR_MATCH, int64(1), true},
	{"a", true, true, QR_NOMATCH, int64(1), true},
	{"a", true, true, QR_MATCH, "[", true},
	{"a", true, true, QR_NOMATCH, "[", true},

	{"a", true, true, QR_IN, int64(1), true},
	{"a", true, true, QR_NOTIN, int64(1), true},

	{"a", true, true, QR_EQ, int32(1), true},
}

func TestBVCreation(t *testing.T) {
	qr := NewQueryRule("rule 1", "r1", QR_FAIL_QUERY)
	for i, tcase := range creationCases {
		err := qr.AddBindVarCond(tcase.name, tcase.onAbsent, tcase.onMismatch, tcase.op, tcase.value)
		haserr := (err != nil)
		if haserr != tcase.expecterr {
			t.Errorf("test %d: received %v for %#v", i, haserr, tcase)
		}
	}
}

type BindVarTestCase struct {
	bvc      BindVarCond
	bvval    interface{}
	expected bool
}

var bvtestcases = []BindVarTestCase{
	{BindVarCond{"b", true, true, QR_NOOP, nil}, 1, true},
	{BindVarCond{"b", false, true, QR_NOOP, nil}, 1, false},
	{BindVarCond{"a", true, true, QR_NOOP, nil}, 1, false},
	{BindVarCond{"a", false, true, QR_NOOP, nil}, 1, true},

	{BindVarCond{"a", true, true, QR_EQ, bvcuint64(10)}, int(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcuint64(10)}, int8(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcuint64(10)}, int8(10), true},
	{BindVarCond{"a", true, true, QR_EQ, bvcuint64(10)}, int16(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcuint64(10)}, int32(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcuint64(10)}, int64(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcuint64(10)}, uint64(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcuint64(10)}, int8(-1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcuint64(10)}, "abc", true},

	{BindVarCond{"a", true, true, QR_NE, bvcuint64(10)}, int8(1), true},
	{BindVarCond{"a", true, true, QR_NE, bvcuint64(10)}, int8(10), false},
	{BindVarCond{"a", true, true, QR_NE, bvcuint64(10)}, int8(11), true},
	{BindVarCond{"a", true, true, QR_NE, bvcuint64(10)}, int8(-1), true},

	{BindVarCond{"a", true, true, QR_LT, bvcuint64(10)}, int8(1), true},
	{BindVarCond{"a", true, true, QR_LT, bvcuint64(10)}, int8(10), false},
	{BindVarCond{"a", true, true, QR_LT, bvcuint64(10)}, int8(11), false},
	{BindVarCond{"a", true, true, QR_LT, bvcuint64(10)}, int8(-1), true},

	{BindVarCond{"a", true, true, QR_GE, bvcuint64(10)}, int8(1), false},
	{BindVarCond{"a", true, true, QR_GE, bvcuint64(10)}, int8(10), true},
	{BindVarCond{"a", true, true, QR_GE, bvcuint64(10)}, int8(11), true},
	{BindVarCond{"a", true, true, QR_GE, bvcuint64(10)}, int8(-1), false},

	{BindVarCond{"a", true, true, QR_GT, bvcuint64(10)}, int8(1), false},
	{BindVarCond{"a", true, true, QR_GT, bvcuint64(10)}, int8(10), false},
	{BindVarCond{"a", true, true, QR_GT, bvcuint64(10)}, int8(11), true},
	{BindVarCond{"a", true, true, QR_GT, bvcuint64(10)}, int8(-1), false},

	{BindVarCond{"a", true, true, QR_LE, bvcuint64(10)}, int8(1), true},
	{BindVarCond{"a", true, true, QR_LE, bvcuint64(10)}, int8(10), true},
	{BindVarCond{"a", true, true, QR_LE, bvcuint64(10)}, int8(11), false},
	{BindVarCond{"a", true, true, QR_LE, bvcuint64(10)}, int8(-1), true},

	{BindVarCond{"a", true, true, QR_EQ, bvcint64(10)}, int(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcint64(10)}, int8(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcint64(10)}, int8(10), true},
	{BindVarCond{"a", true, true, QR_EQ, bvcint64(10)}, int16(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcint64(10)}, int32(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcint64(10)}, int64(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcint64(10)}, uint64(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcint64(10)}, uint64(0xFFFFFFFFFFFFFFFF), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcint64(10)}, "abc", true},

	{BindVarCond{"a", true, true, QR_NE, bvcint64(10)}, int8(1), true},
	{BindVarCond{"a", true, true, QR_NE, bvcint64(10)}, int8(10), false},
	{BindVarCond{"a", true, true, QR_NE, bvcint64(10)}, int8(11), true},
	{BindVarCond{"a", true, true, QR_NE, bvcint64(10)}, uint64(0xFFFFFFFFFFFFFFFF), true},

	{BindVarCond{"a", true, true, QR_LT, bvcint64(10)}, int8(1), true},
	{BindVarCond{"a", true, true, QR_LT, bvcint64(10)}, int8(10), false},
	{BindVarCond{"a", true, true, QR_LT, bvcint64(10)}, int8(11), false},
	{BindVarCond{"a", true, true, QR_LT, bvcint64(10)}, uint64(0xFFFFFFFFFFFFFFFF), false},

	{BindVarCond{"a", true, true, QR_GE, bvcint64(10)}, int8(1), false},
	{BindVarCond{"a", true, true, QR_GE, bvcint64(10)}, int8(10), true},
	{BindVarCond{"a", true, true, QR_GE, bvcint64(10)}, int8(11), true},
	{BindVarCond{"a", true, true, QR_GE, bvcint64(10)}, uint64(0xFFFFFFFFFFFFFFFF), true},

	{BindVarCond{"a", true, true, QR_GT, bvcint64(10)}, int8(1), false},
	{BindVarCond{"a", true, true, QR_GT, bvcint64(10)}, int8(10), false},
	{BindVarCond{"a", true, true, QR_GT, bvcint64(10)}, int8(11), true},
	{BindVarCond{"a", true, true, QR_GT, bvcint64(10)}, uint64(0xFFFFFFFFFFFFFFFF), true},

	{BindVarCond{"a", true, true, QR_LE, bvcint64(10)}, int8(1), true},
	{BindVarCond{"a", true, true, QR_LE, bvcint64(10)}, int8(10), true},
	{BindVarCond{"a", true, true, QR_LE, bvcint64(10)}, int8(11), false},
	{BindVarCond{"a", true, true, QR_LE, bvcint64(10)}, uint64(0xFFFFFFFFFFFFFFFF), false},

	{BindVarCond{"a", true, true, QR_EQ, bvcstring("b")}, "a", false},
	{BindVarCond{"a", true, true, QR_EQ, bvcstring("b")}, "b", true},
	{BindVarCond{"a", true, true, QR_EQ, bvcstring("b")}, "c", false},
	{BindVarCond{"a", true, true, QR_EQ, bvcstring("b")}, []byte("a"), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcstring("b")}, []byte("b"), true},
	{BindVarCond{"a", true, true, QR_EQ, bvcstring("b")}, []byte("c"), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcstring("b")}, int8(1), true},

	{BindVarCond{"a", true, true, QR_NE, bvcstring("b")}, "a", true},
	{BindVarCond{"a", true, true, QR_NE, bvcstring("b")}, "b", false},
	{BindVarCond{"a", true, true, QR_NE, bvcstring("b")}, "c", true},

	{BindVarCond{"a", true, true, QR_LT, bvcstring("b")}, "a", true},
	{BindVarCond{"a", true, true, QR_LT, bvcstring("b")}, "b", false},
	{BindVarCond{"a", true, true, QR_LT, bvcstring("b")}, "c", false},

	{BindVarCond{"a", true, true, QR_GE, bvcstring("b")}, "a", false},
	{BindVarCond{"a", true, true, QR_GE, bvcstring("b")}, "b", true},
	{BindVarCond{"a", true, true, QR_GE, bvcstring("b")}, "c", true},

	{BindVarCond{"a", true, true, QR_GT, bvcstring("b")}, "a", false},
	{BindVarCond{"a", true, true, QR_GT, bvcstring("b")}, "b", false},
	{BindVarCond{"a", true, true, QR_GT, bvcstring("b")}, "c", true},

	{BindVarCond{"a", true, true, QR_LE, bvcstring("b")}, "a", true},
	{BindVarCond{"a", true, true, QR_LE, bvcstring("b")}, "b", true},
	{BindVarCond{"a", true, true, QR_LE, bvcstring("b")}, "c", false},

	{BindVarCond{"a", true, true, QR_MATCH, makere("a.*")}, "c", false},
	{BindVarCond{"a", true, true, QR_MATCH, makere("a.*")}, "a", true},
	{BindVarCond{"a", true, true, QR_MATCH, makere("a.*")}, int8(1), true},

	{BindVarCond{"a", true, true, QR_NOMATCH, makere("a.*")}, "c", true},
	{BindVarCond{"a", true, true, QR_NOMATCH, makere("a.*")}, "a", false},
	{BindVarCond{"a", true, true, QR_NOMATCH, makere("a.*")}, int8(1), true},

	{BindVarCond{"a", true, true, QR_IN, numKeyRange(0x4000000000000000, 0x6000000000000000)}, uint64(0), false},
	{BindVarCond{"a", true, true, QR_IN, numKeyRange(0x4000000000000000, 0x6000000000000000)}, uint64(0x5000000000000000), true},
	{BindVarCond{"a", true, true, QR_IN, numKeyRange(0x4000000000000000, 0x6000000000000000)}, uint64(0x7000000000000000), false},
	{BindVarCond{"a", true, true, QR_IN, strKeyRange("b", "d")}, "a", false},
	{BindVarCond{"a", true, true, QR_IN, strKeyRange("b", "d")}, "c", true},
	{BindVarCond{"a", true, true, QR_IN, strKeyRange("b", "d")}, "e", false},
	{BindVarCond{"a", true, true, QR_IN, strKeyRange("b", "d")}, float64(1.0), true},

	{BindVarCond{"a", true, true, QR_NOTIN, numKeyRange(0x4000000000000000, 0x6000000000000000)}, uint64(0), true},
	{BindVarCond{"a", true, true, QR_NOTIN, numKeyRange(0x4000000000000000, 0x6000000000000000)}, uint64(0x5000000000000000), false},
	{BindVarCond{"a", true, true, QR_NOTIN, numKeyRange(0x4000000000000000, 0x6000000000000000)}, uint64(0x7000000000000000), true},
	{BindVarCond{"a", true, true, QR_NOTIN, strKeyRange("b", "d")}, "a", true},
	{BindVarCond{"a", true, true, QR_NOTIN, strKeyRange("b", "d")}, "c", false},
	{BindVarCond{"a", true, true, QR_NOTIN, strKeyRange("b", "d")}, "e", true},
	{BindVarCond{"a", true, true, QR_NOTIN, strKeyRange("b", "d")}, float64(1.0), true},
}

func makere(s string) bvcre {
	re, _ := regexp.Compile(s)
	return bvcre{re}
}

func numKeyRange(start, end uint64) bvcKeyRange {
	kr := key.KeyRange{
		Start: key.Uint64Key(start).KeyspaceId(),
		End:   key.Uint64Key(end).KeyspaceId(),
	}
	return bvcKeyRange(kr)
}

func strKeyRange(start, end string) bvcKeyRange {
	kr := key.KeyRange{
		Start: key.KeyspaceId(start),
		End:   key.KeyspaceId(end),
	}
	return bvcKeyRange(kr)
}

func TestBVConditions(t *testing.T) {
	bv := make(map[string]interface{})
	for i, tcase := range bvtestcases {
		bv["a"] = tcase.bvval
		if bvMatch(tcase.bvc, bv) != tcase.expected {
			t.Errorf("test %d: expecting %v for %#v, %#v", i, tcase.expected, tcase.bvc, tcase.bvval)
		}
	}
}

func TestAction(t *testing.T) {
	qrs := NewQueryRules()

	qr1 := NewQueryRule("rule 1", "r1", QR_FAIL_QUERY)
	qr1.SetIPCond("123")

	qr2 := NewQueryRule("rule 2", "r2", QR_FAIL_QUERY)
	qr2.SetUserCond("user")

	qr3 := NewQueryRule("rule 3", "r3", QR_FAIL_QUERY)
	qr3.AddBindVarCond("a", true, true, QR_EQ, uint64(1))

	qrs.Add(qr1)
	qrs.Add(qr2)
	qrs.Add(qr3)

	bv := make(map[string]interface{})
	bv["a"] = uint64(0)
	action, desc := qrs.getAction("123", "user1", bv)
	if action != QR_FAIL_QUERY {
		t.Errorf("Expecting fail")
	}
	if desc != "rule 1" {
		t.Errorf("Expecting rule 1, received %s", desc)
	}
	action, desc = qrs.getAction("1234", "user", bv)
	if action != QR_FAIL_QUERY {
		t.Errorf("Expecting fail")
	}
	if desc != "rule 2" {
		t.Errorf("Expecting rule 2, received %s", desc)
	}
	action, desc = qrs.getAction("1234", "user1", bv)
	if action != QR_CONTINUE {
		t.Errorf("Expecting continue")
	}
	bv["a"] = uint64(1)
	action, desc = qrs.getAction("1234", "user1", bv)
	if action != QR_FAIL_QUERY {
		t.Errorf("Expecting fail")
	}
	if desc != "rule 3" {
		t.Errorf("Expecting rule 2, received %s", desc)
	}
}

var jsondata = `[{
	"Description": "desc1",
	"Name": "name1",
	"RequestIP": "123.123.123",
	"User": "user",
	"Query": "query",
	"Plans": ["PASS_SELECT", "INSERT_PK"],
	"BindVarConds": [{
		"Name": "bvname1",
		"OnAbsent": true,
		"Operator": "NOOP"
	},{
		"Name": "bvname2",
		"OnAbsent": true,
		"OnMismatch": true,
		"Operator": "UEQ",
		"Value": "123"
	}]
},{
	"Description": "desc2",
	"Name": "name2"
}]`

func TestImport(t *testing.T) {
	var qrs = NewQueryRules()
	err := qrs.UnmarshalJSON([]byte(jsondata))
	if err != nil {
		t.Errorf("Unexptected: %v", err)
		return
	}
	if qrs.rules[0].Description != "desc1" {
		t.Errorf("Expecting desc1, received %s", qrs.rules[0].Description)
	}
	if qrs.rules[0].Name != "name1" {
		t.Errorf("Expecting name1, received %s", qrs.rules[0].Name)
	}
	if qrs.rules[0].requestIP == nil {
		t.Errorf("Expecting non-nil")
	}
	if qrs.rules[0].user == nil {
		t.Errorf("Expecting non-nil")
	}
	if qrs.rules[0].query == nil {
		t.Errorf("Expecting non-nil")
	}
	if qrs.rules[0].plans[0] != sqlparser.PLAN_PASS_SELECT {
		t.Errorf("Expecting PASS_SELECT, received %s", qrs.rules[0].plans[0].String())
	}
	if qrs.rules[0].plans[1] != sqlparser.PLAN_INSERT_PK {
		t.Errorf("Expecting PASS_INSERT_PK, received %s", qrs.rules[0].plans[0].String())
	}
	bvc := qrs.rules[0].bindVarConds[0]
	if bvc.name != "bvname1" {
		t.Errorf("Expecting bvname1, received %v", bvc.name)
	}
	if !bvc.onAbsent {
		t.Errorf("Expecting true")
	}
	if bvc.op != QR_NOOP {
		t.Errorf("Expecting NOOP, received %v", bvc.op)
	}
	bvc = qrs.rules[0].bindVarConds[1]
	if bvc.name != "bvname2" {
		t.Errorf("Expecting bvname2, received %v", bvc.name)
	}
	if !bvc.onAbsent {
		t.Errorf("Expecting true")
	}
	if !bvc.onMismatch {
		t.Errorf("Expecting true")
	}
	if bvc.op != QR_EQ {
		t.Errorf("Expecting %v, received %v", QR_EQ, bvc.op)
	}
	if bvc.value.(bvcuint64) != 123 {
		t.Errorf("Expecting 123, received %v", bvc.value.(bvcuint64))
	}
	if qrs.rules[1].Description != "desc2" {
		t.Errorf("Expecting desc2, received %s", qrs.rules[0].Description)
	}
	if qrs.rules[1].Name != "name2" {
		t.Errorf("Expecting name2, received %s", qrs.rules[0].Name)
	}
	if qrs.rules[1].requestIP != nil {
		t.Errorf("Expecting nil")
	}
	if qrs.rules[1].user != nil {
		t.Errorf("Expecting nil")
	}
	if qrs.rules[1].query != nil {
		t.Errorf("Expecting nil")
	}
	if qrs.rules[1].plans != nil {
		t.Errorf("Expecting nil")
	}
	if qrs.rules[1].bindVarConds != nil {
		t.Errorf("Expecting nil")
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
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "UEQ", "Value": "123"}]}]`, QR_EQ, UINT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "UNE", "Value": "123"}]}]`, QR_NE, UINT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "ULT", "Value": "123"}]}]`, QR_LT, UINT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "UGE", "Value": "123"}]}]`, QR_GE, UINT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "UGT", "Value": "123"}]}]`, QR_GT, UINT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "ULE", "Value": "123"}]}]`, QR_LE, UINT},

	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "IEQ", "Value": "123"}]}]`, QR_EQ, INT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "INE", "Value": "123"}]}]`, QR_NE, INT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "ILT", "Value": "123"}]}]`, QR_LT, INT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "IGE", "Value": "123"}]}]`, QR_GE, INT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "IGT", "Value": "123"}]}]`, QR_GT, INT},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "ILE", "Value": "123"}]}]`, QR_LE, INT},

	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "SEQ", "Value": "123"}]}]`, QR_EQ, STR},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "SNE", "Value": "123"}]}]`, QR_NE, STR},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "SLT", "Value": "123"}]}]`, QR_LT, STR},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "SGE", "Value": "123"}]}]`, QR_GE, STR},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "SGT", "Value": "123"}]}]`, QR_GT, STR},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "SLE", "Value": "123"}]}]`, QR_LE, STR},

	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "MATCH", "Value": "123"}]}]`, QR_MATCH, REGEXP},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "NOMATCH", "Value": "123"}]}]`, QR_NOMATCH, REGEXP},

	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "IN", "Value": {"Start": "1", "End": "2"}}]}]`, QR_IN, KEYRANGE},
	{`[{"BindVarConds": [{"Name": "bvname1", "OnAbsent": true, "OnMismatch": true, "Operator": "NOTIN", "Value": {"Start": "1", "End": "2"}}]}]`, QR_NOTIN, KEYRANGE},
}

func TestValidJSON(t *testing.T) {
	for i, tcase := range validjsons {
		qrs := NewQueryRules()
		err := qrs.UnmarshalJSON([]byte(tcase.input))
		if err != nil {
			t.Errorf("Unexpected error for case %d: %v", i, err)
		}
		bvc := qrs.rules[0].bindVarConds[0]
		if bvc.op != tcase.op {
			t.Errorf("Expecting %v, received %v", tcase.op, bvc.op)
		}
		switch tcase.typ {
		case UINT:
			if bvc.value.(bvcuint64) != 123 {
				t.Errorf("Expecting %v, received %v", 123, bvc.value.(bvcuint64))
			}
		case INT:
			if bvc.value.(bvcint64) != 123 {
				t.Errorf("Expecting %v, received %v", 123, bvc.value.(bvcint64))
			}
		case STR:
			if bvc.value.(bvcstring) != "123" {
				t.Errorf("Expecting %v, received %v", "123", bvc.value.(bvcint64))
			}
		case REGEXP:
			if bvc.value.(bvcre).re == nil {
				t.Errorf("Expecting non-nil")
			}
		case KEYRANGE:
			if kr := bvc.value.(bvcKeyRange); kr.Start != "1" || kr.End != "2" {
				t.Errorf(`Execting {"1", "2"}, received %v`, kr)
			}
		}
	}
}

type InvalidJSONCase struct {
	input, err string
}

var invalidjsons = []InvalidJSONCase{
	{`[{"Name": 1 }]`, "Expecting string for Name"},
	{`[{"Description": 1 }]`, "Expecting string for Description"},
	{`[{"RequestIP": 1 }]`, "Expecting string for RequestIP"},
	{`[{"User": 1 }]`, "Expecting string for User"},
	{`[{"Query": 1 }]`, "Expecting string for Query"},
	{`[{"Plans": 1 }]`, "Expecting list for Plans"},
	{`[{"BindVarConds": 1 }]`, "Expecting list for BindVarConds"},
	{`[{"RequestIP": "[" }]`, "Could not set IP condition: ["},
	{`[{"User": "[" }]`, "Could not set User condition: ["},
	{`[{"Query": "[" }]`, "Could not set Query condition: ["},
	{`[{"Plans": [1] }]`, "Expecting string for Plans"},
	{`[{"Plans": ["invalid"] }]`, "Invalid plan name: invalid"},
	{`[{"BindVarConds": [1] }]`, "Expecting json object for bind var conditions"},
	{`[{"BindVarConds": [{}] }]`, "Name missing in BindVarConds"},
	{`[{"BindVarConds": [{"Name": 1}] }]`, "Expecting string for Name in BindVarConds"},
	{`[{"BindVarConds": [{"Name": "a"}] }]`, "OnAbsent missing in BindVarConds"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": 1}] }]`, "Expecting bool for OnAbsent"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true}]}]`, "Operator missing in BindVarConds"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "a"}]}]`, "Invalid Operator a"},

	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "UEQ"}]}]`, "Value missing in BindVarConds"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "UEQ", "Value": "a"}]}]`, "Expecting uint64: a"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "UEQ", "Value": "-1"}]}]`, "Expecting uint64: -1"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "UNE", "Value": "-1"}]}]`, "Expecting uint64: -1"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "ULT", "Value": "-1"}]}]`, "Expecting uint64: -1"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "UGE", "Value": "-1"}]}]`, "Expecting uint64: -1"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "UGT", "Value": "-1"}]}]`, "Expecting uint64: -1"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "ULE", "Value": "-1"}]}]`, "Expecting uint64: -1"},

	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "IEQ"}]}]`, "Value missing in BindVarConds"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "IEQ", "Value": "a"}]}]`, "Expecting int64: a"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "IEQ", "Value": "0x8000000000000000"}]}]`, "Expecting int64: 0x8000000000000000"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "INE", "Value": "0x8000000000000000"}]}]`, "Expecting int64: 0x8000000000000000"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "ILT", "Value": "0x8000000000000000"}]}]`, "Expecting int64: 0x8000000000000000"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "IGE", "Value": "0x8000000000000000"}]}]`, "Expecting int64: 0x8000000000000000"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "IGT", "Value": "0x8000000000000000"}]}]`, "Expecting int64: 0x8000000000000000"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "ILE", "Value": "0x8000000000000000"}]}]`, "Expecting int64: 0x8000000000000000"},

	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "SEQ"}]}]`, "Value missing in BindVarConds"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "SEQ", "Value": 1}]}]`, "Expecting string: 1"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "SEQ", "Value": 1}]}]`, "Expecting string: 1"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "SNE", "Value": 1}]}]`, "Expecting string: 1"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "SLT", "Value": 1}]}]`, "Expecting string: 1"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "SGE", "Value": 1}]}]`, "Expecting string: 1"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "SGT", "Value": 1}]}]`, "Expecting string: 1"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "SLE", "Value": 1}]}]`, "Expecting string: 1"},

	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "MATCH", "Value": 1}]}]`, "Expecting string: 1"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "NOMATCH", "Value": 1}]}]`, "Expecting string: 1"},

	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "Operator": "ILE", "Value": "1"}]}]`, "OnMismatch missing in BindVarConds"},

	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "OnMismatch": true, "Operator": "MATCH", "Value": "["}]}]`, "Processing [: error parsing regexp: missing closing ]: `[$`"},
	{`[{"BindVarConds": [{"Name": "a", "OnAbsent": true, "OnMismatch": true, "Operator": "NOMATCH", "Value": "["}]}]`, "Processing [: error parsing regexp: missing closing ]: `[$`"},
}

func TestInvalidJSON(t *testing.T) {
	for i, tcase := range invalidjsons {
		qrs := NewQueryRules()
		err := qrs.UnmarshalJSON([]byte(tcase.input))
		if err == nil {
			t.Errorf("Expecting error for case %d", i)
			continue
		}
		recvd := strings.Replace(err.Error(), "error: ", "", 1)
		if recvd != tcase.err {
			t.Errorf("Expecting '%v', received '%v'", tcase.err, recvd)
		}
	}
}
