// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"regexp"
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

	var qrs2 *QueryRules
	if qrs3 := qrs2.Copy(); qrs3 != nil {
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
	if qrs1 != nil {
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

	var qrsnil1 *QueryRules
	if qrsnil2 := qrsnil1.filterByPlan("", sqlparser.PLAN_PASS_SELECT); qrsnil2 != nil {
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
		t.Errorf("exepecting NOOP, received %s", opname[qr.bindVarConds[1].op])
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

	{BindVarCond{"a", true, true, QR_EQ, bvcuint(10)}, int8(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcuint(10)}, int8(10), true},
	{BindVarCond{"a", true, true, QR_EQ, bvcuint(10)}, int16(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcuint(10)}, int32(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcuint(10)}, int64(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcuint(10)}, uint64(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcuint(10)}, int8(-1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcuint(10)}, "abc", true},

	{BindVarCond{"a", true, true, QR_NE, bvcuint(10)}, int8(1), true},
	{BindVarCond{"a", true, true, QR_NE, bvcuint(10)}, int8(10), false},
	{BindVarCond{"a", true, true, QR_NE, bvcuint(10)}, int8(11), true},
	{BindVarCond{"a", true, true, QR_NE, bvcuint(10)}, int8(-1), true},

	{BindVarCond{"a", true, true, QR_LT, bvcuint(10)}, int8(1), true},
	{BindVarCond{"a", true, true, QR_LT, bvcuint(10)}, int8(10), false},
	{BindVarCond{"a", true, true, QR_LT, bvcuint(10)}, int8(11), false},
	{BindVarCond{"a", true, true, QR_LT, bvcuint(10)}, int8(-1), true},

	{BindVarCond{"a", true, true, QR_GE, bvcuint(10)}, int8(1), false},
	{BindVarCond{"a", true, true, QR_GE, bvcuint(10)}, int8(10), true},
	{BindVarCond{"a", true, true, QR_GE, bvcuint(10)}, int8(11), true},
	{BindVarCond{"a", true, true, QR_GE, bvcuint(10)}, int8(-1), false},

	{BindVarCond{"a", true, true, QR_GT, bvcuint(10)}, int8(1), false},
	{BindVarCond{"a", true, true, QR_GT, bvcuint(10)}, int8(10), false},
	{BindVarCond{"a", true, true, QR_GT, bvcuint(10)}, int8(11), true},
	{BindVarCond{"a", true, true, QR_GT, bvcuint(10)}, int8(-1), false},

	{BindVarCond{"a", true, true, QR_LE, bvcuint(10)}, int8(1), true},
	{BindVarCond{"a", true, true, QR_LE, bvcuint(10)}, int8(10), true},
	{BindVarCond{"a", true, true, QR_LE, bvcuint(10)}, int8(11), false},
	{BindVarCond{"a", true, true, QR_LE, bvcuint(10)}, int8(-1), true},

	{BindVarCond{"a", true, true, QR_EQ, bvcint(10)}, int8(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcint(10)}, int8(10), true},
	{BindVarCond{"a", true, true, QR_EQ, bvcint(10)}, int16(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcint(10)}, int32(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcint(10)}, int64(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcint(10)}, uint64(1), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcint(10)}, uint64(0xFFFFFFFFFFFFFFFF), false},
	{BindVarCond{"a", true, true, QR_EQ, bvcint(10)}, "abc", true},

	{BindVarCond{"a", true, true, QR_NE, bvcint(10)}, int8(1), true},
	{BindVarCond{"a", true, true, QR_NE, bvcint(10)}, int8(10), false},
	{BindVarCond{"a", true, true, QR_NE, bvcint(10)}, int8(11), true},
	{BindVarCond{"a", true, true, QR_NE, bvcint(10)}, uint64(0xFFFFFFFFFFFFFFFF), true},

	{BindVarCond{"a", true, true, QR_LT, bvcint(10)}, int8(1), true},
	{BindVarCond{"a", true, true, QR_LT, bvcint(10)}, int8(10), false},
	{BindVarCond{"a", true, true, QR_LT, bvcint(10)}, int8(11), false},
	{BindVarCond{"a", true, true, QR_LT, bvcint(10)}, uint64(0xFFFFFFFFFFFFFFFF), false},

	{BindVarCond{"a", true, true, QR_GE, bvcint(10)}, int8(1), false},
	{BindVarCond{"a", true, true, QR_GE, bvcint(10)}, int8(10), true},
	{BindVarCond{"a", true, true, QR_GE, bvcint(10)}, int8(11), true},
	{BindVarCond{"a", true, true, QR_GE, bvcint(10)}, uint64(0xFFFFFFFFFFFFFFFF), true},

	{BindVarCond{"a", true, true, QR_GT, bvcint(10)}, int8(1), false},
	{BindVarCond{"a", true, true, QR_GT, bvcint(10)}, int8(10), false},
	{BindVarCond{"a", true, true, QR_GT, bvcint(10)}, int8(11), true},
	{BindVarCond{"a", true, true, QR_GT, bvcint(10)}, uint64(0xFFFFFFFFFFFFFFFF), true},

	{BindVarCond{"a", true, true, QR_LE, bvcint(10)}, int8(1), true},
	{BindVarCond{"a", true, true, QR_LE, bvcint(10)}, int8(10), true},
	{BindVarCond{"a", true, true, QR_LE, bvcint(10)}, int8(11), false},
	{BindVarCond{"a", true, true, QR_LE, bvcint(10)}, uint64(0xFFFFFFFFFFFFFFFF), false},

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
	if qrs.getAction("123", "user1", bv) != QR_FAIL_QUERY {
		t.Errorf("Expecting fail")
	}
	if qrs.getAction("1234", "user", bv) != QR_FAIL_QUERY {
		t.Errorf("Expecting fail")
	}
	if qrs.getAction("1234", "user1", bv) != QR_CONTINUE {
		t.Errorf("Expecting continue")
	}
	bv["a"] = uint64(1)
	if qrs.getAction("1234", "user1", bv) != QR_FAIL_QUERY {
		t.Errorf("Expecting fail")
	}
}
