// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
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

func TestBindVar(t *testing.T) {
	qr := NewQueryRule("rule 1", "r1", QR_FAIL_QUERY)

	var err error
	err = qr.AddBindVarCond("b", false, true, QR_NOOP, nil)
	err = qr.AddBindVarCond("a", true, false, QR_NOOP, nil)
	if err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if qr.bindVars[1].name != "a" {
		t.Errorf("expecting a, received %s", qr.bindVars[1].name)
	}
	if !qr.bindVars[1].onAbsent {
		t.Errorf("expecting true, received false")
	}
	if qr.bindVars[1].onMismatch {
		t.Errorf("expecting false, received true")
	}
	if qr.bindVars[1].op != QR_NOOP {
		t.Errorf("exepecting NOOP, received %s", opname[qr.bindVars[1].op])
	}
	if qr.bindVars[1].value != nil {
		t.Errorf("expecting nil, received %#v", qr.bindVars[1].value)
	}

	if err = qr.AddBindVarCond("a", true, true, QR_UEQ, uint64(1)); err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if err = qr.AddBindVarCond("a", true, true, QR_UNE, uint64(1)); err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if err = qr.AddBindVarCond("a", true, true, QR_ULT, uint64(1)); err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if err = qr.AddBindVarCond("a", true, true, QR_UGE, uint64(1)); err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if err = qr.AddBindVarCond("a", true, true, QR_UGT, uint64(1)); err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if err = qr.AddBindVarCond("a", true, true, QR_ULE, uint64(1)); err != nil {
		t.Errorf("unexpected: %v", err)
	}

	if err = qr.AddBindVarCond("a", true, true, QR_IEQ, int64(1)); err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if err = qr.AddBindVarCond("a", true, true, QR_INE, int64(1)); err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if err = qr.AddBindVarCond("a", true, true, QR_ILT, int64(1)); err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if err = qr.AddBindVarCond("a", true, true, QR_IGE, int64(1)); err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if err = qr.AddBindVarCond("a", true, true, QR_IGT, int64(1)); err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if err = qr.AddBindVarCond("a", true, true, QR_ILE, int64(1)); err != nil {
		t.Errorf("unexpected: %v", err)
	}

	if err = qr.AddBindVarCond("a", true, true, QR_SEQ, "a"); err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if err = qr.AddBindVarCond("a", true, true, QR_SNE, "a"); err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if err = qr.AddBindVarCond("a", true, true, QR_SLT, "a"); err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if err = qr.AddBindVarCond("a", true, true, QR_SGE, "a"); err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if err = qr.AddBindVarCond("a", true, true, QR_SGT, "a"); err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if err = qr.AddBindVarCond("a", true, true, QR_SLE, "a"); err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if err = qr.AddBindVarCond("a", true, true, QR_SMATCH, "a"); err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if err = qr.AddBindVarCond("a", true, true, QR_SNOMATCH, "a"); err != nil {
		t.Errorf("unexpected: %v", err)
	}

	if err = qr.AddBindVarCond("a", true, true, QR_KIN, key.KeyRange{}); err != nil {
		t.Errorf("unexpected: %v", err)
	}
	if err = qr.AddBindVarCond("a", true, true, QR_KNOTIN, key.KeyRange{}); err != nil {
		t.Errorf("unexpected: %v", err)
	}

	// failures
	if err = qr.AddBindVarCond("a", true, true, QR_UEQ, int64(1)); err == nil {
		t.Errorf("expecting error")
	}
	if err = qr.AddBindVarCond("a", true, true, QR_UNE, int64(1)); err == nil {
		t.Errorf("expecting error")
	}
	if err = qr.AddBindVarCond("a", true, true, QR_ULT, int64(1)); err == nil {
		t.Errorf("expecting error")
	}
	if err = qr.AddBindVarCond("a", true, true, QR_UGE, int64(1)); err == nil {
		t.Errorf("expecting error")
	}
	if err = qr.AddBindVarCond("a", true, true, QR_UGT, int64(1)); err == nil {
		t.Errorf("expecting error")
	}
	if err = qr.AddBindVarCond("a", true, true, QR_ULE, int64(1)); err == nil {
		t.Errorf("expecting error")
	}

	if err = qr.AddBindVarCond("a", true, true, QR_IEQ, uint64(1)); err == nil {
		t.Errorf("expecting error")
	}
	if err = qr.AddBindVarCond("a", true, true, QR_INE, uint64(1)); err == nil {
		t.Errorf("expecting error")
	}
	if err = qr.AddBindVarCond("a", true, true, QR_ILT, uint64(1)); err == nil {
		t.Errorf("expecting error")
	}
	if err = qr.AddBindVarCond("a", true, true, QR_IGE, uint64(1)); err == nil {
		t.Errorf("expecting error")
	}
	if err = qr.AddBindVarCond("a", true, true, QR_IGT, uint64(1)); err == nil {
		t.Errorf("expecting error")
	}
	if err = qr.AddBindVarCond("a", true, true, QR_ILE, uint64(1)); err == nil {
		t.Errorf("expecting error")
	}

	if err = qr.AddBindVarCond("a", true, true, QR_SEQ, int64(1)); err == nil {
		t.Errorf("expecting error")
	}
	if err = qr.AddBindVarCond("a", true, true, QR_SNE, int64(1)); err == nil {
		t.Errorf("expecting error")
	}
	if err = qr.AddBindVarCond("a", true, true, QR_SLT, int64(1)); err == nil {
		t.Errorf("expecting error")
	}
	if err = qr.AddBindVarCond("a", true, true, QR_SGE, int64(1)); err == nil {
		t.Errorf("expecting error")
	}
	if err = qr.AddBindVarCond("a", true, true, QR_SGT, int64(1)); err == nil {
		t.Errorf("expecting error")
	}
	if err = qr.AddBindVarCond("a", true, true, QR_SLE, int64(1)); err == nil {
		t.Errorf("expecting error")
	}
	if err = qr.AddBindVarCond("a", true, true, QR_SMATCH, int64(1)); err == nil {
		t.Errorf("expecting error")
	}
	if err = qr.AddBindVarCond("a", true, true, QR_SNOMATCH, int64(1)); err == nil {
		t.Errorf("expecting error")
	}

	if err = qr.AddBindVarCond("a", true, true, QR_KIN, int64(1)); err == nil {
		t.Errorf("expecting error")
	}
	if err = qr.AddBindVarCond("a", true, true, QR_KNOTIN, int64(1)); err == nil {
		t.Errorf("expecting error")
	}

	if err = qr.AddBindVarCond("a", true, true, QR_UEQ, uint32(1)); err == nil {
		t.Errorf("expecting error")
	}
}
