// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/henryanand/vitess/go/vt/key"
	"github.com/henryanand/vitess/go/vt/tabletserver/planbuilder"
)

var (
	keyrangeRules    *QueryRules
	blacklistRules   *QueryRules
	customQueryRules *QueryRules
)

func setupQueryRules() {
	var qr *QueryRule
	// mock keyrange rules
	keyrangeRules = NewQueryRules()
	dml_plans := []struct {
		planID   planbuilder.PlanType
		onAbsent bool
	}{
		{planbuilder.PLAN_INSERT_PK, true},
		{planbuilder.PLAN_INSERT_SUBQUERY, true},
		{planbuilder.PLAN_PASS_DML, false},
		{planbuilder.PLAN_DML_PK, false},
		{planbuilder.PLAN_DML_SUBQUERY, false},
	}
	for _, plan := range dml_plans {
		qr = NewQueryRule(
			fmt.Sprintf("enforce keyspace_id range for %v", plan.planID),
			fmt.Sprintf("keyspace_id_not_in_range_%v", plan.planID),
			QR_FAIL,
		)
		qr.AddPlanCond(plan.planID)
		qr.AddBindVarCond("keyspace_id", plan.onAbsent, true, QR_NOTIN, key.KeyRange{Start: "aa", End: "zz"})
		keyrangeRules.Add(qr)
	}

	// mock blacklisted tables
	blacklistRules = NewQueryRules()
	blacklistedTables := []string{"bannedtable1", "bannedtable2", "bannedtable3"}
	qr = NewQueryRule("enforce blacklisted tables", "blacklisted_table", QR_FAIL_RETRY)
	for _, t := range blacklistedTables {
		qr.AddTableCond(t)
	}
	blacklistRules.Add(qr)

	// mock custom rules
	customQueryRules = NewQueryRules()
	qr = NewQueryRule("sample custom rule", "customrule_ban_bindvar", QR_FAIL)
	qr.AddTableCond("t_customer")
	qr.AddBindVarCond("bindvar1", true, false, QR_NOOP, nil)
	customQueryRules.Add(qr)
}

func TestQueryRuleInfoGetSetQueryRules(t *testing.T) {
	setupQueryRules()
	qri := NewQueryRuleInfo()

	// Test if we can get a QueryRules without a predefined rule set name
	err, qrs := qri.GetRules("Foo")
	if err == nil {
		t.Errorf("GetRules shouldn't succeed with 'Foo' as the rule set name")
	}
	if qrs == nil {
		t.Errorf("GetRules should always return empty QueryRules and never nil")
	}
	if !reflect.DeepEqual(qrs, NewQueryRules()) {
		t.Errorf("QueryRuleInfo contains only empty QueryRules at the beginning")
	}

	// Test if we can set a QueryRules without a predefined rule set name
	err = qri.SetRules("Foo", NewQueryRules())
	if err == nil {
		t.Errorf("SetRules shouldn't succeed with 'Foo' as the rule set name")
	}

	// Test if we can successfully set QueryRules previously mocked into QueryRuleInfo
	err = qri.SetRules(KeyrangeQueryRules, keyrangeRules)
	if err != nil {
		t.Errorf("Failed to set keyrange QueryRules, errmsg: %s", err)
	}
	err = qri.SetRules(BlacklistQueryRules, blacklistRules)
	if err != nil {
		t.Errorf("Failed to set blacklist QueryRules, errmsg: %s", err)
	}
	err = qri.SetRules(CustomQueryRules, customQueryRules)
	if err != nil {
		t.Errorf("Failed to set custom QueryRules, errmsg: %s", err)
	}

	// Test if we can successfully retrive rules that've been set
	err, qrs = qri.GetRules(KeyrangeQueryRules)
	if err != nil {
		t.Errorf("GetRules failed to retrieve KeyrangeQueryRules that has been set, errmsg: %s", err)
	}
	if !reflect.DeepEqual(qrs, keyrangeRules) {
		t.Errorf("KeyrangeQueryRules retrived is %v, but the expected value should be %v", qrs, keyrangeRules)
	}

	err, qrs = qri.GetRules(BlacklistQueryRules)
	if err != nil {
		t.Errorf("GetRules failed to retrieve BlacklistQueryRules that has been set, errmsg: %s", err)
	}
	if !reflect.DeepEqual(qrs, blacklistRules) {
		t.Errorf("BlacklistQueryRules retrived is %v, but the expected value should be %v", qrs, blacklistRules)
	}

	err, qrs = qri.GetRules(CustomQueryRules)
	if err != nil {
		t.Errorf("GetRules failed to retrieve CustomQueryRules that has been set, errmsg: %s", err)
	}
	if !reflect.DeepEqual(qrs, customQueryRules) {
		t.Errorf("CustomQueryRules retrived is %v, but the expected value should be %v", qrs, customQueryRules)
	}
}

func TestQueryRuleInfoFilterByPlan(t *testing.T) {
	setupQueryRules()
	qri := NewQueryRuleInfo()
	qri.SetRules(KeyrangeQueryRules, keyrangeRules)
	qri.SetRules(BlacklistQueryRules, blacklistRules)
	qri.SetRules(CustomQueryRules, customQueryRules)

	// Test filter by keyrange rule
	qrs := qri.filterByPlan("insert into t_test values(123, 456, 'abc')", planbuilder.PLAN_INSERT_PK, "t_test")
	if l := len(qrs.rules); l != 1 {
		t.Errorf("Insert PK query matches %d rules, but we expect %d", l, 1)
	}
	if !strings.HasPrefix(qrs.rules[0].Name, "keyspace_id_not_in_range") {
		t.Errorf("Insert PK query matches rule '%s', but we expect rule with prefix '%s'", qrs.rules[0].Name, "keyspace_id_not_in_range")
	}

	// Test filter by blacklist rule
	qrs = qri.filterByPlan("select * from bannedtable2", planbuilder.PLAN_PASS_SELECT, "bannedtable2")
	if l := len(qrs.rules); l != 1 {
		t.Errorf("Select from bannedtable matches %d rules, but we expect %d", l, 1)
	}
	if !strings.HasPrefix(qrs.rules[0].Name, "blacklisted_table") {
		t.Errorf("Select from bannedtable query matches rule '%s', but we expect rule with prefix '%s'", qrs.rules[0].Name, "blacklisted_table")
	}

	// Test filter by custom rule
	qrs = qri.filterByPlan("select cid from t_customer limit 10", planbuilder.PLAN_PASS_SELECT, "t_customer")
	if l := len(qrs.rules); l != 1 {
		t.Errorf("Select from t_customer matches %d rules, but we expect %d", l, 1)
	}
	if !strings.HasPrefix(qrs.rules[0].Name, "customrule_ban_bindvar") {
		t.Errorf("Select from t_customer matches rule '%s', but we expect rule with prefix '%s'", qrs.rules[0].Name, "customrule_ban_bindvar")
	}

	// Test match two rules: both keyrange rule and custom rule will be matched
	customQueryRules = NewQueryRules()
	qr := NewQueryRule("sample custom rule", "customrule_ban_bindvar", QR_FAIL)
	qr.AddBindVarCond("bindvar1", true, false, QR_NOOP, nil)
	customQueryRules.Add(qr)
	qri.SetRules(CustomQueryRules, customQueryRules)
	qrs = qri.filterByPlan("insert into t_test values (:bindvar1, 123, 'test')", planbuilder.PLAN_INSERT_PK, "t_test")
	if l := len(qrs.rules); l != 2 {
		t.Errorf("Insert into t_test matches %d rules: %v, but we expect %d rules to be matched", l, qrs.rules, 2)
	}
	if !strings.HasPrefix(qrs.rules[0].Name, "keyspace_id_not_in_range") ||
		!strings.HasPrefix(qrs.rules[1].Name, "customrule_ban_bindvar") {

		t.Errorf("Insert into t_test matches rule[0] '%s' and rule[1] '%s', but we expect rule[0] with prefix '%s' and rule[1] with prefix '%s'",
			qrs.rules[0].Name, qrs.rules[1].Name, "keyspace_id_not_in_range", "customrule_ban_bindvar")
	}
}
