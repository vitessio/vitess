/*
Copyright 2017 Google Inc.

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
	"reflect"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
)

var (
	blacklistRules *Rules
	otherRules     *Rules
)

const (
	// mimic query rules from blacklist
	blacklistQueryRules string = "BLACKLIST_QUERY_RULES"
	// mimic query rules from custom source
	customQueryRules string = "CUSTOM_QUERY_RULES"
)

func setupRules() {
	var qr *Rule

	// mock blacklisted tables
	blacklistRules = New()
	blacklistedTables := []string{"bannedtable1", "bannedtable2", "bannedtable3"}
	qr = NewQueryRule("enforce blacklisted tables", "blacklisted_table", QRFailRetry)
	for _, t := range blacklistedTables {
		qr.AddTableCond(t)
	}
	blacklistRules.Add(qr)

	// mock custom rules
	otherRules = New()
	qr = NewQueryRule("sample custom rule", "customrule_ban_bindvar", QRFail)
	qr.AddTableCond("t_customer")
	qr.AddBindVarCond("bindvar1", true, false, QRNoOp, nil)
	otherRules.Add(qr)
}

func TestMapRegisterARegisteredSource(t *testing.T) {
	setupRules()
	qri := NewMap()
	qri.RegisterSource(blacklistQueryRules)
	defer func() {
		err := recover()
		if err == nil {
			t.Fatalf("should get an error for registering a registered query rule source ")
		}
	}()
	qri.RegisterSource(blacklistQueryRules)
}

func TestMapSetRulesWithNil(t *testing.T) {
	setupRules()
	qri := NewMap()

	qri.RegisterSource(blacklistQueryRules)
	err := qri.SetRules(blacklistQueryRules, blacklistRules)
	if err != nil {
		t.Errorf("Failed to set blacklistQueryRules Rules : %s", err)
	}
	qrs, err := qri.Get(blacklistQueryRules)
	if err != nil {
		t.Errorf("GetRules failed to retrieve blacklistQueryRules that has been set: %s", err)
	}
	if !reflect.DeepEqual(qrs, blacklistRules) {
		t.Errorf("blacklistQueryRules retrieved is %v, but the expected value should be %v", qrs, blacklistQueryRules)
	}

	qri.SetRules(blacklistQueryRules, nil)

	qrs, err = qri.Get(blacklistQueryRules)
	if err != nil {
		t.Errorf("GetRules failed to retrieve blacklistQueryRules that has been set: %s", err)
	}
	if !reflect.DeepEqual(qrs, New()) {
		t.Errorf("blacklistQueryRules retrieved is %v, but the expected value should be %v", qrs, blacklistQueryRules)
	}
}

func TestMapGetSetQueryRules(t *testing.T) {
	setupRules()
	qri := NewMap()

	qri.RegisterSource(blacklistQueryRules)
	qri.RegisterSource(customQueryRules)

	// Test if we can get a Rules without a predefined rule set name
	qrs, err := qri.Get("Foo")
	if err == nil {
		t.Errorf("GetRules shouldn't succeed with 'Foo' as the rule set name")
	}
	if qrs == nil {
		t.Errorf("GetRules should always return empty Rules and never nil")
	}
	if !reflect.DeepEqual(qrs, New()) {
		t.Errorf("Map contains only empty Rules at the beginning")
	}

	// Test if we can set a Rules without a predefined rule set name
	err = qri.SetRules("Foo", New())
	if err == nil {
		t.Errorf("SetRules shouldn't succeed with 'Foo' as the rule set name")
	}

	// Test if we can successfully set Rules previously mocked into Map
	err = qri.SetRules(blacklistQueryRules, blacklistRules)
	if err != nil {
		t.Errorf("Failed to set blacklist Rules : %s", err)
	}
	err = qri.SetRules(blacklistQueryRules, blacklistRules)
	if err != nil {
		t.Errorf("Failed to set blacklist Rules: %s", err)
	}
	err = qri.SetRules(customQueryRules, otherRules)
	if err != nil {
		t.Errorf("Failed to set custom Rules: %s", err)
	}

	// Test if we can successfully retrieve rules that've been set
	qrs, err = qri.Get(blacklistQueryRules)
	if err != nil {
		t.Errorf("GetRules failed to retrieve blacklistQueryRules that has been set: %s", err)
	}
	if !reflect.DeepEqual(qrs, blacklistRules) {
		t.Errorf("blacklistQueryRules retrieved is %v, but the expected value should be %v", qrs, blacklistRules)
	}

	qrs, err = qri.Get(blacklistQueryRules)
	if err != nil {
		t.Errorf("GetRules failed to retrieve blacklistQueryRules that has been set: %s", err)
	}
	if !reflect.DeepEqual(qrs, blacklistRules) {
		t.Errorf("blacklistQueryRules retrieved is %v, but the expected value should be %v", qrs, blacklistRules)
	}

	qrs, err = qri.Get(customQueryRules)
	if err != nil {
		t.Errorf("GetRules failed to retrieve customQueryRules that has been set: %s", err)
	}
	if !reflect.DeepEqual(qrs, otherRules) {
		t.Errorf("customQueryRules retrieved is %v, but the expected value should be %v", qrs, customQueryRules)
	}
}

func TestMapFilterByPlan(t *testing.T) {
	var qrs *Rules
	setupRules()
	qri := NewMap()

	qri.RegisterSource(blacklistQueryRules)
	qri.RegisterSource(customQueryRules)

	qri.SetRules(blacklistQueryRules, blacklistRules)
	qri.SetRules(customQueryRules, otherRules)

	// Test filter by blacklist rule
	qrs = qri.FilterByPlan("select * from bannedtable2", planbuilder.PlanPassSelect, "bannedtable2")
	if l := len(qrs.rules); l != 1 {
		t.Errorf("Select from bannedtable matches %d rules, but we expect %d", l, 1)
	}
	if !strings.HasPrefix(qrs.rules[0].Name, "blacklisted_table") {
		t.Errorf("Select from bannedtable query matches rule '%s', but we expect rule with prefix '%s'", qrs.rules[0].Name, "blacklisted_table")
	}

	// Test filter by custom rule
	qrs = qri.FilterByPlan("select cid from t_customer limit 10", planbuilder.PlanPassSelect, "t_customer")
	if l := len(qrs.rules); l != 1 {
		t.Errorf("Select from t_customer matches %d rules, but we expect %d", l, 1)
	}
	if !strings.HasPrefix(qrs.rules[0].Name, "customrule_ban_bindvar") {
		t.Errorf("Select from t_customer matches rule '%s', but we expect rule with prefix '%s'", qrs.rules[0].Name, "customrule_ban_bindvar")
	}

	// Test match two rules: both blacklist rule and custom rule will be matched
	otherRules = New()
	qr := NewQueryRule("sample custom rule", "customrule_ban_bindvar", QRFail)
	qr.AddBindVarCond("bindvar1", true, false, QRNoOp, nil)
	otherRules.Add(qr)
	qri.SetRules(customQueryRules, otherRules)
	qrs = qri.FilterByPlan("select * from bannedtable2", planbuilder.PlanPassSelect, "bannedtable2")
	if l := len(qrs.rules); l != 2 {
		t.Errorf("Insert into bannedtable2 matches %d rules: %v, but we expect %d rules to be matched", l, qrs.rules, 2)
	}
}

func TestMapJSON(t *testing.T) {
	setupRules()
	qri := NewMap()
	qri.RegisterSource(blacklistQueryRules)
	_ = qri.SetRules(blacklistQueryRules, blacklistRules)
	qri.RegisterSource(customQueryRules)
	_ = qri.SetRules(customQueryRules, otherRules)
	got := marshalled(qri)
	want := compacted(`{
		"BLACKLIST_QUERY_RULES":[{
			"Description":"enforce blacklisted tables",
			"Name":"blacklisted_table",
			"TableNames":["bannedtable1","bannedtable2","bannedtable3"],
			"Action":"FAIL_RETRY"
		}],
		"CUSTOM_QUERY_RULES":[{
			"Description":"sample custom rule",
			"Name":"customrule_ban_bindvar",
			"TableNames":["t_customer"],
			"BindVarConds":[{"Name":"bindvar1","OnAbsent":true,"Operator":""}],
			"Action":"FAIL"
		}]
	}`)
	if got != want {
		t.Errorf("MapJSON:\n%v, want\n%v", got, want)
	}
}
