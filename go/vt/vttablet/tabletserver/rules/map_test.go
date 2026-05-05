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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
)

var (
	denyRules  *Rules
	otherRules *Rules
)

const (
	// mimic query rules from denylist
	denyListQueryRules string = "DENYLIST_QUERY_RULES"
	// mimic query rules from custom source
	customQueryRules string = "CUSTOM_QUERY_RULES"
)

func setupRules() {
	var qr *Rule

	// mock denied tables
	denyRules = New()
	deniedTables := []string{"bannedtable1", "bannedtable2", "bannedtable3"}
	qr = NewQueryRule("enforce denied tables", "denied_table", QRFailRetry)
	for _, t := range deniedTables {
		qr.AddTableCond(t)
	}
	denyRules.Add(qr)

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
	qri.RegisterSource(denyListQueryRules)
	defer func() {
		err := recover()
		require.NotNil(t, err, "should get an error for registering a registered query rule source")
	}()
	qri.RegisterSource(denyListQueryRules)
}

func TestMapSetRulesWithNil(t *testing.T) {
	setupRules()
	qri := NewMap()

	qri.RegisterSource(denyListQueryRules)
	err := qri.SetRules(denyListQueryRules, denyRules)
	assert.NoError(t, err)
	qrs, err := qri.Get(denyListQueryRules)
	assert.NoError(t, err)
	assert.Equal(t, denyRules, qrs, "denyListQueryRules")

	qri.SetRules(denyListQueryRules, nil)

	qrs, err = qri.Get(denyListQueryRules)
	assert.NoError(t, err)
	assert.Equal(t, New(), qrs, "denyListQueryRules")
}

func TestMapGetSetQueryRules(t *testing.T) {
	setupRules()
	qri := NewMap()

	qri.RegisterSource(denyListQueryRules)
	qri.RegisterSource(customQueryRules)

	// Test if we can get a Rules without a predefined rule set name
	qrs, err := qri.Get("Foo")
	assert.Error(t, err, "GetRules shouldn't succeed with 'Foo' as the rule set name")
	assert.NotNil(t, qrs, "GetRules should always return empty Rules and never nil")
	assert.Equal(t, New(), qrs, "Map contains only empty Rules at the beginning")

	// Test if we can set a Rules without a predefined rule set name
	err = qri.SetRules("Foo", New())
	assert.Error(t, err, "SetRules shouldn't succeed with 'Foo' as the rule set name")

	// Test if we can successfully set Rules previously mocked into Map
	err = qri.SetRules(denyListQueryRules, denyRules)
	assert.NoError(t, err)
	err = qri.SetRules(denyListQueryRules, denyRules)
	assert.NoError(t, err)
	err = qri.SetRules(customQueryRules, otherRules)
	assert.NoError(t, err)

	// Test if we can successfully retrieve rules which been set
	qrs, err = qri.Get(denyListQueryRules)
	assert.NoError(t, err)
	assert.Equal(t, denyRules, qrs, "denyListQueryRules")

	qrs, err = qri.Get(denyListQueryRules)
	assert.NoError(t, err)
	assert.Equal(t, denyRules, qrs, "denyListQueryRules")

	qrs, err = qri.Get(customQueryRules)
	assert.NoError(t, err)
	assert.Equal(t, otherRules, qrs, "customQueryRules")
}

func TestMapFilterByPlan(t *testing.T) {
	var qrs *Rules
	setupRules()
	qri := NewMap()

	qri.RegisterSource(denyListQueryRules)
	qri.RegisterSource(customQueryRules)

	qri.SetRules(denyListQueryRules, denyRules)
	qri.SetRules(customQueryRules, otherRules)

	// Test filter by denylist rule
	qrs = qri.FilterByPlan("select * from bannedtable2", planbuilder.PlanSelect, "bannedtable2")
	assert.Len(t, qrs.rules, 1, "Select from bannedtable")
	assert.Truef(t, strings.HasPrefix(qrs.rules[0].Name, "denied_table"), "Select from bannedtable query matches rule '%s', but we expect rule with prefix 'denied_table'", qrs.rules[0].Name)

	// Test filter by custom rule
	qrs = qri.FilterByPlan("select cid from t_customer limit 10", planbuilder.PlanSelect, "t_customer")
	assert.Len(t, qrs.rules, 1, "Select from t_customer")
	assert.Truef(t, strings.HasPrefix(qrs.rules[0].Name, "customrule_ban_bindvar"), "Select from t_customer matches rule '%s', but we expect rule with prefix 'customrule_ban_bindvar'", qrs.rules[0].Name)

	// Test match two rules: both denylist rule and custom rule will be matched
	otherRules = New()
	qr := NewQueryRule("sample custom rule", "customrule_ban_bindvar", QRFail)
	qr.AddBindVarCond("bindvar1", true, false, QRNoOp, nil)
	otherRules.Add(qr)
	qri.SetRules(customQueryRules, otherRules)
	qrs = qri.FilterByPlan("select * from bannedtable2", planbuilder.PlanSelect, "bannedtable2")
	assert.Lenf(t, qrs.rules, 2, "Insert into bannedtable2 matches rules: %v", qrs.rules)
}

func TestMapJSON(t *testing.T) {
	setupRules()
	qri := NewMap()
	qri.RegisterSource(denyListQueryRules)
	_ = qri.SetRules(denyListQueryRules, denyRules)
	qri.RegisterSource(customQueryRules)
	_ = qri.SetRules(customQueryRules, otherRules)
	got := marshalled(qri)
	want := compacted(`{
		"CUSTOM_QUERY_RULES":[{
			"Description":"sample custom rule",
			"Name":"customrule_ban_bindvar",
			"TableNames":["t_customer"],
			"BindVarConds":[{"Name":"bindvar1","OnAbsent":true,"Operator":""}],
			"Action":"FAIL"
		}],
		"DENYLIST_QUERY_RULES":[{
			"Description":"enforce denied tables",
			"Name":"denied_table",
			"TableNames":["bannedtable1","bannedtable2","bannedtable3"],
			"Action":"FAIL_RETRY"
		}]
	}`)
	assert.Equal(t, want, got, "MapJSON")
}
