// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"

	"code.google.com/p/vitess/go/vt/key"
	"code.google.com/p/vitess/go/vt/sqlparser"
)

//-----------------------------------------------

// QueryRules is used to store and execute rules for the tabletserver.
type QueryRules struct {
	rules []*QueryRule
}

// NewQueryRules creates a new QueryRules.
func NewQueryRules() *QueryRules {
	return &QueryRules{}
}

// Copy performs a deep copy of QueryRules.
// A nil input produces a nil output.
func (qrs *QueryRules) Copy() (newqrs *QueryRules) {
	newqrs = NewQueryRules()
	if qrs.rules != nil {
		newqrs.rules = make([]*QueryRule, 0, len(qrs.rules))
		for _, qr := range qrs.rules {
			newqrs.rules = append(newqrs.rules, qr.Copy())
		}
	}
	return newqrs
}

// Add adds a QueryRule to QueryRules. It does not check
// for duplicates.
func (qrs *QueryRules) Add(qr *QueryRule) {
	qrs.rules = append(qrs.rules, qr)
}

// Find finds the first occurrence of a QueryRule by matching
// the Name field. It returns nil if the rule was not found.
func (qrs *QueryRules) Find(name string) (qr *QueryRule) {
	for _, qr = range qrs.rules {
		if qr.Name == name {
			return qr
		}
	}
	return nil
}

// Delete deletes a QueryRule by name and returns the rule
// that was deleted. It returns nil if the rule was not found.
func (qrs *QueryRules) Delete(name string) (qr *QueryRule) {
	for i, qr := range qrs.rules {
		if qr.Name == name {
			for j := i; j < len(qrs.rules)-i-1; j++ {
				qrs.rules[j] = qrs.rules[j+1]
			}
			qrs.rules = qrs.rules[:len(qrs.rules)-1]
			return qr
		}
	}
	return nil
}

func (qrs *QueryRules) UnmarshalJSON(data []byte) (err error) {
	var rulesInfo []map[string]interface{}
	err = json.Unmarshal(data, &rulesInfo)
	if err != nil {
		return NewTabletError(FAIL, "%v", err)
	}
	for _, ruleInfo := range rulesInfo {
		qr, err := buildQueryRule(ruleInfo)
		if err != nil {
			return err
		}
		qrs.Add(qr)
	}
	return nil
}

// filterByPlan creates a new QueryRules by prefiltering on the query and planId. This allows
// us to create query plan specific QueryRules out of the original QueryRules. In the new rules,
// query and plans predicates are empty.
func (qrs *QueryRules) filterByPlan(query string, planid sqlparser.PlanType) (newqrs *QueryRules) {
	var newrules []*QueryRule
	for _, qr := range qrs.rules {
		if newrule := qr.filterByPlan(query, planid); newrule != nil {
			newrules = append(newrules, newrule)
		}
	}
	return &QueryRules{newrules}
}

func (qrs *QueryRules) getAction(ip, user string, bindVars map[string]interface{}) (action Action, desc string) {
	for _, qr := range qrs.rules {
		if qr.getAction(ip, user, bindVars) == QR_FAIL_QUERY {
			return QR_FAIL_QUERY, qr.Description
		}
	}
	return QR_CONTINUE, ""
}

//-----------------------------------------------

// QueryRule represents one rule (conditions-action).
// Name is meant to uniquely identify a rule.
// Description is a human readable comment that describes the rule.
// For a QueryRule to fire, all conditions of the QueryRule
// have to match. For example, an empty QueryRule will match
// all requests.
// Every QueryRule has an associated Action. If all the conditions
// of the QueryRule are met, then the Action is triggerred.
type QueryRule struct {
	Description string
	Name        string

	// All defined conditions must match for the rule to fire (AND).

	// Regexp conditions. nil conditions are ignored (TRUE).
	requestIP, user, query *regexp.Regexp

	// Any matched plan will make this condition true (OR)
	plans []sqlparser.PlanType

	// All BindVar conditions have to be fulfilled to make this true (AND)
	bindVarConds []BindVarCond
}

// NewQueryRule creates a new QueryRule.
func NewQueryRule(description, name string, act Action) (qr *QueryRule) {
	// We ignore act because there's only one action right now
	return &QueryRule{Description: description, Name: name}
}

// Copy performs a deep copy of a QueryRule.
func (qr *QueryRule) Copy() (newqr *QueryRule) {
	newqr = &QueryRule{
		Description: qr.Description,
		Name:        qr.Name,
		requestIP:   qr.requestIP,
		user:        qr.user,
		query:       qr.query,
	}
	if qr.plans != nil {
		newqr.plans = make([]sqlparser.PlanType, len(qr.plans))
		copy(newqr.plans, qr.plans)
	}
	if qr.bindVarConds != nil {
		newqr.bindVarConds = make([]BindVarCond, len(qr.bindVarConds))
		copy(newqr.bindVarConds, qr.bindVarConds)
	}
	return newqr
}

// SetIPCond adds a regular expression condition for the client IP.
// It has to be a full match (not substring).
func (qr *QueryRule) SetIPCond(pattern string) (err error) {
	qr.requestIP, err = regexp.Compile(makeExact(pattern))
	return
}

// SetUserCond adds a regular expression condition for the user name
// used by the client.
func (qr *QueryRule) SetUserCond(pattern string) (err error) {
	qr.user, err = regexp.Compile(makeExact(pattern))
	return
}

// AddPlanCond adds to the list of plans that can be matched for
// the rule to fire. Unlke all other condtions, this function acts
// as an OR: Any plan id match is considered a match.
func (qr *QueryRule) AddPlanCond(planType sqlparser.PlanType) {
	qr.plans = append(qr.plans, planType)
}

// SetQueryCond adds a regular expression condition for the query.
func (qr *QueryRule) SetQueryCond(pattern string) (err error) {
	qr.query, err = regexp.Compile(makeExact(pattern))
	return
}

// makeExact forces a full string match for the regex instead of substring
func makeExact(pattern string) string {
	return fmt.Sprintf("^%s$", pattern)
}

// AddBindVarCond adds a bind variable restriction to the QueryRule.
// All bind var conditions have to be satisfied for the QueryRule
// to be a match.
// name represents the name (not regexp) of the bind variable.
// onAbsent specifies the value of the condition if the
// bind variable is absent.
// onMismatch specifies the value of the condition if there's
// a type mismatch on the condition.
// For inequalities, the bindvar is the left operand and the value
// in the condition is the right operand: bindVar Operator value.
// Value & operator rules
// Type     Operators                              Bindvar
// nil      NOOP                                   any type
// uint64   EQ, NE, LT, GE, GT, LE                 whole numbers
// int64    EQ, NE, LT, GE, GT, LE                 whole numbers
// string   EQ, NE, LT, GE, GT, LE, MATCH, NOMATCH []byte, string
// KeyRange IN, NOTIN                              whole numbers
// whole numbers can be: int, int8, int16, int32, int64, uint64
func (qr *QueryRule) AddBindVarCond(name string, onAbsent, onMismatch bool, op Operator, value interface{}) error {
	var converted bvcValue
	if op == QR_NOOP {
		qr.bindVarConds = append(qr.bindVarConds, BindVarCond{name, onAbsent, onMismatch, op, nil})
		return nil
	}
	switch v := value.(type) {
	case uint64:
		if op < QR_EQ || op > QR_LE {
			goto Error
		}
		converted = bvcuint64(v)
	case int64:
		if op < QR_EQ || op > QR_LE {
			goto Error
		}
		converted = bvcint64(v)
	case string:
		if op >= QR_EQ && op <= QR_LE {
			converted = bvcstring(v)
		} else if op >= QR_MATCH && op <= QR_NOMATCH {
			var err error
			// Change the value to compiled regexp
			re, err := regexp.Compile(makeExact(v))
			if err != nil {
				return NewTabletError(FAIL, "Processing %s: %v", v, err)
			}
			converted = bvcre{re}
		} else {
			goto Error
		}
	case key.KeyRange:
		if op < QR_IN && op > QR_NOTIN {
			goto Error
		}
		converted = bvcKeyRange(v)
	default:
		return NewTabletError(FAIL, "Type %T not allowed as condition operand (%v)", value, value)
	}
	qr.bindVarConds = append(qr.bindVarConds, BindVarCond{name, onAbsent, onMismatch, op, converted})
	return nil

Error:
	return NewTabletError(FAIL, "Invalid operator %s for type %T (%v)", op, value, value)
}

// filterByPlan returns a new QueryRule if the query and planid match.
// The new QueryRule will contain all the original constraints other
// than the plan and query. If the plan and query don't match the QueryRule,
// then it returns nil.
func (qr *QueryRule) filterByPlan(query string, planid sqlparser.PlanType) (newqr *QueryRule) {
	if !reMatch(qr.query, query) {
		return nil
	}
	if !planMatch(qr.plans, planid) {
		return nil
	}
	newqr = qr.Copy()
	newqr.query = nil
	newqr.plans = nil
	return newqr
}

func (qr *QueryRule) getAction(ip, user string, bindVars map[string]interface{}) Action {
	if !reMatch(qr.requestIP, ip) {
		return QR_CONTINUE
	}
	if !reMatch(qr.user, user) {
		return QR_CONTINUE
	}
	for _, bvcond := range qr.bindVarConds {
		if !bvMatch(bvcond, bindVars) {
			return QR_CONTINUE
		}
	}
	return QR_FAIL_QUERY
}

func reMatch(re *regexp.Regexp, val string) bool {
	return re == nil || re.MatchString(val)
}

func planMatch(plans []sqlparser.PlanType, plan sqlparser.PlanType) bool {
	if plans == nil {
		return true
	}
	for _, p := range plans {
		if p == plan {
			return true
		}
	}
	return false
}

func bvMatch(bvcond BindVarCond, bindVars map[string]interface{}) bool {
	bv, ok := bindVars[bvcond.name]
	if !ok {
		return bvcond.onAbsent
	}
	if bvcond.op == QR_NOOP {
		return !bvcond.onAbsent
	}
	return bvcond.value.eval(bv, bvcond.op, bvcond.onMismatch)
}

//-----------------------------------------------
// Support types for QueryRule

// Action speficies the list of actions to perform
// when a QueryRule is triggered.
type Action int

const QR_CONTINUE = Action(0)
const QR_FAIL_QUERY = Action(1)

// BindVarCond represents a bind var condition.
type BindVarCond struct {
	name       string
	onAbsent   bool
	onMismatch bool
	op         Operator
	value      bvcValue
}

// Operator represents the list of operators.
type Operator int

const (
	QR_NOOP = Operator(iota)
	QR_EQ
	QR_NE
	QR_LT
	QR_GE
	QR_GT
	QR_LE
	QR_MATCH
	QR_NOMATCH
	QR_IN
	QR_NOTIN
)

var opmap = map[string]Operator{
	"NOOP":    QR_NOOP,
	"UEQ":     QR_EQ,
	"UNE":     QR_NE,
	"ULT":     QR_LT,
	"UGE":     QR_GE,
	"UGT":     QR_GT,
	"ULE":     QR_LE,
	"IEQ":     QR_EQ,
	"INE":     QR_NE,
	"ILT":     QR_LT,
	"IGE":     QR_GE,
	"IGT":     QR_GT,
	"ILE":     QR_LE,
	"SEQ":     QR_EQ,
	"SNE":     QR_NE,
	"SLT":     QR_LT,
	"SGE":     QR_GE,
	"SGT":     QR_GT,
	"SLE":     QR_LE,
	"MATCH":   QR_MATCH,
	"NOMATCH": QR_NOMATCH,
	"IN":      QR_IN,
	"NOTIN":   QR_NOTIN,
}

const (
	QR_OK = iota
	QR_MISMATCH
	QR_OUT_OF_RANGE
)

// bvcValue defines the common interface
// for all bind var condition values
type bvcValue interface {
	eval(bv interface{}, op Operator, onMismatch bool) bool
}

type bvcuint64 uint64

func (uval bvcuint64) eval(bv interface{}, op Operator, onMismatch bool) bool {
	num, status := getuint64(bv)
	switch op {
	case QR_EQ:
		switch status {
		case QR_OK:
			return num == uint64(uval)
		case QR_OUT_OF_RANGE:
			return false
		}
	case QR_NE:
		switch status {
		case QR_OK:
			return num != uint64(uval)
		case QR_OUT_OF_RANGE:
			return true
		}
	case QR_LT:
		switch status {
		case QR_OK:
			return num < uint64(uval)
		case QR_OUT_OF_RANGE:
			return true
		}
	case QR_GE:
		switch status {
		case QR_OK:
			return num >= uint64(uval)
		case QR_OUT_OF_RANGE:
			return false
		}
	case QR_GT:
		switch status {
		case QR_OK:
			return num > uint64(uval)
		case QR_OUT_OF_RANGE:
			return false
		}
	case QR_LE:
		switch status {
		case QR_OK:
			return num <= uint64(uval)
		case QR_OUT_OF_RANGE:
			return true
		}
	default:
		panic("unexpected:")
	}

	return onMismatch
}

type bvcint64 int64

func (ival bvcint64) eval(bv interface{}, op Operator, onMismatch bool) bool {
	num, status := getint64(bv)
	switch op {
	case QR_EQ:
		switch status {
		case QR_OK:
			return num == int64(ival)
		case QR_OUT_OF_RANGE:
			return false
		}
	case QR_NE:
		switch status {
		case QR_OK:
			return num != int64(ival)
		case QR_OUT_OF_RANGE:
			return true
		}
	case QR_LT:
		switch status {
		case QR_OK:
			return num < int64(ival)
		case QR_OUT_OF_RANGE:
			return false
		}
	case QR_GE:
		switch status {
		case QR_OK:
			return num >= int64(ival)
		case QR_OUT_OF_RANGE:
			return true
		}
	case QR_GT:
		switch status {
		case QR_OK:
			return num > int64(ival)
		case QR_OUT_OF_RANGE:
			return true
		}
	case QR_LE:
		switch status {
		case QR_OK:
			return num <= int64(ival)
		case QR_OUT_OF_RANGE:
			return false
		}
	default:
		panic("unexpected:")
	}

	return onMismatch
}

type bvcstring string

func (sval bvcstring) eval(bv interface{}, op Operator, onMismatch bool) bool {
	str, status := getstring(bv)
	if status != QR_OK {
		return onMismatch
	}
	switch op {
	case QR_EQ:
		return str == string(sval)
	case QR_NE:
		return str != string(sval)
	case QR_LT:
		return str < string(sval)
	case QR_GE:
		return str >= string(sval)
	case QR_GT:
		return str > string(sval)
	case QR_LE:
		return str <= string(sval)
	}
	panic("unexpected:")
}

type bvcre struct {
	re *regexp.Regexp
}

func (reval bvcre) eval(bv interface{}, op Operator, onMismatch bool) bool {
	str, status := getstring(bv)
	if status != QR_OK {
		return onMismatch
	}
	switch op {
	case QR_MATCH:
		return reval.re.MatchString(str)
	case QR_NOMATCH:
		return !reval.re.MatchString(str)
	}
	panic("unexpected:")
}

type bvcKeyRange key.KeyRange

func (krval bvcKeyRange) eval(bv interface{}, op Operator, onMismatch bool) bool {
	switch op {
	case QR_IN:
		switch num, status := getuint64(bv); status {
		case QR_OK:
			k := key.Uint64Key(num).KeyspaceId()
			return key.KeyRange(krval).Contains(k)
		case QR_OUT_OF_RANGE:
			return false
		}
		// Not a number. Check string.
		switch str, status := getstring(bv); status {
		case QR_OK:
			return key.KeyRange(krval).Contains(key.KeyspaceId(str))
		}
	case QR_NOTIN:
		switch num, status := getuint64(bv); status {
		case QR_OK:
			k := key.Uint64Key(num).KeyspaceId()
			return !key.KeyRange(krval).Contains(k)
		case QR_OUT_OF_RANGE:
			return true
		}
		// Not a number. Check string.
		switch str, status := getstring(bv); status {
		case QR_OK:
			return !key.KeyRange(krval).Contains(key.KeyspaceId(str))
		}
	default:
		panic("unexpected:")
	}
	return onMismatch
}

// getuint64 returns QR_OUT_OF_RANGE for negative values
func getuint64(val interface{}) (uv uint64, status int) {
	switch v := val.(type) {
	case int:
		if v < 0 {
			return 0, QR_OUT_OF_RANGE
		}
		return uint64(v), QR_OK
	case int8:
		if v < 0 {
			return 0, QR_OUT_OF_RANGE
		}
		return uint64(v), QR_OK
	case int16:
		if v < 0 {
			return 0, QR_OUT_OF_RANGE
		}
		return uint64(v), QR_OK
	case int32:
		if v < 0 {
			return 0, QR_OUT_OF_RANGE
		}
		return uint64(v), QR_OK
	case int64:
		if v < 0 {
			return 0, QR_OUT_OF_RANGE
		}
		return uint64(v), QR_OK
	case uint64:
		return v, QR_OK
	}
	return 0, QR_MISMATCH
}

// getint64 returns QR_OUT_OF_RANGE if a uint64 is too large
func getint64(val interface{}) (iv int64, status int) {
	switch v := val.(type) {
	case int:
		return int64(v), QR_OK
	case int8:
		return int64(v), QR_OK
	case int16:
		return int64(v), QR_OK
	case int32:
		return int64(v), QR_OK
	case int64:
		return int64(v), QR_OK
	case uint64:
		if v > 0x7FFFFFFFFFFFFFFF { // largest int64
			return 0, QR_OUT_OF_RANGE
		}
		return int64(v), QR_OK
	}
	return 0, QR_MISMATCH
}

func getstring(val interface{}) (sv string, status int) {
	switch v := val.(type) {
	case []byte:
		return string(v), QR_OK
	case string:
		return v, QR_OK
	}
	return "", QR_MISMATCH
}

//-----------------------------------------------
// Support functions for JSON

func buildQueryRule(ruleInfo map[string]interface{}) (qr *QueryRule, err error) {
	qr = NewQueryRule("", "", QR_FAIL_QUERY)
	for k, v := range ruleInfo {
		var sv string
		var lv []interface{}
		var ok bool
		switch k {
		case "Name", "Description", "RequestIP", "User", "Query":
			sv, ok = v.(string)
			if !ok {
				return nil, NewTabletError(FAIL, "Expecting string for %s", k)
			}
		case "Plans", "BindVarConds":
			lv, ok = v.([]interface{})
			if !ok {
				return nil, NewTabletError(FAIL, "Expecting list for %s", k)
			}
		default:
			return nil, NewTabletError(FAIL, "unrecognized tag %s", k)
		}
		switch k {
		case "Name":
			qr.Name = sv
		case "Description":
			qr.Description = sv
		case "RequestIP":
			err = qr.SetIPCond(sv)
			if err != nil {
				return nil, NewTabletError(FAIL, "Could not set IP condition: %v", sv)
			}
		case "User":
			err = qr.SetUserCond(sv)
			if err != nil {
				return nil, NewTabletError(FAIL, "Could not set User condition: %v", sv)
			}
		case "Query":
			err = qr.SetQueryCond(sv)
			if err != nil {
				return nil, NewTabletError(FAIL, "Could not set Query condition: %v", sv)
			}
		case "Plans":
			for _, p := range lv {
				pv, ok := p.(string)
				if !ok {
					return nil, NewTabletError(FAIL, "Expecting string for Plans")
				}
				pt, ok := sqlparser.PlanByName(pv)
				if !ok {
					return nil, NewTabletError(FAIL, "Invalid plan name: %s", pv)
				}
				qr.AddPlanCond(pt)
			}
		case "BindVarConds":
			for _, bvc := range lv {
				name, onAbsent, onMismatch, op, value, err := buildBindVarCondition(bvc)
				if err != nil {
					return nil, err
				}
				err = qr.AddBindVarCond(name, onAbsent, onMismatch, op, value)
				// Shouldn't happen, but check anyway
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return qr, nil
}

func buildBindVarCondition(bvc interface{}) (name string, onAbsent, onMismatch bool, op Operator, value interface{}, err error) {
	bvcinfo, ok := bvc.(map[string]interface{})
	if !ok {
		err = NewTabletError(FAIL, "Expecting json object for bind var conditions")
		return
	}

	var v interface{}
	v, ok = bvcinfo["Name"]
	if !ok {
		err = NewTabletError(FAIL, "Name missing in BindVarConds")
		return
	}
	name, ok = v.(string)
	if !ok {
		err = NewTabletError(FAIL, "Expecting string for Name in BindVarConds")
		return
	}

	v, ok = bvcinfo["OnAbsent"]
	if !ok {
		err = NewTabletError(FAIL, "OnAbsent missing in BindVarConds")
		return
	}
	onAbsent, ok = v.(bool)
	if !ok {
		err = NewTabletError(FAIL, "Expecting bool for OnAbsent")
		return
	}

	v, ok = bvcinfo["Operator"]
	if !ok {
		err = NewTabletError(FAIL, "Operator missing in BindVarConds")
		return
	}
	strop, ok := v.(string)
	if !ok {
		err = NewTabletError(FAIL, "Expecting string for Operator")
		return
	}
	op, ok = opmap[strop]
	if !ok {
		err = NewTabletError(FAIL, "Invalid Operator %s", strop)
		return
	}
	if op == QR_NOOP {
		return
	}
	v, ok = bvcinfo["Value"]
	if !ok {
		err = NewTabletError(FAIL, "Value missing in BindVarConds")
		return
	}
	if op >= QR_EQ && op <= QR_LE {
		strvalue, ok := v.(string)
		if !ok {
			err = NewTabletError(FAIL, "Expecting string: %v", v)
			return
		}
		if strop[0] == 'U' {
			value, err = strconv.ParseUint(strvalue, 0, 64)
			if err != nil {
				err = NewTabletError(FAIL, "Expecting uint64: %s", strvalue)
				return
			}
		} else if strop[0] == 'I' {
			value, err = strconv.ParseInt(strvalue, 0, 64)
			if err != nil {
				err = NewTabletError(FAIL, "Expecting int64: %s", strvalue)
				return
			}
		} else if strop[0] == 'S' {
			value = strvalue
		} else {
			panic("unexpected")
		}
	} else if op == QR_MATCH || op == QR_NOMATCH {
		strvalue, ok := v.(string)
		if !ok {
			err = NewTabletError(FAIL, "Expecting string: %v", v)
			return
		}
		value = strvalue
	} else if op == QR_IN || op == QR_NOTIN {
		kr, ok := v.(map[string]interface{})
		if !ok {
			err = NewTabletError(FAIL, "Expecting keyrange for Value")
			return
		}
		var keyrange key.KeyRange
		strstart, ok := kr["Start"]
		if !ok {
			err = NewTabletError(FAIL, "Start missing in KeyRange")
			return
		}
		start, ok := strstart.(string)
		if !ok {
			err = NewTabletError(FAIL, "Expecting string for Start")
			return
		}
		keyrange.Start = key.KeyspaceId(start)

		strend, ok := kr["End"]
		if !ok {
			err = NewTabletError(FAIL, "End missing in KeyRange")
			return
		}
		end, ok := strend.(string)
		if !ok {
			err = NewTabletError(FAIL, "Expecting string for End")
			return
		}
		keyrange.End = key.KeyspaceId(end)
		value = keyrange
	}

	v, ok = bvcinfo["OnMismatch"]
	if !ok {
		err = NewTabletError(FAIL, "OnMismatch missing in BindVarConds")
		return
	}
	onMismatch, ok = v.(bool)
	if !ok {
		err = NewTabletError(FAIL, "Expecting bool for OnAbsent")
		return
	}
	return
}
