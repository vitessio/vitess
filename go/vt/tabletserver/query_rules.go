// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"regexp"

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
	if qrs == nil {
		return nil
	}
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

// filterByPlan creates a new QueryRules by prefiltering on the query and planId. This allows
// us to create query plan specific QueryRules out of the original QueryRules. In the new rules,
// query and plans predicates are empty.
func (qrs *QueryRules) filterByPlan(query string, planid sqlparser.PlanType) (newqrs *QueryRules) {
	if qrs == nil {
		return nil
	}
	var newrules []*QueryRule
	for _, qr := range qrs.rules {
		if newrule := qr.filterByPlan(query, planid); newrule != nil {
			newrules = append(newrules, newrule)
		}
	}
	if newrules == nil {
		return nil
	}
	return &QueryRules{newrules}
}

func (qrs *QueryRules) getAction(ip, user string, bindVars map[string]interface{}) Action {
	for _, qr := range qrs.rules {
		if qr.getAction(ip, user, bindVars) == QR_FAIL_QUERY {
			return QR_FAIL_QUERY
		}
	}
	return QR_CONTINUE
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
// uint64   EQ, NE, LT, GE, GT, LE                 int8, int16, int32, int64, uint64
// int64    EQ, NE, LT, GE, GT, LE                 int8, int16, int32, int64, uint64
// string   EQ, NE, LT, GE, GT, LE, MATCH, NOMATCH []byte, string
// KeyRange IN, NOTIN                              int8, int16, int32, int64, uint64
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
		converted = bvcuint(v)
	case int64:
		if op < QR_EQ || op > QR_LE {
			goto Error
		}
		converted = bvcint(v)
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
	return NewTabletError(FAIL, "Invalid operator %s for type %T (%v)", opname[op], value, value)
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

// Order must exactly match operator constants
var opname = []string{
	"NOOP",
	"EQ",
	"NE",
	"LT",
	"GE",
	"GT",
	"LE",
	"MATCH",
	"NOMATCH",
	"IN",
	"NOTIN",
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

type bvcuint uint64

func (uval bvcuint) eval(bv interface{}, op Operator, onMismatch bool) bool {
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

type bvcint int64

func (ival bvcint) eval(bv interface{}, op Operator, onMismatch bool) bool {
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
