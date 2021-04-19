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
	"fmt"
	"reflect"
	"regexp"
	"strconv"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

//-----------------------------------------------

// Rules is used to store and execute rules for the tabletserver.
type Rules struct {
	rules []*Rule
}

// New creates a new Rules.
func New() *Rules {
	return &Rules{}
}

// Equal returns true if other is equal to this object, otherwise false.
func (qrs *Rules) Equal(other *Rules) bool {
	if len(qrs.rules) != len(other.rules) {
		return false
	}
	for i := 0; i < len(qrs.rules); i++ {
		if !qrs.rules[i].Equal(other.rules[i]) {
			return false
		}
	}
	return true
}

// Copy performs a deep copy of Rules.
// A nil input produces a nil output.
func (qrs *Rules) Copy() (newqrs *Rules) {
	newqrs = New()
	if qrs.rules != nil {
		newqrs.rules = make([]*Rule, 0, len(qrs.rules))
		for _, qr := range qrs.rules {
			newqrs.rules = append(newqrs.rules, qr.Copy())
		}
	}
	return newqrs
}

// CopyUnderlying makes a copy of the underlying rule array and returns it to
// the caller.
func (qrs *Rules) CopyUnderlying() []*Rule {
	cpy := make([]*Rule, 0, len(qrs.rules))
	for _, r := range qrs.rules {
		cpy = append(cpy, r.Copy())
	}
	return cpy
}

// Append merges the rules from another Rules into the receiver
func (qrs *Rules) Append(otherqrs *Rules) {
	qrs.rules = append(qrs.rules, otherqrs.rules...)
}

// Add adds a Rule to Rules. It does not check
// for duplicates.
func (qrs *Rules) Add(qr *Rule) {
	qrs.rules = append(qrs.rules, qr)
}

// Find finds the first occurrence of a Rule by matching
// the Name field. It returns nil if the rule was not found.
func (qrs *Rules) Find(name string) (qr *Rule) {
	for _, qr = range qrs.rules {
		if qr.Name == name {
			return qr
		}
	}
	return nil
}

// Delete deletes a Rule by name and returns the rule
// that was deleted. It returns nil if the rule was not found.
func (qrs *Rules) Delete(name string) (qr *Rule) {
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

// UnmarshalJSON unmarshals Rules.
func (qrs *Rules) UnmarshalJSON(data []byte) (err error) {
	var rulesInfo []map[string]interface{}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	err = dec.Decode(&rulesInfo)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
	}
	for _, ruleInfo := range rulesInfo {
		qr, err := BuildQueryRule(ruleInfo)
		if err != nil {
			return err
		}
		qrs.Add(qr)
	}
	return nil
}

// MarshalJSON marshals to JSON.
func (qrs *Rules) MarshalJSON() ([]byte, error) {
	b := bytes.NewBuffer(nil)
	_, _ = b.WriteString("[")
	for i, rule := range qrs.rules {
		if i != 0 {
			_, _ = b.WriteString(",")
		}
		safeEncode(b, "", rule)
	}
	_, _ = b.WriteString("]")
	return b.Bytes(), nil
}

// FilterByPlan creates a new Rules by prefiltering on the query and planId. This allows
// us to create query plan specific Rules out of the original Rules. In the new rules,
// query, plans and tableNames predicates are empty.
func (qrs *Rules) FilterByPlan(query string, planid planbuilder.PlanType, tableName string) (newqrs *Rules) {
	var newrules []*Rule
	for _, qr := range qrs.rules {
		if newrule := qr.FilterByPlan(query, planid, tableName); newrule != nil {
			newrules = append(newrules, newrule)
		}
	}
	return &Rules{newrules}
}

// GetAction runs the input against the rules engine and returns the action to be performed.
func (qrs *Rules) GetAction(ip, user string, bindVars map[string]*querypb.BindVariable) (action Action, desc string) {
	for _, qr := range qrs.rules {
		if act := qr.GetAction(ip, user, bindVars); act != QRContinue {
			return act, qr.Description
		}
	}
	return QRContinue, ""
}

//-----------------------------------------------

// Rule represents one rule (conditions-action).
// Name is meant to uniquely identify a rule.
// Description is a human readable comment that describes the rule.
// For a Rule to fire, all conditions of the Rule
// have to match. For example, an empty Rule will match
// all requests.
// Every Rule has an associated Action. If all the conditions
// of the Rule are met, then the Action is triggerred.
type Rule struct {
	Description string
	Name        string

	// All defined conditions must match for the rule to fire (AND).

	// Regexp conditions. nil conditions are ignored (TRUE).
	requestIP, user, query namedRegexp

	// Any matched plan will make this condition true (OR)
	plans []planbuilder.PlanType

	// Any matched tableNames will make this condition true (OR)
	tableNames []string

	// All BindVar conditions have to be fulfilled to make this true (AND)
	bindVarConds []BindVarCond

	// Action to be performed on trigger
	act Action
}

type namedRegexp struct {
	name string
	*regexp.Regexp
}

// MarshalJSON marshals to JSON.
func (nr namedRegexp) MarshalJSON() ([]byte, error) {
	return json.Marshal(nr.name)
}

// Equal returns true if other is equal to this namedRegexp, otherwise false.
func (nr namedRegexp) Equal(other namedRegexp) bool {
	if nr.Regexp == nil || other.Regexp == nil {
		return nr.Regexp == nil && other.Regexp == nil && nr.name == other.name
	}
	return nr.name == other.name && nr.String() == other.String()
}

// NewQueryRule creates a new Rule.
func NewQueryRule(description, name string, act Action) (qr *Rule) {
	// We ignore act because there's only one action right now
	return &Rule{Description: description, Name: name, act: act}
}

// Equal returns true if other is equal to this Rule, otherwise false.
func (qr *Rule) Equal(other *Rule) bool {
	if qr == nil || other == nil {
		return qr == nil && other == nil
	}
	return (qr.Description == other.Description &&
		qr.Name == other.Name &&
		qr.requestIP.Equal(other.requestIP) &&
		qr.user.Equal(other.user) &&
		qr.query.Equal(other.query) &&
		reflect.DeepEqual(qr.plans, other.plans) &&
		reflect.DeepEqual(qr.tableNames, other.tableNames) &&
		reflect.DeepEqual(qr.bindVarConds, other.bindVarConds) &&
		qr.act == other.act)
}

// Copy performs a deep copy of a Rule.
func (qr *Rule) Copy() (newqr *Rule) {
	newqr = &Rule{
		Description: qr.Description,
		Name:        qr.Name,
		requestIP:   qr.requestIP,
		user:        qr.user,
		query:       qr.query,
		act:         qr.act,
	}
	if qr.plans != nil {
		newqr.plans = make([]planbuilder.PlanType, len(qr.plans))
		copy(newqr.plans, qr.plans)
	}
	if qr.tableNames != nil {
		newqr.tableNames = make([]string, len(qr.tableNames))
		copy(newqr.tableNames, qr.tableNames)
	}
	if qr.bindVarConds != nil {
		newqr.bindVarConds = make([]BindVarCond, len(qr.bindVarConds))
		copy(newqr.bindVarConds, qr.bindVarConds)
	}
	return newqr
}

// MarshalJSON marshals to JSON.
func (qr *Rule) MarshalJSON() ([]byte, error) {
	b := bytes.NewBuffer(nil)
	safeEncode(b, `{"Description":`, qr.Description)
	safeEncode(b, `,"Name":`, qr.Name)
	if qr.requestIP.Regexp != nil {
		safeEncode(b, `,"RequestIP":`, qr.requestIP)
	}
	if qr.user.Regexp != nil {
		safeEncode(b, `,"User":`, qr.user)
	}
	if qr.query.Regexp != nil {
		safeEncode(b, `,"Query":`, qr.query)
	}
	if qr.plans != nil {
		safeEncode(b, `,"Plans":`, qr.plans)
	}
	if qr.tableNames != nil {
		safeEncode(b, `,"TableNames":`, qr.tableNames)
	}
	if qr.bindVarConds != nil {
		safeEncode(b, `,"BindVarConds":`, qr.bindVarConds)
	}
	if qr.act != QRContinue {
		safeEncode(b, `,"Action":`, qr.act)
	}
	_, _ = b.WriteString("}")
	return b.Bytes(), nil
}

// SetIPCond adds a regular expression condition for the client IP.
// It has to be a full match (not substring).
func (qr *Rule) SetIPCond(pattern string) (err error) {
	qr.requestIP.name = pattern
	qr.requestIP.Regexp, err = regexp.Compile(makeExact(pattern))
	return err
}

// SetUserCond adds a regular expression condition for the user name
// used by the client.
func (qr *Rule) SetUserCond(pattern string) (err error) {
	qr.user.name = pattern
	qr.user.Regexp, err = regexp.Compile(makeExact(pattern))
	return
}

// AddPlanCond adds to the list of plans that can be matched for
// the rule to fire.
// This function acts as an OR: Any plan id match is considered a match.
func (qr *Rule) AddPlanCond(planType planbuilder.PlanType) {
	qr.plans = append(qr.plans, planType)
}

// AddTableCond adds to the list of tableNames that can be matched for
// the rule to fire.
// This function acts as an OR: Any tableName match is considered a match.
func (qr *Rule) AddTableCond(tableName string) {
	qr.tableNames = append(qr.tableNames, tableName)
}

// SetQueryCond adds a regular expression condition for the query.
func (qr *Rule) SetQueryCond(pattern string) (err error) {
	qr.query.name = pattern
	qr.query.Regexp, err = regexp.Compile(makeExact(pattern))
	return
}

// makeExact forces a full string match for the regex instead of substring
func makeExact(pattern string) string {
	return fmt.Sprintf("^%s$", pattern)
}

// AddBindVarCond adds a bind variable restriction to the Rule.
// All bind var conditions have to be satisfied for the Rule
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
// nil      ""                                     any type
// uint64   ==, !=, <, >=, >, <=                   whole numbers
// int64    ==, !=, <, >=, >, <=                   whole numbers
// string   ==, !=, <, >=, >, <=, MATCH, NOMATCH   []byte, string
// whole numbers can be: int, int8, int16, int32, int64, uint64
func (qr *Rule) AddBindVarCond(name string, onAbsent, onMismatch bool, op Operator, value interface{}) error {
	var converted bvcValue
	if op == QRNoOp {
		qr.bindVarConds = append(qr.bindVarConds, BindVarCond{name, onAbsent, onMismatch, op, nil})
		return nil
	}
	switch v := value.(type) {
	case uint64:
		if op < QREqual || op > QRLessEqual {
			goto Error
		}
		converted = bvcuint64(v)
	case int64:
		if op < QREqual || op > QRLessEqual {
			goto Error
		}
		converted = bvcint64(v)
	case string:
		if op >= QREqual && op <= QRLessEqual {
			converted = bvcstring(v)
		} else if op >= QRMatch && op <= QRNoMatch {
			var err error
			// Change the value to compiled regexp
			re, err := regexp.Compile(makeExact(v))
			if err != nil {
				return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "processing %s: %v", v, err)
			}
			converted = bvcre{re}
		} else {
			goto Error
		}
	default:
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "type %T not allowed as condition operand (%v)", value, value)
	}
	qr.bindVarConds = append(qr.bindVarConds, BindVarCond{name, onAbsent, onMismatch, op, converted})
	return nil

Error:
	return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid operator %v for type %T (%v)", op, value, value)
}

// FilterByPlan returns a new Rule if the query and planid match.
// The new Rule will contain all the original constraints other
// than the plan and query. If the plan and query don't match the Rule,
// then it returns nil.
func (qr *Rule) FilterByPlan(query string, planid planbuilder.PlanType, tableName string) (newqr *Rule) {
	if !reMatch(qr.query.Regexp, query) {
		return nil
	}
	if !planMatch(qr.plans, planid) {
		return nil
	}
	if !tableMatch(qr.tableNames, tableName) {
		return nil
	}
	newqr = qr.Copy()
	newqr.query = namedRegexp{}
	newqr.plans = nil
	newqr.tableNames = nil
	return newqr
}

// GetAction returns the action for a single rule.
func (qr *Rule) GetAction(ip, user string, bindVars map[string]*querypb.BindVariable) Action {
	if !reMatch(qr.requestIP.Regexp, ip) {
		return QRContinue
	}
	if !reMatch(qr.user.Regexp, user) {
		return QRContinue
	}
	for _, bvcond := range qr.bindVarConds {
		if !bvMatch(bvcond, bindVars) {
			return QRContinue
		}
	}
	return qr.act
}

func reMatch(re *regexp.Regexp, val string) bool {
	return re == nil || re.MatchString(val)
}

func planMatch(plans []planbuilder.PlanType, plan planbuilder.PlanType) bool {
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

func tableMatch(tableNames []string, tableName string) bool {
	if tableNames == nil {
		return true
	}
	for _, t := range tableNames {
		if t == tableName {
			return true
		}
	}
	return false
}

func bvMatch(bvcond BindVarCond, bindVars map[string]*querypb.BindVariable) bool {
	bv, ok := bindVars[bvcond.name]
	if !ok {
		return bvcond.onAbsent
	}
	if bvcond.op == QRNoOp {
		return !bvcond.onAbsent
	}
	return bvcond.value.eval(bv, bvcond.op, bvcond.onMismatch)
}

//-----------------------------------------------
// Support types for Rule

// Action speficies the list of actions to perform
// when a Rule is triggered.
type Action int

// These are actions.
const (
	QRContinue = Action(iota)
	QRFail
	QRFailRetry
)

// MarshalJSON marshals to JSON.
func (act Action) MarshalJSON() ([]byte, error) {
	// If we add more actions, we'll need to use a map.
	var str string
	switch act {
	case QRFail:
		str = "FAIL"
	case QRFailRetry:
		str = "FAIL_RETRY"
	default:
		str = "INVALID"
	}
	return json.Marshal(str)
}

// BindVarCond represents a bind var condition.
type BindVarCond struct {
	name       string
	onAbsent   bool
	onMismatch bool
	op         Operator
	value      bvcValue
}

// MarshalJSON marshals to JSON.
func (bvc BindVarCond) MarshalJSON() ([]byte, error) {
	b := bytes.NewBuffer(nil)
	safeEncode(b, `{"Name":`, bvc.name)
	safeEncode(b, `,"OnAbsent":`, bvc.onAbsent)
	if bvc.op != QRNoOp {
		safeEncode(b, `,"OnMismatch":`, bvc.onMismatch)
	}
	safeEncode(b, `,"Operator":`, bvc.op)
	if bvc.op != QRNoOp {
		safeEncode(b, `,"Value":`, bvc.value)
	}
	_, _ = b.WriteString("}")
	return b.Bytes(), nil
}

// Operator represents the list of operators.
type Operator int

// These are comparison operators.
const (
	QRNoOp = Operator(iota)
	QREqual
	QRNotEqual
	QRLessThan
	QRGreaterEqual
	QRGreaterThan
	QRLessEqual
	QRMatch
	QRNoMatch
	QRNumOp
)

var opmap = map[string]Operator{
	"":        QRNoOp,
	"==":      QREqual,
	"!=":      QRNotEqual,
	"<":       QRLessThan,
	">=":      QRGreaterEqual,
	">":       QRGreaterThan,
	"<=":      QRLessEqual,
	"MATCH":   QRMatch,
	"NOMATCH": QRNoMatch,
}

var opnames []string

func init() {
	opnames = make([]string, QRNumOp)
	for k, v := range opmap {
		opnames[v] = k
	}
}

// These are return statii.
const (
	QROK = iota
	QRMismatch
	QROutOfRange
)

// MarshalJSON marshals to JSON.
func (op Operator) MarshalJSON() ([]byte, error) {
	return json.Marshal(opnames[op])
}

// bvcValue defines the common interface
// for all bind var condition values
type bvcValue interface {
	eval(bv *querypb.BindVariable, op Operator, onMismatch bool) bool
}

type bvcuint64 uint64

func (uval bvcuint64) eval(bv *querypb.BindVariable, op Operator, onMismatch bool) bool {
	num, status := getuint64(bv)
	switch op {
	case QREqual:
		switch status {
		case QROK:
			return num == uint64(uval)
		case QROutOfRange:
			return false
		}
	case QRNotEqual:
		switch status {
		case QROK:
			return num != uint64(uval)
		case QROutOfRange:
			return true
		}
	case QRLessThan:
		switch status {
		case QROK:
			return num < uint64(uval)
		case QROutOfRange:
			return true
		}
	case QRGreaterEqual:
		switch status {
		case QROK:
			return num >= uint64(uval)
		case QROutOfRange:
			return false
		}
	case QRGreaterThan:
		switch status {
		case QROK:
			return num > uint64(uval)
		case QROutOfRange:
			return false
		}
	case QRLessEqual:
		switch status {
		case QROK:
			return num <= uint64(uval)
		case QROutOfRange:
			return true
		}
	default:
		panic("unreachable")
	}

	return onMismatch
}

type bvcint64 int64

func (ival bvcint64) eval(bv *querypb.BindVariable, op Operator, onMismatch bool) bool {
	num, status := getint64(bv)
	switch op {
	case QREqual:
		switch status {
		case QROK:
			return num == int64(ival)
		case QROutOfRange:
			return false
		}
	case QRNotEqual:
		switch status {
		case QROK:
			return num != int64(ival)
		case QROutOfRange:
			return true
		}
	case QRLessThan:
		switch status {
		case QROK:
			return num < int64(ival)
		case QROutOfRange:
			return false
		}
	case QRGreaterEqual:
		switch status {
		case QROK:
			return num >= int64(ival)
		case QROutOfRange:
			return true
		}
	case QRGreaterThan:
		switch status {
		case QROK:
			return num > int64(ival)
		case QROutOfRange:
			return true
		}
	case QRLessEqual:
		switch status {
		case QROK:
			return num <= int64(ival)
		case QROutOfRange:
			return false
		}
	default:
		panic("unreachable")
	}

	return onMismatch
}

type bvcstring string

func (sval bvcstring) eval(bv *querypb.BindVariable, op Operator, onMismatch bool) bool {
	str, status := getstring(bv)
	if status != QROK {
		return onMismatch
	}
	switch op {
	case QREqual:
		return str == string(sval)
	case QRNotEqual:
		return str != string(sval)
	case QRLessThan:
		return str < string(sval)
	case QRGreaterEqual:
		return str >= string(sval)
	case QRGreaterThan:
		return str > string(sval)
	case QRLessEqual:
		return str <= string(sval)
	}
	panic("unreachable")
}

type bvcre struct {
	re *regexp.Regexp
}

func (reval bvcre) eval(bv *querypb.BindVariable, op Operator, onMismatch bool) bool {
	str, status := getstring(bv)
	if status != QROK {
		return onMismatch
	}
	switch op {
	case QRMatch:
		return reval.re.MatchString(str)
	case QRNoMatch:
		return !reval.re.MatchString(str)
	}
	panic("unreachable")
}

// getuint64 returns QROutOfRange for negative values
func getuint64(val *querypb.BindVariable) (uv uint64, status int) {
	bv, err := sqltypes.BindVariableToValue(val)
	if err != nil {
		return 0, QROutOfRange
	}
	v, err := evalengine.ToUint64(bv)
	if err != nil {
		return 0, QROutOfRange
	}
	return v, QROK
}

// getint64 returns QROutOfRange if a uint64 is too large
func getint64(val *querypb.BindVariable) (iv int64, status int) {
	bv, err := sqltypes.BindVariableToValue(val)
	if err != nil {
		return 0, QROutOfRange
	}
	v, err := evalengine.ToInt64(bv)
	if err != nil {
		return 0, QROutOfRange
	}
	return v, QROK
}

// TODO(sougou): this is inefficient. Optimize to use []byte.
func getstring(val *querypb.BindVariable) (s string, status int) {
	if sqltypes.IsIntegral(val.Type) || sqltypes.IsFloat(val.Type) || sqltypes.IsText(val.Type) || sqltypes.IsBinary(val.Type) {
		return string(val.Value), QROK
	}
	return "", QRMismatch
}

//-----------------------------------------------
// Support functions for JSON

// MapStrOperator maps a string representation to an Operator.
func MapStrOperator(strop string) (op Operator, err error) {
	if op, ok := opmap[strop]; ok {
		return op, nil
	}
	return QRNoOp, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid Operator %s", strop)
}

// BuildQueryRule builds a query rule from a ruleInfo.
func BuildQueryRule(ruleInfo map[string]interface{}) (qr *Rule, err error) {
	qr = NewQueryRule("", "", QRFail)
	for k, v := range ruleInfo {
		var sv string
		var lv []interface{}
		var ok bool
		switch k {
		case "Name", "Description", "RequestIP", "User", "Query", "Action":
			sv, ok = v.(string)
			if !ok {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want string for %s", k)
			}
		case "Plans", "BindVarConds", "TableNames":
			lv, ok = v.([]interface{})
			if !ok {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want list for %s", k)
			}
		default:
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unrecognized tag %s", k)
		}
		switch k {
		case "Name":
			qr.Name = sv
		case "Description":
			qr.Description = sv
		case "RequestIP":
			err = qr.SetIPCond(sv)
			if err != nil {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "could not set IP condition: %v", sv)
			}
		case "User":
			err = qr.SetUserCond(sv)
			if err != nil {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "could not set User condition: %v", sv)
			}
		case "Query":
			err = qr.SetQueryCond(sv)
			if err != nil {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "could not set Query condition: %v", sv)
			}
		case "Plans":
			for _, p := range lv {
				pv, ok := p.(string)
				if !ok {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want string for Plans")
				}
				pt, ok := planbuilder.PlanByName(pv)
				if !ok {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid plan name: %s", pv)
				}
				qr.AddPlanCond(pt)
			}
		case "TableNames":
			for _, t := range lv {
				tableName, ok := t.(string)
				if !ok {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want string for TableNames")
				}
				qr.AddTableCond(tableName)
			}
		case "BindVarConds":
			for _, bvc := range lv {
				name, onAbsent, onMismatch, op, value, err := buildBindVarCondition(bvc)
				if err != nil {
					return nil, err
				}
				err = qr.AddBindVarCond(name, onAbsent, onMismatch, op, value)
				if err != nil {
					return nil, err
				}
			}
		case "Action":
			switch sv {
			case "FAIL":
				qr.act = QRFail
			case "FAIL_RETRY":
				qr.act = QRFailRetry
			default:
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid Action %s", sv)
			}
		}
	}
	return qr, nil
}

func buildBindVarCondition(bvc interface{}) (name string, onAbsent, onMismatch bool, op Operator, value interface{}, err error) {
	bvcinfo, ok := bvc.(map[string]interface{})
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want json object for bind var conditions")
		return
	}

	var v interface{}
	v, ok = bvcinfo["Name"]
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Name missing in BindVarConds")
		return
	}
	name, ok = v.(string)
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want string for Name in BindVarConds")
		return
	}

	v, ok = bvcinfo["OnAbsent"]
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "OnAbsent missing in BindVarConds")
		return
	}
	onAbsent, ok = v.(bool)
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want bool for OnAbsent")
		return
	}

	v, ok = bvcinfo["Operator"]
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Operator missing in BindVarConds")
		return
	}
	strop, ok := v.(string)
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want string for Operator")
		return
	}
	op, err = MapStrOperator(strop)
	if err != nil {
		return
	}
	if op == QRNoOp {
		return
	}
	v, ok = bvcinfo["Value"]
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Value missing in BindVarConds")
		return
	}
	if op >= QREqual && op <= QRLessEqual {
		switch v := v.(type) {
		case json.Number:
			value, err = v.Int64()
			if err != nil {
				// Maybe uint64
				value, err = strconv.ParseUint(string(v), 10, 64)
				if err != nil {
					err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want int64/uint64: %s", string(v))
					return
				}
			}
		case string:
			value = v
		default:
			err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want string or number: %v", v)
			return
		}
	} else if op == QRMatch || op == QRNoMatch {
		strvalue, ok := v.(string)
		if !ok {
			err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want string: %v", v)
			return
		}
		value = strvalue
	}

	v, ok = bvcinfo["OnMismatch"]
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "OnMismatch missing in BindVarConds")
		return
	}
	onMismatch, ok = v.(bool)
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want bool for OnMismatch")
		return
	}
	return
}

func safeEncode(b *bytes.Buffer, prefix string, v interface{}) {
	enc := json.NewEncoder(b)
	_, _ = b.WriteString(prefix)
	if err := enc.Encode(v); err != nil {
		_ = enc.Encode(err.Error())
	}
}
