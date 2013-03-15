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

type QueryRules struct {
	rules []*QueryRule
}

func NewQueryRules() *QueryRules {
	return &QueryRules{}
}

func (qrs *QueryRules) Add(qr *QueryRule) {
	qrs.rules = append(qrs.rules, qr)
}

func (qrs *QueryRules) Find(name string) (qr *QueryRule) {
	for _, qr = range qrs.rules {
		if qr.Name == name {
			return qr
		}
	}
	return nil
}

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

type QueryRule struct {
	Description string
	Name        string

	// All defined conditions must match for the rule to fire (AND).

	// Regexp conditions, nil conditions are ignored (TRUE).
	requestIP, user, query *regexp.Regexp

	// Any matched plan will make this condition true (OR)
	plans []sqlparser.PlanType

	// All BindVar conditions have to be fulfilled to make this true (AND)
	bindVars []BindVarCond
}

type Action int

const QR_FAIL_QUERY = Action(0)

func NewQueryRule(description, name string, act Action) (qr *QueryRule) {
	// We ignore act because there's only one action right now
	return &QueryRule{Description: description, Name: name}
}

func (qr *QueryRule) SetIPCond(pattern string) (err error) {
	qr.requestIP, err = regexp.Compile(makeExact(pattern))
	return
}

func (qr *QueryRule) SetUserCond(pattern string) (err error) {
	qr.user, err = regexp.Compile(makeExact(pattern))
	return
}

func (qr *QueryRule) AddPlanCond(planType sqlparser.PlanType) {
	qr.plans = append(qr.plans, planType)
}

func (qr *QueryRule) SetQueryCond(pattern string) (err error) {
	qr.query, err = regexp.Compile(makeExact(pattern))
	return
}

// makeExact forces a full string match for the regex instead of substring
func makeExact(pattern string) string {
	return fmt.Sprintf("^%s$", pattern)
}

func (qr *QueryRule) AddBindVarCond(name string, onAbsent, onMismatch bool, op Operator, value interface{}) error {
	if op == QR_NOOP {
		goto InputValid
	}
	switch value.(type) {
	case int64:
		if op >= QR_IEQ && op <= QR_ILE {
			goto InputValid
		}
	case uint64:
		if op >= QR_UEQ && op <= QR_ULE {
			goto InputValid
		}
	case string:
		if op >= QR_SEQ && op <= QR_SNOMATCH {
			goto InputValid
		}
	case key.KeyRange:
		if op >= QR_KIN && op <= QR_KNOTIN {
			goto InputValid
		}
	default:
		return fmt.Errorf("Type %T not allowed as condition operand (%v)", value, value)
	}

	// If we're here, it's a type mismatch
	return fmt.Errorf("Invalid operator %s for type %T (%v)", opname[op], value, value)

InputValid:
	qr.bindVars = append(qr.bindVars, BindVarCond{name, onAbsent, onMismatch, op, value})
	return nil
}

type BindVarCond struct {
	name       string
	onAbsent   bool
	onMismatch bool
	op         Operator
	value      interface{}
}

type Operator int

const (
	QR_NOOP = Operator(iota)
	QR_UEQ
	QR_UNE
	QR_ULT
	QR_UGE
	QR_UGT
	QR_ULE
	QR_IEQ
	QR_INE
	QR_ILT
	QR_IGE
	QR_IGT
	QR_ILE
	QR_SEQ
	QR_SNE
	QR_SLT
	QR_SGE
	QR_SGT
	QR_SLE
	QR_SMATCH
	QR_SNOMATCH
	QR_KIN
	QR_KNOTIN
)

// Order must exactly match operator constants
var opname = []string{
	"NOOP",
	"UEQ",
	"UNE",
	"ULT",
	"UGE",
	"UGT",
	"ULE",
	"IEQ",
	"INE",
	"ILT",
	"IGE",
	"IGT",
	"ILE",
	"SEQ",
	"SNE",
	"SLT",
	"SGE",
	"SGT",
	"SLE",
	"SMATCH",
	"SNOMATCH",
	"KIN",
	"KNOTIN",
}
