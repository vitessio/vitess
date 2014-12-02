// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"encoding/json"
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

type PlanID int

const (
	NoPlan = PlanID(iota)
	SelectUnsharded
	SelectEqual
	SelectIN
	SelectScatter
	UpdateUnsharded
	UpdateEqual
	DeleteUnsharded
	DeleteEqual
	InsertUnsharded
	InsertSharded
	NumPlans
)

// Must exactly match order of plan constants.
var planName = [NumPlans]string{
	"NoPlan",
	"SelectUnsharded",
	"SelectEqual",
	"SelectIN",
	"SelectScatter",
	"UpdateUnsharded",
	"UpdateEqual",
	"DeleteUnsharded",
	"DeleteEqual",
	"InsertUnsharded",
	"InsertSharded",
}

type Plan struct {
	ID        PlanID
	Reason    string
	Table     *Table
	Original  string
	Rewritten string
	ColVindex *ColVindex
	Values    interface{}
}

func (pln *Plan) Size() int {
	return 1
}

func (pln *Plan) MarshalJSON() ([]byte, error) {
	var tname, vindexName, col string
	if pln.Table != nil {
		tname = pln.Table.Name
	}
	if pln.ColVindex != nil {
		vindexName = pln.ColVindex.Name
		col = pln.ColVindex.Col
	}
	marshalPlan := struct {
		ID        PlanID
		Reason    string
		Table     string
		Original  string
		Rewritten string
		Vindex    string
		Col       string
		Values    interface{}
	}{
		ID:        pln.ID,
		Reason:    pln.Reason,
		Table:     tname,
		Original:  pln.Original,
		Rewritten: pln.Rewritten,
		Vindex:    vindexName,
		Col:       col,
		Values:    pln.Values,
	}
	return json.Marshal(marshalPlan)
}

// IsMulti returns true if the SELECT query can potentially
// be sent to more than one shard.
func (pln *Plan) IsMulti() bool {
	if pln.ID == SelectIN || pln.ID == SelectScatter {
		return true
	}
	if pln.ID == SelectEqual && !IsUnique(pln.ColVindex.Vindex) {
		return true
	}
	return false
}

func (id PlanID) String() string {
	if id < 0 || id >= NumPlans {
		return ""
	}
	return planName[id]
}

func PlanByName(s string) (id PlanID, ok bool) {
	for i, v := range planName {
		if v == s {
			return PlanID(i), true
		}
	}
	return NumPlans, false
}

func (id PlanID) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", id.String())), nil
}

func BuildPlan(query string, schema *Schema) *Plan {
	statement, err := sqlparser.Parse(query)
	if err != nil {
		return &Plan{
			ID:       NoPlan,
			Reason:   err.Error(),
			Original: query,
		}
	}
	noplan := &Plan{
		ID:       NoPlan,
		Reason:   "too complex",
		Original: query,
	}
	var plan *Plan
	switch statement := statement.(type) {
	case *sqlparser.Select:
		plan = buildSelectPlan(statement, schema)
	case *sqlparser.Insert:
		plan = buildInsertPlan(statement, schema)
	case *sqlparser.Update:
		plan = buildUpdatePlan(statement, schema)
	case *sqlparser.Delete:
		plan = buildDeletePlan(statement, schema)
	case *sqlparser.Union, *sqlparser.Set, *sqlparser.DDL, *sqlparser.Other:
		return noplan
	default:
		panic("unexpected")
	}
	plan.Original = query
	return plan
}

func generateQuery(statement sqlparser.Statement) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	statement.Format(buf)
	return buf.String()
}
