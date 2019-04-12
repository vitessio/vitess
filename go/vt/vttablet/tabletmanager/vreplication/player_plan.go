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

package vreplication

import (
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// PlayerPlan is the execution plan for a player stream.
type PlayerPlan struct {
	VStreamFilter *binlogdatapb.Filter
	TargetTables  map[string]*TablePlan
	TablePlans    map[string]*TablePlan
}

// TablePlan is the execution plan for a table within a player stream.
type TablePlan struct {
	Name         string
	SendRule     *binlogdatapb.Rule
	PKReferences []string               `json:",omitempty"`
	Insert       *sqlparser.ParsedQuery `json:",omitempty"`
	Update       *sqlparser.ParsedQuery `json:",omitempty"`
	Delete       *sqlparser.ParsedQuery `json:",omitempty"`
	Fields       []*querypb.Field       `json:",omitempty"`
}

func (tp *TablePlan) generateStatements(rowChange *binlogdatapb.RowChange) ([]string, error) {
	// MakeRowTrusted is needed here because Proto3ToResult is not convenient.
	var before, after bool
	bindvars := make(map[string]*querypb.BindVariable)
	if rowChange.Before != nil {
		before = true
		vals := sqltypes.MakeRowTrusted(tp.Fields, rowChange.Before)
		for i, field := range tp.Fields {
			bindvars["b_"+field.Name] = sqltypes.ValueBindVariable(vals[i])
		}
	}
	if rowChange.After != nil {
		after = true
		vals := sqltypes.MakeRowTrusted(tp.Fields, rowChange.After)
		for i, field := range tp.Fields {
			bindvars["a_"+field.Name] = sqltypes.ValueBindVariable(vals[i])
		}
	}
	switch {
	case !before && after:
		query, err := tp.Insert.GenerateQuery(bindvars, nil)
		if err != nil {
			return nil, err
		}
		return []string{query}, nil
	case before && !after:
		if tp.Delete == nil {
			return nil, nil
		}
		query, err := tp.Delete.GenerateQuery(bindvars, nil)
		if err != nil {
			return nil, err
		}
		return []string{query}, nil
	case before && after:
		if !tp.pkChanged(bindvars) {
			query, err := tp.Update.GenerateQuery(bindvars, nil)
			if err != nil {
				return nil, err
			}
			return []string{query}, nil
		}

		queries := make([]string, 0, 2)
		if tp.Delete != nil {
			query, err := tp.Delete.GenerateQuery(bindvars, nil)
			if err != nil {
				return nil, err
			}
			queries = append(queries, query)
		}
		query, err := tp.Insert.GenerateQuery(bindvars, nil)
		if err != nil {
			return nil, err
		}
		queries = append(queries, query)
		return queries, nil
	}
	return nil, nil
}

func (tp *TablePlan) pkChanged(bindvars map[string]*querypb.BindVariable) bool {
	for _, pkref := range tp.PKReferences {
		v1, _ := sqltypes.BindVariableToValue(bindvars["b_"+pkref])
		v2, _ := sqltypes.BindVariableToValue(bindvars["a_"+pkref])
		if !valsEqual(v1, v2) {
			return true
		}
	}
	return false
}

func valsEqual(v1, v2 sqltypes.Value) bool {
	if v1.IsNull() && v2.IsNull() {
		return true
	}
	// If any one of them is null, something has changed.
	if v1.IsNull() || v2.IsNull() {
		return false
	}
	// Compare content only if none are null.
	return v1.ToString() == v2.ToString()
}
