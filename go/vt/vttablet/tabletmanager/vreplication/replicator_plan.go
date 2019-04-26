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
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// ReplicatorPlan is the execution plan for the replicator.
// The constructor for this is buildReplicatorPlan in table_plan_builder.go
// The initial build identifies the tables that need to be replicated,
// and builds partial TablePlan objects for them. The partial plan is used
// to send streaming requests. As the responses return field info, this
// information is used to build the final execution plan (buildExecutionPlan).
type ReplicatorPlan struct {
	VStreamFilter *binlogdatapb.Filter
	TargetTables  map[string]*TablePlan
	TablePlans    map[string]*TablePlan
	tableKeys     map[string][]string
}

// buildExecution plan uses the field info as input and the partially built
// TablePlan for that table to build a full plan.
func (rp *ReplicatorPlan) buildExecutionPlan(fieldEvent *binlogdatapb.FieldEvent) (*TablePlan, error) {
	prelim := rp.TablePlans[fieldEvent.TableName]
	if prelim == nil {
		// Unreachable code.
		return nil, fmt.Errorf("plan not found for %s", fieldEvent.TableName)
	}
	if prelim.Insert != nil {
		tplanv := *prelim
		tplanv.Fields = fieldEvent.Fields
		return &tplanv, nil
	}
	// select * construct was used. We need to use the field names.
	tplan, err := rp.buildFromFields(prelim.TargetName, fieldEvent.Fields)
	if err != nil {
		return nil, err
	}
	tplan.Fields = fieldEvent.Fields
	return tplan, nil
}

// buildFromFields builds a full TablePlan, but uses the field info as the
// full column list. This happens when the query used was a 'select *', which
// requires us to wait for the field info sent by the source.
func (rp *ReplicatorPlan) buildFromFields(tableName string, fields []*querypb.Field) (*TablePlan, error) {
	tpb := &tablePlanBuilder{
		name: sqlparser.NewTableIdent(tableName),
	}
	for _, field := range fields {
		colName := sqlparser.NewColIdent(field.Name)
		cexpr := &colExpr{
			colName: colName,
			expr: &sqlparser.ColName{
				Name: colName,
			},
			references: map[string]bool{
				field.Name: true,
			},
		}
		tpb.colExprs = append(tpb.colExprs, cexpr)
	}
	if err := tpb.analyzePK(rp.tableKeys); err != nil {
		return nil, err
	}
	return tpb.generate(rp.tableKeys), nil
}

// MarshalJSON performs a custom JSON Marshalling.
func (rp *ReplicatorPlan) MarshalJSON() ([]byte, error) {
	var targets []string
	for k := range rp.TargetTables {
		targets = append(targets, k)
	}
	sort.Strings(targets)
	v := struct {
		VStreamFilter *binlogdatapb.Filter
		TargetTables  []string
		TablePlans    map[string]*TablePlan
	}{
		VStreamFilter: rp.VStreamFilter,
		TargetTables:  targets,
		TablePlans:    rp.TablePlans,
	}
	return json.Marshal(&v)
}

// TablePlan is the execution plan for a table within a player stream.
// The ParsedQuery objects assume that a map of before and after values
// will be built based on the streaming rows. Before image values will
// be prefixed with a "b_", and after image values will be prefixed
// with a "a_".
type TablePlan struct {
	TargetName   string
	SendRule     *binlogdatapb.Rule
	PKReferences []string
	// BulkInsertFront, BulkInsertValues and BulkInsertOnDup are used
	// by vcopier.
	BulkInsertFront  *sqlparser.ParsedQuery
	BulkInsertValues *sqlparser.ParsedQuery
	BulkInsertOnDup  *sqlparser.ParsedQuery
	// Insert, Update and Delete are used by vplayer.
	// If the plan is an insertIgnore type, then Insert
	// and Update contain 'insert ignore' statements and
	// Delete is nil.
	Insert *sqlparser.ParsedQuery
	Update *sqlparser.ParsedQuery
	Delete *sqlparser.ParsedQuery
	Fields []*querypb.Field
}

// MarshalJSON performs a custom JSON Marshalling.
func (tp *TablePlan) MarshalJSON() ([]byte, error) {
	v := struct {
		TargetName   string
		SendRule     string
		PKReferences []string               `json:",omitempty"`
		InsertFront  *sqlparser.ParsedQuery `json:",omitempty"`
		InsertValues *sqlparser.ParsedQuery `json:",omitempty"`
		InsertOnDup  *sqlparser.ParsedQuery `json:",omitempty"`
		Insert       *sqlparser.ParsedQuery `json:",omitempty"`
		Update       *sqlparser.ParsedQuery `json:",omitempty"`
		Delete       *sqlparser.ParsedQuery `json:",omitempty"`
	}{
		TargetName:   tp.TargetName,
		SendRule:     tp.SendRule.Match,
		PKReferences: tp.PKReferences,
		InsertFront:  tp.BulkInsertFront,
		InsertValues: tp.BulkInsertValues,
		InsertOnDup:  tp.BulkInsertOnDup,
		Insert:       tp.Insert,
		Update:       tp.Update,
		Delete:       tp.Delete,
	}
	return json.Marshal(&v)
}

func (tp *TablePlan) generateBulkInsert(rows *binlogdatapb.VStreamRowsResponse) (string, error) {
	bindvars := make(map[string]*querypb.BindVariable, len(tp.Fields))
	var buf strings.Builder
	if err := tp.BulkInsertFront.Append(&buf, nil, nil); err != nil {
		return "", err
	}
	buf.WriteString(" values ")
	separator := ""
	for _, row := range rows.Rows {
		vals := sqltypes.MakeRowTrusted(tp.Fields, row)
		for i, field := range tp.Fields {
			bindvars["a_"+field.Name] = sqltypes.ValueBindVariable(vals[i])
		}
		buf.WriteString(separator)
		separator = ", "
		tp.BulkInsertValues.Append(&buf, bindvars, nil)
	}
	if tp.BulkInsertOnDup != nil {
		tp.BulkInsertOnDup.Append(&buf, nil, nil)
	}
	return buf.String(), nil
}

func (tp *TablePlan) generateStatements(rowChange *binlogdatapb.RowChange) ([]string, error) {
	// MakeRowTrusted is needed here because Proto3ToResult is not convenient.
	var before, after bool
	bindvars := make(map[string]*querypb.BindVariable, len(tp.Fields))
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
