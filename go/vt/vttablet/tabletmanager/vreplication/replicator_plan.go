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

// ReplicatorPlan is the execution plan for the replicator. It contains
// plans for all the tables it's replicating. Every phase of vreplication
// builds its own instance of the ReplicatorPlan. This is because the plan
// depends on copyState, which changes on every iteration.
// The table plans within ReplicatorPlan will not be fully populated because
// all the information is not available initially.
// For simplicity, the ReplicatorPlan is immutable.
// Once we get the field info for a table from the stream response,
// we'll have all the necessary info to build the final plan.
// At that time, buildExecutionPlan is invoked, which will make a copy
// of the TablePlan from ReplicatorPlan, and fill the rest
// of the members, leaving the original plan unchanged.
// The constructor is buildReplicatorPlan in table_plan_builder.go
type ReplicatorPlan struct {
	VStreamFilter *binlogdatapb.Filter
	TargetTables  map[string]*TablePlan
	TablePlans    map[string]*TablePlan
	PKInfoMap     map[string][]*PrimaryKeyInfo
}

// buildExecution plan uses the field info as input and the partially built
// TablePlan for that table to build a full plan.
func (rp *ReplicatorPlan) buildExecutionPlan(fieldEvent *binlogdatapb.FieldEvent) (*TablePlan, error) {
	prelim := rp.TablePlans[fieldEvent.TableName]
	if prelim == nil {
		// Unreachable code.
		return nil, fmt.Errorf("plan not found for %s", fieldEvent.TableName)
	}
	// If Insert is initialized, then it means that we knew the column
	// names and have already built most of the plan.
	if prelim.Insert != nil {
		tplanv := *prelim
		// We know that we sent only column names, but they may be backticked.
		// If so, we have to strip them out to allow them to match the expected
		// bind var names.
		tplanv.Fields = make([]*querypb.Field, 0, len(fieldEvent.Fields))
		for _, fld := range fieldEvent.Fields {
			trimmed := *fld
			trimmed.Name = strings.Trim(trimmed.Name, "`")
			tplanv.Fields = append(tplanv.Fields, &trimmed)
		}
		return &tplanv, nil
	}
	// select * construct was used. We need to use the field names.
	tplan, err := rp.buildFromFields(prelim.TargetName, prelim.Lastpk, fieldEvent.Fields)
	if err != nil {
		return nil, err
	}
	tplan.Fields = fieldEvent.Fields
	return tplan, nil
}

// buildFromFields builds a full TablePlan, but uses the field info as the
// full column list. This happens when the query used was a 'select *', which
// requires us to wait for the field info sent by the source.
func (rp *ReplicatorPlan) buildFromFields(tableName string, lastpk *sqltypes.Result, fields []*querypb.Field) (*TablePlan, error) {
	tpb := &tablePlanBuilder{
		name:    sqlparser.NewTableIdent(tableName),
		lastpk:  lastpk,
		pkInfos: rp.PKInfoMap[tableName],
	}
	for _, field := range fields {
		colName := sqlparser.NewColIdent(field.Name)
		cexpr := &colExpr{
			colName: colName,
			colType: field.Type,
			expr: &sqlparser.ColName{
				Name: colName,
			},
			references: map[string]bool{
				field.Name: true,
			},
		}
		tpb.colExprs = append(tpb.colExprs, cexpr)
	}
	// The following actions are a subset of buildTablePlan.
	if err := tpb.analyzePK(rp.PKInfoMap); err != nil {
		return nil, err
	}
	return tpb.generate(), nil
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

// TablePlan is the execution plan for a table within a replicator.
// If the column names are not known at the time of plan building (like
// select *), then only TargetName, SendRule and Lastpk are initialized.
// When the stream returns the field info, those are used as column
// names to build the final plan.
// Lastpk comes from copyState. If it's set, then the generated plans
// are significantly different because any events that fall beyond
// Lastpk must be excluded.
// If column names were known upfront, then all fields of TablePlan
// are built except for Fields. This member is populated only after
// the field info is received from the stream.
// The ParsedQuery objects assume that a map of before and after values
// will be built based on the streaming rows. Before image values will
// be prefixed with a "b_", and after image values will be prefixed
// with a "a_". The TablePlan structure is used during all the phases
// of vreplication: catchup, copy, fastforward, or regular replication.
type TablePlan struct {
	// TargetName, SendRule will always be initialized.
	TargetName string
	SendRule   *binlogdatapb.Rule
	// Lastpk will be initialized if it was specified, and
	// will be used for building the final plan after field info
	// is received.
	Lastpk *sqltypes.Result
	// BulkInsertFront, BulkInsertValues and BulkInsertOnDup are used
	// by vcopier. These three parts are combined to build bulk insert
	// statements. This is functionally equivalent to generating
	// multiple statements using the "Insert" construct, but much more
	// efficient for the copy phase.
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
	// PKReferences is used to check if an event changed
	// a primary key column (row move).
	PKReferences []string
}

// MarshalJSON performs a custom JSON Marshalling.
func (tp *TablePlan) MarshalJSON() ([]byte, error) {
	v := struct {
		TargetName   string
		SendRule     string
		InsertFront  *sqlparser.ParsedQuery `json:",omitempty"`
		InsertValues *sqlparser.ParsedQuery `json:",omitempty"`
		InsertOnDup  *sqlparser.ParsedQuery `json:",omitempty"`
		Insert       *sqlparser.ParsedQuery `json:",omitempty"`
		Update       *sqlparser.ParsedQuery `json:",omitempty"`
		Delete       *sqlparser.ParsedQuery `json:",omitempty"`
		PKReferences []string               `json:",omitempty"`
	}{
		TargetName:   tp.TargetName,
		SendRule:     tp.SendRule.Match,
		InsertFront:  tp.BulkInsertFront,
		InsertValues: tp.BulkInsertValues,
		InsertOnDup:  tp.BulkInsertOnDup,
		Insert:       tp.Insert,
		Update:       tp.Update,
		Delete:       tp.Delete,
		PKReferences: tp.PKReferences,
	}
	return json.Marshal(&v)
}

func (tp *TablePlan) applyBulkInsert(rows *binlogdatapb.VStreamRowsResponse, executor func(string) (*sqltypes.Result, error)) (*sqltypes.Result, error) {
	bindvars := make(map[string]*querypb.BindVariable, len(tp.Fields))
	var buf strings.Builder
	if err := tp.BulkInsertFront.Append(&buf, nil, nil); err != nil {
		return nil, err
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
	return executor(buf.String())
}

func (tp *TablePlan) applyChange(rowChange *binlogdatapb.RowChange, executor func(string) (*sqltypes.Result, error)) (*sqltypes.Result, error) {
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
		return execParsedQuery(tp.Insert, bindvars, executor)
	case before && !after:
		if tp.Delete == nil {
			return nil, nil
		}
		return execParsedQuery(tp.Delete, bindvars, executor)
	case before && after:
		if !tp.pkChanged(bindvars) {
			return execParsedQuery(tp.Update, bindvars, executor)
		}
		if tp.Delete != nil {
			if _, err := execParsedQuery(tp.Delete, bindvars, executor); err != nil {
				return nil, err
			}
		}
		return execParsedQuery(tp.Insert, bindvars, executor)
	}
	// Unreachable.
	return nil, nil
}

func execParsedQuery(pq *sqlparser.ParsedQuery, bindvars map[string]*querypb.BindVariable, executor func(string) (*sqltypes.Result, error)) (*sqltypes.Result, error) {
	sql, err := pq.GenerateQuery(bindvars, nil)
	if err != nil {
		return nil, err
	}
	return executor(sql)
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
