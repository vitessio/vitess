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

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/bytes2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
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
	ColInfoMap    map[string][]*ColumnInfo
	stats         *binlogplayer.Stats
	Source        *binlogdatapb.BinlogSource
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
			trimmed := proto.Clone(fld).(*querypb.Field)
			trimmed.Name = strings.Trim(trimmed.Name, "`")
			tplanv.Fields = append(tplanv.Fields, trimmed)
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
		name:     sqlparser.NewIdentifierCS(tableName),
		lastpk:   lastpk,
		colInfos: rp.ColInfoMap[tableName],
		stats:    rp.stats,
		source:   rp.Source,
	}
	for _, field := range fields {
		colName := sqlparser.NewIdentifierCI(field.Name)
		isGenerated := false
		for _, colInfo := range tpb.colInfos {
			if !strings.EqualFold(colInfo.Name, field.Name) {
				continue
			}
			if colInfo.IsGenerated {
				isGenerated = true
			}
			break
		}
		if isGenerated {
			continue
		}
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
	if err := tpb.analyzePK(rp.ColInfoMap[tableName]); err != nil {
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
	Insert        *sqlparser.ParsedQuery
	Update        *sqlparser.ParsedQuery
	Delete        *sqlparser.ParsedQuery
	Fields        []*querypb.Field
	EnumValuesMap map[string](map[string]string)
	// PKReferences is used to check if an event changed
	// a primary key column (row move).
	PKReferences            []string
	Stats                   *binlogplayer.Stats
	FieldsToSkip            map[string]bool
	ConvertCharset          map[string](*binlogdatapb.CharsetConversion)
	HasExtraSourcePkColumns bool
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

func (tp *TablePlan) applyBulkInsert(sqlbuffer *bytes2.Buffer, rows *binlogdatapb.VStreamRowsResponse, executor func(string) (*sqltypes.Result, error)) (*sqltypes.Result, error) {
	sqlbuffer.Reset()
	sqlbuffer.WriteString(tp.BulkInsertFront.Query)
	sqlbuffer.WriteString(" values ")

	for i, row := range rows.Rows {
		if i > 0 {
			sqlbuffer.WriteString(", ")
		}
		if err := tp.BulkInsertValues.AppendFromRow(sqlbuffer, tp.Fields, row, tp.FieldsToSkip); err != nil {
			return nil, err
		}
	}
	if tp.BulkInsertOnDup != nil {
		sqlbuffer.WriteString(tp.BulkInsertOnDup.Query)
	}
	return executor(sqlbuffer.StringUnsafe())
}

// During the copy phase we run catchup and fastforward, which stream binlogs. While streaming we should only process
// rows whose PK has already been copied. Ideally we should compare the PKs before applying the change and never send
// such rows to the target mysql server. However reliably comparing primary keys in a manner compatible to MySQL will require a lot of
// coding: consider composite PKs, character sets, collations ... So we send these rows to the mysql server which then does the comparison
// in sql, through where clauses like "pk_val <= last_seen_pk".
//
// But this does generate a lot of unnecessary load of, effectively, no-ops since the where
// clauses are always false. This can create a significant cpu load on the target for high qps servers resulting in a
// much lower copy bandwidth (or provisioning more powerful servers).
// isOutsidePKRange currently checks for rows with single primary keys which are currently comparable in Vitess:
// (see NullsafeCompare() for types supported). It returns true if pk is not to be applied
//
// At this time we have decided to only perform this for Insert statements. Insert statements form a significant majority of
// the generated noop load during catchup and are easier to test for. Update and Delete statements are very difficult to
// unit test reliably and without flakiness with our current test framework. So as a pragmatic decision we support Insert
// now and punt on the others.
func (tp *TablePlan) isOutsidePKRange(bindvars map[string]*querypb.BindVariable, before, after bool, stmtType string) bool {
	// added empty comments below, otherwise gofmt removes the spaces between the bitwise & and obfuscates this check!
	if vreplicationExperimentalFlags /**/ & /**/ vreplicationExperimentalFlagOptimizeInserts == 0 {
		return false
	}
	// Ensure there is one and only one value in lastpk and pkrefs.
	if tp.Lastpk != nil && len(tp.Lastpk.Fields) == 1 && len(tp.Lastpk.Rows) == 1 && len(tp.Lastpk.Rows[0]) == 1 && len(tp.PKReferences) == 1 {
		// check again that this is an insert
		var bindvar *querypb.BindVariable
		switch {
		case !before && after:
			bindvar = bindvars["a_"+tp.PKReferences[0]]
		}
		if bindvar == nil { //should never happen
			return false
		}

		rowVal, _ := sqltypes.BindVariableToValue(bindvar)
		// TODO(king-11) make collation aware
		result, err := evalengine.NullsafeCompare(rowVal, tp.Lastpk.Rows[0][0], collations.Unknown)
		// If rowVal is > last pk, transaction will be a noop, so don't apply this statement
		if err == nil && result > 0 {
			tp.Stats.NoopQueryCount.Add(stmtType, 1)
			return true
		}
	}
	return false
}

// bindFieldVal returns a bind variable based on given field and value.
// Most values will just bind directly. But some values may need manipulation:
// - text values with charset conversion
// - enum values converted to text via Online DDL
// - ...any other future possible values
func (tp *TablePlan) bindFieldVal(field *querypb.Field, val *sqltypes.Value) (*querypb.BindVariable, error) {
	if conversion, ok := tp.ConvertCharset[field.Name]; ok && !val.IsNull() {
		// Non-null string value, for which we have a charset conversion instruction
		valString := val.ToString()
		fromEncoding, encodingOK := mysql.CharacterSetEncoding[conversion.FromCharset]
		if !encodingOK {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Character set %s not supported for column %s", conversion.FromCharset, field.Name)
		}
		if fromEncoding != nil {
			// As reminder, encoding can be nil for trivial charsets, like utf8 or ascii.
			// encoding will be non-nil for charsets like latin1, gbk, etc.
			var err error
			valString, err = fromEncoding.NewDecoder().String(valString)
			if err != nil {
				return nil, err
			}
		}
		return sqltypes.StringBindVariable(valString), nil
	}
	if enumValues, ok := tp.EnumValuesMap[field.Name]; ok && !val.IsNull() {
		// The fact that this field has a EnumValuesMap entry, means we must
		// use the enum's text value as opposed to the enum's numerical value.
		// Once known use case is with Online DDL, when a column is converted from
		// ENUM to a VARCHAR/TEXT.
		enumValue, enumValueOK := enumValues[val.ToString()]
		if !enumValueOK {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Invalid enum value: %v for field %s", val, field.Name)
		}
		// get the enum text for this val
		return sqltypes.StringBindVariable(enumValue), nil
	}
	if field.Type == querypb.Type_ENUM {
		// This is an ENUM w/o a values map, which means that we are most likely using
		// the index value -- what is stored and binlogged vs. the list of strings
		// defined in the table schema -- and we must use an int bindvar or we'll have
		// invalid/incorrect predicates like WHERE enumcol='2'.
		// This will be the case when applying binlog events.
		enumIndexVal := sqltypes.MakeTrusted(querypb.Type_UINT64, val.Raw())
		if enumIndex, err := enumIndexVal.ToUint64(); err == nil {
			return sqltypes.Uint64BindVariable(enumIndex), nil
		}
	}
	return sqltypes.ValueBindVariable(*val), nil
}

func (tp *TablePlan) applyChange(rowChange *binlogdatapb.RowChange, executor func(string) (*sqltypes.Result, error)) (*sqltypes.Result, error) {
	// MakeRowTrusted is needed here because Proto3ToResult is not convenient.
	var before, after bool
	bindvars := make(map[string]*querypb.BindVariable, len(tp.Fields))
	if rowChange.Before != nil {
		before = true
		vals := sqltypes.MakeRowTrusted(tp.Fields, rowChange.Before)
		for i, field := range tp.Fields {
			bindVar, err := tp.bindFieldVal(field, &vals[i])
			if err != nil {
				return nil, err
			}
			bindvars["b_"+field.Name] = bindVar
		}
	}
	if rowChange.After != nil {
		after = true
		vals := sqltypes.MakeRowTrusted(tp.Fields, rowChange.After)
		for i, field := range tp.Fields {
			bindVar, err := tp.bindFieldVal(field, &vals[i])
			if err != nil {
				return nil, err
			}
			bindvars["a_"+field.Name] = bindVar
		}
	}
	switch {
	case !before && after:
		// only apply inserts for rows whose primary keys are within the range of rows already copied
		if tp.isOutsidePKRange(bindvars, before, after, "insert") {
			return nil, nil
		}
		return execParsedQuery(tp.Insert, bindvars, executor)
	case before && !after:
		if tp.Delete == nil {
			return nil, nil
		}
		return execParsedQuery(tp.Delete, bindvars, executor)
	case before && after:
		if !tp.pkChanged(bindvars) && !tp.HasExtraSourcePkColumns {
			return execParsedQuery(tp.Update, bindvars, executor)
		}
		if tp.Delete != nil {
			if _, err := execParsedQuery(tp.Delete, bindvars, executor); err != nil {
				return nil, err
			}
		}
		if tp.isOutsidePKRange(bindvars, before, after, "insert") {
			return nil, nil
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
