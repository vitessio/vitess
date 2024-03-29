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

	"vitess.io/vitess/go/bytes2"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	vjson "vitess.io/vitess/go/mysql/json"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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
	collationEnv  *collations.Environment
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
			trimmed := fld.CloneVT()
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
		name:         sqlparser.NewIdentifierCS(tableName),
		lastpk:       lastpk,
		colInfos:     rp.ColInfoMap[tableName],
		stats:        rp.stats,
		source:       rp.Source,
		collationEnv: rp.collationEnv,
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
	Insert           *sqlparser.ParsedQuery
	Update           *sqlparser.ParsedQuery
	Delete           *sqlparser.ParsedQuery
	MultiDelete      *sqlparser.ParsedQuery
	Fields           []*querypb.Field
	EnumValuesMap    map[string](map[string]string)
	ConvertIntToEnum map[string]bool
	// PKReferences is used to check if an event changed
	// a primary key column (row move).
	PKReferences []string
	// PKIndices is an array, length = #columns, true if column is part of the PK
	PKIndices               []bool
	Stats                   *binlogplayer.Stats
	FieldsToSkip            map[string]bool
	ConvertCharset          map[string](*binlogdatapb.CharsetConversion)
	HasExtraSourcePkColumns bool

	TablePlanBuilder *tablePlanBuilder
	// PartialInserts is a dynamically generated cache of insert ParsedQueries, which update only some columns.
	// This is when we use a binlog_row_image which is not "full". The key is a serialized bitmap of data columns
	// which are sent as part of the RowEvent.
	PartialInserts map[string]*sqlparser.ParsedQuery
	// PartialUpdates are same as PartialInserts, but for update statements
	PartialUpdates map[string]*sqlparser.ParsedQuery

	CollationEnv *collations.Environment
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

func (tp *TablePlan) applyBulkInsert(sqlbuffer *bytes2.Buffer, rows []*querypb.Row, executor func(string) (*sqltypes.Result, error)) (*sqltypes.Result, error) {
	sqlbuffer.Reset()
	sqlbuffer.WriteString(tp.BulkInsertFront.Query)
	sqlbuffer.WriteString(" values ")

	for i, row := range rows {
		if i > 0 {
			sqlbuffer.WriteString(", ")
		}
		if err := appendFromRow(tp.BulkInsertValues, sqlbuffer, tp.Fields, row, tp.FieldsToSkip); err != nil {
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
	if vttablet.VReplicationExperimentalFlags /**/ & /**/ vttablet.VReplicationExperimentalFlagOptimizeInserts == 0 {
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
		result, err := evalengine.NullsafeCompare(rowVal, tp.Lastpk.Rows[0][0], tp.CollationEnv, collations.Unknown)
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
		fromCollation := tp.CollationEnv.DefaultCollationForCharset(conversion.FromCharset)
		if fromCollation == collations.Unknown {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Character set %s not supported for column %s", conversion.FromCharset, field.Name)
		}
		out, err := charset.Convert(nil, charset.Charset_utf8mb4{}, val.Raw(), colldata.Lookup(fromCollation).Charset())
		if err != nil {
			return nil, err
		}
		return sqltypes.StringBindVariable(string(out)), nil
	}
	if tp.ConvertIntToEnum[field.Name] && !val.IsNull() {
		// An integer converted to an enum. We must write the textual value of the int. i.e. 0 turns to '0'
		return sqltypes.StringBindVariable(val.ToString()), nil
	}
	if enumValues, ok := tp.EnumValuesMap[field.Name]; ok && !val.IsNull() {
		// The fact that this field has a EnumValuesMap entry, means we must
		// use the enum's text value as opposed to the enum's numerical value.
		// This may be needed in Online DDL, when the enum column could be modified:
		// - Either from ENUM to a text type (VARCHAR/TEXT)
		// - Or from ENUM to another ENUM with different value ordering,
		//   e.g. from `('red', 'green', 'blue')` to `('red', 'blue')`.
		// By applying the textual value of an enum we eliminate the ordering concern.
		// In non-Online DDL this shouldn't be a concern because the schema is static,
		// and so passing the enum's numerical value is sufficient.
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
			var bindVar *querypb.BindVariable
			var newVal *sqltypes.Value
			var err error
			if field.Type == querypb.Type_JSON {
				if vals[i].IsNull() { // An SQL NULL and not an actual JSON value
					newVal = &sqltypes.NULL
				} else { // A JSON value (which may be a JSON null literal value)
					newVal, err = vjson.MarshalSQLValue(vals[i].Raw())
					if err != nil {
						return nil, err
					}
				}
				bindVar, err = tp.bindFieldVal(field, newVal)
			} else {
				bindVar, err = tp.bindFieldVal(field, &vals[i])
			}
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
		if tp.isPartial(rowChange) {
			ins, err := tp.getPartialInsertQuery(rowChange.DataColumns)
			if err != nil {
				return nil, err
			}
			tp.Stats.PartialQueryCount.Add([]string{"insert"}, 1)
			return execParsedQuery(ins, bindvars, executor)
		} else {
			return execParsedQuery(tp.Insert, bindvars, executor)
		}
	case before && !after:
		if tp.Delete == nil {
			return nil, nil
		}
		return execParsedQuery(tp.Delete, bindvars, executor)
	case before && after:
		if !tp.pkChanged(bindvars) && !tp.HasExtraSourcePkColumns {
			if tp.isPartial(rowChange) {
				upd, err := tp.getPartialUpdateQuery(rowChange.DataColumns)
				if err != nil {
					return nil, err
				}
				tp.Stats.PartialQueryCount.Add([]string{"update"}, 1)
				return execParsedQuery(upd, bindvars, executor)
			} else {
				return execParsedQuery(tp.Update, bindvars, executor)
			}
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

// applyBulkDeleteChanges applies a bulk DELETE statement from the row changes
// to the target table -- which resulted from a DELETE statement executed on the
// source that deleted N rows -- using an IN clause with the primary key values
// of the rows to be deleted. This currently only supports tables with single
// column primary keys. This limitation is in place for now as we know that case
// will still be efficient. When using large multi-column IN or OR group clauses
// in DELETES we could end up doing large (table) scans that actually make things
// slower.
// TODO: Add support for multi-column primary keys.
func (tp *TablePlan) applyBulkDeleteChanges(rowDeletes []*binlogdatapb.RowChange, executor func(string) (*sqltypes.Result, error), maxQuerySize int64) (*sqltypes.Result, error) {
	if len(rowDeletes) == 0 {
		return &sqltypes.Result{}, nil
	}
	if (len(tp.TablePlanBuilder.pkCols) + len(tp.TablePlanBuilder.extraSourcePkCols)) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "bulk delete is only supported for tables with a single primary key column")
	}
	if tp.MultiDelete == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "plan has no bulk delete query")
	}

	baseQuerySize := int64(len(tp.MultiDelete.Query))
	querySize := baseQuerySize

	execQuery := func(pkVals *[]sqltypes.Value) (*sqltypes.Result, error) {
		pksBV, err := sqltypes.BuildBindVariable(*pkVals)
		if err != nil {
			return nil, err
		}
		query, err := tp.MultiDelete.GenerateQuery(map[string]*querypb.BindVariable{"bulk_pks": pksBV}, nil)
		if err != nil {
			return nil, err
		}
		tp.TablePlanBuilder.stats.BulkQueryCount.Add("delete", 1)
		return executor(query)
	}

	pkIndex := -1
	pkVals := make([]sqltypes.Value, 0, len(rowDeletes))
	for _, rowDelete := range rowDeletes {
		vals := sqltypes.MakeRowTrusted(tp.Fields, rowDelete.Before)
		if pkIndex == -1 {
			for i := range vals {
				if tp.PKIndices[i] {
					pkIndex = i
					break
				}
			}
		}
		addedSize := int64(len(vals[pkIndex].Raw()) + 2) // Plus 2 for the comma and space
		if querySize+addedSize > maxQuerySize {
			if _, err := execQuery(&pkVals); err != nil {
				return nil, err
			}
			pkVals = nil
			querySize = baseQuerySize
		}
		pkVals = append(pkVals, vals[pkIndex])
		querySize += addedSize
	}

	return execQuery(&pkVals)
}

// applyBulkInsertChanges generates a multi-row INSERT statement from the row
// changes generated from a multi-row INSERT statement executed on the source.
func (tp *TablePlan) applyBulkInsertChanges(rowInserts []*binlogdatapb.RowChange, executor func(string) (*sqltypes.Result, error), maxQuerySize int64) (*sqltypes.Result, error) {
	if len(rowInserts) == 0 {
		return &sqltypes.Result{}, nil
	}
	if tp.BulkInsertFront == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "plan has no bulk insert query")
	}

	prefix := &strings.Builder{}
	prefix.WriteString(tp.BulkInsertFront.Query)
	prefix.WriteString(" values ")
	insertPrefix := prefix.String()
	maxQuerySize -= int64(len(insertPrefix))
	values := &strings.Builder{}

	execQuery := func(vals *strings.Builder) (*sqltypes.Result, error) {
		if tp.BulkInsertOnDup != nil {
			vals.WriteString(tp.BulkInsertOnDup.Query)
		}
		tp.TablePlanBuilder.stats.BulkQueryCount.Add("insert", 1)
		return executor(insertPrefix + vals.String())
	}

	newStmt := true
	for _, rowInsert := range rowInserts {
		rowValues := &strings.Builder{}
		bindvars := make(map[string]*querypb.BindVariable, len(tp.Fields))
		vals := sqltypes.MakeRowTrusted(tp.Fields, rowInsert.After)
		for n, field := range tp.Fields {
			bindVar, err := tp.bindFieldVal(field, &vals[n])
			if err != nil {
				return nil, err
			}
			bindvars["a_"+field.Name] = bindVar
		}
		if err := tp.BulkInsertValues.Append(rowValues, bindvars, nil); err != nil {
			return nil, err
		}
		if int64(values.Len()+2+rowValues.Len()) > maxQuerySize { // Plus 2 for the comma and space
			if _, err := execQuery(values); err != nil {
				return nil, err
			}
			values.Reset()
			newStmt = true
		}
		if !newStmt {
			values.WriteString(", ")
		}
		values.WriteString(rowValues.String())
		newStmt = false
	}

	return execQuery(values)
}

func getQuery(pq *sqlparser.ParsedQuery, bindvars map[string]*querypb.BindVariable) (string, error) {
	sql, err := pq.GenerateQuery(bindvars, nil)
	if err != nil {
		return "", err
	}
	return sql, nil
}
func execParsedQuery(pq *sqlparser.ParsedQuery, bindvars map[string]*querypb.BindVariable, executor func(string) (*sqltypes.Result, error)) (*sqltypes.Result, error) {
	query, err := getQuery(pq, bindvars)
	if err != nil {
		return nil, err
	}
	return executor(query)
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

// AppendFromRow behaves like Append but takes a querypb.Row directly, assuming that
// the fields in the row are in the same order as the placeholders in this query. The fields might include generated
// columns which are dropped, by checking against skipFields, before binding the variables
// note: there can be more fields than bind locations since extra columns might be requested from the source if not all
// primary keys columns are present in the target table, for example. Also some values in the row may not correspond for
// values from the database on the source: sum/count for aggregation queries, for example
func appendFromRow(pq *sqlparser.ParsedQuery, buf *bytes2.Buffer, fields []*querypb.Field, row *querypb.Row, skipFields map[string]bool) error {
	bindLocations := pq.BindLocations()
	if len(fields) < len(bindLocations) {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "wrong number of fields: got %d fields for %d bind locations ",
			len(fields), len(bindLocations))
	}

	type colInfo struct {
		typ    querypb.Type
		length int64
		offset int64
	}
	rowInfo := make([]*colInfo, 0)

	offset := int64(0)
	for i, field := range fields { // collect info required for fields to be bound
		length := row.Lengths[i]
		if !skipFields[strings.ToLower(field.Name)] {
			rowInfo = append(rowInfo, &colInfo{
				typ:    field.Type,
				length: length,
				offset: offset,
			})
		}
		if length > 0 {
			offset += row.Lengths[i]
		}
	}

	// bind field values to locations
	var offsetQuery int
	for i, loc := range bindLocations {
		col := rowInfo[i]
		buf.WriteString(pq.Query[offsetQuery:loc.Offset])
		typ := col.typ

		switch typ {
		case querypb.Type_TUPLE:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected Type_TUPLE for value %d", i)
		case querypb.Type_JSON:
			if col.length < 0 { // An SQL NULL and not an actual JSON value
				buf.WriteString(sqltypes.NullStr)
			} else { // A JSON value (which may be a JSON null literal value)
				buf2 := row.Values[col.offset : col.offset+col.length]
				vv, err := vjson.MarshalSQLValue(buf2)
				if err != nil {
					return err
				}
				buf.WriteString(vv.RawStr())
			}
		default:
			if col.length < 0 {
				// -1 means a null variable; serialize it directly
				buf.WriteString(sqltypes.NullStr)
			} else {
				vv := sqltypes.MakeTrusted(typ, row.Values[col.offset:col.offset+col.length])
				vv.EncodeSQLBytes2(buf)
			}
		}
		offsetQuery = loc.Offset + loc.Length
	}
	buf.WriteString(pq.Query[offsetQuery:])
	return nil
}
