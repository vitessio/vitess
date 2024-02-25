/*
Copyright 2024 The Vitess Authors.

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

package vstreamer

// This file contains the test framework for testing the event generation logic in vstreamer.
// The test framework is designed to be used in the following way:
// 1. Define a TestSpec with the following fields:
//    - ddls: a list of create table statements for the tables to be used in the test
//    - tests: a list of test cases, each test case is a list of TestQuery
//    - options: test-specific options, if any
// 2. Call ts.Init() to initialize the test.
// 3. Call ts.Run() to run the test. This will run the queries and validate the events.
// 4. Call ts.Close() to clean up the tables created in the test.
// The test framework will take care of creating the tables, running the queries, and validating the events for
// simpler cases. For more complex cases, the test framework provides hooks to customize the event generation.

// Note: To simplify the initial implementation, the test framework is designed to be used in the vstreamer package only.
// It makes several assumptions about  how the test cases are written. For example, queries are expected to
// use single quotes for string literals, for example:
// `"insert into t1 values (1, 'blob1', 'aaa')"`.
// The test framework will not work if the queries use double quotes for string literals at the moment.

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/schemadiff"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer/testenv"
)

const (
	lengthInt  = 11
	lengthBlob = 65535
	lengthText = 262140
	lengthSet  = 56
)

var (
	// noEvents is used to indicate that a query is expected to generate no events.
	noEvents = []TestRowEvent{}
)

// TestColumn has all the attributes of a column required for the test cases.
type TestColumn struct {
	name, dataType, colType string
	collationID             collations.ID
	len                     int64
	dataTypeLowered         string
	skip                    bool
}

// TestFieldEvent has all the attributes of a table required for creating a field event.
type TestFieldEvent struct {
	table, db string
	cols      []*TestColumn
}

// TestQuery represents a database query and the expected events it generates.
type TestQuery struct {
	query  string
	events []TestRowEvent
}

// TestRowChange represents the before and after state of a row due to a dml
type TestRowChange struct {
	before []string
	after  []string
}

// TestRowEventSpec is used for defining a custom row event.
type TestRowEventSpec struct {
	table   string
	changes []TestRowChange
}

// Generates a string representation for a custom row event.
func (s *TestRowEventSpec) String() string {
	ev := &binlogdata.RowEvent{
		TableName: s.table,
	}
	var rowChanges []*binlogdata.RowChange
	if s.changes != nil && len(s.changes) > 0 {
		for _, c := range s.changes {
			rowChange := binlogdata.RowChange{}
			if c.before != nil && len(c.before) > 0 {
				rowChange.Before = &query.Row{}
				for _, val := range c.before {
					rowChange.Before.Lengths = append(rowChange.Before.Lengths, int64(len(val)))
					rowChange.Before.Values = append(rowChange.Before.Values, []byte(val)...)
				}
			}
			if c.after != nil && len(c.after) > 0 {
				rowChange.After = &query.Row{}
				for _, val := range c.after {
					rowChange.After.Lengths = append(rowChange.After.Lengths, int64(len(val)))
					rowChange.After.Values = append(rowChange.After.Values, []byte(val)...)
				}
			}
			rowChanges = append(rowChanges, &rowChange)
		}
		ev.RowChanges = rowChanges
	}
	vEvent := &binlogdata.VEvent{
		Type:     binlogdata.VEventType_ROW,
		RowEvent: ev,
	}
	return vEvent.String()
}

// TestRowEvent is used to define either the actual row event string (the `event` field) or a custom row event
// (the `spec` field). Only one should be specified. If a test validates `flags` of a RowEvent then it is set.
type TestRowEvent struct {
	event string
	spec  *TestRowEventSpec
	flags int
}

// TestSpecOptions has any non-standard test-specific options which can modify the event generation behaviour.
type TestSpecOptions struct {
	noblob bool
	filter *binlogdata.Filter
}

// TestSpec is defined one per unit test.
type TestSpec struct {
	// test=specific parameters
	t       *testing.T
	ddls    []string         // create table statements
	tests   [][]*TestQuery   // list of input queries and expected events for each query
	options *TestSpecOptions // test-specific options

	// internal state
	inited          bool                       // whether the test has been initialized
	tables          []string                   // list of tables in the schema (created in `ddls`)
	pkColumns       map[string][]string        // map of table name to primary key columns
	schema          *schemadiff.Schema         // parsed schema from `ddls` using `schemadiff`
	fieldEvents     map[string]*TestFieldEvent // map of table name to field event for the table
	fieldEventsSent map[string]bool            // whether the field event has been sent for the table in the test
	state           map[string]*query.Row      // last row inserted for each table. Useful to generate events only for inserts
	metadata        map[string][]string        // list of enum/set values for enum/set columns
}

func (ts *TestSpec) getCurrentState(table string) *query.Row {
	return ts.state[table]
}

func (ts *TestSpec) setCurrentState(table string, row *query.Row) {
	ts.state[table] = row
}

// Init() initializes the test. It creates the tables and sets up the internal state.
func (ts *TestSpec) Init() error {
	var err error
	if ts.inited {
		return nil
	}
	defer func() { ts.inited = true }()
	if ts.options == nil {
		ts.options = &TestSpecOptions{}
	}
	// Add the unicode character set to each table definition.
	// The collation used will then be the default for that character set
	// in the given MySQL version used in the test:
	// - 5.7: utf8mb4_general_ci
	// - 8.0: utf8mb4_0900_ai_ci
	tableOptions := "ENGINE=InnoDB CHARSET=utf8mb4"
	for i := range ts.ddls {
		ts.ddls[i] = fmt.Sprintf("%s %s", ts.ddls[i], tableOptions)
	}
	ts.schema, err = schemadiff.NewSchemaFromQueries(schemadiff.NewTestEnv(), ts.ddls)
	if err != nil {
		return err
	}
	ts.fieldEvents = make(map[string]*TestFieldEvent)
	ts.fieldEventsSent = make(map[string]bool)
	ts.state = make(map[string]*query.Row)
	ts.metadata = make(map[string][]string)
	ts.pkColumns = make(map[string][]string)
	// create tables
	require.Equal(ts.t, len(ts.ddls), len(ts.schema.Tables()), "number of tables in ddls and schema do not match")
	for i, t := range ts.schema.Tables() {
		execStatement(ts.t, ts.ddls[i])
		fe := ts.getFieldEvent(t)
		ts.fieldEvents[t.Name()] = fe

		var pkColumns []string
		var hasPK bool
		for _, index := range t.TableSpec.Indexes {
			require.NotNil(ts.t, index.Info, "index.Info is nil")
			if index.Info.Type == sqlparser.IndexTypePrimary {
				for _, col := range index.Columns {
					pkColumns = append(pkColumns, col.Column.String())
				}
				hasPK = true
			}
		}
		if !hasPK {
			// add all columns as pk columns
			for _, col := range t.TableSpec.Columns {
				pkColumns = append(pkColumns, col.Name.String())
			}
		}
		ts.pkColumns[t.Name()] = pkColumns
	}
	engine.se.Reload(context.Background())
	return nil
}

// Close() should be called (via defer) at the end of the test to clean up the tables created in the test.
func (ts *TestSpec) Close() {
	dropStatement := fmt.Sprintf("drop tables %s", strings.Join(ts.schema.TableNames(), ", "))
	execStatement(ts.t, dropStatement)
}

func (ts *TestSpec) getBindVarsForInsert(stmt sqlparser.Statement) (string, map[string]string) {
	bv := make(map[string]string)
	ins := stmt.(*sqlparser.Insert)
	tn, err := ins.Table.TableName()
	require.NoError(ts.t, err)
	table := tn.Name.String()
	fe := ts.fieldEvents[table]
	vals, ok := ins.Rows.(sqlparser.Values)
	require.True(ts.t, ok, "insert statement does not have values")
	for _, val := range vals {
		for i, v := range val {
			bufV := sqlparser.NewTrackedBuffer(nil)
			v.Format(bufV)
			s := bufV.String()
			switch fe.cols[i].dataTypeLowered {
			case "varchar", "char", "binary", "varbinary", "blob", "text":
				s = strings.Trim(s, "'")
			case "set", "enum":
				s = ts.getMetadataMap(table, fe.cols[i], s)
			}
			bv[fe.cols[i].name] = s
		}
	}
	return table, bv
}

func (ts *TestSpec) getBindVarsForUpdate(stmt sqlparser.Statement) (string, map[string]string) {
	bv := make(map[string]string)
	upd := stmt.(*sqlparser.Update)
	//buf := sqlparser.NewTrackedBuffer(nil)
	table := sqlparser.String(upd.TableExprs[0].(*sqlparser.AliasedTableExpr).Expr)
	//upd.TableExprs[0].(*sqlparser.AliasedTableExpr).Expr.Format(buf)
	//table := buf.String()
	fe, ok := ts.fieldEvents[table]
	require.True(ts.t, ok, "field event for table %s not found", table)
	index := int64(0)
	state := ts.getCurrentState(table)
	for i, col := range fe.cols {
		bv[col.name] = string(state.Values[index : index+state.Lengths[i]])
		index += state.Lengths[i]
	}
	for _, expr := range upd.Exprs {
		bufV := sqlparser.NewTrackedBuffer(nil)
		bufN := sqlparser.NewTrackedBuffer(nil)
		expr.Expr.Format(bufV)
		expr.Name.Format(bufN)
		bv[bufN.String()] = strings.Trim(bufV.String(), "'")
	}
	return table, bv
}

// Run() runs the test. It first initializes the test, then runs the queries and validates the events.
func (ts *TestSpec) Run() {
	require.NoError(ts.t, engine.se.Reload(context.Background()))
	if !ts.inited {
		require.NoError(ts.t, ts.Init())
	}
	var testcases []testcase
	for _, t := range ts.tests {
		var tc testcase
		var input []string
		var output []string
		for _, tq := range t {
			var table string
			input = append(input, tq.query)
			switch {
			case tq.events != nil && len(tq.events) == 0: // when an input query is expected to generate no events
				continue
			case tq.events != nil && // when we define the actual events either as a serialized string or as a TestRowEvent
				(len(tq.events) > 0 &&
					!(len(tq.events) == 1 && tq.events[0].event == "" && tq.events[0].spec == nil)):
				for _, e := range tq.events {
					if e.event != "" {
						output = append(output, e.event)
					} else if e.spec != nil {
						output = append(output, e.spec.String())
					} else {
						panic("invalid event")
					}
				}
				continue
			default:
				// when we don't define the actual events, we generate them based on the input query
				flags := 0
				if len(tq.events) == 1 {
					flags = tq.events[0].flags
				}
				stmt, err := sqlparser.NewTestParser().Parse(tq.query)
				require.NoError(ts.t, err)
				bv := make(map[string]string)
				isRowEvent := false
				switch stmt.(type) {
				case *sqlparser.Begin:
					output = append(output, "begin")
				case *sqlparser.Commit:
					output = append(output, "gtid", "commit")
				case *sqlparser.Insert:
					isRowEvent = true
					table, bv = ts.getBindVarsForInsert(stmt)
				case *sqlparser.Update:
					isRowEvent = true
					table, bv = ts.getBindVarsForUpdate(stmt)
				case *sqlparser.Delete:
					isRowEvent = true
					del := stmt.(*sqlparser.Delete)
					table = del.TableExprs[0].(*sqlparser.AliasedTableExpr).As.String()
				default:
					require.FailNowf(ts.t, "unsupported statement type", "stmt: %s", stmt)
				}
				if isRowEvent {
					fe := ts.fieldEvents[table]
					if fe == nil {
						require.FailNowf(ts.t, "field event for table %s not found", table)
					}
					if !ts.fieldEventsSent[table] {
						output = append(output, fe.String())
						ts.fieldEventsSent[table] = true
					}
					output = append(output, ts.getRowEvent(table, bv, fe, stmt, uint32(flags)))
				}
			}

		}
		tc.input = input
		tc.output = append(tc.output, output)
		testcases = append(testcases, tc)
	}
	runCases(ts.t, ts.options.filter, testcases, "current", nil)
}

func (ts *TestSpec) getFieldEvent(table *schemadiff.CreateTableEntity) *TestFieldEvent {
	var tfe TestFieldEvent
	tfe.table = table.Name()
	tfe.db = testenv.DBName
	for _, col := range table.TableSpec.Columns {
		tc := TestColumn{}
		tc.name = col.Name.String()
		sqlType := col.Type.SQLType()
		tc.dataType = sqlType.String()
		tc.dataTypeLowered = strings.ToLower(tc.dataType)
		collationName := col.Type.Options.Collate
		if collationName == "" {
			// Use the default, which is derived from the mysqld server default set
			// in the testenv.
			tc.collationID = testenv.DefaultCollationID
		} else {
			tc.collationID = testenv.CollationEnv.LookupByName(collationName)
		}
		collation := colldata.Lookup(tc.collationID)
		switch tc.dataTypeLowered {
		case "int32":
			tc.len = lengthInt
			tc.collationID = collations.CollationBinaryID
			tc.colType = "int(11)"
		case "varchar", "varbinary", "char", "binary":
			l := *col.Type.Length
			switch tc.dataTypeLowered {
			case "binary", "varbinary":
				tc.len = int64(l)
				tc.collationID = collations.CollationBinaryID
			default:
				tc.len = int64(collation.Charset().MaxWidth()) * int64(l)
				if tc.dataTypeLowered == "char" && collation.IsBinary() {
					tc.dataType = "BINARY"
				}
			}
			tc.colType = fmt.Sprintf("%s(%d)", tc.dataTypeLowered, l)
		case "blob":
			tc.len = lengthBlob
			tc.colType = "blob"
			tc.collationID = collations.CollationBinaryID
		case "text":
			tc.len = lengthText
			tc.colType = "text"
		case "set":
			tc.len = lengthSet
			tc.colType = fmt.Sprintf("%s(%s)", tc.dataTypeLowered, strings.Join(col.Type.EnumValues, ","))
			ts.metadata[getMetadataKey(table.Name(), tc.name)] = col.Type.EnumValues
		case "enum":
			tc.len = int64(len(col.Type.EnumValues) + 1)
			tc.colType = fmt.Sprintf("%s(%s)", tc.dataTypeLowered, strings.Join(col.Type.EnumValues, ","))
			ts.metadata[getMetadataKey(table.Name(), tc.name)] = col.Type.EnumValues
		default:
			log.Infof(fmt.Sprintf("unknown sqlTypeString %s", tc.dataTypeLowered))
		}
		tfe.cols = append(tfe.cols, &tc)
	}
	return &tfe
}

func getMetadataKey(table, col string) string {
	return fmt.Sprintf("%s:%s", table, col)
}

func (ts *TestSpec) setMetadataMap(table, col, value string) {
	values := strings.Split(value, ",")
	valuesReversed := slices.Clone(values)
	slices.Reverse(valuesReversed)
	ts.metadata[getMetadataKey(table, col)] = valuesReversed
}

func (ts *TestSpec) getMetadataMap(table string, col *TestColumn, value string) string {
	var bits int64
	value = strings.Trim(value, "'")
	meta := ts.metadata[getMetadataKey(table, col.name)]
	values := strings.Split(value, ",")
	for _, v := range values {
		v2 := strings.Trim(v, "'")
		for i, m := range meta {
			m2 := strings.Trim(m, "'")
			if m2 == v2 {
				switch col.dataTypeLowered {
				case "set":
					bits |= 1 << uint(i)
				case "enum":
					bits = int64(i) + 1
				}
			}
		}
	}
	return strconv.FormatInt(bits, 10)
}

func (ts *TestSpec) getRowEvent(table string, bv map[string]string, fe *TestFieldEvent, stmt sqlparser.Statement, flags uint32) string {
	ev := &binlogdata.RowEvent{
		TableName: table,
		RowChanges: []*binlogdata.RowChange{
			{
				Before: nil,
				After:  nil,
			},
		},
		Flags: flags,
	}
	var row query.Row
	for i, col := range fe.cols {
		if fe.cols[i].skip {
			continue
		}
		if col.dataTypeLowered == "binary" {
			bv[col.name] = strings.TrimSuffix(bv[col.name], "\\0")
		}
		val := []byte(bv[col.name])
		l := int64(len(val))
		if col.dataTypeLowered == "binary" {
			for l < col.len {
				val = append(val, "\x00"...)
				l++
			}
		}
		row.Values = append(row.Values, val...)
		row.Lengths = append(row.Lengths, l)
	}
	ev.RowChanges = ts.getRowChanges(table, stmt, &row)
	vEvent := &binlogdata.VEvent{
		Type:     binlogdata.VEventType_ROW,
		RowEvent: ev,
	}
	return vEvent.String()
}

func (ts *TestSpec) getRowChanges(table string, stmt sqlparser.Statement, row *query.Row) []*binlogdata.RowChange {
	var rowChanges []*binlogdata.RowChange
	var rowChange binlogdata.RowChange
	switch stmt.(type) {
	case *sqlparser.Insert:
		rowChange.After = row
		ts.setCurrentState(table, row)
	case *sqlparser.Update:
		rowChange = *ts.getRowChangeForUpdate(table, row)
		ts.setCurrentState(table, row)
	case *sqlparser.Delete:
		rowChange.Before = row
		ts.setCurrentState(table, nil)
	}
	rowChanges = append(rowChanges, &rowChange)
	return rowChanges
}

func (ts *TestSpec) getRowChangeForUpdate(table string, newState *query.Row) *binlogdata.RowChange {
	var rowChange binlogdata.RowChange
	var bitmap byte
	var before, after query.Row

	currentState := ts.getCurrentState(table)
	if currentState == nil {
		return nil
	}
	var currentValueIndex int64
	var hasSkip bool
	for i, l := range currentState.Lengths {
		skip := false
		isPKColumn := false
		for _, pkColumn := range ts.pkColumns[table] {
			if pkColumn == ts.fieldEvents[table].cols[i].name {
				isPKColumn = true
				break
			}
		}
		if ts.options.noblob {
			switch ts.fieldEvents[table].cols[i].dataTypeLowered {
			case "blob", "text":
				currentValue := currentState.Values[currentValueIndex : currentValueIndex+l]
				newValue := newState.Values[currentValueIndex : currentValueIndex+l]
				if string(currentValue) == string(newValue) {
					skip = true
					hasSkip = true
				}
			}
		}
		if skip && !isPKColumn {
			before.Lengths = append(before.Lengths, -1)
		} else {
			before.Values = append(before.Values, currentState.Values[currentValueIndex:currentValueIndex+l]...)
			before.Lengths = append(before.Lengths, l)
		}
		if skip {
			after.Lengths = append(after.Lengths, -1)
		} else {
			after.Values = append(after.Values, newState.Values[currentValueIndex:currentValueIndex+l]...)
			after.Lengths = append(after.Lengths, l)
			bitmap |= 1 << uint(i)
		}
		currentValueIndex += l
	}
	rowChange.Before = &before
	rowChange.After = &after
	if hasSkip {
		rowChange.DataColumns = &binlogdata.RowChange_Bitmap{
			Count: int64(len(currentState.Lengths)),
			Cols:  []byte{bitmap},
		}
	}
	return &rowChange
}

func (ts *TestSpec) getBefore(table string) *query.Row {
	currentState := ts.getCurrentState(table)
	if currentState == nil {
		return nil
	}
	var row query.Row
	var currentValueIndex int64
	for i, l := range currentState.Lengths {
		dataTypeIsRedacted := false
		switch ts.fieldEvents[table].cols[i].dataTypeLowered {
		case "blob", "text":
			dataTypeIsRedacted = true
		}
		if ts.options.noblob && dataTypeIsRedacted {
			row.Lengths = append(row.Lengths, -1)
		} else {
			row.Values = append(row.Values, currentState.Values[currentValueIndex:currentValueIndex+l]...)
			row.Lengths = append(row.Lengths, l)
		}
		currentValueIndex += l
	}
	return &row
}
