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

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer/testenv"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/schemadiff"
	"vitess.io/vitess/go/vt/sqlparser"
)

var noEvents = []TestRowEvent{}

type TestQuery struct {
	query  string
	events []TestRowEvent
}
type TestRowChange struct {
	before []string
	after  []string
}
type TestRowEventSpec struct {
	table   string
	changes []TestRowChange
}

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

type TestRowEvent struct {
	event string
	spec  *TestRowEventSpec
	flags int
}

type TestSpecOptions struct {
	noblob bool
	filter *binlogdata.Filter
}

type TestSpec struct {
	t       *testing.T
	ddls    []string
	input   []string
	tests   [][]*TestQuery
	options *TestSpecOptions

	tables          []string
	pkColumns       map[string][]string
	inited          bool
	schema          *schemadiff.Schema
	fieldEvents     map[string]*TestFieldEvent
	fieldEventsSent map[string]bool
	state           map[string]*query.Row
	metadata        map[string][]string
}

func (ts *TestSpec) Init() error {
	var err error
	if ts.inited {
		return nil
	}

	ts.inited = true
	if ts.options == nil {
		ts.options = &TestSpecOptions{}
	}
	ts.schema, err = schemadiff.NewSchemaFromQueries(ts.ddls, sqlparser.NewTestParser())
	if err != nil {
		return err
	}
	ts.fieldEvents = make(map[string]*TestFieldEvent)
	ts.fieldEventsSent = make(map[string]bool)
	ts.state = make(map[string]*query.Row)
	ts.metadata = make(map[string][]string)
	ts.pkColumns = make(map[string][]string)
	// create tables
	for i, t := range ts.schema.Tables() {
		execStatement(ts.t, ts.ddls[i])
		fe := ts.getFieldEvent(t)
		ts.fieldEvents[t.Name()] = fe

		var pkColumns []string
		var hasPK bool
		for _, index := range t.TableSpec.Indexes {
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

func (ts *TestSpec) Close() {
	dropStatement := fmt.Sprintf("drop tables %s", strings.Join(ts.schema.TableNames(), ", "))
	execStatement(ts.t, dropStatement)
}

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
			case tq.events != nil && len(tq.events) == 0:
				continue
			case tq.events != nil &&
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
					ins := stmt.(*sqlparser.Insert)
					tn, _ := ins.Table.TableName()
					table = tn.Name.String()
					fe := ts.fieldEvents[table]
					mids, _ := ins.Rows.(sqlparser.Values)
					for _, mid := range mids {
						for i, v := range mid {
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
				case *sqlparser.Update:
					isRowEvent = true
					upd := stmt.(*sqlparser.Update)
					buf := sqlparser.NewTrackedBuffer(nil)
					upd.TableExprs[0].(*sqlparser.AliasedTableExpr).Expr.Format(buf)
					table = buf.String()
					fe, ok := ts.fieldEvents[table]
					require.True(ts.t, ok, "field event for table %s not found", table)
					index := int64(0)
					state := ts.state[table]
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
				case *sqlparser.Delete:
					isRowEvent = true
					del := stmt.(*sqlparser.Delete)
					table = del.TableExprs[0].(*sqlparser.AliasedTableExpr).As.String()
				}
				if isRowEvent {
					fe := ts.fieldEvents[table]
					if fe == nil {
						panic(fmt.Sprintf("field event for table %s not found", table))
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
		log.Flush()
		testcases = append(testcases, tc)
	}
	runCases(ts.t, ts.options.filter, testcases, "current", nil)
}

func (ts *TestSpec) getFieldEvent(table *schemadiff.CreateTableEntity) *TestFieldEvent {
	var tfe TestFieldEvent
	tfe.table = table.Name()
	tfe.db = testenv.TestDBName
	for _, col := range table.TableSpec.Columns {
		tc := TestColumn{}
		tc.name = col.Name.String()
		sqlType := col.Type.SQLType()
		tc.dataType = sqlType.String()
		tc.dataTypeLowered = strings.ToLower(tc.dataType)
		tc.collate = col.Type.Options.Collate
		switch tc.dataTypeLowered {
		case "int32":
			tc.len = 11
			tc.charset = 63
			tc.colType = "int(11)"
		case "varchar", "varbinary", "char", "binary":
			l, _ := strconv.Atoi(col.Type.Length.Val)
			switch tc.dataTypeLowered {
			case "binary", "varbinary":
				tc.len = int64(l)
				tc.charset = 63
			default:
				tc.len = 4 * int64(l)
				tc.charset = 45
				if tc.dataTypeLowered == "char" && strings.Contains(tc.collate, "bin") {
					tc.dataType = "BINARY"
				}
			}
			tc.colType = fmt.Sprintf("%s(%d)", tc.dataTypeLowered, l)
		case "blob":
			tc.len = 65535
			tc.charset = 63
			tc.colType = "blob"
		case "text":
			tc.len = 262140
			tc.charset = 45
			tc.colType = "text"
		case "set":
			tc.len = 56
			tc.charset = 45
			tc.colType = fmt.Sprintf("%s(%s)", tc.dataTypeLowered, strings.Join(col.Type.EnumValues, ","))
			ts.metadata[getMetadataKey(table.Name(), tc.name)] = col.Type.EnumValues
		case "enum":
			tc.len = int64(len(col.Type.EnumValues) + 1)
			tc.charset = 45
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
	valuesReversed := make([]string, len(values))
	for i, v := range values {
		valuesReversed[len(values)-1-i] = v
	}
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
		ts.state[table] = row
	case *sqlparser.Update:
		rowChange = *ts.getRowChangeForUpdate(table, row, stmt)
		ts.state[table] = row
	case *sqlparser.Delete:
		rowChange.Before = row
		ts.state[table] = nil
	}
	rowChanges = append(rowChanges, &rowChange)
	return rowChanges
}

// isBitSet returns true if the bit at index is set
func isBitSet(data []byte, index int) bool {
	byteIndex := index / 8
	bitMask := byte(1 << (uint(index) & 0x7))
	return data[byteIndex]&bitMask > 0
}

func (ts *TestSpec) getRowChangeForUpdate(table string, newState *query.Row, stmt sqlparser.Statement) *binlogdata.RowChange {
	var rowChange binlogdata.RowChange
	var bitmap byte
	var before, after query.Row

	currentState := ts.state[table]
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
	currentState := ts.state[table]
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
