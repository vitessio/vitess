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

type VStreamerTestQuery struct {
	query  string
	events []*VStreamerTestEvent
}

type VStreamerTestEvent struct {
	typ    string
	values []string
}

type VStreamerTestSpec struct {
	t     *testing.T
	ddls  []string
	input []string
	tests [][]*VStreamerTestQuery

	tables          []string
	inited          bool
	schema          *schemadiff.Schema
	fieldEvents     map[string]*VStreamerTestFieldEvent
	fieldEventsSent map[string]bool
	state           map[string]*query.Row
	metadata        map[string][]string
}

var beginEvent, commitEvent, gtidEvent, rowEvent *VStreamerTestEvent

func getMetadataKey(table, col string) string {
	return fmt.Sprintf("%s:%s", table, col)
}

func (ts *VStreamerTestSpec) Init() error {
	var err error
	if ts.inited {
		return nil
	}

	ts.inited = true
	ts.schema, err = schemadiff.NewSchemaFromQueries(ts.ddls, sqlparser.NewTestParser())
	if err != nil {
		return err
	}
	ts.fieldEvents = make(map[string]*VStreamerTestFieldEvent)
	ts.fieldEventsSent = make(map[string]bool)
	ts.state = make(map[string]*query.Row)
	ts.metadata = make(map[string][]string)
	// create tables
	for i, t := range ts.schema.Tables() {
		env.Mysqld.FetchSuperQuery(context.Background(), ts.ddls[i])
		fe := ts.getFieldEvent(t)
		ts.fieldEvents[t.Name()] = fe
	}
	beginEvent = &VStreamerTestEvent{typ: "begin"}
	commitEvent = &VStreamerTestEvent{typ: "commit"}
	gtidEvent = &VStreamerTestEvent{typ: "gtid"}
	rowEvent = &VStreamerTestEvent{typ: "rowEvent"}
	engine.se.Reload(context.Background())
	return nil
}

func (ts *VStreamerTestSpec) getFieldEvent(table *schemadiff.CreateTableEntity) *VStreamerTestFieldEvent {
	var tfe VStreamerTestFieldEvent
	tfe.table = table.Name()
	tfe.db = testenv.TestDBName

	for _, col := range table.TableSpec.Columns {
		tc := VStreamerTestColumn{}
		tc.name = col.Name.String()
		sqlType := col.Type.SQLType()
		tc.dataType = sqlType.String()
		tc.dataTypeLowered = strings.ToLower(tc.dataType)
		switch tc.dataTypeLowered {
		case "int32":
			tc.len = 11
			tc.charset = 63
			tc.colType = "int(11)"
		case "varchar", "varbinary", "char", "binary", "blob", "text":
			l, _ := strconv.Atoi(col.Type.Length.Val)
			tc.len = int64(l)
			switch tc.dataTypeLowered {
			case "binary", "varbinary":
				tc.charset = 63
			default:
				tc.charset = 45
			}
			tc.colType = fmt.Sprintf("%s(%d)", tc.dataTypeLowered, l)
		case "set":
			tc.len = 56 // todo: works for the unit test with sets, but check if this is computable from the set values ...
			tc.charset = 45
			tc.colType = fmt.Sprintf("%s(%s)", tc.dataTypeLowered, strings.Join(col.Type.EnumValues, ","))
			ts.metadata[getMetadataKey(table.Name(), tc.name)] = col.Type.EnumValues
		case "enum":
			tc.len = int64(len(col.Type.EnumValues) + 1)
			tc.charset = 45
			tc.colType = fmt.Sprintf("%s(%s)", tc.dataTypeLowered, strings.Join(col.Type.EnumValues, ","))
			ts.metadata[getMetadataKey(table.Name(), tc.name)] = col.Type.EnumValues
		default:
			panic(fmt.Sprintf("unknown sqlTypeString %s", tc.dataTypeLowered))
		}
		tfe.cols = append(tfe.cols, &tc)
	}
	return &tfe
}

func (ts *VStreamerTestSpec) setMetadataMap(table, col, value string) {
	values := strings.Split(value, ",")
	valuesReversed := make([]string, len(values))
	for i, v := range values {
		valuesReversed[len(values)-1-i] = v
	}
	ts.metadata[getMetadataKey(table, col)] = valuesReversed
}

func (ts *VStreamerTestSpec) getMetadataMap(table string, col *VStreamerTestColumn, value string) string {
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

func (ts *VStreamerTestSpec) getRowEvent(table string, bv map[string]string, fe *VStreamerTestFieldEvent, event *VStreamerTestEvent) string {
	rowEvent := &binlogdata.RowEvent{
		TableName: table,
		RowChanges: []*binlogdata.RowChange{
			{
				Before: nil,
				After:  nil,
			},
		},
	}
	var row query.Row
	for _, col := range fe.cols {
		val := []byte(bv[col.name])
		l := int64(len(val))
		if col.dataTypeLowered == "binary" {
			val = append(val, "\x00"...)
			l++
		}
		row.Values = append(row.Values, val...)
		row.Lengths = append(row.Lengths, l)
	}
	rowEvent.RowChanges[0].After = &row
	ts.state[table] = &row
	vEvent := &binlogdata.VEvent{
		Type:     binlogdata.VEventType_ROW,
		RowEvent: rowEvent,
	}
	return vEvent.String()
}

func (ts *VStreamerTestSpec) Run() {
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
			stmt, err := sqlparser.NewTestParser().Parse(tq.query)
			require.NoError(ts.t, err)
			bv := make(map[string]string)
			switch stmt.(type) {
			case *sqlparser.Insert:
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
				upd := stmt.(*sqlparser.Update)
				buf := sqlparser.NewTrackedBuffer(nil)
				upd.TableExprs[0].(*sqlparser.AliasedTableExpr).Expr.Format(buf)
				table := buf.String()
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
					bv[bufN.String()] = bufV.String()
				}
			case *sqlparser.Delete:
				del := stmt.(*sqlparser.Delete)
				table = del.TableExprs[0].(*sqlparser.AliasedTableExpr).As.String()
			}
			for _, ev := range tq.events {
				var fe *VStreamerTestFieldEvent
				if ev.typ == "rowEvent" {
					fe = ts.fieldEvents[table]
					if fe == nil {
						panic(fmt.Sprintf("field event for table %s not found", table))
					}
					if !ts.fieldEventsSent[table] {
						output = append(output, fe.String())
						ts.fieldEventsSent[table] = true
					}
				}
				output = append(output, ts.getOutput(table, bv, fe, ev))
			}
		}
		tc.input = input
		tc.output = append(tc.output, output)
		log.Flush()
		testcases = append(testcases, tc)
	}
	runCases(ts.t, nil, testcases, "current", nil)
}

func (ts *VStreamerTestSpec) getOutput(table string, bv map[string]string, fe *VStreamerTestFieldEvent, ev *VStreamerTestEvent) string {
	switch ev.typ {
	case "begin", "commit", "gtid":
		return ev.typ
	case "rowEvent":
		vEvent := ts.getRowEvent(table, bv, fe, ev)
		return vEvent
	}
	panic(fmt.Sprintf("unknown event %v", ev))
}

func (ts *VStreamerTestSpec) Close() {
	dropStatement := fmt.Sprintf("drop tables %s", strings.Join(ts.schema.TableNames(), ", "))
	execStatement(ts.t, dropStatement)
}
