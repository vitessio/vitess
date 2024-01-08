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

type TestEvents struct {
	Query  string
	Events []*TestEvent
}

type TestEvent struct {
	Type   string
	Values []string
}

type TestSpec struct {
	t     *testing.T
	ddls  []string
	input []string
	tests [][]*TestEvents

	tables          []string
	inited          bool
	schema          *schemadiff.Schema
	fieldEvents     map[string]*TestFieldEvent
	fieldEventsSent map[string]bool
	state           map[string]*query.Row
	metadata        map[string][]string
}

var beginEvent, commitEvent, gtidEvent *TestEvent

func RowEvent(values []string) *TestEvent {
	return &TestEvent{Type: "rowEvent", Values: values}
}
func (ts *TestSpec) Init() error {
	var err error
	if ts.inited {
		return nil
	}

	ts.inited = true
	ts.schema, err = schemadiff.NewSchemaFromQueries(ts.ddls, sqlparser.NewTestParser())
	if err != nil {
		return err
	}
	ts.fieldEvents = make(map[string]*TestFieldEvent)
	ts.fieldEventsSent = make(map[string]bool)
	ts.state = make(map[string]*query.Row)
	ts.metadata = make(map[string][]string)
	// create tables
	for i, t := range ts.schema.Tables() {
		env.Mysqld.FetchSuperQuery(context.Background(), ts.ddls[i])
		fe := ts.getFieldEvent(t)
		ts.fieldEvents[t.Name()] = fe
	}
	beginEvent = &TestEvent{Type: "begin"}
	commitEvent = &TestEvent{Type: "commit"}
	gtidEvent = &TestEvent{Type: "gtid"}
	engine.se.Reload(context.Background())
	return nil
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

func (ts *TestSpec) getRowEvent(table string, bv map[string]string, fe *TestFieldEvent, event *TestEvent) string {
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
			input = append(input, tq.Query)
			stmt, err := sqlparser.NewTestParser().Parse(tq.Query)
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
			for _, ev := range tq.Events {
				var fe *TestFieldEvent
				if ev.Type == "rowEvent" {
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

func (ts *TestSpec) getOutput(table string, bv map[string]string, fe *TestFieldEvent, ev *TestEvent) string {
	switch ev.Type {
	case "begin", "commit", "gtid":
		return ev.Type
	case "rowEvent":
		vEvent := ts.getRowEvent(table, bv, fe, ev)
		return vEvent
	}
	panic(fmt.Sprintf("unknown event %v", ev))
}

func (ts *TestSpec) Close() func() {
	dropStatement := fmt.Sprintf("drop tables %s", strings.Join(ts.schema.TableNames(), ", "))
	return func() {
		_, _ = env.Mysqld.FetchSuperQuery(context.Background(), dropStatement)
	}
}
