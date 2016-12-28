package tabletserver

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
)

func getSchemaInfo() *SchemaInfo {
	table := &schema.Table{
		Name: sqlparser.NewTableIdent("test_table"),
	}
	zero, _ := sqltypes.BuildValue(0)
	table.AddColumn("id", sqltypes.Int64, zero, "")
	table.AddColumn("id2", sqltypes.Int64, zero, "")
	table.AddColumn("count", sqltypes.Int64, zero, "")
	table.PKColumns = []int{0}
	primaryIndex := table.AddIndex("PRIMARY")
	primaryIndex.AddColumn("id", 12345)

	id2Index := table.AddIndex("idx_id2")
	id2Index.AddColumn("id2", 1234)

	tables := make(map[string]*TableInfo, 1)
	tables["test_table"] = &TableInfo{Table: table}

	tableNoPK := &schema.Table{
		Name: sqlparser.NewTableIdent("test_table_no_pk"),
	}
	tableNoPK.AddColumn("id", sqltypes.Int64, zero, "")
	tableNoPK.PKColumns = []int{}
	tables["test_table_no_pk"] = &TableInfo{Table: tableNoPK}

	return &SchemaInfo{tables: tables}
}

func TestValidateQuery(t *testing.T) {
	schemaInfo := getSchemaInfo()

	splitter := NewQuerySplitter("delete from test_table", nil, "", 3, schemaInfo)
	got := splitter.validateQuery()
	want := fmt.Errorf("not a select statement")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("non-select validation failed, got:%v, want:%v", got, want)
	}

	splitter = NewQuerySplitter("select * from test_table order by id", nil, "", 3, schemaInfo)
	got = splitter.validateQuery()
	want = fmt.Errorf("unsupported query")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("order by query validation failed, got:%v, want:%v", got, want)
	}

	splitter = NewQuerySplitter("select * from test_table group by id", nil, "", 3, schemaInfo)
	got = splitter.validateQuery()
	want = fmt.Errorf("unsupported query")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("group by query validation failed, got:%v, want:%v", got, want)
	}

	splitter = NewQuerySplitter("select A.* from test_table A JOIN test_table B", nil, "", 3, schemaInfo)
	got = splitter.validateQuery()
	want = fmt.Errorf("unsupported query")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("join query validation failed, got:%v, want:%v", got, want)
	}

	splitter = NewQuerySplitter("select * from test_table_no_pk", nil, "", 3, schemaInfo)
	got = splitter.validateQuery()
	want = fmt.Errorf("no primary keys")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("no PK table validation failed, got:%v, want:%v", got, want)
	}

	splitter = NewQuerySplitter("select * from unknown_table", nil, "", 3, schemaInfo)
	got = splitter.validateQuery()
	want = fmt.Errorf("can't find table in schema")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("unknown table validation failed, got:%v, want:%v", got, want)
	}

	splitter = NewQuerySplitter("select * from test_table", nil, "", 3, schemaInfo)
	got = splitter.validateQuery()
	want = nil
	if !reflect.DeepEqual(got, want) {
		t.Errorf("valid query validation failed, got:%v, want:%v", got, want)
	}

	splitter = NewQuerySplitter("select * from test_table where count > :count", nil, "", 3, schemaInfo)
	got = splitter.validateQuery()
	want = nil
	if !reflect.DeepEqual(got, want) {
		t.Errorf("valid query validation failed, got:%v, want:%v", got, want)
	}

	splitter = NewQuerySplitter("select * from test_table where count > :count", nil, "id2", 0, schemaInfo)
	got = splitter.validateQuery()
	want = nil
	if !reflect.DeepEqual(got, want) {
		t.Errorf("valid query validation failed, got:%v, want:%v", got, want)
	}

	splitter = NewQuerySplitter("invalid select * from test_table where count > :count", nil, "id2", 0, schemaInfo)
	if err := splitter.validateQuery(); err == nil {
		t.Fatalf("validateQuery() = %v, want: nil", err)
	}

	// column id2 is indexed
	splitter = NewQuerySplitter("select * from test_table where count > :count", nil, "id2", 3, schemaInfo)
	got = splitter.validateQuery()
	want = nil
	if !reflect.DeepEqual(got, want) {
		t.Errorf("valid query validation failed, got:%v, want:%v", got, want)
	}

	// column does not exist
	splitter = NewQuerySplitter("select * from test_table where count > :count", nil, "unknown_column", 3, schemaInfo)
	got = splitter.validateQuery()
	wantStr := "split column is not indexed or does not exist in table schema"
	if !strings.Contains(got.Error(), wantStr) {
		t.Errorf("unknown table validation failed, got:%v, want:%v", got, wantStr)
	}

	// column is not indexed
	splitter = NewQuerySplitter("select * from test_table where count > :count", nil, "count", 3, schemaInfo)
	got = splitter.validateQuery()
	wantStr = "split column is not indexed or does not exist in table schema"
	if !strings.Contains(got.Error(), wantStr) {
		t.Errorf("unknown table validation failed, got:%v, want:%v", got, wantStr)
	}
}

func TestGetWhereClause(t *testing.T) {
	splitter := &QuerySplitter{}
	sql := "select * from test_table where count > :count"
	statement, _ := sqlparser.Parse(sql)
	splitter.sel, _ = statement.(*sqlparser.Select)
	splitter.splitColumn = sqlparser.NewColIdent("id")
	bindVars := make(map[string]interface{})
	// no boundary case, start = end = nil, should not change the where clause
	nilValue := sqltypes.Value{}
	clause := splitter.getWhereClause(splitter.sel.Where, bindVars, nilValue, nilValue)
	want := " where count > :count"
	got := sqlparser.String(clause)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect where clause for nil ranges, got:%v, want:%v", got, want)
	}

	// Set lower bound, should add the lower bound condition to where clause
	startVal := int64(20)
	start, _ := sqltypes.BuildValue(startVal)
	bindVars = make(map[string]interface{})
	bindVars[":count"] = 300
	clause = splitter.getWhereClause(splitter.sel.Where, bindVars, start, nilValue)
	want = " where (count > :count) and (id >= :" + startBindVarName + ")"
	got = sqlparser.String(clause)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect where clause, got:%v, want:%v", got, want)
	}
	v, ok := bindVars[startBindVarName]
	if !ok {
		t.Fatalf("bind var: %s not found got: nil, want: %v", startBindVarName, startVal)
	}
	if v != startVal {
		t.Fatalf("bind var: %s not found got: %v, want: %v", startBindVarName, v, startVal)
	}
	// Set upper bound, should add the upper bound condition to where clause
	endVal := int64(40)
	end, _ := sqltypes.BuildValue(endVal)
	bindVars = make(map[string]interface{})
	clause = splitter.getWhereClause(splitter.sel.Where, bindVars, nilValue, end)
	want = " where (count > :count) and (id < :" + endBindVarName + ")"
	got = sqlparser.String(clause)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect where clause, got:%v, want:%v", got, want)
	}
	v, ok = bindVars[endBindVarName]
	if !ok {
		t.Fatalf("bind var: %s not found got: nil, want: %v", endBindVarName, endVal)
	}
	if v != endVal {
		t.Fatalf("bind var: %s not found got: %v, want: %v", endBindVarName, v, endVal)
	}

	// Set both bounds, should add two conditions to where clause
	bindVars = make(map[string]interface{})
	clause = splitter.getWhereClause(splitter.sel.Where, bindVars, start, end)
	want = fmt.Sprintf(" where (count > :count) and (id >= :%s and id < :%s)", startBindVarName, endBindVarName)
	got = sqlparser.String(clause)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect where clause, got:%v, want:%v", got, want)
	}

	// Original query with no where clause
	sql = "select * from test_table"
	statement, _ = sqlparser.Parse(sql)
	splitter.sel, _ = statement.(*sqlparser.Select)
	bindVars = make(map[string]interface{})
	// no boundary case, start = end = nil should return no where clause
	clause = splitter.getWhereClause(splitter.sel.Where, bindVars, nilValue, nilValue)
	want = ""
	got = sqlparser.String(clause)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect where clause for nil ranges, got:%v, want:%v", got, want)
	}
	bindVars = make(map[string]interface{})
	// Set both bounds, should add two conditions to where clause
	clause = splitter.getWhereClause(splitter.sel.Where, bindVars, start, end)
	want = fmt.Sprintf(" where id >= :%s and id < :%s", startBindVarName, endBindVarName)
	got = sqlparser.String(clause)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect where clause, got:%v, want:%v", got, want)
	}
	v, ok = bindVars[startBindVarName]
	if !ok {
		t.Fatalf("bind var: %s not found got: nil, want: %v", startBindVarName, startVal)
	}
	if v != startVal {
		t.Fatalf("bind var: %s not found got: %v, want: %v", startBindVarName, v, startVal)
	}
	v, ok = bindVars[endBindVarName]
	if !ok {
		t.Fatalf("bind var: %s not found got: nil, want: %v", endBindVarName, endVal)
	}
	if v != endVal {
		t.Fatalf("bind var: %s not found got: %v, want: %v", endBindVarName, v, endVal)
	}
}

func TestSplitBoundaries(t *testing.T) {
	min, _ := sqltypes.BuildValue(10)
	max, _ := sqltypes.BuildValue(60)
	row := []sqltypes.Value{min, max}
	rows := [][]sqltypes.Value{row}

	minField := &querypb.Field{Name: "min", Type: sqltypes.Int64}
	maxField := &querypb.Field{Name: "max", Type: sqltypes.Int64}
	fields := []*querypb.Field{minField, maxField}

	pkMinMax := &sqltypes.Result{
		Fields: fields,
		Rows:   rows,
	}

	splitter := &QuerySplitter{}
	splitter.splitCount = 5
	boundaries, err := splitter.splitBoundaries(sqltypes.Int64, pkMinMax)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(boundaries) != int(splitter.splitCount-1) {
		t.Errorf("wrong number of boundaries got: %v, want: %v", len(boundaries), splitter.splitCount-1)
	}
	got, err := splitter.splitBoundaries(sqltypes.Int64, pkMinMax)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []sqltypes.Value{buildVal(20), buildVal(30), buildVal(40), buildVal(50)}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect boundaries, got: %v, want: %v", got, want)
	}

	// Test negative min value
	min, _ = sqltypes.BuildValue(-100)
	max, _ = sqltypes.BuildValue(100)
	row = []sqltypes.Value{min, max}
	rows = [][]sqltypes.Value{row}
	pkMinMax.Rows = rows
	got, err = splitter.splitBoundaries(sqltypes.Int64, pkMinMax)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want = []sqltypes.Value{buildVal(-60), buildVal(-20), buildVal(20), buildVal(60)}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect boundaries, got: %v, want: %v", got, want)
	}

	// Test float min max
	min, _ = sqltypes.BuildValue(10.5)
	max, _ = sqltypes.BuildValue(60.5)
	row = []sqltypes.Value{min, max}
	rows = [][]sqltypes.Value{row}
	minField = &querypb.Field{Name: "min", Type: sqltypes.Float64}
	maxField = &querypb.Field{Name: "max", Type: sqltypes.Float64}
	fields = []*querypb.Field{minField, maxField}
	pkMinMax.Rows = rows
	pkMinMax.Fields = fields
	got, err = splitter.splitBoundaries(sqltypes.Float64, pkMinMax)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want = []sqltypes.Value{buildVal(20.5), buildVal(30.5), buildVal(40.5), buildVal(50.5)}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect boundaries, got: %v, want: %v", got, want)
	}
}

func buildVal(val interface{}) sqltypes.Value {
	v, _ := sqltypes.BuildValue(val)
	return v
}

func TestSplitQuery(t *testing.T) {
	schemaInfo := getSchemaInfo()
	splitter := NewQuerySplitter("select * from test_table where count > :count", nil, "", 3, schemaInfo)
	splitter.validateQuery()
	min, _ := sqltypes.BuildValue(0)
	max, _ := sqltypes.BuildValue(300)
	minField := &querypb.Field{
		Name: "min",
		Type: sqltypes.Int64,
	}
	maxField := &querypb.Field{
		Name: "max",
		Type: sqltypes.Int64,
	}
	fields := []*querypb.Field{minField, maxField}
	pkMinMax := &sqltypes.Result{
		Fields: fields,
	}

	// Ensure that empty min max does not cause panic or return any error
	splits, err := splitter.split(sqltypes.Int64, pkMinMax)
	if err != nil {
		t.Errorf("unexpected error while splitting on empty pkMinMax, %s", err)
	}

	pkMinMax.Rows = [][]sqltypes.Value{{min, max}}
	splits, err = splitter.split(sqltypes.Int64, pkMinMax)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := []querytypes.BoundQuery{}
	for _, split := range splits {
		if split.RowCount != 100 {
			t.Errorf("wrong RowCount, got: %v, want: %v", split.RowCount, 100)
		}
		got = append(got, querytypes.BoundQuery{
			Sql:           split.Sql,
			BindVariables: split.BindVariables,
		})
	}
	want := []querytypes.BoundQuery{
		{
			Sql:           "select * from test_table where (count > :count) and (id < :" + endBindVarName + ")",
			BindVariables: map[string]interface{}{endBindVarName: int64(100)},
		},
		{
			Sql: fmt.Sprintf("select * from test_table where (count > :count) and (id >= :%s and id < :%s)", startBindVarName, endBindVarName),
			BindVariables: map[string]interface{}{
				startBindVarName: int64(100),
				endBindVarName:   int64(200),
			},
		},
		{
			Sql:           "select * from test_table where (count > :count) and (id >= :" + startBindVarName + ")",
			BindVariables: map[string]interface{}{startBindVarName: int64(200)},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("wrong splits, got: %v, want: %v", got, want)
	}
}

func TestSplitQueryFractionalColumn(t *testing.T) {
	schemaInfo := getSchemaInfo()
	splitter := NewQuerySplitter("select * from test_table where count > :count", nil, "", 3, schemaInfo)
	splitter.validateQuery()
	min, _ := sqltypes.BuildValue(10.5)
	max, _ := sqltypes.BuildValue(490.5)
	minField := &querypb.Field{
		Name: "min",
		Type: sqltypes.Float32,
	}
	maxField := &querypb.Field{
		Name: "max",
		Type: sqltypes.Float32,
	}
	fields := []*querypb.Field{minField, maxField}
	pkMinMax := &sqltypes.Result{
		Fields: fields,
		Rows:   [][]sqltypes.Value{{min, max}},
	}

	splits, err := splitter.split(sqltypes.Float32, pkMinMax)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := []querytypes.BoundQuery{}
	for _, split := range splits {
		if split.RowCount != 160 {
			t.Errorf("wrong RowCount, got: %v, want: %v", split.RowCount, 160)
		}
		got = append(got, querytypes.BoundQuery{
			Sql:           split.Sql,
			BindVariables: split.BindVariables,
		})
	}
	want := []querytypes.BoundQuery{
		{
			Sql:           "select * from test_table where (count > :count) and (id < :" + endBindVarName + ")",
			BindVariables: map[string]interface{}{endBindVarName: 170.5},
		},
		{
			Sql: fmt.Sprintf("select * from test_table where (count > :count) and (id >= :%s and id < :%s)", startBindVarName, endBindVarName),
			BindVariables: map[string]interface{}{
				startBindVarName: 170.5,
				endBindVarName:   330.5,
			},
		},
		{
			Sql:           "select * from test_table where (count > :count) and (id >= :" + startBindVarName + ")",
			BindVariables: map[string]interface{}{startBindVarName: 330.5},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("wrong splits, got: %v, want: %v", got, want)
	}
}

func TestSplitQueryVarBinaryColumn(t *testing.T) {
	schemaInfo := getSchemaInfo()
	splitter := NewQuerySplitter("select * from test_table where count > :count", nil, "", 3, schemaInfo)
	splitter.validateQuery()
	splits, err := splitter.split(sqltypes.VarBinary, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := []querytypes.BoundQuery{}
	for _, split := range splits {
		got = append(got, querytypes.BoundQuery{
			Sql:           split.Sql,
			BindVariables: split.BindVariables,
		})
	}
	want := []querytypes.BoundQuery{
		{
			Sql:           "select * from test_table where (count > :count) and (id < :" + endBindVarName + ")",
			BindVariables: map[string]interface{}{endBindVarName: hexToByteUInt32(0x55555555)},
		},
		{
			Sql: fmt.Sprintf("select * from test_table where (count > :count) and (id >= :%s and id < :%s)", startBindVarName, endBindVarName),
			BindVariables: map[string]interface{}{
				startBindVarName: hexToByteUInt32(0x55555555),
				endBindVarName:   hexToByteUInt32(0xAAAAAAAA),
			},
		},
		{
			Sql:           "select * from test_table where (count > :count) and (id >= :" + startBindVarName + ")",
			BindVariables: map[string]interface{}{startBindVarName: hexToByteUInt32(0xAAAAAAAA)},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("wrong splits, got: %v, want: %v", got, want)
	}
}

func TestSplitQueryVarCharColumn(t *testing.T) {
	schemaInfo := getSchemaInfo()
	splitter := NewQuerySplitter("select * from test_table where count > :count", map[string]interface{}{"count": 123}, "", 3, schemaInfo)
	splitter.validateQuery()
	splits, err := splitter.split(sqltypes.VarChar, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := []querytypes.BoundQuery{}
	for _, split := range splits {
		got = append(got, querytypes.BoundQuery{
			Sql:           split.Sql,
			BindVariables: split.BindVariables,
		})
	}
	want := []querytypes.BoundQuery{
		{
			Sql:           "select * from test_table where count > :count",
			BindVariables: map[string]interface{}{"count": 123},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("wrong splits, got: %v, want: %v", got, want)
	}
}

func hexToByteUInt32(val uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, val)
	return buf
}
