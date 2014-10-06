package tabletserver

import (
	"fmt"
	"reflect"
	"testing"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

func getSchemaInfo() *SchemaInfo {
	table := &schema.Table{
		Name: "test_table",
	}
	zero, _ := sqltypes.BuildValue(0)
	table.AddColumn("id", "int", zero, "")
	table.AddColumn("count", "int", zero, "")
	table.PKColumns = []int{0}

	tables := make(map[string]*TableInfo, 1)
	tables["test_table"] = &TableInfo{Table: table}

	tableNoPK := &schema.Table{
		Name: "test_table_no_pk",
	}
	tableNoPK.AddColumn("id", "int", zero, "")
	tableNoPK.PKColumns = []int{}
	tables["test_table_no_pk"] = &TableInfo{Table: tableNoPK}

	return &SchemaInfo{tables: tables}
}

func TestValidateQuery(t *testing.T) {
	schemaInfo := getSchemaInfo()
	query := &proto.BoundQuery{}
	splitter := NewQuerySplitter(query, 3, schemaInfo)

	query.Sql = "delete from test_table"
	got := splitter.validateQuery()
	want := fmt.Errorf("not a select statement")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("non-select validation failed, got:%v, want:%v", got, want)
	}

	query.Sql = "select * from test_table order by id"
	got = splitter.validateQuery()
	want = fmt.Errorf("unsupported query")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("order by query validation failed, got:%v, want:%v", got, want)
	}

	query.Sql = "select * from test_table group by id"
	got = splitter.validateQuery()
	want = fmt.Errorf("unsupported query")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("group by query validation failed, got:%v, want:%v", got, want)
	}

	query.Sql = "select A.* from test_table A JOIN test_table B"
	got = splitter.validateQuery()
	want = fmt.Errorf("unsupported query")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("join query validation failed, got:%v, want:%v", got, want)
	}

	query.Sql = "select * from test_table_no_pk"
	got = splitter.validateQuery()
	want = fmt.Errorf("no primary keys")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("no PK table validation failed, got:%v, want:%v", got, want)
	}

	query.Sql = "select * from unknown_table"
	got = splitter.validateQuery()
	want = fmt.Errorf("can't find table in schema")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("unknown table validation failed, got:%v, want:%v", got, want)
	}

	query.Sql = "select * from test_table"
	got = splitter.validateQuery()
	want = nil
	if !reflect.DeepEqual(got, want) {
		t.Errorf("valid query validation failed, got:%v, want:%v", got, want)
	}

	query.Sql = "select * from test_table where count > :count"
	got = splitter.validateQuery()
	want = nil
	if !reflect.DeepEqual(got, want) {
		t.Errorf("valid query validation failed, got:%v, want:%v", got, want)
	}
}

func TestGetWhereClause(t *testing.T) {
	splitter := &QuerySplitter{}
	sql := "select * from test_table where count > :count"
	statement, _ := sqlparser.Parse(sql)
	splitter.sel, _ = statement.(*sqlparser.Select)
	splitter.pkCol = "id"

	// no boundary case, start = end = nil, should not change the where clause
	nilValue := sqltypes.Value{}
	clause := splitter.getWhereClause(nilValue, nilValue)
	want := " where count > :count"
	got := sqlparser.String(clause)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect where clause for nil ranges, got:%v, want:%v", got, want)
	}

	// Set lower bound, should add the lower bound condition to where clause
	start, _ := sqltypes.BuildValue(20)
	clause = splitter.getWhereClause(start, nilValue)
	want = " where count > :count and id >= 20"
	got = sqlparser.String(clause)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect where clause, got:%v, want:%v", got, want)
	}

	// Set upper bound, should add the upper bound condition to where clause
	end, _ := sqltypes.BuildValue(40)
	clause = splitter.getWhereClause(nilValue, end)
	want = " where count > :count and id < 40"
	got = sqlparser.String(clause)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect where clause, got:%v, want:%v", got, want)
	}

	// Set both bounds, should add two conditions to where clause
	clause = splitter.getWhereClause(start, end)
	want = " where count > :count and id >= 20 and id < 40"
	got = sqlparser.String(clause)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect where clause, got:%v, want:%v", got, want)
	}

	// Original query with no where clause
	sql = "select * from test_table"
	statement, _ = sqlparser.Parse(sql)
	splitter.sel, _ = statement.(*sqlparser.Select)

	// no boundary case, start = end = nil should return no where clause
	clause = splitter.getWhereClause(nilValue, nilValue)
	want = ""
	got = sqlparser.String(clause)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect where clause for nil ranges, got:%v, want:%v", got, want)
	}

	// Set both bounds, should add two conditions to where clause
	clause = splitter.getWhereClause(start, end)
	want = " where id >= 20 and id < 40"
	got = sqlparser.String(clause)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect where clause, got:%v, want:%v", got, want)
	}
}

func TestGetSplitBoundaries(t *testing.T) {
	min, _ := sqltypes.BuildValue(10)
	max, _ := sqltypes.BuildValue(60)
	row := []sqltypes.Value{min, max}
	rows := [][]sqltypes.Value{row}

	minField := mproto.Field{Name: "min", Type: mproto.VT_LONGLONG}
	maxField := mproto.Field{Name: "max", Type: mproto.VT_LONGLONG}
	fields := []mproto.Field{minField, maxField}

	pkMinMax := &mproto.QueryResult{
		Fields: fields,
		Rows:   rows,
	}

	splitter := &QuerySplitter{}
	splitter.splitCount = 5
	boundaries := splitter.getSplitBoundaries(pkMinMax)
	if len(boundaries) != splitter.splitCount-1 {
		t.Errorf("wrong number of boundaries got: %v, want: %v", len(boundaries), splitter.splitCount-1)
	}
	got := splitter.getSplitBoundaries(pkMinMax)
	want := []sqltypes.Value{buildVal(20), buildVal(30), buildVal(40), buildVal(50)}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect boundaries, got: %v, want: %v", got, want)
	}

	// No min max rows should return empty bounary list
	pkMinMax.Rows = [][]sqltypes.Value{}
	noBounds := []sqltypes.Value{}
	boundaries = splitter.getSplitBoundaries(pkMinMax)
	if !reflect.DeepEqual(boundaries, noBounds) {
		t.Errorf("should return no boundaries")
	}

	// Null min row should return empty boundary list
	min = sqltypes.Value{}
	row = []sqltypes.Value{min, max}
	rows = [][]sqltypes.Value{row}
	pkMinMax.Rows = rows
	boundaries = splitter.getSplitBoundaries(pkMinMax)
	if !reflect.DeepEqual(boundaries, noBounds) {
		t.Errorf("should return no boundaries")
	}

	// Test negative min value
	min, _ = sqltypes.BuildValue(-100)
	max, _ = sqltypes.BuildValue(100)
	row = []sqltypes.Value{min, max}
	rows = [][]sqltypes.Value{row}
	pkMinMax.Rows = rows
	got = splitter.getSplitBoundaries(pkMinMax)
	want = []sqltypes.Value{buildVal(-60), buildVal(-20), buildVal(20), buildVal(60)}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect boundaries, got: %v, want: %v", got, want)
	}

	// Test float min max
	min, _ = sqltypes.BuildValue(10.5)
	max, _ = sqltypes.BuildValue(60.5)
	row = []sqltypes.Value{min, max}
	rows = [][]sqltypes.Value{row}
	minField = mproto.Field{Name: "min", Type: mproto.VT_DOUBLE}
	maxField = mproto.Field{Name: "max", Type: mproto.VT_DOUBLE}
	fields = []mproto.Field{minField, maxField}
	pkMinMax.Rows = rows
	pkMinMax.Fields = fields
	got = splitter.getSplitBoundaries(pkMinMax)
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
	query := &proto.BoundQuery{
		Sql: "select * from test_table where count > :count",
	}
	splitter := NewQuerySplitter(query, 3, schemaInfo)
	splitter.validateQuery()
	min, _ := sqltypes.BuildValue(0)
	max, _ := sqltypes.BuildValue(300)
	minField := mproto.Field{
		Name: "min",
		Type: mproto.VT_LONGLONG,
	}
	maxField := mproto.Field{
		Name: "min",
		Type: mproto.VT_LONGLONG,
	}
	fields := []mproto.Field{minField, maxField}
	row := []sqltypes.Value{min, max}
	rows := [][]sqltypes.Value{row}
	pkMinMax := &mproto.QueryResult{
		Rows:   rows,
		Fields: fields,
	}
	splits := splitter.split(pkMinMax)
	got := []string{}
	for _, split := range splits {
		if split.RowCount != 100 {
			t.Errorf("wrong RowCount, got: %v, want: %v", split.RowCount, 100)
		}
		got = append(got, split.Query.Sql)
	}
	want := []string{
		"select * from test_table where count > :count and id < 100",
		"select * from test_table where count > :count and id >= 100 and id < 200",
		"select * from test_table where count > :count and id >= 200",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("wrong splits, got: %v, want: %v", got, want)
	}
}
