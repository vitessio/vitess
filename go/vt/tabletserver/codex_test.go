package tabletserver

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/schema"
)

func TestBuildValuesList(t *testing.T) {
	pk1 := "pk1"
	pk2 := "pk2"
	tableInfo := createTableInfo("Table",
		map[string]string{pk1: "int", pk2: "varchar(128)", "col1": "int"},
		[]string{pk1, pk2})

	// case 1: simple PK clause. e.g. where pk1 = 1
	bindVars := map[string]interface{}{}
	pk1Val, _ := sqltypes.BuildValue(1)
	pkValues := []interface{}{pk1Val}
	// want [[1]]
	want := [][]sqltypes.Value{[]sqltypes.Value{pk1Val}}
	got := buildValueList(&tableInfo, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("case 1 failed, got %v, want %v", got, want)
	}

	// case 2: simple PK clause with bindVars. e.g. where pk1 = :pk1
	bindVars[pk1] = 1
	pkValues = []interface{}{":pk1"}
	// want [[1]]
	want = [][]sqltypes.Value{[]sqltypes.Value{pk1Val}}
	got = buildValueList(&tableInfo, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("case 2 failed, got %v, want %v", got, want)
	}

	// case 3: composite pK clause. e.g. where pk1 = 1 and pk2 = "abc"
	pk2Val, _ := sqltypes.BuildValue("abc")
	pkValues = []interface{}{pk1Val, pk2Val}
	// want [[1 abc]]
	want = [][]sqltypes.Value{[]sqltypes.Value{pk1Val, pk2Val}}
	got = buildValueList(&tableInfo, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("case 3 failed, got %v, want %v", got, want)
	}

	// case 4: multi row composite PK insert
	// e.g. insert into Table(pk1,pk2) values (1, "abc"), (2, "xyz")
	pk1Val2, _ := sqltypes.BuildValue(2)
	pk2Val2, _ := sqltypes.BuildValue("xyz")
	pkValues = []interface{}{
		[]interface{}{pk1Val, pk1Val2},
		[]interface{}{pk2Val, pk2Val2}}
	// want [[1 abc][2 xyz]]
	want = [][]sqltypes.Value{
		[]sqltypes.Value{pk1Val, pk2Val},
		[]sqltypes.Value{pk1Val2, pk2Val2}}
	got = buildValueList(&tableInfo, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("case 4 failed, got %v, want %v", got, want)
	}

	// case 5: composite PK IN clause
	// e.g. where pk1 = 1 and pk2 IN ("abc", "xyz")
	pkValues = []interface{}{
		pk1Val,
		[]interface{}{pk2Val, pk2Val2}}
	// want [[1 abc][1 xyz]]
	want = [][]sqltypes.Value{
		[]sqltypes.Value{pk1Val, pk2Val},
		[]sqltypes.Value{pk1Val, pk2Val2}}

	got = buildValueList(&tableInfo, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("case 5 failed, got %v, want %v", got, want)
	}

}

func TestBuildINValueList(t *testing.T) {
	pk1 := "pk1"
	tableInfo := createTableInfo("Table",
		map[string]string{pk1: "int", "col1": "int"},
		[]string{pk1})

	// case 1: single PK IN clause
	// e.g. where pk1 in(1, 2, 3)
	bindVars := map[string]interface{}{}
	pk1Val, _ := sqltypes.BuildValue(1)
	pk1Val2, _ := sqltypes.BuildValue(2)
	pk1Val3, _ := sqltypes.BuildValue(3)
	pkValues := []interface{}{pk1Val, pk1Val2, pk1Val3}
	// want [[1][2][3]]
	want := [][]sqltypes.Value{
		[]sqltypes.Value{pk1Val},
		[]sqltypes.Value{pk1Val2},
		[]sqltypes.Value{pk1Val3}}
	got := buildINValueList(&tableInfo, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("case 1 failed, got %v, want %v", got, want)
	}
}

func TestBuildSecondaryList(t *testing.T) {
	pk1 := "pk1"
	pk2 := "pk2"
	tableInfo := createTableInfo("Table",
		map[string]string{pk1: "int", pk2: "varchar(128)", "col1": "int"},
		[]string{pk1, pk2})

	// set pk2 = 'xyz' where pk1=1 and pk2 = 'abc'
	bindVars := map[string]interface{}{}
	pk1Val, _ := sqltypes.BuildValue(1)
	pk2Val, _ := sqltypes.BuildValue("abc")
	pkValues := []interface{}{pk1Val, pk2Val}
	pkList := buildValueList(&tableInfo, pkValues, bindVars)
	pk2SecVal, _ := sqltypes.BuildValue("xyz")
	secondaryPKValues := []interface{}{nil, pk2SecVal}
	// want [[1 xyz]]
	want := [][]sqltypes.Value{
		[]sqltypes.Value{pk1Val, pk2SecVal}}
	got := buildSecondaryList(&tableInfo, pkList, secondaryPKValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("case 1 failed, got %v, want %v", got, want)
	}
}

func TestBuildStreamComment(t *testing.T) {
	pk1 := "pk1"
	pk2 := "pk2"
	tableInfo := createTableInfo("Table",
		map[string]string{pk1: "int", pk2: "varchar(128)", "col1": "int"},
		[]string{pk1, pk2})

	// set pk2 = 'xyz' where pk1=1 and pk2 = 'abc'
	bindVars := map[string]interface{}{}
	pk1Val, _ := sqltypes.BuildValue(1)
	pk2Val, _ := sqltypes.BuildValue("abc")
	pkValues := []interface{}{pk1Val, pk2Val}
	pkList := buildValueList(&tableInfo, pkValues, bindVars)
	pk2SecVal, _ := sqltypes.BuildValue("xyz")
	secondaryPKValues := []interface{}{nil, pk2SecVal}
	secondaryList := buildSecondaryList(&tableInfo, pkList, secondaryPKValues, bindVars)
	want := []byte(" /* _stream Table (pk1 pk2 ) (1 'YWJj' ) (1 'eHl6' ); */")
	got := buildStreamComment(&tableInfo, pkList, secondaryList)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("case 1 failed, got %v, want %v", got, want)
	}
}

func createTableInfo(name string, cols map[string]string, pKeys []string) TableInfo {
	table := schema.NewTable(name)
	for colName, colType := range cols {
		table.AddColumn(colName, colType, sqltypes.Value{nil}, "")
	}
	tableInfo := TableInfo{Table: table}
	tableInfo.SetPK(pKeys)
	return tableInfo
}
