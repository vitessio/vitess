package tabletserver

import (
	"reflect"
	"testing"

	"github.com/henryanand/vitess/go/sqltypes"
	"github.com/henryanand/vitess/go/vt/schema"
)

func TestBuildValuesList(t *testing.T) {
	tableInfo := createTableInfo("Table",
		map[string]string{"pk1": "int", "pk2": "varbinary(128)", "col1": "int"},
		[]string{"pk1", "pk2"})

	// simple PK clause. e.g. where pk1 = 1
	bindVars := map[string]interface{}{}
	pk1Val, _ := sqltypes.BuildValue(1)
	pkValues := []interface{}{pk1Val}
	// want [[1]]
	want := [][]sqltypes.Value{[]sqltypes.Value{pk1Val}}
	got, _ := buildValueList(&tableInfo, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// simple PK clause with bindVars. e.g. where pk1 = :pk1
	bindVars["pk1"] = 1
	pkValues = []interface{}{":pk1"}
	// want [[1]]
	want = [][]sqltypes.Value{[]sqltypes.Value{pk1Val}}
	got, _ = buildValueList(&tableInfo, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// null value
	bindVars["pk1"] = nil
	pkValues = []interface{}{":pk1"}
	// want [[1]]
	want = [][]sqltypes.Value{[]sqltypes.Value{sqltypes.Value{}}}
	got, _ = buildValueList(&tableInfo, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// invalid value
	bindVars["pk1"] = struct{}{}
	pkValues = []interface{}{":pk1"}
	wantErr := "error: unsupported bind variable type struct {}: {}"

	got, err := buildValueList(&tableInfo, pkValues, bindVars)
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v, want %v", err, wantErr)
	}

	// type mismatch int
	bindVars["pk1"] = "str"
	pkValues = []interface{}{":pk1"}
	wantErr = "error: type mismatch, expecting numeric type for str"

	got, err = buildValueList(&tableInfo, pkValues, bindVars)
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v, want %v", err, wantErr)
	}

	// type mismatch binary
	bindVars["pk1"] = 1
	bindVars["pk2"] = 1
	pkValues = []interface{}{":pk1", ":pk2"}
	wantErr = "error: type mismatch, expecting string type for 1"

	got, err = buildValueList(&tableInfo, pkValues, bindVars)
	t.Logf("%v", got)
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v, want %v", err, wantErr)
	}

	// composite PK clause. e.g. where pk1 = 1 and pk2 = "abc"
	pk2Val, _ := sqltypes.BuildValue("abc")
	pkValues = []interface{}{pk1Val, pk2Val}
	// want [[1 abc]]
	want = [][]sqltypes.Value{[]sqltypes.Value{pk1Val, pk2Val}}
	got, _ = buildValueList(&tableInfo, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// multi row composite PK insert
	// e.g. insert into Table(pk1,pk2) values (1, "abc"), (2, "xyz")
	pk1Val2, _ := sqltypes.BuildValue(2)
	pk2Val2, _ := sqltypes.BuildValue("xyz")
	pkValues = []interface{}{
		[]interface{}{pk1Val, pk1Val2},
		[]interface{}{pk2Val, pk2Val2},
	}
	// want [[1 abc][2 xyz]]
	want = [][]sqltypes.Value{
		[]sqltypes.Value{pk1Val, pk2Val},
		[]sqltypes.Value{pk1Val2, pk2Val2}}
	got, _ = buildValueList(&tableInfo, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// composite PK IN clause
	// e.g. where pk1 = 1 and pk2 IN ("abc", "xyz")
	pkValues = []interface{}{
		pk1Val,
		[]interface{}{pk2Val, pk2Val2},
	}
	// want [[1 abc][1 xyz]]
	want = [][]sqltypes.Value{
		[]sqltypes.Value{pk1Val, pk2Val},
		[]sqltypes.Value{pk1Val, pk2Val2},
	}

	got, _ = buildValueList(&tableInfo, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// list arg
	// e.g. where pk1 = 1 and pk2 IN ::list
	bindVars = map[string]interface{}{
		"list": []interface{}{
			"abc",
			"xyz",
		},
	}
	pkValues = []interface{}{
		pk1Val,
		"::list",
	}
	// want [[1 abc][1 xyz]]
	want = [][]sqltypes.Value{
		[]sqltypes.Value{pk1Val, pk2Val},
		[]sqltypes.Value{pk1Val, pk2Val2},
	}

	// list arg one value
	// e.g. where pk1 = 1 and pk2 IN ::list
	bindVars = map[string]interface{}{
		"list": []interface{}{
			"abc",
		},
	}
	pkValues = []interface{}{
		pk1Val,
		"::list",
	}
	// want [[1 abc][1 xyz]]
	want = [][]sqltypes.Value{
		[]sqltypes.Value{pk1Val, pk2Val},
	}

	got, _ = buildValueList(&tableInfo, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// list arg empty list
	bindVars = map[string]interface{}{
		"list": []interface{}{},
	}
	pkValues = []interface{}{
		pk1Val,
		"::list",
	}
	wantErr = "error: empty list supplied for list"

	got, err = buildValueList(&tableInfo, pkValues, bindVars)
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v, want %v", err, wantErr)
	}

	// list arg for non-list
	bindVars = map[string]interface{}{
		"list": []interface{}{},
	}
	pkValues = []interface{}{
		pk1Val,
		":list",
	}
	wantErr = "error: unexpected arg type []interface {} for key list"

	got, err = buildValueList(&tableInfo, pkValues, bindVars)
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v, want %v", err, wantErr)
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
	pkList, _ := buildValueList(&tableInfo, pkValues, bindVars)
	pk2SecVal, _ := sqltypes.BuildValue("xyz")
	secondaryPKValues := []interface{}{nil, pk2SecVal}
	// want [[1 xyz]]
	want := [][]sqltypes.Value{
		[]sqltypes.Value{pk1Val, pk2SecVal}}
	got, _ := buildSecondaryList(&tableInfo, pkList, secondaryPKValues, bindVars)
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
	pkList, _ := buildValueList(&tableInfo, pkValues, bindVars)
	pk2SecVal, _ := sqltypes.BuildValue("xyz")
	secondaryPKValues := []interface{}{nil, pk2SecVal}
	secondaryList, _ := buildSecondaryList(&tableInfo, pkList, secondaryPKValues, bindVars)
	want := []byte(" /* _stream Table (pk1 pk2 ) (1 'YWJj' ) (1 'eHl6' ); */")
	got := buildStreamComment(&tableInfo, pkList, secondaryList)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("case 1 failed, got %v, want %v", got, want)
	}
}

func TestGetLimit(t *testing.T) {
	bv := map[string]interface{}{
		"negative": -1,
		"int64":    int64(1),
		"int32":    int32(1),
		"int":      int(1),
		"uint":     uint(1),
	}
	if result := getLimit(int64(1), bv); result != 1 {
		t.Errorf("got %d, want 1", result)
	}
	if result := getLimit(nil, bv); result != -1 {
		t.Errorf("got %d, want -1", result)
	}
	func() {
		defer func() {
			x := recover().(error).Error()
			want := "error: negative limit -1"
			if x != want {
				t.Errorf("got %s, want %s", x, want)
			}
		}()
		getLimit(":negative", bv)
	}()
	if result := getLimit(":int64", bv); result != 1 {
		t.Errorf("got %d, want 1", result)
	}
	if result := getLimit(":int32", bv); result != 1 {
		t.Errorf("got %d, want 1", result)
	}
	if result := getLimit(":int", bv); result != 1 {
		t.Errorf("got %d, want 1", result)
	}
	func() {
		defer func() {
			x := recover().(error).Error()
			want := "error: want number type for :uint, got uint"
			if x != want {
				t.Errorf("got %s, want %s", x, want)
			}
		}()
		getLimit(":uint", bv)
	}()
}

func createTableInfo(name string, cols map[string]string, pKeys []string) TableInfo {
	table := schema.NewTable(name)
	for colName, colType := range cols {
		table.AddColumn(colName, colType, sqltypes.Value{}, "")
	}
	tableInfo := TableInfo{Table: table}
	tableInfo.SetPK(pKeys)
	return tableInfo
}
