// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/schema"
)

func TestCodexBuildValuesList(t *testing.T) {
	tableInfo := createTableInfo("Table",
		[]string{"pk1", "pk2", "col1"},
		[]querypb.Type{sqltypes.Int64, sqltypes.VarBinary, sqltypes.Int32},
		[]string{"pk1", "pk2"})

	// simple PK clause. e.g. where pk1 = 1
	bindVars := map[string]interface{}{}
	pk1Val, _ := sqltypes.BuildValue(1)
	pkValues := []interface{}{pk1Val}
	// want [[1]]
	want := [][]sqltypes.Value{[]sqltypes.Value{pk1Val}}
	got, _ := buildValueList(&tableInfo, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	// simple PK clause with bindVars. e.g. where pk1 = :pk1
	bindVars["pk1"] = 1
	pkValues = []interface{}{":pk1"}
	// want [[1]]
	want = [][]sqltypes.Value{[]sqltypes.Value{pk1Val}}
	got, _ = buildValueList(&tableInfo, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	// null value
	bindVars["pk1"] = nil
	pkValues = []interface{}{":pk1"}
	// want [[1]]
	want = [][]sqltypes.Value{[]sqltypes.Value{sqltypes.Value{}}}
	got, _ = buildValueList(&tableInfo, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	// invalid value
	bindVars["pk1"] = struct{}{}
	pkValues = []interface{}{":pk1"}
	wantErr := "error: unexpected type struct {}: {}"

	got, err := buildValueList(&tableInfo, pkValues, bindVars)

	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("got %v, want %v", err, wantErr)
	}

	// type mismatch int
	bindVars["pk1"] = "str"
	pkValues = []interface{}{":pk1"}
	wantErr = "error: type mismatch, expecting numeric type for str"

	got, err = buildValueList(&tableInfo, pkValues, bindVars)
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("got %v, want %v", err, wantErr)
	}

	// type mismatch binary
	bindVars["pk1"] = 1
	bindVars["pk2"] = 1
	pkValues = []interface{}{":pk1", ":pk2"}
	wantErr = "error: type mismatch, expecting string type for 1"

	got, err = buildValueList(&tableInfo, pkValues, bindVars)
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("got %v, want %v", err, wantErr)
	}

	// composite PK clause. e.g. where pk1 = 1 and pk2 = "abc"
	pk2Val, _ := sqltypes.BuildValue("abc")
	pkValues = []interface{}{pk1Val, pk2Val}
	// want [[1 abc]]
	want = [][]sqltypes.Value{[]sqltypes.Value{pk1Val, pk2Val}}
	got, _ = buildValueList(&tableInfo, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
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
		t.Fatalf("got %v, want %v", got, want)
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
		t.Fatalf("got %v, want %v", got, want)
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
		t.Fatalf("got %v, want %v", got, want)
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
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("got %v, want %v", err, wantErr)
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
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("got %v, want %v", err, wantErr)
	}
}

func TestCodexResolvePKValues(t *testing.T) {
	testUtils := newTestUtils()
	tableInfo := createTableInfo("Table",
		[]string{"pk1", "pk2", "col1"},
		[]querypb.Type{sqltypes.Int64, sqltypes.VarBinary, sqltypes.Int32},
		[]string{"pk1", "pk2"})
	key := "var"
	bindVariables := make(map[string]interface{})
	bindVariables[key] = "1"

	pkValues := make([]interface{}, 0, 10)
	pkValues = append(pkValues, []interface{}{":" + key})
	// resolvePKValues fail because type mismatch. pk column 0 has int type but
	// list variables are strings.
	_, _, err := resolvePKValues(&tableInfo, pkValues, bindVariables)
	testUtils.checkTabletError(t, err, ErrFail, "type mismatch")
	// pkValues is a list of sqltypes.Value and bypasses bind variables.
	// But, the type mismatches, pk column 0 is int but variable is string.
	pkValues = make([]interface{}, 0, 10)
	pkValues = append(pkValues, sqltypes.MakeString([]byte("type_mismatch")))
	_, _, err = resolvePKValues(&tableInfo, pkValues, nil)
	testUtils.checkTabletError(t, err, ErrFail, "type mismatch")
	// pkValues with different length
	bindVariables = make(map[string]interface{})
	bindVariables[key] = 1
	key2 := "var2"
	key3 := "var3"
	bindVariables[key2] = "2"
	bindVariables[key3] = "3"
	pkValues = make([]interface{}, 0, 10)
	pkValues = append(pkValues, []interface{}{":" + key})
	pkValues = append(pkValues, []interface{}{":" + key2, ":" + key3})
	_, _, err = resolvePKValues(&tableInfo, pkValues, bindVariables)
	testUtils.checkTabletError(t, err, ErrFail, "mismatched lengths")
}

func TestCodexResolveListArg(t *testing.T) {
	testUtils := newTestUtils()
	tableInfo := createTableInfo("Table",
		[]string{"pk1", "pk2", "col1"},
		[]querypb.Type{sqltypes.Int64, sqltypes.VarBinary, sqltypes.Int32},
		[]string{"pk1", "pk2"})

	key := "var"
	bindVariables := make(map[string]interface{})
	bindVariables[key] = []interface{}{fmt.Errorf("error is not supported")}

	_, err := resolveListArg(tableInfo.GetPKColumn(0), "::"+key, bindVariables)
	testUtils.checkTabletError(t, err, ErrFail, "")

	bindVariables[key] = []interface{}{"1"}
	_, err = resolveListArg(tableInfo.GetPKColumn(0), "::"+key, bindVariables)
	testUtils.checkTabletError(t, err, ErrFail, "type mismatch")

	bindVariables[key] = []interface{}{10}
	result, err := resolveListArg(tableInfo.GetPKColumn(0), "::"+key, bindVariables)
	if err != nil {
		t.Fatalf("should not get an error, but got error: %v", err)
	}
	testUtils.checkEqual(t, []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Int64, []byte("10"))}, result)
}

func TestCodexBuildSecondaryList(t *testing.T) {
	pk1 := "pk1"
	pk2 := "pk2"
	tableInfo := createTableInfo("Table",
		[]string{"pk1", "pk2", "col1"},
		[]querypb.Type{sqltypes.Int64, sqltypes.VarBinary, sqltypes.Int32},
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
		t.Fatalf("case 1 failed, got %v, want %v", got, want)
	}

	secondaryPKValues = []interface{}{"invalid_type", 1}
	_, err := buildSecondaryList(&tableInfo, pkList, secondaryPKValues, bindVars)
	if err == nil {
		t.Fatalf("should get an error, column 0 is int type, but secondary list provides a string")
	}
}

func TestCodexBuildStreamComment(t *testing.T) {
	pk1 := "pk1"
	pk2 := "pk2"
	tableInfo := createTableInfo("Table",
		[]string{"pk1", "pk2", "col1"},
		[]querypb.Type{sqltypes.Int64, sqltypes.VarBinary, sqltypes.Int32},
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
		t.Fatalf("case 1 failed, got %v, want %v", got, want)
	}
}

func TestCodexResolveValueWithIncompatibleValueType(t *testing.T) {
	testUtils := newTestUtils()
	tableInfo := createTableInfo("Table",
		[]string{"pk1", "pk2", "col1"},
		[]querypb.Type{sqltypes.Int64, sqltypes.VarBinary, sqltypes.Int32},
		[]string{"pk1", "pk2"})
	_, err := resolveValue(tableInfo.GetPKColumn(0), 0, nil)
	testUtils.checkTabletError(t, err, ErrFail, "incompatible value type ")
}

func TestCodexValidateRow(t *testing.T) {
	testUtils := newTestUtils()
	tableInfo := createTableInfo("Table",
		[]string{"pk1", "pk2", "col1"},
		[]querypb.Type{sqltypes.Int64, sqltypes.VarBinary, sqltypes.Int32},
		[]string{"pk1", "pk2"})
	// #columns and #rows do not match
	err := validateRow(&tableInfo, []int{1}, []sqltypes.Value{})
	testUtils.checkTabletError(t, err, ErrFail, "data inconsistency")
	// column 0 is int type but row is in string type
	err = validateRow(&tableInfo, []int{0}, []sqltypes.Value{sqltypes.MakeString([]byte("str"))})
	testUtils.checkTabletError(t, err, ErrFail, "type mismatch")
}

func TestCodexGetLimit(t *testing.T) {
	bv := map[string]interface{}{
		"negative": -1,
		"int64":    int64(1),
		"int32":    int32(1),
		"int":      int(1),
		"uint":     uint(1),
	}
	testUtils := newTestUtils()
	_, err := getLimit(":unknown", bv)
	if err == nil {
		t.Fatal("got nil, want error: missing bind var")
	}
	testUtils.checkTabletError(t, err, ErrFail, "missing bind var")
	result, err := getLimit(int64(1), bv)
	if err != nil {
		t.Fatalf("getLimit(1, bv) = %v, want nil", err)
	}
	if result != 1 {
		t.Fatalf("got %d, want 1", result)
	}
	result, err = getLimit(nil, bv)
	if err != nil {
		t.Fatalf("getLimit(nil, bv) = %v, want nil", err)
	}
	if result != -1 {
		t.Fatalf("got %d, want -1", result)
	}

	result, err = getLimit(":negative", bv)
	if err == nil {
		t.Fatalf("getLimit(':negative', bv) should return an error")
	}
	want := "error: negative limit -1"
	if err.Error() != want {
		t.Fatalf("got %s, want %s", err.Error(), want)
	}
	if result, _ := getLimit(":int64", bv); result != 1 {
		t.Fatalf("got %d, want 1", result)
	}
	if result, _ := getLimit(":int32", bv); result != 1 {
		t.Fatalf("got %d, want 1", result)
	}
	if result, _ := getLimit(":int", bv); result != 1 {
		t.Fatalf("got %d, want 1", result)
	}

	_, err = getLimit(":uint", bv)
	if err == nil {
		t.Fatalf("getLimit(':uint', bv) should return an error")
	}
	want = "error: want number type for :uint, got uint"
	if err.Error() != want {
		t.Fatalf("got %s, want %s", err.Error(), want)
	}
}

func TestCodexBuildKey(t *testing.T) {
	testUtils := newTestUtils()
	newKey := buildKey([]sqltypes.Value{
		sqltypes.MakeTrusted(sqltypes.Int64, []byte("1")),
		sqltypes.MakeTrusted(sqltypes.Int64, []byte("2")),
	})
	testUtils.checkEqual(t, "1.2", newKey)

	newKey = buildKey([]sqltypes.Value{
		sqltypes.MakeString([]byte("a")),
		sqltypes.NULL,
	})
	testUtils.checkEqual(t, "", newKey)
}

func TestCodexApplyFilterWithPKDefaults(t *testing.T) {
	testUtils := newTestUtils()
	tableInfo := createTableInfo("Table",
		[]string{"pk1", "pk2", "col1"},
		[]querypb.Type{sqltypes.Int64, sqltypes.VarBinary, sqltypes.Int32},
		[]string{"pk1", "pk2"})
	output := applyFilterWithPKDefaults(&tableInfo, []int{-1}, []sqltypes.Value{})
	if len(output) != 1 {
		t.Fatalf("expect to only one output but got: %v", output)
	}
	val, err := output[0].ParseInt64()
	if err != nil {
		t.Fatalf("should not get an error, but got err: %v", err)
	}
	testUtils.checkEqual(t, int64(0), val)
}

func TestCodexUnicoded(t *testing.T) {
	testUtils := newTestUtils()
	in := "test"
	out := unicoded(in)
	testUtils.checkEqual(t, in, out)
	in = "tes\xFFFDt"
	out = unicoded(in)
	testUtils.checkEqual(t, "tes", out)
}

func createTableInfo(
	name string, colNames []string, colTypes []querypb.Type, pKeys []string) TableInfo {
	table := schema.NewTable(name)
	for i, colName := range colNames {
		colType := colTypes[i]
		defaultVal := sqltypes.Value{}
		if sqltypes.IsIntegral(colType) {
			defaultVal = sqltypes.MakeTrusted(sqltypes.Int64, []byte("0"))
		} else if colType == sqltypes.VarBinary {
			defaultVal = sqltypes.MakeString([]byte(""))
		}
		table.AddColumn(colName, colType, defaultVal, "")
	}
	tableInfo := TableInfo{Table: table}
	tableInfo.SetPK(pKeys)
	return tableInfo
}
