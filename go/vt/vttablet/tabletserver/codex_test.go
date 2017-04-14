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
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"
	"github.com/youtube/vitess/go/vt/vterrors"
)

func TestCodexBuildValuesList(t *testing.T) {
	table := createTable("Table",
		[]string{"pk1", "pk2", "col1"},
		[]querypb.Type{sqltypes.Int64, sqltypes.VarBinary, sqltypes.Int32},
		[]string{"pk1", "pk2"})

	// simple PK clause. e.g. where pk1 = 1
	bindVars := map[string]interface{}{}
	pk1Val, _ := sqltypes.BuildValue(1)
	pkValues := []interface{}{pk1Val}
	// want [[1]]
	want := [][]sqltypes.Value{{pk1Val}}
	got, _ := buildValueList(table, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	// simple PK clause with bindVars. e.g. where pk1 = :pk1
	bindVars["pk1"] = 1
	pkValues = []interface{}{":pk1"}
	// want [[1]]
	want = [][]sqltypes.Value{{pk1Val}}
	got, _ = buildValueList(table, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	// null value
	bindVars["pk1"] = nil
	pkValues = []interface{}{":pk1"}
	// want [[1]]
	want = [][]sqltypes.Value{{{}}}
	got, _ = buildValueList(table, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	// invalid value
	bindVars["pk1"] = struct{}{}
	pkValues = []interface{}{":pk1"}
	wantErr := "unexpected type struct {}: {}"

	got, err := buildValueList(table, pkValues, bindVars)

	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("got %v, want %v", err, wantErr)
	}

	// type mismatch int
	bindVars["pk1"] = "str"
	pkValues = []interface{}{":pk1"}
	wantErr = "strconv.ParseInt"

	got, err = buildValueList(table, pkValues, bindVars)
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("got %v, want %v", err, wantErr)
	}

	// type mismatch binary
	bindVars["pk1"] = 1
	bindVars["pk2"] = 1
	pkValues = []interface{}{":pk1", ":pk2"}
	wantErr = "type mismatch, expecting string type for 1"

	got, err = buildValueList(table, pkValues, bindVars)
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("got %v, want %v", err, wantErr)
	}

	// composite PK clause. e.g. where pk1 = 1 and pk2 = "abc"
	pk2Val, _ := sqltypes.BuildValue("abc")
	pkValues = []interface{}{pk1Val, pk2Val}
	// want [[1 abc]]
	want = [][]sqltypes.Value{{pk1Val, pk2Val}}
	got, _ = buildValueList(table, pkValues, bindVars)
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
		{pk1Val, pk2Val},
		{pk1Val2, pk2Val2}}
	got, _ = buildValueList(table, pkValues, bindVars)
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
		{pk1Val, pk2Val},
		{pk1Val, pk2Val2},
	}

	got, _ = buildValueList(table, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	// list arg two values
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
		{pk1Val, pk2Val},
		{pk1Val, pk2Val2},
	}
	got, _ = buildValueList(table, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	// list arg two values, using *querypb.BindVariable of type TUPLE
	// e.g. where pk1 = 1 and pk2 IN ::list
	bindVars = map[string]interface{}{
		"list": &querypb.BindVariable{
			Type: querypb.Type_TUPLE,
			Values: []*querypb.Value{
				{
					Type:  querypb.Type_VARBINARY,
					Value: []byte("abc"),
				},
				{
					Type:  querypb.Type_VARBINARY,
					Value: []byte("xyz"),
				},
			},
		},
	}
	pkValues = []interface{}{
		pk1Val,
		"::list",
	}
	// want [[1 abc][1 xyz]]
	want = [][]sqltypes.Value{
		{pk1Val, pk2Val},
		{pk1Val, pk2Val2},
	}
	got, err = buildValueList(table, pkValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v / %v, want %v", got, err, want)
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
		{pk1Val, pk2Val},
	}
	got, _ = buildValueList(table, pkValues, bindVars)
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
	wantErr = "empty list supplied for list"
	got, err = buildValueList(table, pkValues, bindVars)
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
	wantErr = "unexpected arg type []interface {} for key list"
	got, err = buildValueList(table, pkValues, bindVars)
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("got %v, want %v", err, wantErr)
	}
}

func TestCodexResolvePKValues(t *testing.T) {
	table := createTable("Table",
		[]string{"pk1", "pk2", "col1"},
		[]querypb.Type{sqltypes.Int64, sqltypes.VarBinary, sqltypes.Int32},
		[]string{"pk1", "pk2"})
	key := "var"
	bindVariables := make(map[string]interface{})
	bindVariables[key] = "1"

	pkValues := make([]interface{}, 0, 10)
	pkValues = append(pkValues, []interface{}{":" + key})
	// resolvePKValues should succeed for strings that can be converted to int.
	v, _, err := resolvePKValues(table, pkValues, bindVariables)
	if err != nil {
		t.Error(err)
	}
	wantV := []interface{}{[]sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Int64, []byte("1"))}}
	if !reflect.DeepEqual(v, wantV) {
		t.Errorf("resolvePKValues: %#v, want %#v", v, wantV)
	}
	// resolvePKValues should fail because of conversion error.
	pkValues = make([]interface{}, 0, 10)
	pkValues = append(pkValues, sqltypes.MakeString([]byte("type_mismatch")))
	_, _, err = resolvePKValues(table, pkValues, nil)
	if code := vterrors.Code(err); code != vtrpcpb.Code_INVALID_ARGUMENT {
		t.Errorf("resolvePKValues: %v, want %v", code, vtrpcpb.Code_INVALID_ARGUMENT)
	}
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
	_, _, err = resolvePKValues(table, pkValues, bindVariables)
	if code := vterrors.Code(err); code != vtrpcpb.Code_INVALID_ARGUMENT {
		t.Errorf("resolvePKValues: %v, want %v", code, vtrpcpb.Code_INVALID_ARGUMENT)
	}
}

func TestCodexResolveListArg(t *testing.T) {
	testUtils := newTestUtils()
	table := createTable("Table",
		[]string{"pk1", "pk2", "col1"},
		[]querypb.Type{sqltypes.Int64, sqltypes.VarBinary, sqltypes.Int32},
		[]string{"pk1", "pk2"})

	key := "var"
	bindVariables := make(map[string]interface{})
	bindVariables[key] = []interface{}{fmt.Errorf("error is not supported")}

	_, err := resolveListArg(table.GetPKColumn(0), "::"+key, bindVariables)
	if code := vterrors.Code(err); code != vtrpcpb.Code_INVALID_ARGUMENT {
		t.Errorf("resolvePKValues: %v, want %v", code, vtrpcpb.Code_INVALID_ARGUMENT)
	}

	// This should successfully convert.
	bindVariables[key] = []interface{}{"1"}
	v, err := resolveListArg(table.GetPKColumn(0), "::"+key, bindVariables)
	if err != nil {
		t.Error(err)
	}
	wantV := []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Int64, []byte("1"))}
	if !reflect.DeepEqual(v, wantV) {
		t.Errorf("resolvePKValues: %#v, want %#v", v, wantV)
	}

	bindVariables[key] = []interface{}{10}
	result, err := resolveListArg(table.GetPKColumn(0), "::"+key, bindVariables)
	if err != nil {
		t.Fatalf("should not get an error, but got error: %v", err)
	}
	testUtils.checkEqual(t, []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.Int64, []byte("10"))}, result)
}

func TestResolveNumber(t *testing.T) {
	testcases := []struct {
		v      interface{}
		bv     map[string]interface{}
		out    int64
		outErr string
	}{{
		v: ":a",
		bv: map[string]interface{}{
			"a": 10,
		},
		out: int64(10),
	}, {
		v: "::a",
		bv: map[string]interface{}{
			"a": []interface{}{10},
		},
		outErr: "unexpected type []interface {}: [10]",
	}, {
		v:      ":a",
		outErr: "missing bind var a",
	}, {
		v:      make(chan int),
		outErr: "unexpected type chan int",
	}, {
		v:   int64(1),
		out: int64(1),
	}, {
		v:      1.2,
		outErr: "strconv.ParseInt",
	}}
	for _, tc := range testcases {
		got, err := resolveNumber(tc.v, tc.bv)
		if err != nil {
			if !strings.Contains(err.Error(), tc.outErr) {
				t.Errorf("resolveNumber(%#v, %v): %v, must contain %s", tc.v, tc.bv, err, tc.outErr)
			}
			continue
		}
		if got != tc.out {
			t.Errorf("resolveNumber(%#v, %v): %d, want %d", tc.v, tc.bv, got, tc.out)
		}
	}
}

func TestCodexBuildSecondaryList(t *testing.T) {
	pk1 := "pk1"
	pk2 := "pk2"
	table := createTable("Table",
		[]string{"pk1", "pk2", "col1"},
		[]querypb.Type{sqltypes.Int64, sqltypes.VarBinary, sqltypes.Int32},
		[]string{pk1, pk2})

	// set pk2 = 'xyz' where pk1=1 and pk2 = 'abc'
	bindVars := map[string]interface{}{}
	pk1Val, _ := sqltypes.BuildValue(1)
	pk2Val, _ := sqltypes.BuildValue("abc")
	pkValues := []interface{}{pk1Val, pk2Val}
	pkList, _ := buildValueList(table, pkValues, bindVars)
	pk2SecVal, _ := sqltypes.BuildValue("xyz")
	secondaryPKValues := []interface{}{nil, pk2SecVal}
	// want [[1 xyz]]
	want := [][]sqltypes.Value{
		{pk1Val, pk2SecVal}}
	got, _ := buildSecondaryList(table, pkList, secondaryPKValues, bindVars)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("case 1 failed, got %v, want %v", got, want)
	}

	secondaryPKValues = []interface{}{"invalid_type", 1}
	_, err := buildSecondaryList(table, pkList, secondaryPKValues, bindVars)
	if err == nil {
		t.Fatalf("should get an error, column 0 is int type, but secondary list provides a string")
	}
}

func TestCodexBuildStreamComment(t *testing.T) {
	pk1 := "pk1"
	pk2 := "pk2"
	table := createTable("Table",
		[]string{"pk1", "pk2", "col1"},
		[]querypb.Type{sqltypes.Int64, sqltypes.VarBinary, sqltypes.Int32},
		[]string{pk1, pk2})

	// set pk2 = 'xyz' where pk1=1 and pk2 = 'abc'
	bindVars := map[string]interface{}{}
	pk1Val, _ := sqltypes.BuildValue(1)
	pk2Val, _ := sqltypes.BuildValue("abc")
	pkValues := []interface{}{pk1Val, pk2Val}
	pkList, _ := buildValueList(table, pkValues, bindVars)
	pk2SecVal, _ := sqltypes.BuildValue("xyz")
	secondaryPKValues := []interface{}{nil, pk2SecVal}
	secondaryList, _ := buildSecondaryList(table, pkList, secondaryPKValues, bindVars)
	want := []byte(" /* _stream `Table` (pk1 pk2 ) (1 'YWJj' ) (1 'eHl6' ); */")
	got := buildStreamComment(table, pkList, secondaryList)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("case 1 failed, got\n%s, want\n%s", got, want)
	}
}

func TestCodexValidateRow(t *testing.T) {
	table := createTable("Table",
		[]string{"pk1", "pk2", "col1"},
		[]querypb.Type{sqltypes.Int64, sqltypes.VarBinary, sqltypes.Int32},
		[]string{"pk1", "pk2"})
	// #columns and #rows do not match
	err := validateRow(table, []int{1}, []sqltypes.Value{})
	if code := vterrors.Code(err); code != vtrpcpb.Code_INVALID_ARGUMENT {
		t.Errorf("validateRow: %v, want %v", code, vtrpcpb.Code_INVALID_ARGUMENT)
	}
	// column 0 is int type but row is in string type
	err = validateRow(table, []int{0}, []sqltypes.Value{sqltypes.MakeString([]byte("str"))})
	if code := vterrors.Code(err); code != vtrpcpb.Code_INVALID_ARGUMENT {
		t.Errorf("validateRow: %v, want %v", code, vtrpcpb.Code_INVALID_ARGUMENT)
	}
}

func TestCodexApplyFilterWithPKDefaults(t *testing.T) {
	testUtils := newTestUtils()
	table := createTable("Table",
		[]string{"pk1", "pk2", "col1"},
		[]querypb.Type{sqltypes.Int64, sqltypes.VarBinary, sqltypes.Int32},
		[]string{"pk1", "pk2"})
	output := applyFilterWithPKDefaults(table, []int{-1}, []sqltypes.Value{})
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

func createTable(name string, colNames []string, colTypes []querypb.Type, pKeys []string) *schema.Table {
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
	setPK(table, pKeys)
	return table
}

func setPK(ta *schema.Table, colnames []string) error {
	if len(ta.Indexes) != 0 {
		panic("setPK must be called before adding other indexes")
	}
	pkIndex := ta.AddIndex("PRIMARY")
	for _, colname := range colnames {
		pkIndex.AddColumn(colname, 1)
	}
	ta.Done()
	return nil
}
