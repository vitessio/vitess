/*
Copyright 2019 The Vitess Authors.

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

package tabletserver

import (
	"reflect"
	"testing"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

func TestCodexBuildValuesList(t *testing.T) {
	table := createTable("Table",
		[]string{"pk1", "pk2", "col1"},
		[]querypb.Type{sqltypes.Int64, sqltypes.VarBinary, sqltypes.Int32},
		[]string{"pk1", "pk2"})
	bindVars := map[string]*querypb.BindVariable{
		"key": sqltypes.Int64BindVariable(10),
	}
	tcases := []struct {
		pkValues []sqltypes.PlanValue
		out      [][]sqltypes.Value
		err      string
	}{{
		pkValues: []sqltypes.PlanValue{{
			Key: "key",
		}, {
			Value: sqltypes.NewVarBinary("aa"),
		}},
		out: [][]sqltypes.Value{
			{sqltypes.NewInt64(10), sqltypes.NewVarBinary("aa")},
		},
	}, {
		pkValues: []sqltypes.PlanValue{{
			Key: "nokey",
		}},
		err: "missing bind var nokey",
	}, {
		pkValues: []sqltypes.PlanValue{{
			Value: sqltypes.NewVarChar("aa"),
		}},
		err: `strconv.ParseInt: parsing "aa": invalid syntax`,
	}}
	for _, tc := range tcases {
		got, err := buildValueList(table, tc.pkValues, bindVars)
		if tc.err != "" {
			if err == nil || err.Error() != tc.err {
				t.Errorf("buildValueList(%v) error: %v, want %s", tc.pkValues, err, tc.err)
			}
			continue
		}
		if err != nil {
			t.Errorf("buildValueList(%v) error: %v", tc.pkValues, err)
		}
		if !reflect.DeepEqual(got, tc.out) {
			t.Errorf("buildValueList(%v): %v, want %s", tc.pkValues, got, tc.out)
		}
	}
}

func TestBuildSecondaryList(t *testing.T) {
	table := createTable("Table",
		[]string{"pk1", "pk2", "col1"},
		[]querypb.Type{sqltypes.Int64, sqltypes.VarBinary, sqltypes.Int32},
		[]string{"pk1", "pk2"})
	bindVars := map[string]*querypb.BindVariable{
		"key": sqltypes.Int64BindVariable(10),
	}
	r := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"pk1|pk2",
			"int64|varchar",
		),
		"1|aa",
		"2|bb",
	)
	pkList := r.Rows
	tcases := []struct {
		secondaryList []sqltypes.PlanValue
		out           [][]sqltypes.Value
		err           string
	}{{
		secondaryList: nil,
		out:           nil,
	}, {
		secondaryList: []sqltypes.PlanValue{{}, {
			Value: sqltypes.NewVarBinary("cc"),
		}},
		out: [][]sqltypes.Value{
			{sqltypes.NewInt64(1), sqltypes.NewVarBinary("cc")},
			{sqltypes.NewInt64(2), sqltypes.NewVarBinary("cc")},
		},
	}, {
		secondaryList: []sqltypes.PlanValue{{
			Key: "nokey",
		}},
		err: "missing bind var nokey",
	}}
	for _, tc := range tcases {
		got, err := buildSecondaryList(table, pkList, tc.secondaryList, bindVars)
		if tc.err != "" {
			if err == nil || err.Error() != tc.err {
				t.Errorf("buildSecondaryList(%v) error: %v, want %s", tc.secondaryList, err, tc.err)
			}
			continue
		}
		if err != nil {
			t.Errorf("buildSecondaryList(%v) error: %v", tc.secondaryList, err)
		}
		if !reflect.DeepEqual(got, tc.out) {
			t.Errorf("buildSecondaryList(%v): %v, want %s", tc.secondaryList, got, tc.out)
		}
	}
}

func TestResolveNumber(t *testing.T) {
	bindVars := map[string]*querypb.BindVariable{
		"key": sqltypes.Int64BindVariable(10),
	}
	tcases := []struct {
		pv  sqltypes.PlanValue
		out int64
		err string
	}{{
		pv:  sqltypes.PlanValue{Key: "key"},
		out: 10,
	}, {
		pv:  sqltypes.PlanValue{Key: "nokey"},
		err: "missing bind var nokey",
	}, {
		pv:  sqltypes.PlanValue{Value: sqltypes.NewVarChar("aa")},
		err: "could not parse value: 'aa'",
	}}
	for _, tc := range tcases {
		got, err := resolveNumber(tc.pv, bindVars)
		if tc.err != "" {
			if err == nil || err.Error() != tc.err {
				t.Errorf("resolveNumber(%v) error: %v, want %s", tc.pv, err, tc.err)
			}
			continue
		}
		if err != nil {
			t.Errorf("resolveNumber(%v) error: %v", tc.pv, err)
		}
		if got != tc.out {
			t.Errorf("resolveNumber(%v): %d, want %d", tc.pv, got, tc.out)
		}
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
	bindVars := map[string]*querypb.BindVariable{}
	pk1Val := sqltypes.NewInt64(1)
	pk2Val := sqltypes.NewVarChar("abc")
	pkValues := []sqltypes.PlanValue{{Value: pk1Val}, {Value: pk2Val}}
	pkList, _ := buildValueList(table, pkValues, bindVars)
	pk2SecVal := sqltypes.NewVarChar("xyz")
	secondaryPKValues := []sqltypes.PlanValue{{}, {Value: pk2SecVal}}
	secondaryList, _ := buildSecondaryList(table, pkList, secondaryPKValues, bindVars)
	want := " /* _stream `Table` (pk1 pk2 ) (1 'YWJj' ) (1 'eHl6' ); */"
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
	err = validateRow(table, []int{0}, []sqltypes.Value{sqltypes.NewVarBinary("str")})
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
	val, err := sqltypes.ToInt64(output[0])
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
			defaultVal = sqltypes.NewInt64(0)
		} else if colType == sqltypes.VarBinary {
			defaultVal = sqltypes.NewVarBinary("")
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
	pkIndex := ta.AddIndex("PRIMARY", true)
	for _, colname := range colnames {
		pkIndex.AddColumn(colname, 1)
	}
	ta.Done()
	return nil
}
