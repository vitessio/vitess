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

package sqltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/test/utils"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestRepair(t *testing.T) {
	fields := []*querypb.Field{{
		Type: Int64,
	}, {
		Type: VarChar,
	}}
	in := &Result{
		Rows: [][]Value{
			{TestValue(VarBinary, "1"), TestValue(VarBinary, "aa")},
			{TestValue(VarBinary, "2"), TestValue(VarBinary, "bb")},
		},
	}
	want := &Result{
		Rows: [][]Value{
			{TestValue(Int64, "1"), TestValue(VarChar, "aa")},
			{TestValue(Int64, "2"), TestValue(VarChar, "bb")},
		},
	}
	in.Repair(fields)
	if !in.Equal(want) {
		t.Errorf("Repair:\n%#v, want\n%#v", in, want)
	}
}

func TestCopy(t *testing.T) {
	in := &Result{
		Fields: []*querypb.Field{{
			Type: Int64,
		}, {
			Type: VarChar,
		}},
		InsertID:     1,
		RowsAffected: 2,
		Rows: [][]Value{
			{TestValue(Int64, "1"), MakeTrusted(Null, nil)},
			{TestValue(Int64, "2"), MakeTrusted(VarChar, nil)},
			{TestValue(Int64, "3"), TestValue(VarChar, "")},
		},
	}
	out := in.Copy()
	utils.MustMatch(t, in, out)
}

func TestTruncate(t *testing.T) {
	in := &Result{
		Fields: []*querypb.Field{{
			Type: Int64,
		}, {
			Type: VarChar,
		}},
		InsertID:     1,
		RowsAffected: 2,
		Rows: [][]Value{
			{TestValue(Int64, "1"), MakeTrusted(Null, nil)},
			{TestValue(Int64, "2"), MakeTrusted(VarChar, nil)},
			{TestValue(Int64, "3"), TestValue(VarChar, "")},
		},
	}

	out := in.Truncate(0)
	if !out.Equal(in) {
		t.Errorf("Truncate(0):\n%v, want\n%v", out, in)
	}

	out = in.Truncate(1)
	want := &Result{
		Fields: []*querypb.Field{{
			Type: Int64,
		}},
		InsertID:     1,
		RowsAffected: 2,
		Rows: [][]Value{
			{TestValue(Int64, "1")},
			{TestValue(Int64, "2")},
			{TestValue(Int64, "3")},
		},
	}
	if !out.Equal(want) {
		t.Errorf("Truncate(1):\n%v, want\n%v", out, want)
	}
}

func TestStripMetaData(t *testing.T) {
	testcases := []struct {
		name           string
		in             *Result
		expected       *Result
		includedFields querypb.ExecuteOptions_IncludedFields
	}{{
		name:     "no fields",
		in:       &Result{},
		expected: &Result{},
	}, {
		name: "empty fields",
		in: &Result{
			Fields: []*querypb.Field{},
		},
		expected: &Result{
			Fields: []*querypb.Field{},
		},
	}, {
		name:           "no name",
		includedFields: querypb.ExecuteOptions_TYPE_ONLY,
		in: &Result{
			Fields: []*querypb.Field{{
				Type: Int64,
			}, {
				Type: VarChar,
			}},
		},
		expected: &Result{
			Fields: []*querypb.Field{{
				Type: Int64,
			}, {
				Type: VarChar,
			}},
		},
	}, {
		name:           "names",
		includedFields: querypb.ExecuteOptions_TYPE_ONLY,
		in: &Result{
			Fields: []*querypb.Field{{
				Name: "field1",
				Type: Int64,
			}, {
				Name: "field2",
				Type: VarChar,
			}},
		},
		expected: &Result{
			Fields: []*querypb.Field{{
				Type: Int64,
			}, {
				Type: VarChar,
			}},
		},
	}, {
		name:           "all fields - strip to type",
		includedFields: querypb.ExecuteOptions_TYPE_ONLY,
		in: &Result{
			Fields: []*querypb.Field{{
				Name:         "field1",
				Table:        "table1",
				OrgTable:     "orgtable1",
				OrgName:      "orgname1",
				ColumnLength: 5,
				Charset:      63,
				Decimals:     0,
				Flags:        2,
				Type:         Int64,
			}, {
				Name:         "field2",
				Table:        "table2",
				OrgTable:     "orgtable2",
				OrgName:      "orgname2",
				ColumnLength: 5,
				Charset:      63,
				Decimals:     0,
				Flags:        2,
				Type:         VarChar,
			}},
		},
		expected: &Result{
			Fields: []*querypb.Field{{
				Type: Int64,
			}, {
				Type: VarChar,
			}},
		},
	}, {
		name:           "all fields - not stripped",
		includedFields: querypb.ExecuteOptions_ALL,
		in: &Result{
			Fields: []*querypb.Field{{
				Name:         "field1",
				Table:        "table1",
				OrgTable:     "orgtable1",
				OrgName:      "orgname1",
				ColumnLength: 5,
				Charset:      63,
				Decimals:     0,
				Flags:        2,
				Type:         Int64,
			}, {
				Name:         "field2",
				Table:        "table2",
				OrgTable:     "orgtable2",
				OrgName:      "orgname2",
				ColumnLength: 5,
				Charset:      63,
				Decimals:     0,
				Flags:        2,
				Type:         VarChar,
			}},
		},
		expected: &Result{
			Fields: []*querypb.Field{{
				Name:         "field1",
				Table:        "table1",
				OrgTable:     "orgtable1",
				OrgName:      "orgname1",
				ColumnLength: 5,
				Charset:      63,
				Decimals:     0,
				Flags:        2,
				Type:         Int64,
			}, {
				Name:         "field2",
				Table:        "table2",
				OrgTable:     "orgtable2",
				OrgName:      "orgname2",
				ColumnLength: 5,
				Charset:      63,
				Decimals:     0,
				Flags:        2,
				Type:         VarChar,
			}},
		},
	}, {
		name: "all fields - strip to type and name",
		in: &Result{
			Fields: []*querypb.Field{{
				Name:         "field1",
				Table:        "table1",
				OrgTable:     "orgtable1",
				OrgName:      "orgname1",
				ColumnLength: 5,
				Charset:      63,
				Decimals:     0,
				Flags:        2,
				Type:         Int64,
			}, {
				Name:         "field2",
				Table:        "table2",
				OrgTable:     "orgtable2",
				OrgName:      "orgname2",
				ColumnLength: 5,
				Charset:      63,
				Decimals:     0,
				Flags:        2,
				Type:         VarChar,
			}},
		},
		expected: &Result{
			Fields: []*querypb.Field{{
				Name: "field1",
				Type: Int64,
			}, {
				Name: "field2",
				Type: VarChar,
			}},
		},
	}}
	for _, tcase := range testcases {
		t.Run(tcase.name, func(t *testing.T) {
			inCopy := tcase.in.Copy()
			out := inCopy.StripMetadata(tcase.includedFields)
			if !out.Equal(tcase.expected) {
				t.Errorf("StripMetaData unexpected result for %v: %v", tcase.name, out)
			}
			if len(tcase.in.Fields) > 0 {
				// check the out array is different than the in array.
				if out.Fields[0] == inCopy.Fields[0] && tcase.includedFields != querypb.ExecuteOptions_ALL {
					t.Errorf("StripMetaData modified original Field for %v", tcase.name)
				}
			}
			// check we didn't change the original result.
			utils.MustMatch(t, tcase.in, inCopy)
		})
	}
}

func TestAppendResult(t *testing.T) {
	src := &Result{
		Fields: []*querypb.Field{{
			Type: Int64,
		}, {
			Type: VarChar,
		}},
		InsertID:     1,
		RowsAffected: 2,
		Rows: [][]Value{
			{TestValue(Int64, "2"), MakeTrusted(VarChar, nil)},
			{TestValue(Int64, "3"), TestValue(VarChar, "")},
		},
	}

	result := &Result{
		Fields: []*querypb.Field{{
			Type: Int64,
		}, {
			Type: VarChar,
		}},
		InsertID:     3,
		RowsAffected: 4,
		Rows: [][]Value{
			{TestValue(Int64, "1"), MakeTrusted(Null, nil)},
		},
	}

	want := &Result{
		Fields: []*querypb.Field{{
			Type: Int64,
		}, {
			Type: VarChar,
		}},
		InsertID:     1,
		RowsAffected: 6,
		Rows: [][]Value{
			{TestValue(Int64, "1"), MakeTrusted(Null, nil)},
			{TestValue(Int64, "2"), MakeTrusted(VarChar, nil)},
			{TestValue(Int64, "3"), TestValue(VarChar, "")},
		},
	}

	result.AppendResult(src)

	if !result.Equal(want) {
		t.Errorf("Got:\n%#v, want:\n%#v", result, want)
	}
}

func TestReplaceKeyspace(t *testing.T) {
	result := &Result{
		Fields: []*querypb.Field{{
			Type:     Int64,
			Database: "vttest",
		}, {
			Type:     VarChar,
			Database: "vttest",
		}, {
			Type: VarBinary,
		}},
	}

	result.ReplaceKeyspace("keyspace-name")
	assert.Equal(t, "keyspace-name", result.Fields[0].Database)
	assert.Equal(t, "keyspace-name", result.Fields[1].Database)
	// Expect empty database identifiers to remain empty
	assert.Equal(t, "", result.Fields[2].Database)
}

func TestShallowCopy(t *testing.T) {
	result := &Result{
		Fields: []*querypb.Field{{
			Type:     Int64,
			Database: "vttest",
		}, {
			Type:     VarChar,
			Database: "vttest",
		}},
		Rows: [][]Value{
			{
				MakeTrusted(querypb.Type_INT32, []byte("10")),
				MakeTrusted(querypb.Type_VARCHAR, []byte("name")),
			},
		},
	}

	res := result.ShallowCopy()
	assert.Equal(t, result, res)
}

func TestMetadata(t *testing.T) {
	result := &Result{
		Fields: []*querypb.Field{{
			Type:     Int64,
			Database: "vttest",
		}, {
			Type:     VarChar,
			Database: "vttest",
		}},
		Rows: [][]Value{
			{
				MakeTrusted(querypb.Type_INT32, []byte("10")),
				MakeTrusted(querypb.Type_VARCHAR, []byte("name")),
			},
		},
	}

	res := result.Metadata()
	assert.Nil(t, res.Rows)
	assert.Equal(t, result.Fields, res.Fields)
}

func TestResultsEqualUnordered(t *testing.T) {
	result1 := &Result{
		Fields: []*querypb.Field{{
			Type:     Int64,
			Database: "vttest",
		}, {
			Type:     VarChar,
			Database: "vttest",
		}},
		Rows: [][]Value{
			{
				MakeTrusted(querypb.Type_INT32, []byte("24")),
				MakeTrusted(querypb.Type_VARCHAR, []byte("test-name1")),
			},
		},
		RowsAffected: 2,
	}

	result2 := &Result{
		Fields: []*querypb.Field{{
			Type:     Int64,
			Database: "vttest",
		}, {
			Type:     VarChar,
			Database: "vttest",
		}},
		Rows: [][]Value{
			{
				MakeTrusted(querypb.Type_INT32, []byte("10")),
				MakeTrusted(querypb.Type_VARCHAR, []byte("test-name2")),
			},
		},
		RowsAffected: 2,
	}

	result3 := &Result{
		Fields: []*querypb.Field{{
			Type:     Int64,
			Database: "vttest",
		}, {
			Type:     VarChar,
			Database: "vttest",
		}},
		Rows: [][]Value{
			{
				MakeTrusted(querypb.Type_INT32, []byte("10")),
				MakeTrusted(querypb.Type_VARCHAR, []byte("test-name2")),
			},
			{
				MakeTrusted(querypb.Type_INT32, []byte("24")),
				MakeTrusted(querypb.Type_VARCHAR, []byte("test-name1")),
			},
		},
		RowsAffected: 3,
	}

	eq := ResultsEqualUnordered([]Result{*result1, *result2}, []Result{*result2, *result1})
	assert.True(t, eq)

	eq = ResultsEqualUnordered([]Result{*result1}, []Result{*result2, *result1})
	assert.False(t, eq)

	eq = ResultsEqualUnordered([]Result{*result1}, []Result{*result2})
	assert.False(t, eq)

	eq = ResultsEqualUnordered([]Result{*result1, *result3}, []Result{*result2, *result1})
	assert.False(t, eq)
}

func TestStatusFlags(t *testing.T) {
	result := &Result{
		Fields: []*querypb.Field{{
			Type:     Int64,
			Database: "vttest",
		}, {
			Type:     VarChar,
			Database: "vttest",
		}},
		StatusFlags: ServerMoreResultsExists,
	}

	assert.True(t, result.IsMoreResultsExists())
	assert.False(t, result.IsInTransaction())

	result.StatusFlags = ServerStatusInTrans

	assert.False(t, result.IsMoreResultsExists())
	assert.True(t, result.IsInTransaction())
}

func TestIncludeFieldsOrDefault(t *testing.T) {
	// Should return default if nil is passed
	r := IncludeFieldsOrDefault(nil)
	assert.Equal(t, querypb.ExecuteOptions_TYPE_AND_NAME, r)

	r = IncludeFieldsOrDefault(&querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_TYPE_ONLY})
	assert.Equal(t, querypb.ExecuteOptions_TYPE_ONLY, r)
}
