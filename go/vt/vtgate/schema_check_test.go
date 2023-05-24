/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    `http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtgate

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestSchemaCheckWithAddColVindexDDL(t *testing.T) {
	type testCase struct {
		vindexSpec    *sqlparser.VindexSpec
		sandboxRes    *sqltypes.Result
		expectedError string
	}

	vindexParams := []sqlparser.VindexParam{{
		Key: sqlparser.NewIdentifierCI("ksa"),
		Val: "10",
	}, {
		Key: sqlparser.NewIdentifierCI("ks2"),
		Val: "shard-0000",
	}}

	tests := []testCase{
		{
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("myhash"),
				Type:   sqlparser.NewIdentifierCI("hash"),
				Params: vindexParams,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|tinyint|NO|PRI|NULL|",
			),
			expectedError: "",
		},
		{
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("myhash"),
				Type:   sqlparser.NewIdentifierCI("hash"),
				Params: vindexParams,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|mediumint unsigned|NO|PRI|NULL|",
			),
			expectedError: "",
		},
		{
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("myhash"),
				Type:   sqlparser.NewIdentifierCI("hash"),
				Params: vindexParams,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|varchar|NO|PRI|NULL|",
			),
			expectedError: "column id should not be varchar for vindex type hash",
		},
		{
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("myhash"),
				Type:   sqlparser.NewIdentifierCI("hash"),
				Params: vindexParams,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|blob|NO|PRI|NULL|",
			),
			expectedError: "column id should not be blob for vindex type hash",
		},
		{
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("myhash"),
				Type:   sqlparser.NewIdentifierCI("hash"),
				Params: vindexParams,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|year|NO|PRI|NULL|",
			),
			expectedError: "column id should not be year for vindex type hash",
		},
		{
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("myregion_json"),
				Type:   sqlparser.NewIdentifierCI("region_json"),
				Params: vindexParams,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|tinyint|NO|PRI|NULL|",
			),
			expectedError: "",
		},
		{
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("myregion_json"),
				Type:   sqlparser.NewIdentifierCI("region_json"),
				Params: vindexParams,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|mediumint unsigned|NO|PRI|NULL|",
			),
			expectedError: "",
		},
		{
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("myregion_json"),
				Type:   sqlparser.NewIdentifierCI("region_json"),
				Params: vindexParams,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|varchar|NO|PRI|NULL|",
			),
			expectedError: "",
		},
		{
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("myregion_json"),
				Type:   sqlparser.NewIdentifierCI("region_json"),
				Params: vindexParams,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|blob|NO|PRI|NULL|",
			),
			expectedError: "column id should not be blob for vindex type region_json",
		},
		{
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("myregion_json"),
				Type:   sqlparser.NewIdentifierCI("region_json"),
				Params: vindexParams,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|year|NO|PRI|NULL|",
			),
			expectedError: "column id should not be year for vindex type region_json",
		},
		{
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("myunicode_loose_md5"),
				Type:   sqlparser.NewIdentifierCI("unicode_loose_md5"),
				Params: vindexParams,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|tinyint|NO|PRI|NULL|",
			),
			expectedError: "column id should not be tinyint for vindex type unicode_loose_md5",
		},
		{
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("myunicode_loose_md5"),
				Type:   sqlparser.NewIdentifierCI("unicode_loose_md5"),
				Params: vindexParams,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|mediumint unsigned|NO|PRI|NULL|",
			),
			expectedError: "column id should not be mediumint unsigned for vindex type unicode_loose_md5",
		},
		{
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("myunicode_loose_md5"),
				Type:   sqlparser.NewIdentifierCI("unicode_loose_md5"),
				Params: vindexParams,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|varchar|NO|PRI|NULL|",
			),
			expectedError: "",
		},
		{
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("myunicode_loose_md5"),
				Type:   sqlparser.NewIdentifierCI("unicode_loose_md5"),
				Params: vindexParams,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|blob|NO|PRI|NULL|",
			),
			expectedError: "",
		},
		{
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("myunicode_loose_md5"),
				Type:   sqlparser.NewIdentifierCI("unicode_loose_md5"),
				Params: vindexParams,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|year|NO|PRI|NULL|",
			),
			expectedError: "column id should not be year for vindex type unicode_loose_md5",
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			r, sb, _, _ := createExecutorEnv()
			sb.SetResults([]*sqltypes.Result{tc.sandboxRes})

			vc, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)
			var ksName = "TestExecutor"

			alterVschema := &sqlparser.AlterVschema{
				Action: sqlparser.AddColVindexDDLAction,
				Table: sqlparser.TableName{
					Qualifier: sqlparser.NewIdentifierCS("TestExecutor"),
					Name:      sqlparser.NewIdentifierCS("t2"),
				},
				VindexSpec: tc.vindexSpec,
				AutoIncSpec: &sqlparser.AutoIncSpec{
					Column: sqlparser.NewIdentifierCI("id"),
					Sequence: sqlparser.TableName{
						Qualifier: sqlparser.NewIdentifierCS("TestExecutor"),
						Name:      sqlparser.NewIdentifierCS("t2"),
					},
				},
				VindexCols: []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("id")},
			}

			err := SchemaCheck(vc, ctx, ksName, alterVschema)
			if tc.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedError)
			}
		})
	}
}

func TestSchemaCheckWithAddAutoIncDDL(t *testing.T) {
	type testCase struct {
		vschema       *sqlparser.AlterVschema
		sandboxRes    *sqltypes.Result
		expectedError string
	}
	vindexParams := []sqlparser.VindexParam{{
		Key: sqlparser.NewIdentifierCI("ksa"),
		Val: "10",
	}, {
		Key: sqlparser.NewIdentifierCI("ks2"),
		Val: "shard-0000",
	}}

	alterVschema1 := &sqlparser.AlterVschema{
		Action: sqlparser.AddAutoIncDDLAction,
		Table: sqlparser.TableName{
			Qualifier: sqlparser.NewIdentifierCS("TestExecutor"),
			Name:      sqlparser.NewIdentifierCS("t1"),
		},
		VindexSpec: &sqlparser.VindexSpec{
			Name:   sqlparser.NewIdentifierCI("hash"),
			Type:   sqlparser.NewIdentifierCI("hash"),
			Params: vindexParams,
		},
		AutoIncSpec: &sqlparser.AutoIncSpec{
			Column: sqlparser.NewIdentifierCI("col_tinyint"),
			Sequence: sqlparser.TableName{
				Qualifier: sqlparser.NewIdentifierCS("TestExecutor"),
				Name:      sqlparser.NewIdentifierCS("t1"),
			},
		},
		VindexCols: []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("col_tinyint")},
	}

	tests := []testCase{
		{
			vschema: alterVschema1,
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"col_tinyint|tinyint|NO|PRI|NULL|",
			),
			expectedError: "",
		},
		{
			vschema: alterVschema1,
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"col_xxx|tinyint|NO|PRI|NULL|",
			),
			expectedError: "auto column col_tinyint does not exist in table t1",
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			r, sb, _, _ := createExecutorEnv()
			sb.SetResults([]*sqltypes.Result{tc.sandboxRes})

			vc, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)
			var ksName = "TestExecutor"
			err := SchemaCheck(vc, ctx, ksName, tc.vschema)

			if tc.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedError)
			}
		})
	}
}

func TestSchemaCheckWithAddSequenceDDL(t *testing.T) {
	type testCase struct {
		vschema       *sqlparser.AlterVschema
		sandboxRes    *sqltypes.Result
		sandboxRes1   *sqltypes.Result
		expectedError string
	}

	vindexParams := []sqlparser.VindexParam{{
		Key: sqlparser.NewIdentifierCI("ksa"),
		Val: "10",
	}, {
		Key: sqlparser.NewIdentifierCI("ks2"),
		Val: "shard-0000",
	}}

	alterVschema1 := &sqlparser.AlterVschema{
		Action: sqlparser.AddSequenceDDLAction,
		Table: sqlparser.TableName{
			Qualifier: sqlparser.NewIdentifierCS("TestExecutor"),
			Name:      sqlparser.NewIdentifierCS("t2"),
		},
		VindexSpec: &sqlparser.VindexSpec{
			Name:   sqlparser.NewIdentifierCI("hash"),
			Type:   sqlparser.NewIdentifierCI("hash"),
			Params: vindexParams,
		},
		AutoIncSpec: &sqlparser.AutoIncSpec{
			Column: sqlparser.NewIdentifierCI("id"),
			Sequence: sqlparser.TableName{
				Qualifier: sqlparser.NewIdentifierCS("TestExecutor"),
				Name:      sqlparser.NewIdentifierCS("t2"),
			},
		},
		VindexCols: []sqlparser.IdentifierCI{sqlparser.NewIdentifierCI("col_tinyint")},
	}

	tests := []testCase{
		{
			vschema: alterVschema1,
			sandboxRes: sqltypes.MakeTestResult(sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|bigint|NO|PRI|NULL|", "next_id|bigint|YES||NULL|", "cache|bigint|YES||NULL|"),
			sandboxRes1: sqltypes.MakeTestResult(sqltypes.MakeTestFields("TABLE_NAME|TABLE_COMMENT", "varchar|varchar"),
				"test_seq2|vitess_sequence"),
			expectedError: "",
		},
		{
			vschema: alterVschema1,
			sandboxRes: sqltypes.MakeTestResult(sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|bigint|NO|PRI|NULL|", "next_id|bigint|YES||NULL|", "cache|bigint|YES||NULL|"),
			sandboxRes1:   sqltypes.MakeTestResult(nil),
			expectedError: "can not get table_comment from table t2 in keyspace TestExecutor",
		},
		{
			vschema:       alterVschema1,
			sandboxRes:    sqltypes.MakeTestResult(nil),
			sandboxRes1:   sqltypes.MakeTestResult(nil),
			expectedError: "table t2 does not exist in keyspace TestExecutor",
		},
		{
			vschema: alterVschema1,
			sandboxRes: sqltypes.MakeTestResult(sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|bigint|NO||NULL|", "next_id|bigint|YES||NULL|", "cache|bigint|YES||NULL|"),
			sandboxRes1: sqltypes.MakeTestResult(sqltypes.MakeTestFields("TABLE_NAME|TABLE_COMMENT", "varchar|varchar"),
				"test_seq2|vitess_sequence"),
			expectedError: "column id should be primary key",
		},
		{
			vschema: alterVschema1,
			sandboxRes: sqltypes.MakeTestResult(sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id1|bigint|NO|PRI|NULL|", "next_id|bigint|YES||NULL|", "cache|bigint|YES||NULL|"),
			sandboxRes1: sqltypes.MakeTestResult(sqltypes.MakeTestFields("TABLE_NAME|TABLE_COMMENT", "varchar|varchar"),
				"test_seq2|vitess_sequence"),
			expectedError: "column name should be id/next_id/cache",
		},
		{
			vschema: alterVschema1,
			sandboxRes: sqltypes.MakeTestResult(sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|bigint|NO|PRI|NULL|", "next_id|bigint|YES||NULL|"),
			sandboxRes1: sqltypes.MakeTestResult(sqltypes.MakeTestFields("TABLE_NAME|TABLE_COMMENT", "varchar|varchar"),
				"test_seq2|vitess_sequence"),
			expectedError: "column cache does not exist",
		},
		{
			vschema: alterVschema1,
			sandboxRes: sqltypes.MakeTestResult(sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id1|int|NO|PRI|NULL|", "next_id|bigint|YES||NULL|", "cache|bigint|YES||NULL|"),
			sandboxRes1: sqltypes.MakeTestResult(sqltypes.MakeTestFields("TABLE_NAME|TABLE_COMMENT", "varchar|varchar"),
				"test_seq2|vitess_sequence"),
			expectedError: "column type should be bigint",
		},
		{
			vschema: alterVschema1,
			sandboxRes: sqltypes.MakeTestResult(sqltypes.MakeTestFields("Field|Type|Null|Key|Default|Extra", "varchar|varchar|varchar|varchar|varchar|varchar"),
				"id|bigint|NO|PRI|NULL|", "next_id|bigint|YES||NULL|", "cache|bigint|YES||NULL|"),
			sandboxRes1: sqltypes.MakeTestResult(sqltypes.MakeTestFields("TABLE_NAME|TABLE_COMMENT", "varchar|varchar"),
				"test_seq2|vitess_sequencexxxxxxx"),
			expectedError: "table t2 comment should be 'vitess_sequence'",
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			r, sb, _, _ := createExecutorEnv()
			sb.SetResults([]*sqltypes.Result{tc.sandboxRes, tc.sandboxRes1})

			vc, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)
			var ksName = "TestExecutor"
			err := SchemaCheck(vc, ctx, ksName, tc.vschema)
			if tc.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedError)
			}
		})
	}

}

func TestSchemaCheckWithCreateVindexDDL(t *testing.T) {
	type testCase struct {
		vindexSpec    *sqlparser.VindexSpec
		sandboxRes    *sqltypes.Result
		sandboxRes2   *sqltypes.Result
		expectedError string
	}

	normal := []sqlparser.VindexParam{
		{
			Key: sqlparser.NewIdentifierCI("owner"),
			Val: "t2_lookup",
		}, {
			Key: sqlparser.NewIdentifierCI("table"),
			Val: "TestUnsharded.wo_lu_idx",
		}, {
			Key: sqlparser.NewIdentifierCI("from"),
			Val: "wo_lu_col",
		}, {
			Key: sqlparser.NewIdentifierCI("to"),
			Val: "keyspace_id",
		},
	}
	normal_owner := []sqlparser.VindexParam{
		{
			Key: sqlparser.NewIdentifierCI("owner"),
			Val: "TestExecutor.t2_lookup",
		}, {
			Key: sqlparser.NewIdentifierCI("table"),
			Val: "TestUnsharded.wo_lu_idx",
		}, {
			Key: sqlparser.NewIdentifierCI("from"),
			Val: "wo_lu_col",
		}, {
			Key: sqlparser.NewIdentifierCI("to"),
			Val: "keyspace_id",
		},
	}

	normal_lookup := []sqlparser.VindexParam{
		{
			Key: sqlparser.NewIdentifierCI("owner"),
			Val: "t2_lookup",
		}, {
			Key: sqlparser.NewIdentifierCI("table"),
			Val: "t1",
		}, {
			Key: sqlparser.NewIdentifierCI("from"),
			Val: "id",
		}, {
			Key: sqlparser.NewIdentifierCI("to"),
			Val: "keyspace_id",
		},
	}

	owner_not_exists := []sqlparser.VindexParam{
		{
			Key: sqlparser.NewIdentifierCI("owner"),
			Val: "ttt",
		}, {
			Key: sqlparser.NewIdentifierCI("table"),
			Val: "TestUnsharded.wo_lu_idx",
		}, {
			Key: sqlparser.NewIdentifierCI("from"),
			Val: "wo_lu_col",
		}, {
			Key: sqlparser.NewIdentifierCI("to"),
			Val: "keyspace_id",
		},
	}

	lookup_not_exists := []sqlparser.VindexParam{
		{
			Key: sqlparser.NewIdentifierCI("owner"),
			Val: "t2_lookup",
		}, {
			Key: sqlparser.NewIdentifierCI("table"),
			Val: "TestUnsharded.sss",
		}, {
			Key: sqlparser.NewIdentifierCI("from"),
			Val: "wo_lu_col",
		}, {
			Key: sqlparser.NewIdentifierCI("to"),
			Val: "keyspace_id",
		},
	}

	from_not_exists := []sqlparser.VindexParam{
		{
			Key: sqlparser.NewIdentifierCI("owner"),
			Val: "t2_lookup",
		}, {
			Key: sqlparser.NewIdentifierCI("table"),
			Val: "TestUnsharded.wo_lu_idx",
		}, {
			Key: sqlparser.NewIdentifierCI("from"),
			Val: "ddd",
		}, {
			Key: sqlparser.NewIdentifierCI("to"),
			Val: "keyspace_id",
		},
	}

	// to_not_exists := []sqlparser.VindexParam{
	// 	{
	// 		Key: sqlparser.NewIdentifierCI("owner"),
	// 		Val: "corder",
	// 	}, {
	// 		Key: sqlparser.NewIdentifierCI("table"),
	// 		Val: "order_lookup_table",
	// 	}, {
	// 		Key: sqlparser.NewIdentifierCI("from"),
	// 		Val: "corder_id",
	// 	}, {
	// 		Key: sqlparser.NewIdentifierCI("to"),
	// 		Val: "xxx",
	// 	},
	// }

	all_not_exists := []sqlparser.VindexParam{}

	not_owner_keyword := []sqlparser.VindexParam{
		{
			Key: sqlparser.NewIdentifierCI("table"),
			Val: "TestUnsharded.wo_lu_idx",
		}, {
			Key: sqlparser.NewIdentifierCI("from"),
			Val: "wo_lu_col",
		}, {
			Key: sqlparser.NewIdentifierCI("to"),
			Val: "keyspace_id",
		},
	}
	not_table_keyword := []sqlparser.VindexParam{
		{
			Key: sqlparser.NewIdentifierCI("owner"),
			Val: "t2_lookup",
		}, {
			Key: sqlparser.NewIdentifierCI("from"),
			Val: "wo_lu_col",
		}, {
			Key: sqlparser.NewIdentifierCI("to"),
			Val: "keyspace_id",
		},
	}

	not_from_keyword := []sqlparser.VindexParam{
		{
			Key: sqlparser.NewIdentifierCI("owner"),
			Val: "t2_lookup",
		}, {
			Key: sqlparser.NewIdentifierCI("table"),
			Val: "TestUnsharded.wo_lu_idx",
		}, {
			Key: sqlparser.NewIdentifierCI("to"),
			Val: "keyspace_id",
		},
	}

	not_to_keyword := []sqlparser.VindexParam{
		{
			Key: sqlparser.NewIdentifierCI("owner"),
			Val: "t2_lookup",
		}, {
			Key: sqlparser.NewIdentifierCI("table"),
			Val: "TestUnsharded.wo_lu_idx",
		}, {
			Key: sqlparser.NewIdentifierCI("from"),
			Val: "wo_lu_col",
		},
	}

	primary_exists := []sqlparser.VindexParam{
		{
			Key: sqlparser.NewIdentifierCI("owner"),
			Val: "TestUnsharded.wo_lu_idx",
		}, {
			Key: sqlparser.NewIdentifierCI("table"),
			Val: "TestUnsharded.wo_lu_idx",
		}, {
			Key: sqlparser.NewIdentifierCI("from"),
			Val: "wo_lu_col",
		}, {
			Key: sqlparser.NewIdentifierCI("to"),
			Val: "keyspace_id",
		},
	}

	tests := []testCase{
		{
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("t2_wo_lu_vdx_test"),
				Type:   sqlparser.NewIdentifierCI("lookup_unique"),
				Params: normal,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"t2_lookup|id|bigint|PRI|1|NULL",
				"t2_lookup|wo_lu_col|bigint|UNI|NULL|NULL"),
			sandboxRes2: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"wo_lu_idx|wo_lu_col|bigint|PRI|1|NULL",
				"wo_lu_idx|keyspace_id|varchar(25)|NULL|NULL|NULL"),
			expectedError: "",
		}, {
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("t2_wo_lu_vdx_test"),
				Type:   sqlparser.NewIdentifierCI("lookup_unique"),
				Params: normal_owner,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"t2_lookup|id|bigint|PRI|1|NULL",
				"t2_lookup|wo_lu_col|bigint|UNI|NULL|NULL"),
			sandboxRes2: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"wo_lu_idx|wo_lu_col|bigint|PRI|1|NULL",
				"wo_lu_idx|keyspace_id|varchar(25)|NULL|NULL|NULL"),
			expectedError: "",
		}, {
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("t2_wo_lu_vdx_test"),
				Type:   sqlparser.NewIdentifierCI("lookup_unique"),
				Params: normal_lookup,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"t2_lookup|id|bigint|PRI|1|NULL",
				"t2_lookup|wo_lu_col|bigint|UNI|NULL|NULL",
				"t1|id|bigint|PRI|1|NULL",
				"t1|unq_col|bigint|UNI|NULL|NULL"),
			sandboxRes2:   nil,
			expectedError: "",
		}, {
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("t2_wo_lu_vdx_test"),
				Type:   sqlparser.NewIdentifierCI("lookup_unique"),
				Params: not_owner_keyword,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"t2_lookup|id|bigint|PRI|1|NULL",
				"t2_lookup|wo_lu_col|bigint|UNI|NULL|NULL"),
			sandboxRes2: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"wo_lu_idx|wo_lu_col|bigint|PRI|1|NULL",
				"wo_lu_idx|keyspace_id|varchar(25)|NULL|NULL|NULL"),
			expectedError: vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Syntax error: please specify the value for the keyword `owner`").Error(),
		}, {
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("t2_wo_lu_vdx_test"),
				Type:   sqlparser.NewIdentifierCI("lookup_unique"),
				Params: not_table_keyword,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"t2_lookup|id|bigint|PRI|1|NULL",
				"t2_lookup|wo_lu_col|bigint|UNI|NULL|NULL"),
			sandboxRes2: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"wo_lu_idx|wo_lu_col|bigint|PRI|1|NULL",
				"wo_lu_idx|keyspace_id|varchar(25)|NULL|NULL|NULL"),
			expectedError: vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Syntax error: please specify the value for the keyword `table`").Error(),
		}, {
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("t2_wo_lu_vdx_test"),
				Type:   sqlparser.NewIdentifierCI("lookup_unique"),
				Params: not_from_keyword,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"t2_lookup|id|bigint|PRI|1|NULL",
				"t2_lookup|wo_lu_col|bigint|UNI|NULL|NULL"),
			sandboxRes2: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"wo_lu_idx|wo_lu_col|bigint|PRI|1|NULL",
				"wo_lu_idx|keyspace_id|varchar(25)|NULL|NULL|NULL"),
			expectedError: vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Syntax error: please specify the value for the keyword `from`").Error(),
		}, {
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("t2_wo_lu_vdx_test"),
				Type:   sqlparser.NewIdentifierCI("lookup_unique"),
				Params: not_to_keyword,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"t2_lookup|id|bigint|PRI|1|NULL",
				"t2_lookup|wo_lu_col|bigint|UNI|NULL|NULL"),
			sandboxRes2: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"wo_lu_idx|wo_lu_col|bigint|PRI|1|NULL",
				"wo_lu_idx|keyspace_id|varchar(25)|NULL|NULL|NULL"),
			expectedError: vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Syntax error: please specify the value for the keyword `to`").Error(),
		}, {
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("test_hash"),
				Type:   sqlparser.NewIdentifierCI("hash"),
				Params: all_not_exists,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"t2_lookup|id|bigint|PRI|1|NULL",
				"t2_lookup|wo_lu_col|bigint|UNI|NULL|NULL"),
			sandboxRes2: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"wo_lu_idx|wo_lu_col|bigint|PRI|1|NULL",
				"wo_lu_idx|keyspace_id|varchar(25)|NULL|NULL|NULL"),
			expectedError: "",
		}, {
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("t2_wo_lu_vdx_test"),
				Type:   sqlparser.NewIdentifierCI("lookup_unique"),
				Params: owner_not_exists,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"t2_lookup|id|bigint|PRI|1|NULL",
				"t2_lookup|wo_lu_col|bigint|UNI|NULL|NULL"),
			sandboxRes2: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"wo_lu_idx|wo_lu_col|bigint|PRI|1|NULL",
				"wo_lu_idx|keyspace_id|varchar(25)|NULL|NULL|NULL"),
			expectedError: vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vschema does not contain owner table '%s' in keyspace %s", "ttt", "TestExecutor").Error(),
		}, {
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("t2_wo_lu_vdx_test"),
				Type:   sqlparser.NewIdentifierCI("lookup_unique"),
				Params: lookup_not_exists,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"t2_lookup|id|bigint|PRI|1|NULL",
				"t2_lookup|wo_lu_col|bigint|UNI|NULL|NULL"),
			sandboxRes2: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"wo_lu_idx|wo_lu_col|bigint|PRI|1|NULL",
				"wo_lu_idx|keyspace_id|varchar(25)|NULL|NULL|NULL"),
			expectedError: vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vschema does not contain lookup table '%s' in keyspace %s", "sss", "TestUnsharded").Error(),
		}, {
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("t2_wo_lu_vdx_test"),
				Type:   sqlparser.NewIdentifierCI("lookup_unique"),
				Params: from_not_exists,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"t2_lookup|id|bigint|PRI|1|NULL",
				"t2_lookup|wo_lu_col|bigint|UNI|NULL|NULL"),
			sandboxRes2: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"wo_lu_idx|wo_lu_col|bigint|PRI|1|NULL",
				"wo_lu_idx|keyspace_id|varchar(25)|NULL|NULL|NULL"),
			expectedError: vterrors.Errorf(vtrpcpb.Code_INTERNAL, "from column '%s' does not exist in ownertable %s", "ddd", "t2_lookup").Error(),
		}, {
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("t2_wo_lu_vdx_test"),
				Type:   sqlparser.NewIdentifierCI("lookup_unique"),
				Params: normal,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"t2_lookup|id|bigint|PRI|1|NULL",
				"t2_lookup|wo_lu_col|bigint|UNI|NULL|NULL"),
			sandboxRes2: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"wo_lu_idx_test|wo_lu_col|bigint|PRI|1|NULL",
				"wo_lu_idx_test|keyspace_id|varchar(25)|NULL|NULL|NULL"),
			expectedError: vterrors.Errorf(vtrpcpb.Code_INTERNAL, "lookup table '%s' does not exist in keyspace %s", "wo_lu_idx", "TestUnsharded").Error(),
		}, {
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("t2_wo_lu_vdx_test"),
				Type:   sqlparser.NewIdentifierCI("lookup_unique"),
				Params: normal,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"t2_lookup_test|id|bigint|PRI|1|NULL",
				"t2_lookup_test|wo_lu_col|bigint|UNI|NULL|NULL"),
			sandboxRes2: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"wo_lu_idx|wo_lu_col|bigint|PRI|1|NULL",
				"wo_lu_idx|keyspace_id|varchar(25)|NULL|NULL|NULL"),
			expectedError: vterrors.Errorf(vtrpcpb.Code_INTERNAL, "owner table '%s' does not exist in keyspace %s", "t2_lookup", "TestExecutor").Error(),
		},
		// {
		// 	vindexSpec: &sqlparser.VindexSpec{
		// 		Name:   sqlparser.NewIdentifierCI("order_lookup_idx"),
		// 		Type:   sqlparser.NewIdentifierCI("consistent_lookup_unique"),
		// 		Params: not_to_keyword,
		// 	},
		// 	sandboxRes: sqltypes.MakeTestResult(
		// 		sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
		// 			"varchar|varchar|varchar|varchar|bool|varchar"),
		// 		"t2_lookup|id|bigint|PRI|1|NULL",
		// 		"t2_lookup|wo_lu_col|bigint|UNI|NULL|NULL"),
		// 	sandboxRes2: sqltypes.MakeTestResult(
		// 		sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
		// 			"varchar|varchar|varchar|varchar|bool|varchar"),
		// 		"wo_lu_idx|wo_lu_col|bigint|PRI|1|NULL",
		// 		"wo_lu_idx|keyspace_id|varchar(25)|NULL|NULL|NULL"),
		// 	expectedError: "",
		// },
		{
			vindexSpec: &sqlparser.VindexSpec{
				Name:   sqlparser.NewIdentifierCI("t2_wo_lu_vdx_test"),
				Type:   sqlparser.NewIdentifierCI("lookup_unique"),
				Params: primary_exists,
			},
			sandboxRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"t2_lookup|id|bigint|PRI|1|NULL",
				"t2_lookup|wo_lu_col|bigint|UNI|NULL|NULL"),
			sandboxRes2: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("table_name|column_name|data_type|column_key|auto_increment|collation_name",
					"varchar|varchar|varchar|varchar|bool|varchar"),
				"wo_lu_idx|wo_lu_col|bigint|PRI|1|NULL",
				"wo_lu_idx|keyspace_id|varchar(25)|NULL|NULL|NULL"),
			expectedError: vterrors.Errorf(vtrpcpb.Code_INTERNAL, "The owner table '%s' does not have a primary vindex", "wo_lu_idx").Error(),
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			r, sbc1, _, sbclookup := createExecutorEnv()
			sbc1.SetResults([]*sqltypes.Result{tt.sandboxRes})
			sbclookup.SetResults([]*sqltypes.Result{tt.sandboxRes2})
			vc, _ := newVCursorImpl(NewSafeSession(&vtgatepb.Session{TargetString: "@unknown"}), makeComments(""), r, nil, r.vm, r.VSchema(), r.resolver.resolver, nil, false, pv)
			alterVschema := &sqlparser.AlterVschema{
				Action:     sqlparser.CreateVindexDDLAction,
				VindexSpec: tt.vindexSpec,
			}

			err := SchemaCheck(vc, ctx, "TestExecutor", alterVschema)
			if tt.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.expectedError)
			}
		})
	}
}
