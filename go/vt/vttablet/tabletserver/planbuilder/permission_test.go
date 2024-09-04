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

package planbuilder

import (
	"testing"

	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
)

func TestBuildPermissions(t *testing.T) {
	tcases := []struct {
		input  string
		output []Permission
	}{{
		input: "select * from t",
		output: []Permission{{
			TableName: "t",
			Role:      tableacl.READER,
		}},
	}, {
		input: "select * from t1 union select * from t2",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.READER,
		}, {
			TableName: "t2",
			Role:      tableacl.READER,
		}},
	}, {
		input: "insert into t values()",
		output: []Permission{{
			TableName: "t",
			Role:      tableacl.WRITER,
		}},
	}, {
		input: "update t set a=1",
		output: []Permission{{
			TableName: "t",
			Role:      tableacl.WRITER,
		}},
	}, {
		input: "delete from t",
		output: []Permission{{
			TableName: "t",
			Role:      tableacl.WRITER,
		}},
	}, {
		input:  "set a=1",
		output: nil,
	}, {
		input:  "show variable like 'a%'",
		output: nil,
	}, {
		input:  "describe select * from t",
		output: nil,
	}, {
		input: "create table t",
		output: []Permission{{
			TableName: "t",
			Role:      tableacl.ADMIN,
		}},
	}, {
		input: "rename table t1 to t2",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.ADMIN,
		}, {
			TableName: "t2",
			Role:      tableacl.ADMIN,
		}},
	}, {
		input: "flush tables t1, t2",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.ADMIN,
		}, {
			TableName: "t2",
			Role:      tableacl.ADMIN,
		}},
	}, {
		input: "drop table t",
		output: []Permission{{
			TableName: "t",
			Role:      tableacl.ADMIN,
		}},
	}, {
		input:  "repair t",
		output: nil,
	}, {
		input: "select (select a from t2) from t1",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.READER,
		}, {
			TableName: "t2",
			Role:      tableacl.READER,
		}},
	}, {
		input: "insert into t1 values((select a from t2), 1)",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.WRITER,
		}, {
			TableName: "t2",
			Role:      tableacl.READER,
		}},
	}, {
		input: "update t1 set a = (select b from t2)",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.WRITER,
		}, {
			TableName: "t2",
			Role:      tableacl.READER,
		}},
	}, {
		input: "delete from t1 where a = (select b from t2)",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.WRITER,
		}, {
			TableName: "t2",
			Role:      tableacl.READER,
		}},
	}, {
		input: "select * from t1, t2",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.READER,
		}, {
			TableName: "t2",
			Role:      tableacl.READER,
		}},
	}, {
		input: "select * from (t1, t2)",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.READER,
		}, {
			TableName: "t2",
			Role:      tableacl.READER,
		}},
	}, {
		input: "update t1 join t2 on a=b set c=d",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.WRITER,
		}, {
			TableName: "t2",
			Role:      tableacl.WRITER,
		}},
	}, {
		input: "update (select * from t1) as a join t2 on a=b set c=d",
		output: []Permission{{
			TableName: "t2",
			Role:      tableacl.WRITER,
		}, {
			TableName: "t1", // derived table in update or delete needs reader permission as they cannot be modified.
		}},
	}, {
		input: "with t as (select count(*) as a from user) select a from t",
		output: []Permission{{
			TableName: "user",
			Role:      tableacl.READER,
		}},
	}, {
		input: "with d as (select id, count(*) as a from user) select d.a from music join d on music.user_id = d.id group by 1",
		output: []Permission{{
			TableName: "music",
			Role:      tableacl.READER,
		}, {
			TableName: "user",
			Role:      tableacl.READER,
		}},
	}, {
		input: "WITH t1 AS ( SELECT id FROM t2 ) SELECT * FROM t1 JOIN ks.t1 AS t3",
		output: []Permission{{
			TableName: "t1",
			Role:      tableacl.READER,
		}, {
			TableName: "t2",
			Role:      tableacl.READER,
		}},
	}, {
		input: "WITH RECURSIVE t1 (n) AS ( SELECT id from t2 UNION ALL SELECT n + 1 FROM t1 WHERE n < 5 ) SELECT * FROM t1 JOIN t1 AS t3",
		output: []Permission{{
			TableName: "t2",
			Role:      tableacl.READER,
		}},
	}, {
		input: "(with t1 as (select count(*) as a from user) select a from t1) union  select * from t1",
		output: []Permission{{
			TableName: "user",
			Role:      tableacl.READER,
		}, {
			TableName: "t1",
			Role:      tableacl.READER,
		}},
	}}

	for _, tcase := range tcases {
		t.Run(tcase.input, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tcase.input)
			if err != nil {
				t.Fatal(err)
			}
			got := BuildPermissions(stmt)
			utils.MustMatch(t, tcase.output, got)
		})
	}
}
