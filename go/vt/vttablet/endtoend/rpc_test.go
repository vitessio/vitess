/*
Copyright 2023 The Vitess Authors.

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

package endtoend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/callerid"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

// TestGetSchemaRPC will validate GetSchema RPC.
func TestGetSchemaRPC(t *testing.T) {
	testcases := []struct {
		name               string
		queries            []string
		deferQueries       []string
		getSchemaQueryType querypb.SchemaTableType
		getSchemaTables    []string
		mapToExpect        map[string]string
	}{
		{
			name: "All views",
			queries: []string{
				"create view vitess_view1 as select id from vitess_a",
				"create view vitess_view2 as select id from vitess_b",
			},
			deferQueries: []string{
				"drop view vitess_view1",
				"drop view vitess_view2",
			},
			mapToExpect: map[string]string{
				"vitess_view1": "CREATE ALGORITHM=UNDEFINED DEFINER=`vt_dba`@`localhost` SQL SECURITY DEFINER VIEW `vitess_view1` AS select `vitess_a`.`id` AS `id` from `vitess_a`",
				"vitess_view2": "CREATE ALGORITHM=UNDEFINED DEFINER=`vt_dba`@`localhost` SQL SECURITY DEFINER VIEW `vitess_view2` AS select `vitess_b`.`id` AS `id` from `vitess_b`",
			},
			getSchemaQueryType: querypb.SchemaTableType_VIEWS,
		}, {
			name: "Views listed",
			queries: []string{
				"create view vitess_view1 as select eid from vitess_a",
				"create view vitess_view2 as select eid from vitess_b",
				"create view vitess_view3 as select eid from vitess_c",
			},
			deferQueries: []string{
				"drop view vitess_view1",
				"drop view vitess_view2",
				"drop view vitess_view3",
			},
			mapToExpect: map[string]string{
				"vitess_view3": "CREATE ALGORITHM=UNDEFINED DEFINER=`vt_dba`@`localhost` SQL SECURITY DEFINER VIEW `vitess_view3` AS select `vitess_c`.`eid` AS `eid` from `vitess_c`",
				"vitess_view2": "CREATE ALGORITHM=UNDEFINED DEFINER=`vt_dba`@`localhost` SQL SECURITY DEFINER VIEW `vitess_view2` AS select `vitess_b`.`eid` AS `eid` from `vitess_b`",
				// These shouldn't be part of the result so we verify it is empty.
				"vitess_view1": "",
				"unknown_view": "",
			},
			getSchemaTables:    []string{"vitess_view3", "vitess_view2", "unknown_view"},
			getSchemaQueryType: querypb.SchemaTableType_VIEWS,
		}, {
			name: "All tables",
			queries: []string{
				"create table vitess_temp1 (id int);",
				"create table vitess_temp2 (id int);",
				"create table vitess_temp3 (id int);",
			},
			deferQueries: []string{
				"drop table vitess_temp1",
				"drop table vitess_temp2",
				"drop table vitess_temp3",
			},
			mapToExpect: map[string]string{
				"vitess_temp1": "CREATE TABLE `vitess_temp1` (\n  `id` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
				"vitess_temp2": "CREATE TABLE `vitess_temp2` (\n  `id` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
				"vitess_temp3": "CREATE TABLE `vitess_temp3` (\n  `id` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			},
			getSchemaQueryType: querypb.SchemaTableType_TABLES,
		}, {
			name: "Tables listed",
			queries: []string{
				"create table vitess_temp1 (eid int);",
				"create table vitess_temp2 (eid int);",
				"create table vitess_temp3 (eid int);",
			},
			deferQueries: []string{
				"drop table vitess_temp1",
				"drop table vitess_temp2",
				"drop table vitess_temp3",
			},
			mapToExpect: map[string]string{
				"vitess_temp1": "CREATE TABLE `vitess_temp1` (\n  `eid` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
				"vitess_temp3": "CREATE TABLE `vitess_temp3` (\n  `eid` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
				// These shouldn't be part of the result so we verify it is empty.
				"vitess_temp2":  "",
				"unknown_table": "",
			},
			getSchemaQueryType: querypb.SchemaTableType_TABLES,
			getSchemaTables:    []string{"vitess_temp1", "vitess_temp3", "unknown_table"},
		}, {
			name: "All tables and views",
			queries: []string{
				"create table vitess_temp1 (id int);",
				"create table vitess_temp2 (id int);",
				"create table vitess_temp3 (id int);",
				"create view vitess_view1 as select id from vitess_a",
				"create view vitess_view2 as select id from vitess_b",
			},
			deferQueries: []string{
				"drop table vitess_temp1",
				"drop table vitess_temp2",
				"drop table vitess_temp3",
				"drop view vitess_view1",
				"drop view vitess_view2",
			},
			mapToExpect: map[string]string{
				"vitess_view1": "CREATE ALGORITHM=UNDEFINED DEFINER=`vt_dba`@`localhost` SQL SECURITY DEFINER VIEW `vitess_view1` AS select `vitess_a`.`id` AS `id` from `vitess_a`",
				"vitess_view2": "CREATE ALGORITHM=UNDEFINED DEFINER=`vt_dba`@`localhost` SQL SECURITY DEFINER VIEW `vitess_view2` AS select `vitess_b`.`id` AS `id` from `vitess_b`",
				"vitess_temp1": "CREATE TABLE `vitess_temp1` (\n  `id` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
				"vitess_temp2": "CREATE TABLE `vitess_temp2` (\n  `id` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
				"vitess_temp3": "CREATE TABLE `vitess_temp3` (\n  `id` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			},
			getSchemaQueryType: querypb.SchemaTableType_ALL,
		}, {
			name: "Listed tables and views",
			queries: []string{
				"create table vitess_temp1 (eid int);",
				"create table vitess_temp2 (eid int);",
				"create table vitess_temp3 (eid int);",
				"create view vitess_view1 as select eid from vitess_a",
				"create view vitess_view2 as select eid from vitess_b",
				"create view vitess_view3 as select eid from vitess_c",
			},
			deferQueries: []string{
				"drop table vitess_temp1",
				"drop table vitess_temp2",
				"drop table vitess_temp3",
				"drop view vitess_view1",
				"drop view vitess_view2",
				"drop view vitess_view3",
			},
			mapToExpect: map[string]string{
				"vitess_view1": "CREATE ALGORITHM=UNDEFINED DEFINER=`vt_dba`@`localhost` SQL SECURITY DEFINER VIEW `vitess_view1` AS select `vitess_a`.`eid` AS `eid` from `vitess_a`",
				"vitess_view3": "CREATE ALGORITHM=UNDEFINED DEFINER=`vt_dba`@`localhost` SQL SECURITY DEFINER VIEW `vitess_view3` AS select `vitess_c`.`eid` AS `eid` from `vitess_c`",
				"vitess_temp1": "CREATE TABLE `vitess_temp1` (\n  `eid` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
				"vitess_temp3": "CREATE TABLE `vitess_temp3` (\n  `eid` int DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
				// These shouldn't be part of the result so we verify it is empty.
				"vitess_temp2":  "",
				"vitess_view2":  "",
				"unknown_view":  "",
				"unknown_table": "",
			},
			getSchemaQueryType: querypb.SchemaTableType_ALL,
			getSchemaTables:    []string{"vitess_temp1", "vitess_temp3", "unknown_table", "vitess_view3", "vitess_view1", "unknown_view"},
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			client := framework.NewClient()
			client.UpdateContext(callerid.NewContext(
				context.Background(),
				&vtrpcpb.CallerID{},
				&querypb.VTGateCallerID{Username: "dev"}))

			for _, query := range testcase.queries {
				_, err := client.Execute(query, nil)
				require.NoError(t, err)
			}
			defer func() {
				for _, query := range testcase.deferQueries {
					_, err := client.Execute(query, nil)
					require.NoError(t, err)
				}
			}()

			timeout := 1 * time.Minute
			wait := time.After(timeout)
			for {
				select {
				case <-wait:
					t.Errorf("Schema tracking hasn't caught up")
					return
				case <-time.After(1 * time.Second):
					schemaDefs, err := client.GetSchema(testcase.getSchemaQueryType, testcase.getSchemaTables...)
					require.NoError(t, err)
					success := true
					for tableName, expectedCreateStatement := range testcase.mapToExpect {
						if schemaDefs[tableName] != expectedCreateStatement {
							success = false
							break
						}
					}
					if success {
						return
					}
				}
			}
		})
	}
}
