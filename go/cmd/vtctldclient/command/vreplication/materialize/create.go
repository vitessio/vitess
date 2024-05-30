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

package materialize

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/topoproto"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	createOptions = struct {
		SourceKeyspace string
		TableSettings  tableSettings
	}{}

	// create makes a MaterializeCreate gRPC call to a vtctld.
	create = &cobra.Command{
		Use:     "create",
		Short:   "Create and run a Materialize VReplication workflow.",
		Example: `vtctldclient --server localhost:15999 materialize --workflow product_sales --target-keyspace commerce create --source-keyspace commerce --table-settings '[{"target_table": "sales_by_sku", "create_ddl": "create table sales_by_sku (sku varbinary(128) not null primary key, orders bigint, revenue bigint)", "source_expression": "select sku, count(*) as orders, sum(price) as revenue from corder group by sku"}]' --cells zone1 --cells zone2 --tablet-types replica`,
		Long: `Materialize is a lower level VReplication command that allows for generalized materialization
of tables. The target tables can be copies, aggregations, or views. The target tables are kept
in sync in near-realtime. The primary flag used to define the materializations (you can have
multiple per workflow) is table-settings which is a JSON array where each value must contain
two key/value pairs. The first required key is 'target_table' and it is the name of the table
in the target-keyspace to store the results in. The second required key is 'source_expression'
and its value is the select query to run against the source table. An optional key/value pair
can also be specified for 'create_ddl' which provides the DDL to create the target table if it
does not exist -- you can alternatively specify a value of 'copy' if the target table schema
should be copied as-is from the source keyspace. Here's an example value for table-settings:
[
  {
    "target_table": "customer_one_email",
    "source_expression": "select email from customer where customer_id = 1"
  },
  {
    "target_table": "states",
    "source_expression": "select * from states",
    "create_ddl": "copy"
  },
  {
    "target_table": "sales_by_sku",
    "source_expression": "select sku, count(*) as orders, sum(price) as revenue from corder group by sku",
    "create_ddl": "create table sales_by_sku (sku varbinary(128) not null primary key, orders bigint, revenue bigint)"
  }
]
`,
		SilenceUsage:          true,
		DisableFlagsInUseLine: true,
		Aliases:               []string{"Create"},
		Args:                  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if err := common.ParseAndValidateCreateOptions(cmd); err != nil {
				return err
			}
			return nil
		},
		RunE: commandCreate,
	}
)

func commandCreate(cmd *cobra.Command, args []string) error {
	format, err := common.GetOutputFormat(cmd)
	if err != nil {
		return err
	}
	tsp := common.GetTabletSelectionPreference(cmd)
	cli.FinishedParsing(cmd)

	ms := &vtctldatapb.MaterializeSettings{
		Workflow:                  common.BaseOptions.Workflow,
		MaterializationIntent:     vtctldatapb.MaterializationIntent_CUSTOM,
		TargetKeyspace:            common.BaseOptions.TargetKeyspace,
		SourceKeyspace:            createOptions.SourceKeyspace,
		TableSettings:             createOptions.TableSettings.val,
		StopAfterCopy:             common.CreateOptions.StopAfterCopy,
		Cell:                      strings.Join(common.CreateOptions.Cells, ","),
		TabletTypes:               topoproto.MakeStringTypeCSV(common.CreateOptions.TabletTypes),
		TabletSelectionPreference: tsp,
	}

	createOptions.TableSettings.parser, err = sqlparser.New(sqlparser.Options{
		MySQLServerVersion: common.CreateOptions.MySQLServerVersion,
		TruncateUILen:      common.CreateOptions.TruncateUILen,
		TruncateErrLen:     common.CreateOptions.TruncateErrLen,
	})
	if err != nil {
		return err
	}

	req := &vtctldatapb.MaterializeCreateRequest{
		Settings: ms,
	}

	_, err = common.GetClient().MaterializeCreate(common.GetCommandCtx(), req)
	if err != nil {
		return err
	}

	if format == "json" {
		resp := struct {
			Action string
			Status string
		}{
			Action: "create",
			Status: "success",
		}
		jsonText, _ := cli.MarshalJSONPretty(resp)
		fmt.Println(string(jsonText))
	} else {
		fmt.Printf("Materialization workflow %s successfully created in the %s keyspace. Use show to view the status.\n",
			common.BaseOptions.Workflow, common.BaseOptions.TargetKeyspace)
	}

	return nil
}

// tableSettings is a wrapper around a slice of TableMaterializeSettings
// proto messages that implements the pflag.Value interface.
type tableSettings struct {
	val    []*vtctldatapb.TableMaterializeSettings
	parser *sqlparser.Parser
}

func (ts *tableSettings) String() string {
	tsj, _ := json.Marshal(ts.val)
	return string(tsj)
}

func (ts *tableSettings) Set(v string) error {
	ts.val = make([]*vtctldatapb.TableMaterializeSettings, 0)
	err := json.Unmarshal([]byte(v), &ts.val)
	if err != nil {
		return fmt.Errorf("table-settings is not valid JSON")
	}
	if len(ts.val) == 0 {
		return fmt.Errorf("empty table-settings")
	}

	// Validate the provided queries.
	seenSourceTables := make(map[string]bool)
	for _, tms := range ts.val {
		if tms.TargetTable == "" || tms.SourceExpression == "" {
			return fmt.Errorf("missing target_table or source_expression")
		}
		// Validate that the query is valid.
		stmt, err := ts.parser.Parse(tms.SourceExpression)
		if err != nil {
			return fmt.Errorf("invalid source_expression: %q", tms.SourceExpression)
		}
		// Validate that each source-expression uses a different table.
		// If any of them query the same table the materialize workflow
		// will fail.
		err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case sqlparser.TableName:
				if node.Name.NotEmpty() {
					if seenSourceTables[node.Name.String()] {
						return false, fmt.Errorf("multiple source_expression queries use the same table: %q", node.Name.String())
					}
					seenSourceTables[node.Name.String()] = true
				}
			}
			return true, nil
		}, stmt)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ts *tableSettings) Type() string {
	return "JSON"
}
