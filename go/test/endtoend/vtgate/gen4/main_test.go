/*
Copyright 2021 The Vitess Authors.

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

package vtgate

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"

	"vitess.io/vitess/go/vt/vtgate/planbuilder"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance  *cluster.LocalProcessCluster
	vtParams         mysql.ConnParams
	mysqlParams      mysql.ConnParams
	shardedKs        = "ks"
	unshardedKs      = "uks"
	shardedKsShards  = []string{"-19a0", "19a0-20", "20-20c0", "20c0-"}
	Cell             = "test"
	shardedSchemaSQL = `create table t1(
	id bigint,
	col bigint,
	primary key(id)
) Engine=InnoDB;

create table t2(
	id bigint,
	tcol1 varchar(50),
	tcol2 varchar(50),
	primary key(id)
) Engine=InnoDB;

create table t3(
	id bigint,
	tcol1 varchar(50),
	tcol2 varchar(50),
	primary key(id)
) Engine=InnoDB;

create table user_region(
	id bigint,
	cola bigint,
	colb bigint,
	primary key(id)
) Engine=InnoDB;

create table region_tbl(
	rg bigint,
	uid bigint,
	msg varchar(50),
	primary key(uid)
) Engine=InnoDB;

create table multicol_tbl(
	cola bigint,
	colb varbinary(50),
	colc varchar(50),
	msg varchar(50),
	primary key(cola, colb, colc)
) Engine=InnoDB;
`
	unshardedSchemaSQL = `create table u_a(
	id bigint,
	a bigint,
	primary key(id)
) Engine=InnoDB;

create table u_b(
	id bigint,
	b varchar(50),
	primary key(id)
) Engine=InnoDB;
`

	shardedVSchema = `
{
  "sharded": true,
  "vindexes": {
    "xxhash": {
      "type": "xxhash"
    },
    "regional_vdx": {
	  "type": "region_experimental",
	  "params": {
		"region_bytes": "1"
	  }
    },
    "multicol_vdx": {
	  "type": "multicol",
	  "params": {
		"column_count": "3",
		"column_bytes": "1,3,4",
		"column_vindex": "hash,binary,unicode_loose_xxhash"
	  }
    }
  },
  "tables": {
    "t1": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "xxhash"
        }
      ]
    },
    "t2": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "xxhash"
        }
      ],
      "columns": [
        {
          "name": "tcol1",
          "type": "VARCHAR"
        }
      ]
    },
    "t3": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "xxhash"
        }
      ],
      "columns": [
        {
          "name": "tcol1",
          "type": "VARCHAR"
        }
      ]
    },
    "user_region": {
	  "column_vindexes": [
	    {
          "columns": ["cola","colb"],
		  "name": "regional_vdx"
		}
      ]
    },
    "region_tbl": {
	  "column_vindexes": [
	    {
          "columns": ["rg","uid"],
		  "name": "regional_vdx"
		}
      ]
    },
    "multicol_tbl": {
	  "column_vindexes": [
	    {
          "columns": ["cola","colb","colc"],
		  "name": "multicol_vdx"
		}
      ]
	}
  }
}`

	unshardedVSchema = `
{
  "sharded": false,
  "tables": {
    "u_a": {},
    "u_b": {}
  }
}`

	routingRules = `
{"rules": [
  {
    "from_table": "ks.t1000",
	"to_tables": ["ks.t1"]
  }
]}
`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(Cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		sKs := &cluster.Keyspace{
			Name:      shardedKs,
			SchemaSQL: shardedSchemaSQL,
			VSchema:   shardedVSchema,
		}

		clusterInstance.VtGateExtraArgs = []string{"--schema_change_signal"}
		clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-schema-change-signal", "--queryserver-config-schema-change-signal-interval", "0.1"}
		err = clusterInstance.StartKeyspace(*sKs, shardedKsShards, 0, false)
		if err != nil {
			return 1
		}

		uKs := &cluster.Keyspace{
			Name:      unshardedKs,
			SchemaSQL: unshardedSchemaSQL,
			VSchema:   unshardedVSchema,
		}
		err = clusterInstance.StartUnshardedKeyspace(*uKs, 0, false)
		if err != nil {
			return 1
		}

		// apply routing rules
		err = clusterInstance.VtctlclientProcess.ApplyRoutingRules(routingRules)
		if err != nil {
			return 1
		}

		err = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildVSchemaGraph")
		if err != nil {
			return 1
		}

		// Start vtgate
		clusterInstance.VtGatePlannerVersion = planbuilder.Gen4 // enable Gen4 planner.
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}

		conn, closer, err := utils.NewMySQL(clusterInstance, shardedKs, shardedSchemaSQL)
		if err != nil {
			fmt.Println(err)
			return 1
		}
		defer closer()
		mysqlParams = conn
		return m.Run()
	}()
	os.Exit(exitCode)
}

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)
	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, mcmp.VtConn, "set workload = oltp")

		tables := []string{"t1", "t2", "t3", "user_region", "region_tbl", "multicol_tbl", "t1_id2_idx", "t2_id4_idx", "u_a", "u_b"}
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
	}

	deleteAll()

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
		cluster.PanicHandler(t)
	}
}
