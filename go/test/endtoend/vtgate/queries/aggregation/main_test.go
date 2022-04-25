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

package aggregation

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	mysqlParams     mysql.ConnParams
	keyspaceName    = "ks_union"
	cell            = "test_union"
	schemaSQL       = `create table t3(
	id5 bigint,
	id6 bigint,
	id7 bigint,
	primary key(id5)
) Engine=InnoDB;

create table t3_id7_idx(
    id bigint not null auto_increment,
	id7 bigint,
	id6 bigint,
    primary key(id)
) Engine=InnoDB;

create table t9(
	id1 bigint,
	id2 varchar(10),
	id3 varchar(10),
	primary key(id1)
) ENGINE=InnoDB DEFAULT charset=utf8mb4 COLLATE=utf8mb4_general_ci;

create table aggr_test(
	id bigint,
	val1 varchar(16),
	val2 bigint,
	primary key(id)
) Engine=InnoDB;

create table aggr_test_dates(
	id bigint,
	val1 datetime default current_timestamp,
	val2 datetime default current_timestamp,
	primary key(id)
) Engine=InnoDB;

create table t7_xxhash(
	uid varchar(50),
	phone bigint,
    msg varchar(100),
    primary key(uid)
) Engine=InnoDB;

create table t7_xxhash_idx(
	phone bigint,
	keyspace_id varbinary(50),
	primary key(phone, keyspace_id)
) Engine=InnoDB;
`

	vschema = `
{
  "sharded": true,
  "vindexes": {
    "hash": {
      "type": "hash"
    },
	"unicode_loose_xxhash" : {
	  "type": "unicode_loose_xxhash"
    },
    "t3_id7_vdx": {
      "type": "lookup_hash",
      "params": {
        "table": "t3_id7_idx",
        "from": "id7",
        "to": "id6"
      },
      "owner": "t3"
    },
    "t7_xxhash_vdx": {
      "type": "consistent_lookup",
      "params": {
        "table": "t7_xxhash_idx",
        "from": "phone",
        "to": "keyspace_id",
        "ignore_nulls": "true"
      },
      "owner": "t7_xxhash"
    }
  },
  "tables": {
	"t3": {
      "column_vindexes": [
        {
          "column": "id6",
          "name": "hash"
        },
        {
          "column": "id7",
          "name": "t3_id7_vdx"
        }
      ]
    },
    "t3_id7_idx": {
      "column_vindexes": [
        {
          "column": "id7",
          "name": "hash"
        }
      ]
    },
	"t9": {
      "column_vindexes": [
        {
          "column": "id1",
          "name": "hash"
        }
      ]
	},
	"aggr_test": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "hash"
        }
      ],
      "columns": [
        {
          "name": "val1",
          "type": "VARCHAR"
        }
      ]
    },
	"aggr_test_dates": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "hash"
        }
      ],
      "columns": [
        {
          "name": "val1",
          "type": "DATETIME"
        },
        {
          "name": "val2",
          "type": "DATETIME"
        }
      ]
    },
	"t7_xxhash": {
      "column_vindexes": [
        {
          "column": "uid",
          "name": "unicode_loose_xxhash"
        },
        {
          "column": "phone",
          "name": "t7_xxhash_vdx"
        }
      ]
    },
    "t7_xxhash_idx": {
      "column_vindexes": [
        {
          "column": "phone",
          "name": "unicode_loose_xxhash"
        }
      ]
    }
  }
}`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: schemaSQL,
			VSchema:   vschema,
		}
		clusterInstance.VtGateExtraArgs = []string{"--schema_change_signal"}
		clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-schema-change-signal", "--queryserver-config-schema-change-signal-interval", "0.1"}
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, true)
		if err != nil {
			return 1
		}

		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, "--enable_system_settings=true")
		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}

		// create mysql instance and connection parameters
		conn, closer, err := utils.NewMySQL(clusterInstance, keyspaceName, schemaSQL)
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
