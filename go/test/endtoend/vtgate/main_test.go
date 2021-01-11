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

package vtgate

import (
	"flag"
	"os"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	KeyspaceName    = "ks"
	Cell            = "test"
	SchemaSQL       = `create table t1(
	id1 bigint,
	id2 bigint,
	primary key(id1)
) Engine=InnoDB;

create table t1_id2_idx(
	id2 bigint,
	keyspace_id varbinary(10),
	primary key(id2)
) Engine=InnoDB;

create table vstream_test(
	id bigint,
	val bigint,
	primary key(id)
) Engine=InnoDB;

create table aggr_test(
	id bigint,
	val1 varchar(16),
	val2 bigint,
	primary key(id)
) Engine=InnoDB;

create table t2(
	id3 bigint,
	id4 bigint,
	primary key(id3)
) Engine=InnoDB;

create table t2_id4_idx(
	id bigint not null auto_increment,
	id4 bigint,
	id3 bigint,
	primary key(id),
	key idx_id4(id4)
) Engine=InnoDB;

create table t3(
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

create table t4(
	id1 bigint,
	id2 varchar(10),
	primary key(id1)
) ENGINE=InnoDB DEFAULT charset=utf8mb4 COLLATE=utf8mb4_general_ci;

create table t4_id2_idx(
	id2 varchar(10),
	id1 bigint,
	keyspace_id varbinary(50),
    primary key(id2, id1)
) Engine=InnoDB DEFAULT charset=utf8mb4 COLLATE=utf8mb4_general_ci;

create table t5_null_vindex(
	id bigint not null,
	idx varchar(50),
	primary key(id)
) Engine=InnoDB;

create table t6(
	id1 bigint,
	id2 varchar(10),
	primary key(id1)
) Engine=InnoDB;

create table t6_id2_idx(
	id2 varchar(10),
	id1 bigint,
	keyspace_id varbinary(50),
	primary key(id1),
	key(id2)
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

create table t7_fk(
	id bigint,
	t7_uid varchar(50),
    primary key(id),
    CONSTRAINT t7_fk_ibfk_1 foreign key (t7_uid) references t7_xxhash(uid)
    on delete set null on update cascade
) Engine=InnoDB;
`

	VSchema = `
{
  "sharded": true,
  "vindexes": {
    "unicode_loose_xxhash" : {
	  "type": "unicode_loose_xxhash"
    },
    "unicode_loose_md5" : {
	  "type": "unicode_loose_md5"
    },
    "hash": {
      "type": "hash"
    },
    "xxhash": {
      "type": "xxhash"
    },
    "t1_id2_vdx": {
      "type": "consistent_lookup_unique",
      "params": {
        "table": "t1_id2_idx",
        "from": "id2",
        "to": "keyspace_id"
      },
      "owner": "t1"
    },
    "t2_id4_idx": {
      "type": "lookup_hash",
      "params": {
        "table": "t2_id4_idx",
        "from": "id4",
        "to": "id3",
        "autocommit": "true"
      },
      "owner": "t2"
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
    "t4_id2_vdx": {
      "type": "consistent_lookup",
      "params": {
        "table": "t4_id2_idx",
        "from": "id2,id1",
        "to": "keyspace_id"
      },
      "owner": "t4"
    },
    "t6_id2_vdx": {
      "type": "consistent_lookup",
      "params": {
        "table": "t6_id2_idx",
        "from": "id2,id1",
        "to": "keyspace_id",
        "ignore_nulls": "true"
      },
      "owner": "t6"
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
    "t1": {
      "column_vindexes": [
        {
          "column": "id1",
          "name": "hash"
        },
        {
          "column": "id2",
          "name": "t1_id2_vdx"
        }
      ]
    },
    "t1_id2_idx": {
      "column_vindexes": [
        {
          "column": "id2",
          "name": "hash"
        }
      ]
    },
    "t2": {
      "column_vindexes": [
        {
          "column": "id3",
          "name": "hash"
        },
        {
          "column": "id4",
          "name": "t2_id4_idx"
        }
      ]
    },
    "t2_id4_idx": {
      "column_vindexes": [
        {
          "column": "id4",
          "name": "hash"
        }
      ]
    },
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
	"t4": {
      "column_vindexes": [
        {
          "column": "id1",
          "name": "hash"
        },
        {
          "columns": ["id2", "id1"],
          "name": "t4_id2_vdx"
        }
      ]
    },
    "t4_id2_idx": {
      "column_vindexes": [
        {
          "column": "id2",
          "name": "unicode_loose_md5"
        }
      ]
    },
	"t6": {
      "column_vindexes": [
        {
          "column": "id1",
          "name": "hash"
        },
        {
          "columns": ["id2", "id1"],
          "name": "t6_id2_vdx"
        }
      ]
    },
    "t6_id2_idx": {
      "column_vindexes": [
        {
          "column": "id2",
          "name": "xxhash"
        }
      ]
    },
	"t5_null_vindex": {
      "column_vindexes": [
        {
          "column": "idx",
          "name": "xxhash"
        }
      ]
    },
    "vstream_test": {
      "column_vindexes": [
        {
          "column": "id",
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
    },
	"t7_fk": {
      "column_vindexes": [
        {
          "column": "t7_uid",
          "name": "unicode_loose_xxhash"
        }
      ]
    }
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
		keyspace := &cluster.Keyspace{
			Name:      KeyspaceName,
			SchemaSQL: SchemaSQL,
			VSchema:   VSchema,
		}
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, true)
		if err != nil {
			return 1
		}

		err = clusterInstance.VtctlclientProcess.ApplyRoutingRules(routingRules)
		if err != nil {
			return 1
		}

		err = clusterInstance.VtctlclientProcess.ExecuteCommand("RebuildVSchemaGraph")
		if err != nil {
			return 1
		}

		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}
