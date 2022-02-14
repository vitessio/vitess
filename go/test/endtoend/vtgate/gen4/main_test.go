package vtgate

import (
	"flag"
	"os"
	"testing"

	"vitess.io/vitess/go/vt/vtgate/planbuilder"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance  *cluster.LocalProcessCluster
	vtParams         mysql.ConnParams
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
		return m.Run()
	}()
	os.Exit(exitCode)
}
