package vreplication

import (
	"fmt"
	"strings"
	"testing"
	"time"
	"vitess.io/vitess/go/mysql"

	"github.com/stretchr/testify/assert"
)

const (
	walesSchema = `
create table warehouse(wid int, name varchar(128), primary key (wid)); 
create table coal(cid int, type varchar(128), primary key (cid)); 
`
	newcastleSchema = `
create table mine(mid int, name varchar(128), primary key (mid));
create table coal(cid int, type varchar(128), primary key (cid)); 
`
	walesVSchema = `
{
  "sharded": true,
  "vindexes": {
	    "reverse_bits": {
	      "type": "reverse_bits"
	    }
	  },
   "tables": {
	    "warehouse": {
	      "column_vindexes": [
	        {
	          "column": "wid",
	          "name": "reverse_bits"
	        }
	      ]
	    },
	    "coal": {
	      "column_vindexes": [
	        {
	          "column": "cid",
	          "name": "reverse_bits"
	        }
	      ]
	    }
   }
}
`
	newcastleVSchema = `
{
  "sharded": true,
  "vindexes": {
	    "reverse_bits": {
	      "type": "reverse_bits"
	    }
	  },
   "tables": {
	    "mine": {
	      "column_vindexes": [
	        {
	          "column": "mid",
	          "name": "reverse_bits"
	        }
	      ]
	    },
	    "coal": {
	      "column_vindexes": [
	        {
	          "column": "cid",
	          "name": "reverse_bits"
	        }
	      ]
	    }
   }
}
`
	initialWalesInserts = `
insert into warehouse(wid, name) values (1, 'Cardiff'), (2, 'Swansea');
`
	initialNewcastleInserts = `
insert into mine(mid, name) values (1, 'Elswick'), (2, 'Winlaton'), (3, 'Heworth'), (4, 'Tyneside'), (5, 'Quayside');
`
)

func insertCoal(t *testing.T, conn *mysql.Conn) {
	for i := 0; i < 10000; i ++ {
		sql := fmt.Sprintf("insert into coal (cid, type) values (%d, 'coal%d')", i, i)
		execMultipleQueries(t, conn, "wales", sql)
	}
}

func TestShardedMoveTables(t *testing.T) {
	cellName := "zone1"

	vc = InitCluster(t, cellName)
	assert.NotNil(t, vc)

	//defer vc.TearDown()

	cell = vc.Cells[cellName]
	sourceKeyspace := "wales"
	sourceShards := "-80,80-"
	vc.AddKeyspace(t, cell, sourceKeyspace, sourceShards, walesVSchema, walesSchema, 1, 0, 100)
	vtgate = cell.Vtgates[0]
	assert.NotNil(t, vtgate)
	for _, shard := range strings.Split(sourceShards, ",") {
		vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", sourceKeyspace, shard), 1)
	}

	vtgateConn = getConnection(t, globalConfig.vtgateMySQLPort)
	defer vtgateConn.Close()

	targetKeyspace := "newcastle"
	targetShards := "-40,40-80,80-c0,c0-"
	vc.AddKeyspace(t, cell, "newcastle", targetShards, newcastleVSchema, newcastleSchema, 1, 0, 300)
	for _, shard := range strings.Split(targetShards, ",") {
		vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", targetKeyspace, shard), 1)
	}

	execMultipleQueries(t, vtgateConn, "wales", initialWalesInserts)
	execMultipleQueries(t, vtgateConn, "newcastle", initialNewcastleInserts)
	insertCoal(t, vtgateConn)
	return
	workflow := "c2n"
	ksWorkflow := targetKeyspace + "." + workflow
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MoveTables", "-cell="+cell.Name, "-workflow="+workflow,
		"-tablet_types="+"replica,rdonly", sourceKeyspace, targetKeyspace, "coal"); err != nil {
		t.Fatalf("MoveTables command failed with %s\n %v\n", output, err)
	}

	tablets := vc.getVttabletsInKeyspace(t, cell, targetKeyspace, "master")
	for _, tab := range tablets {
		if vc.WaitForVReplicationToCatchup(tab, workflow, "vt_newcastle", 1*time.Second) != nil {
			t.Fatalf("MoveTables timed out for %s:%d", ksWorkflow, tab.TabletUID)
		}
	}
	for _, tab := range tablets {
		assert.Empty(t, validateCountInTablet(t, tab, "newcastle", "coal", 1))

	}
	assert.Empty(t, validateCount(t, vtgateConn, "newcastle", "newcastle.coal", 4))

}
