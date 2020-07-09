package vreplication

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

const (
	walesSchema = `
create table warehouse(wid int, name varchar(128), primary key (wid)); 
create table coal(cid int, type varchar(128), primary key (cid)); 
`
	newcastleSchema = `
create table mine(mid int, name varchar(128), primary key (mid));
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
insert into coal(cid, type) values (2, 'Anthracite');
`
	initialNewcastleInserts = `
insert into mine(mid, name) values (1, 'Elswick'), (2, 'Winlaton'), (3, 'Heworth'), (4, 'Tyneside'), (5, 'Quayside');
`
)

func TestShardedMoveTables(t *testing.T) {
	cellName := "zone1"

	vc = InitCluster(t, cellName)
	assert.NotNil(t, vc)

	//defer vc.TearDown()

	cell = vc.Cells[cellName]
	//vc.AddKeyspace(t, cell, "wales", "0", walesVSchema, walesSchema, 1, 0, 100)
	vc.AddKeyspace(t, cell, "wales", "-80,80-", walesVSchema, walesSchema, 1, 0, 100)
	vtgate = cell.Vtgates[0]
	assert.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "wales", "0"), 1)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "wales", "80-"), 1)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "wales", "-80"), 1)

	vtgateConn = getConnection(t, globalConfig.vtgateMySQLPort)
	defer vtgateConn.Close()

	vc.AddKeyspace(t, cell, "newcastle", "-80,80-c0,c0-", newcastleVSchema, newcastleSchema, 1, 0, 300)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "newcastle", "-80"), 1)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "newcastle", "80-c0"), 1)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", "newcastle", "c0-"), 1)

	execMultipleQueries(t, vtgateConn, "wales", initialWalesInserts)
	execMultipleQueries(t, vtgateConn, "newcastle", initialNewcastleInserts)

	workflow := "c2n"
	ksWorkflow := "newcastle." + workflow
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("MoveTables", "-cell="+cell.Name, "-workflow="+workflow,
		"-tablet_types="+"replica,rdonly", "wales", "newcastle", "coal"); err != nil {
		t.Fatalf("MoveTables command failed with %s\n %v\n", output, err)
	}

	tab1 := vc.Cells[cell.Name].Keyspaces["newcastle"].Shards["-80"].Tablets["zone1-300"].Vttablet
	tab2 := vc.Cells[cell.Name].Keyspaces["newcastle"].Shards["80-c0"].Tablets["zone1-400"].Vttablet
	tab3 := vc.Cells[cell.Name].Keyspaces["newcastle"].Shards["c0-"].Tablets["zone1-500"].Vttablet

	tabs := []*cluster.VttabletProcess{tab1, tab2, tab3}
	for _, tab := range tabs {
		if vc.WaitForVReplicationToCatchup(tab, workflow, "vt_newcastle", 1*time.Second) != nil {
			t.Fatalf("MoveTables timed out for %s:%d", ksWorkflow, tab.TabletUID)
		}
	}
	assert.Empty(t, validateCountInTablet(t, tab1, "newcastle", "coal", 1))
	assert.Empty(t, validateCountInTablet(t, tab2, "newcastle", "coal", 0))
	assert.Empty(t, validateCountInTablet(t, tab3, "newcastle", "coal", 0))
	assert.Empty(t, validateCount(t, vtgateConn, "newcastle", "newcastle.coal", 1))

}
