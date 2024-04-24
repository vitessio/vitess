package vreplication

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	uksSchema = `
create table product (id int, mfg_id int, cat_id int, name varchar(128), primary key(id));
create table cat (id int, name varchar(128), primary key(id));
create table mfg (id int, name varchar(128), primary key(id));
`
	sksSchema = `
create table product (id int, mfg_id int, cat_id int, name varchar(128), primary key(id));
create table cat (id int, name varchar(128), primary key(id));
create table mfg2 (id int, name varchar(128), primary key(id));
`
	uksVSchema = `
{
	"sharded": false,
	"tables": {
		"product": {},
		"cat": {},
		"mfg": {}
	}
}`

	sksVSchema = `
{	
	"sharded": true,
	"tables": {
		"product": {
			"column_vindexes": [
				{	
					"column": "id",	
					"name": "hash"
				}
			]
		},
  		"cat": {
			"type": "reference",
      		"source": "uks.cat"
		},
		"mfg2": {
			"type": "reference",
		  		"source": "uks.mfg"
		}
	},
	"vindexes": {
		"hash": {
			"type": "hash"
		}
	}
}`
	materializeCatSpec = `
{
	"workflow": "wfCat", 
	"source_keyspace": "uks", 
	"target_keyspace": "sks", 
	"table_settings": [ {"target_table": "cat", "source_expression": "select id, name from cat"  }] 
}`
	materializeMfgSpec = `
{
	"workflow": "wfMfg",
	"source_keyspace": "uks",
	"target_keyspace": "sks",
	"table_settings": [ {"target_table": "mfg2", "source_expression": "select id, name from mfg"  }] 
}`
	initializeTables = `
use uks;
insert into product values (1, 1, 1, 'p1');
insert into product values (2, 2, 2, 'p2');
insert into product values (3, 3, 3, 'p3');
insert into cat values (1, 'c1');
insert into cat values (2, 'c2');
insert into cat values (3, 'c3');
insert into mfg values (1, 'm1');
insert into mfg values (2, 'm2');
insert into mfg values (3, 'm3');
`
)

func TestReferenceTableMaterializationAndRouting(t *testing.T) {
	var err error
	defaultCellName := "zone1"
	allCells := []string{defaultCellName}
	allCellNames = defaultCellName
	vc = NewVitessCluster(t, "TestReferenceTableMaterializationAndRouting", allCells, mainClusterConfig)
	defer vc.TearDown(t)
	defaultReplicas = 0 // because of CI resource constraints we can only run this test with primary tablets
	defer func() { defaultReplicas = 1 }()
	uks := "uks"
	sks := "sks"

	defaultCell := vc.Cells[defaultCellName]
	vc.AddKeyspace(t, []*Cell{defaultCell}, uks, "0", uksVSchema, uksSchema, defaultReplicas, defaultRdonly, 100, nil)
	vc.AddKeyspace(t, []*Cell{defaultCell}, sks, "-80,80-", sksVSchema, sksSchema, defaultReplicas, defaultRdonly, 200, nil)
	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	verifyClusterHealth(t, vc)
	_, _, err = vtgateConn.ExecuteFetchMulti(initializeTables, 0, false)
	require.NoError(t, err)
	materialize(t, materializeCatSpec, false)
	materialize(t, materializeMfgSpec, false)

	tabDash80 := vc.getPrimaryTablet(t, sks, "-80")
	tab80Dash := vc.getPrimaryTablet(t, sks, "80-")
	catchup(t, tabDash80, "wfCat", "Materialize Category")
	catchup(t, tab80Dash, "wfCat", "Materialize Category")
	catchup(t, tabDash80, "wfMfg", "Materialize Manufacturer")
	catchup(t, tab80Dash, "wfMfg", "Materialize Manufacturer")

	vtgateConn.Close()
	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	waitForRowCount(t, vtgateConn, sks, "cat", 3)
	waitForRowCount(t, vtgateConn, sks, "mfg2", 3)

	insertQuery := "insert into mfg values (4, 'm4')"
	_, err = vtgateConn.ExecuteFetch(insertQuery, 0, false)
	require.Contains(t, err.Error(), "table mfg not found")

	insertQuery = "insert into mfg2 values (4, 'm4')"
	_, err = vtgateConn.ExecuteFetch(insertQuery, 0, false)
	require.Contains(t, err.Error(), "Table 'vt_uks.mfg2' doesn't exist")

	insertQuery = "insert into uks.mfg values (4, 'm4')"
	_, err = vtgateConn.ExecuteFetch(insertQuery, 0, false)
	require.NoError(t, err)
}
