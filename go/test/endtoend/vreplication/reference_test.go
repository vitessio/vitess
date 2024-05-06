/*
Copyright 2024 The Vitess Authors.

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
insert into mfg values (4, 'm4');
`
)

func TestReferenceTableMaterializationAndRouting(t *testing.T) {
	var err error
	defaultCellName := "zone1"
	vc = NewVitessCluster(t, nil)
	defer vc.TearDown()
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
	vtgateConn.Close()

	materialize(t, materializeCatSpec, false)
	materialize(t, materializeMfgSpec, false)

	tabDash80 := vc.getPrimaryTablet(t, sks, "-80")
	tab80Dash := vc.getPrimaryTablet(t, sks, "80-")
	catchup(t, tabDash80, "wfCat", "Materialize Category")
	catchup(t, tab80Dash, "wfCat", "Materialize Category")
	catchup(t, tabDash80, "wfMfg", "Materialize Manufacturer")
	catchup(t, tab80Dash, "wfMfg", "Materialize Manufacturer")

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	waitForRowCount(t, vtgateConn, sks, "cat", 3)
	waitForRowCount(t, vtgateConn, sks, "mfg2", 4)

	execRefQuery(t, "insert into mfg values (5, 'm5')")
	execRefQuery(t, "insert into mfg2 values (6, 'm6')")
	execRefQuery(t, "insert into uks.mfg values (7, 'm7')")
	execRefQuery(t, "insert into sks.mfg2 values (8, 'm8')")
	waitForRowCount(t, vtgateConn, uks, "mfg", 8)

	execRefQuery(t, "update mfg set name = concat(name, '-updated') where id = 1")
	execRefQuery(t, "update mfg2 set name = concat(name, '-updated') where id = 2")
	execRefQuery(t, "update uks.mfg set name = concat(name, '-updated') where id = 3")
	execRefQuery(t, "update sks.mfg2 set name = concat(name, '-updated') where id = 4")

	waitForRowCount(t, vtgateConn, uks, "mfg", 8)
	qr := execVtgateQuery(t, vtgateConn, "uks", "select count(*) from uks.mfg where name like '%updated%'")
	require.NotNil(t, qr)
	require.Equal(t, "4", qr.Rows[0][0].ToString())

	execRefQuery(t, "delete from mfg where id = 5")
	execRefQuery(t, "delete from mfg2 where id = 6")
	execRefQuery(t, "delete from uks.mfg where id = 7")
	execRefQuery(t, "delete from sks.mfg2 where id = 8")
	waitForRowCount(t, vtgateConn, uks, "mfg", 4)

}

func execRefQuery(t *testing.T, query string) {
	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	_, err := vtgateConn.ExecuteFetch(query, 0, false)
	require.NoError(t, err)
}
