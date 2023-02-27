/*
Copyright 2022 The Vitess Authors.

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
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

// TestVSchemaChangesUnderLoad tests vstreamer under a load of high binlog events and simultaneous multiple vschema changes
// see https://github.com/vitessio/vitess/issues/11169
func TestVSchemaChangesUnderLoad(t *testing.T) {

	extendedTimeout := defaultTimeout * 4

	defaultCellName := "zone1"
	allCells := []string{"zone1"}
	allCellNames = "zone1"
	vc = NewVitessCluster(t, "TestVSchemaChanges", allCells, mainClusterConfig)

	require.NotNil(t, vc)

	defer vc.TearDown(t)

	defaultCell = vc.Cells[defaultCellName]
	vc.AddKeyspace(t, []*Cell{defaultCell}, "product", "0", initialProductVSchema, initialProductSchema, 1, 0, 100, sourceKsOpts)
	vtgate = defaultCell.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", "product", "0"), 1)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.replica", "product", "0"), 1)
	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()

	// ch is used to signal that there is significant data inserted into the tables and when a lot of vschema changes have been applied
	ch := make(chan bool, 1)

	ctx := context.Background()
	initialDataInserted := false
	startCid := 100
	warmupRowCount := startCid + 2000
	insertData := func() {
		timer := time.NewTimer(extendedTimeout)
		defer timer.Stop()
		log.Infof("Inserting data into customer")
		cid := startCid
		for {
			if !initialDataInserted && cid > warmupRowCount {
				log.Infof("Done inserting initial data into customer")
				initialDataInserted = true
				ch <- true
			}
			query := fmt.Sprintf("insert into customer(cid, name) values (%d, 'a')", cid)
			_, _ = vtgateConn.ExecuteFetch(query, 1, false)
			cid++
			query = "update customer set name = concat(name, 'a')"
			_, _ = vtgateConn.ExecuteFetch(query, 10000, false)
			select {
			case <-timer.C:
				log.Infof("Done inserting data into customer")
				return
			default:
			}
		}
	}
	go func() {
		log.Infof("Starting to vstream from replica")
		vgtid := &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: "product",
				Shard:    "0",
				Gtid:     "",
			}}}

		filter := &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "customer",
				Filter: "select * from customer",
			}},
		}
		conn, err := vtgateconn.Dial(ctx, net.JoinHostPort("localhost", strconv.Itoa(vc.ClusterConfig.vtgateGrpcPort)))
		require.NoError(t, err)
		defer conn.Close()

		flags := &vtgatepb.VStreamFlags{}

		ctx2, cancel := context.WithTimeout(ctx, extendedTimeout/2)
		defer cancel()
		reader, err := conn.VStream(ctx2, topodatapb.TabletType_REPLICA, vgtid, filter, flags)
		require.NoError(t, err)
		_, err = reader.Recv()
		require.NoError(t, err)
		log.Infof("About to sleep in vstreaming to block the vstream Recv() channel")
		time.Sleep(extendedTimeout)
		log.Infof("Done vstreaming")
	}()

	go insertData()
	<-ch // wait for enough data to be inserted before ApplyVSchema
	const maxApplyVSchemas = 20
	go func() {
		numApplyVSchema := 0
		timer := time.NewTimer(extendedTimeout)
		defer timer.Stop()
		log.Infof("Started ApplyVSchema")
		for {
			if err := vc.VtctlClient.ExecuteCommand("ApplyVSchema", "--", "--vschema={}", "product"); err != nil {
				log.Errorf("ApplyVSchema command failed with %+v\n", err)
				return
			}
			numApplyVSchema++
			if numApplyVSchema > maxApplyVSchemas {
				ch <- true
			}
			select {
			case <-timer.C:
				log.Infof("Done ApplyVSchema")
				ch <- true
				return
			default:
				time.Sleep(defaultTick)
			}
		}
	}()

	<-ch // wait for enough ApplyVSchema calls before doing a PRS
	if err := vc.VtctlClient.ExecuteCommand("PlannedReparentShard", "--", "--keyspace_shard", "product/0",
		"--new_primary", "zone1-101", "--wait_replicas_timeout", defaultTimeout.String()); err != nil {
		require.NoError(t, err, "PlannedReparentShard command failed")
	}
}
