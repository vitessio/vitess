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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/buffer"
)

// TestGatewayBufferingWhenPrimarySwitchesServingState is used to test that the buffering mechanism buffers the queries when a primary goes to a non serving state and
// stops buffering when the primary is healthy again
func TestGatewayBufferingWhenPrimarySwitchesServingState(t *testing.T) {
	bufferImplementation = "keyspace_events"
	buffer.SetBufferingModeInTestingEnv(true)
	defer func() {
		buffer.SetBufferingModeInTestingEnv(false)
		bufferImplementation = "healthcheck"
	}()

	keyspace := "ks1"
	shard := "-80"
	tabletType := topodatapb.TabletType_PRIMARY
	host := "1.1.1.1"
	port := int32(1001)
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
	}

	ts := &fakeTopoServer{}
	// create a new fake health check. We want to check the buffering code which uses Subscribe, so we must also pass a channel
	hc := discovery.NewFakeHealthCheck(make(chan *discovery.TabletHealth))
	// create a new tablet gateway
	tg := NewTabletGateway(context.Background(), hc, ts, "cell")

	// add a primary tabelt which is serving
	sbc := hc.AddTestTablet("cell", host, port, keyspace, shard, tabletType, true, 10, nil)

	// add a result to the sandbox connection
	sqlResult1 := &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name:    "col1",
			Type:    sqltypes.VarChar,
			Charset: uint32(collations.Default()),
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeTrusted(sqltypes.VarChar, []byte("bb")),
		}},
	}
	sbc.SetResults([]*sqltypes.Result{sqlResult1})

	// run a query that we indeed get the result added to the sandbox connection back
	res, err := tg.Execute(context.Background(), target, "query", nil, 0, 0, nil)
	require.NoError(t, err)
	require.Equal(t, res, sqlResult1)

	// get the primary tablet from the fake health check
	primaryTablet := hc.GetPrimaryTablet()
	require.NotNil(t, primaryTablet)
	hc.Broadcast(primaryTablet)

	// set the serving type for the primary tablet false and broadcast it so that the buffering code registers this change
	hc.SetServing(primaryTablet, false)
	hc.Broadcast(primaryTablet)
	// add another result to the sandbox connection
	sbc.SetResults([]*sqltypes.Result{sqlResult1})

	// execute the query in a go routine since it should be buffered, and check that it eventually succeed
	queryChan := make(chan struct{})
	go func() {
		res, err = tg.Execute(context.Background(), target, "query", nil, 0, 0, nil)
		queryChan <- struct{}{}
	}()

	// set the serving type for the primary tablet true and broadcast it so that the buffering code registers this change
	// this should stop the buffering and the query executed in the go routine should work. This should be done with some delay so
	// that we know that the query was buffered
	time.Sleep(1 * time.Second)
	hc.SetServing(primaryTablet, true)
	hc.Broadcast(primaryTablet)

	// wait for the query to execute before checking for results
	select {
	case <-queryChan:
		require.NoError(t, err)
		require.Equal(t, res, sqlResult1)
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for query to execute")
	}
}

// TestGatewayBufferingWhileReparenting is used to test that the buffering mechanism buffers the queries when a PRS happens
// the healthchecks that happen during a PRS are simulated in this test
func TestGatewayBufferingWhileReparenting(t *testing.T) {
	bufferImplementation = "keyspace_events"
	buffer.SetBufferingModeInTestingEnv(true)
	defer func() {
		buffer.SetBufferingModeInTestingEnv(false)
		bufferImplementation = "healthcheck"
	}()

	keyspace := "ks1"
	shard := "-80"
	tabletType := topodatapb.TabletType_PRIMARY
	host := "1.1.1.1"
	hostReplica := "1.1.1.2"
	port := int32(1001)
	portReplica := int32(1002)
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
	}

	ts := &fakeTopoServer{}
	// create a new fake health check. We want to check the buffering code which uses Subscribe, so we must also pass a channel
	hc := discovery.NewFakeHealthCheck(make(chan *discovery.TabletHealth))
	// create a new tablet gateway
	tg := NewTabletGateway(context.Background(), hc, ts, "cell")

	// add a primary tabelt which is serving
	sbc := hc.AddTestTablet("cell", host, port, keyspace, shard, tabletType, true, 10, nil)
	// also add a replica which is serving
	sbcReplica := hc.AddTestTablet("cell", hostReplica, portReplica, keyspace, shard, topodatapb.TabletType_REPLICA, true, 0, nil)

	// add a result to the sandbox connection
	sqlResult1 := &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name:    "col1",
			Type:    sqltypes.VarChar,
			Charset: uint32(collations.Default()),
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeTrusted(sqltypes.VarChar, []byte("bb")),
		}},
	}
	sbc.SetResults([]*sqltypes.Result{sqlResult1})

	// run a query that we indeed get the result added to the sandbox connection back
	// this also checks that the query reaches the primary tablet and not the replica
	res, err := tg.Execute(context.Background(), target, "query", nil, 0, 0, nil)
	require.NoError(t, err)
	require.Equal(t, res, sqlResult1)

	// get the primary and replica tablet from the fake health check
	tablets := hc.GetAllTablets()
	var primaryTablet *topodatapb.Tablet
	var replicaTablet *topodatapb.Tablet

	for _, tablet := range tablets {
		if tablet.Type == topodatapb.TabletType_PRIMARY {
			primaryTablet = tablet
		} else {
			replicaTablet = tablet
		}
	}
	require.NotNil(t, primaryTablet)
	require.NotNil(t, replicaTablet)

	// broadcast its state initially
	hc.Broadcast(primaryTablet)
	// set the serving type for the primary tablet false and broadcast it so that the buffering code registers this change
	hc.SetServing(primaryTablet, false)
	// We call the broadcast twice to ensure that the change has been processed by the keyspace event watcher.
	// The second broadcast call is blocking until the first one has been processed.
	hc.Broadcast(primaryTablet)
	hc.Broadcast(primaryTablet)

	require.Len(t, tg.hc.GetHealthyTabletStats(target), 0, "GetHealthyTabletStats has tablets even though it shouldn't")
	_, isNotServing := tg.kev.PrimaryIsNotServing(target)
	require.True(t, isNotServing)

	// add a result to the sandbox connection of the new primary
	sbcReplica.SetResults([]*sqltypes.Result{sqlResult1})

	// execute the query in a go routine since it should be buffered, and check that it eventually succeed
	queryChan := make(chan struct{})
	go func() {
		res, err = tg.Execute(context.Background(), target, "query", nil, 0, 0, nil)
		queryChan <- struct{}{}
	}()

	// set the serving type for the new primary tablet true and broadcast it so that the buffering code registers this change
	// this should stop the buffering and the query executed in the go routine should work. This should be done with some delay so
	// that we know that the query was buffered
	time.Sleep(1 * time.Second)
	// change the tablets types to simulate a PRS.
	hc.SetTabletType(primaryTablet, topodatapb.TabletType_REPLICA)
	hc.Broadcast(primaryTablet)
	hc.SetTabletType(replicaTablet, topodatapb.TabletType_PRIMARY)
	hc.SetServing(replicaTablet, true)
	hc.Broadcast(replicaTablet)

	timeout := time.After(1 * time.Minute)
outer:
	for {
		select {
		case <-timeout:
			require.Fail(t, "timed out - could not verify the new primary")
		case <-time.After(10 * time.Millisecond):
			newPrimary, notServing := tg.kev.PrimaryIsNotServing(target)
			if newPrimary != nil && newPrimary.Uid == 1 && !notServing {
				break outer
			}
		}
	}

	// wait for the query to execute before checking for results
	select {
	case <-queryChan:
		require.NoError(t, err)
		require.Equal(t, sqlResult1, res)
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for query to execute")
	}
}

// TestInconsistentStateDetectedBuffering simulates the case where we have used up all our buffering retries and in the
// last attempt we are in an inconsistent state. Meaning that we initially thought that there are no available tablets
// but after a moment the primary is found to be serving.
// This is inconsistent and we want to fail properly. This scenario used to panic since no error and no results were
// returned.
func TestInconsistentStateDetectedBuffering(t *testing.T) {
	bufferImplementation = "keyspace_events"
	buffer.SetBufferingModeInTestingEnv(true)
	defer func() {
		buffer.SetBufferingModeInTestingEnv(false)
		bufferImplementation = "healthcheck"
	}()

	keyspace := "ks1"
	shard := "-80"
	tabletType := topodatapb.TabletType_PRIMARY
	host := "1.1.1.1"
	port := int32(1001)
	target := &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
	}

	ts := &fakeTopoServer{}
	// create a new fake health check. We want to check the buffering code which uses Subscribe, so we must also pass a channel
	hc := discovery.NewFakeHealthCheck(make(chan *discovery.TabletHealth))
	// create a new tablet gateway
	tg := NewTabletGateway(context.Background(), hc, ts, "cell")

	tg.retryCount = 0

	// add a primary tabelt which is serving
	sbc := hc.AddTestTablet("cell", host, port, keyspace, shard, tabletType, true, 10, nil)

	// add a result to the sandbox connection
	sqlResult1 := &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name:    "col1",
			Type:    sqltypes.VarChar,
			Charset: uint32(collations.Default()),
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeTrusted(sqltypes.VarChar, []byte("bb")),
		}},
	}
	sbc.SetResults([]*sqltypes.Result{sqlResult1})

	// get the primary and replica tablet from the fake health check
	tablets := hc.GetAllTablets()
	var primaryTablet *topodatapb.Tablet

	for _, tablet := range tablets {
		if tablet.Type == topodatapb.TabletType_PRIMARY {
			primaryTablet = tablet
		}
	}
	require.NotNil(t, primaryTablet)
	hc.SetServing(primaryTablet, true)
	hc.Broadcast(primaryTablet)
	hc.SetServing(primaryTablet, false)

	var res *sqltypes.Result
	var err error
	queryChan := make(chan struct{})
	go func() {
		res, err = tg.Execute(context.Background(), target, "query", nil, 0, 0, nil)
		queryChan <- struct{}{}
	}()

	select {
	case <-queryChan:
		require.Nil(t, res)
		require.Error(t, err)
		require.Equal(t, "target: ks1.-80.primary: inconsistent state detected, primary is serving but initially found no available tablet", err.Error())
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for query to execute")
	}
}
