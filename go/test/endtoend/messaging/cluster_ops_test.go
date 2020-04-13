/*
Copyright 2020 The Vitess Authors.

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

package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

func TestSharded(t *testing.T) {
	// validate the messaging for sharded keyspace(user)
	testMessaging(t, "sharded_message", userKeyspace)
}

func TestUnsharded(t *testing.T) {
	// validate messaging for unsharded keyspace(lookup)
	testMessaging(t, "unsharded_message", lookupKeyspace)
}

// TestReparenting checks the client connection count after reparenting.
func TestReparenting(t *testing.T) {
	defer cluster.PanicHandler(t)
	name := "sharded_message"

	ctx := context.Background()
	// start grpc connection with vtgate and validate client
	// connection counts in tablets
	stream, err := VtgateGrpcConn(ctx, clusterInstance)
	require.Nil(t, err)
	defer stream.Close()
	_, err = stream.MessageStream(userKeyspace, "", nil, name)
	require.Nil(t, err)

	assert.Equal(t, 1, getClientCount(shard0Master))
	assert.Equal(t, 0, getClientCount(shard0Replica))
	assert.Equal(t, 1, getClientCount(shard1Master))

	// do planned reparenting, make one replica as master
	// and validate client connection count in correspond tablets
	clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"PlannedReparentShard",
		"-keyspace_shard", userKeyspace+"/-80",
		"-new_master", shard0Replica.Alias)
	// validate topology
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Validate")
	require.Nil(t, err)

	// Verify connection has migrated.
	// The wait must be at least 6s which is how long vtgate will
	// wait before retrying: that is 30s/5 where 30s is the default
	// message_stream_grace_period.
	time.Sleep(10 * time.Second)
	assert.Equal(t, 0, getClientCount(shard0Master))
	assert.Equal(t, 1, getClientCount(shard0Replica))
	assert.Equal(t, 1, getClientCount(shard1Master))
	session := stream.Session("@master", nil)
	cluster.ExecuteQueriesUsingVtgate(t, session, "insert into sharded_message (id, message) values (3,'hello world 3')")

	// validate that we have received inserted message
	stream.Next()

	// make old master again as new master
	clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"PlannedReparentShard",
		"-keyspace_shard", userKeyspace+"/-80",
		"-new_master", shard0Master.Alias)
	// validate topology
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Validate")
	require.Nil(t, err)
	time.Sleep(10 * time.Second)
	assert.Equal(t, 1, getClientCount(shard0Master))
	assert.Equal(t, 0, getClientCount(shard0Replica))
	assert.Equal(t, 1, getClientCount(shard1Master))

	_, err = session.Execute(context.Background(), "update "+name+" set time_acked = 1, time_next = null where id in (3) and time_acked is null", nil)
	require.Nil(t, err)
}

// TestConnection validate the connection count and message streaming.
func TestConnection(t *testing.T) {
	defer cluster.PanicHandler(t)

	name := "sharded_message"

	// 1 sec sleep added to avoid invalid connection count
	time.Sleep(time.Second)

	// create two grpc connection with vtgate and verify
	// client connection count in vttablet of the master
	assert.Equal(t, 0, getClientCount(shard0Master))
	assert.Equal(t, 0, getClientCount(shard1Master))

	ctx := context.Background()
	// first connection with vtgate
	stream, err := VtgateGrpcConn(ctx, clusterInstance)
	require.Nil(t, err)
	_, err = stream.MessageStream(userKeyspace, "", nil, name)
	require.Nil(t, err)
	// validate client count of vttablet
	assert.Equal(t, 1, getClientCount(shard0Master))
	assert.Equal(t, 1, getClientCount(shard1Master))
	// second connection with vtgate, secont connection
	// will only be used for client connection counts
	stream1, err := VtgateGrpcConn(ctx, clusterInstance)
	require.Nil(t, err)
	_, err = stream1.MessageStream(userKeyspace, "", nil, name)
	require.Nil(t, err)
	// validate client count of vttablet
	assert.Equal(t, 2, getClientCount(shard0Master))
	assert.Equal(t, 2, getClientCount(shard1Master))

	// insert data in master and validate that we receive this
	// in message stream
	session := stream.Session("@master", nil)
	// insert data in master
	cluster.ExecuteQueriesUsingVtgate(t, session, "insert into sharded_message (id, message) values (2,'hello world 2')")
	cluster.ExecuteQueriesUsingVtgate(t, session, "insert into sharded_message (id, message) values (5,'hello world 5')")
	// validate in msg stream
	_, err = stream.Next()
	require.Nil(t, err)
	_, err = stream.Next()
	require.Nil(t, err)

	_, err = session.Execute(context.Background(), "update "+name+" set time_acked = 1, time_next = null where id in (2, 5) and time_acked is null", nil)
	require.Nil(t, err)
	// After closing one stream, ensure vttablets have dropped it.
	stream.Close()
	time.Sleep(time.Second)
	assert.Equal(t, 1, getClientCount(shard0Master))
	assert.Equal(t, 1, getClientCount(shard1Master))

	stream1.Close()
}

func testMessaging(t *testing.T, name, ks string) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	stream, err := VtgateGrpcConn(ctx, clusterInstance)
	require.Nil(t, err)
	defer stream.Close()

	session := stream.Session("@master", nil)
	cluster.ExecuteQueriesUsingVtgate(t, session, "insert into "+name+" (id, message) values (1,'hello world 1')")
	cluster.ExecuteQueriesUsingVtgate(t, session, "insert into "+name+" (id, message) values (4,'hello world 4')")

	// validate fields
	res, err := stream.MessageStream(ks, "", nil, name)
	require.Nil(t, err)
	require.Equal(t, 2, len(res.Fields))
	validateField(t, res.Fields[0], "id", query.Type_INT64)
	validateField(t, res.Fields[1], "message", query.Type_VARCHAR)

	// validate recieved msgs
	resMap := make(map[string]string)
	res, err = stream.Next()
	require.Nil(t, err)
	for _, row := range res.Rows {
		resMap[row[0].ToString()] = row[1].ToString()
	}

	res, err = stream.Next()
	require.Nil(t, err)
	for _, row := range res.Rows {
		resMap[row[0].ToString()] = row[1].ToString()
	}

	assert.Equal(t, "hello world 1", resMap["1"])
	assert.Equal(t, "hello world 4", resMap["4"])

	resMap = make(map[string]string)
	// validate message ack with id 4
	qr, err := session.Execute(context.Background(), "update "+name+" set time_acked = 1, time_next = null where id in (4) and time_acked is null", nil)
	require.Nil(t, err)
	assert.Equal(t, uint64(1), qr.RowsAffected)
	res, err = stream.Next()
	require.Nil(t, err)
	for _, row := range res.Rows {
		resMap[row[0].ToString()] = row[1].ToString()
	}

	res, err = stream.Next()
	require.Nil(t, err)
	for _, row := range res.Rows {
		resMap[row[0].ToString()] = row[1].ToString()
	}

	assert.Equal(t, "hello world 1", resMap["1"])

	// validate message ack with 1 and 4, only 1 should be ack
	qr, err = session.Execute(context.Background(), "update "+name+" set time_acked = 1, time_next = null where id in (1, 4) and time_acked is null", nil)
	require.Nil(t, err)
	assert.Equal(t, uint64(1), qr.RowsAffected)
}

func validateField(t *testing.T, field *query.Field, name string, _type query.Type) {
	assert.Equal(t, name, field.Name)
	assert.Equal(t, _type, field.Type)
}

// MsgStream handles all meta required for grpc connection with vtgate.
type VTGateStream struct {
	ctx      context.Context
	host     string
	respChan chan *sqltypes.Result
	*vtgateconn.VTGateConn
}

// VtgateGrpcConn create new msg stream for grpc connection with vtgate.
func VtgateGrpcConn(ctx context.Context, cluster *cluster.LocalProcessCluster) (*VTGateStream, error) {
	stream := new(VTGateStream)
	stream.ctx = ctx
	stream.host = fmt.Sprintf("%s:%d", cluster.Hostname, cluster.VtgateProcess.GrpcPort)
	conn, err := vtgateconn.Dial(ctx, stream.host)
	// init components
	stream.respChan = make(chan *sqltypes.Result)
	stream.VTGateConn = conn

	return stream, err
}

// MessageStream strarts the stream for the corresponding connection.
func (stream *VTGateStream) MessageStream(ks, shard string, keyRange *topodata.KeyRange, name string) (*sqltypes.Result, error) {
	// start message stream which send received message to the respChan
	session := stream.Session("@master", nil)
	resultStream, err := session.StreamExecute(stream.ctx, fmt.Sprintf("stream * from %s", name), nil)
	if err != nil {
		return nil, err
	}
	qr, err := resultStream.Recv()
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			qr, err := resultStream.Recv()
			if err != nil {
				log.Infof("Message stream ended: %v", err)
				return
			}
			stream.respChan <- qr
		}
	}()
	return qr, nil
}

// Next reads the new msg available in stream.
func (stream *VTGateStream) Next() (*sqltypes.Result, error) {
	ticker := time.Tick(10 * time.Second)
	select {
	case s := <-stream.respChan:
		return s, nil
	case <-ticker:
		return nil, fmt.Errorf("time limit exceeded")
	}
}

// getClientCount read connected client count from the vttablet debug vars.
func getClientCount(vttablet *cluster.Vttablet) int {
	vars, err := getVar(vttablet)
	if err != nil {
		return 0
	}

	msg, ok := vars["Messages"]
	if !ok {
		return 0
	}

	v, ok := msg.(map[string]interface{})
	if !ok {
		return 0
	}

	countStr, ok := v["sharded_message.ClientCount"]
	if !ok {
		return 0
	}

	i, err := strconv.ParseInt(fmt.Sprint(countStr), 10, 16)
	if err != nil {
		return 0
	}

	return int(i)
}

// getVar read debug vars from the vttablet.
func getVar(vttablet *cluster.Vttablet) (map[string]interface{}, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s:%d/debug/vars", vttablet.VttabletProcess.TabletHostname, vttablet.HTTPPort))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == 200 {
		resultMap := make(map[string]interface{})
		respByte, _ := ioutil.ReadAll(resp.Body)
		err := json.Unmarshal(respByte, &resultMap)
		return resultMap, err
	}
	return nil, nil
}
