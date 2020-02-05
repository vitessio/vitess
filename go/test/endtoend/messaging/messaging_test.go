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
	"vitess.io/vitess/go/test/endtoend/recovery"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

func TestSharded(t *testing.T) {
	testMessaging(t, "sharded_message", userKeyspace)
}

func TestUnsharded(t *testing.T) {
	testMessaging(t, "unsharded_message", lookupKeyspace)
}

func TestRepareting(t *testing.T) {
	name := "sharded_message"

	ctx := context.Background()
	stream, err := NewConn(ctx, clusterInstance)
	require.Nil(t, err)
	defer stream.Close()
	stream.MessageStream(userKeyspace, "", nil, name)

	assert.Equal(t, 1, getClientCount(shard0Master))
	assert.Equal(t, 0, getClientCount(shard0Replica))
	assert.Equal(t, 1, getClientCount(shard1Master))

	// planned reparenting
	clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"PlannedReparentShard",
		"-keyspace_shard", userKeyspace+"/-80",
		"-new_master", shard0Replica.Alias)
	// validate topology
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Validate")
	assert.Nil(t, err)

	// Verify connection has migrated.
	// The wait must be at least 6s which is how long vtgate will
	// wait before retrying: that is 30s/5 where 30s is the default
	// message_stream_grace_period.
	time.Sleep(10 * time.Second)
	assert.Equal(t, 0, getClientCount(shard0Master))
	assert.Equal(t, 1, getClientCount(shard0Replica))
	assert.Equal(t, 1, getClientCount(shard1Master))
	session := stream.Session("@master", nil)
	recovery.ExecuteQueriesUsingVtgate(t, session, "insert into sharded_message (id, message) values (3,'hello world 3')")

	stream.Next()

	clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"PlannedReparentShard",
		"-keyspace_shard", userKeyspace+"/-80",
		"-new_master", shard0Master.Alias)
	// validate topology
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Validate")
	assert.Nil(t, err)
	time.Sleep(10 * time.Second)
	assert.Equal(t, 1, getClientCount(shard0Master))
	assert.Equal(t, 0, getClientCount(shard0Replica))
	assert.Equal(t, 1, getClientCount(shard1Master))

	stream.MessageAck(ctx, userKeyspace, name, keyRange(3))

}

func TestConnection(t *testing.T) {

	name := "sharded_message"

	assert.Equal(t, 0, getClientCount(shard0Master))
	assert.Equal(t, 0, getClientCount(shard1Master))

	ctx := context.Background()
	stream, err := NewConn(ctx, clusterInstance)
	require.Nil(t, err)
	stream.MessageStream(userKeyspace, "", nil, name)

	assert.Equal(t, 1, getClientCount(shard0Master))
	assert.Equal(t, 1, getClientCount(shard1Master))

	stream1, err := NewConn(ctx, clusterInstance)
	require.Nil(t, err)
	stream1.MessageStream(userKeyspace, "", nil, name)

	assert.Equal(t, 2, getClientCount(shard0Master))
	assert.Equal(t, 2, getClientCount(shard1Master))

	session := stream.Session("@master", nil)
	recovery.ExecuteQueriesUsingVtgate(t, session, "insert into sharded_message (id, message) values (2,'hello world 2')")
	recovery.ExecuteQueriesUsingVtgate(t, session, "insert into sharded_message (id, message) values (5,'hello world 5')")

	stream.Next()
	stream.Next()

	stream.MessageAck(ctx, userKeyspace, name, keyRange(2, 5))

	stream.Close()
	time.Sleep(1 * time.Second)
	assert.Equal(t, 1, getClientCount(shard0Master))
	assert.Equal(t, 1, getClientCount(shard1Master))

	stream1.Close()
}

func testMessaging(t *testing.T, name, ks string) {
	ctx := context.Background()
	stream, err := NewConn(ctx, clusterInstance)
	require.Nil(t, err)
	defer stream.Close()

	session := stream.Session("@master", nil)
	recovery.ExecuteQueriesUsingVtgate(t, session, "insert into "+name+" (id, message) values (1,'hello world 1')")
	recovery.ExecuteQueriesUsingVtgate(t, session, "insert into "+name+" (id, message) values (4,'hello world 4')")

	// validate fields
	res := stream.MessageStream(ks, "", nil, name)
	require.Equal(t, 3, len(res.Fields))
	validateField(t, res.Fields[0], "id", query.Type_INT64)
	validateField(t, res.Fields[1], "time_scheduled", query.Type_INT64)
	validateField(t, res.Fields[2], "message", query.Type_VARCHAR)

	// validate recieved msgs
	resMap := make(map[string]string)
	res = stream.Next()
	for _, row := range res.Rows {
		resMap[row[0].ToString()] = row[2].ToString()
	}

	res = stream.Next()
	for _, row := range res.Rows {
		resMap[row[0].ToString()] = row[2].ToString()
	}

	assert.Equal(t, "hello world 1", resMap["1"])
	assert.Equal(t, "hello world 4", resMap["4"])

	// validate message ack with id 4
	count, err := stream.MessageAck(ctx, ks, name, keyRange(4))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), count)
	res = stream.Next()
	for _, row := range res.Rows {
		resMap[row[0].ToString()] = row[2].ToString()
	}

	res = stream.Next()
	for _, row := range res.Rows {
		resMap[row[0].ToString()] = row[2].ToString()
	}

	assert.Equal(t, "hello world 1", resMap["1"])

	// validate message ack with 1 and 4, only 1 should be ack
	count, err = stream.MessageAck(ctx, ks, name, keyRange(1, 4))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), count)
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

// NewConn create new msg stream for grpc connection with vtgate.
func NewConn(ctx context.Context, cluster *cluster.LocalProcessCluster) (*VTGateStream, error) {
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
func (stream *VTGateStream) MessageStream(ks, shard string, keyRange *topodata.KeyRange, name string) *sqltypes.Result {
	go stream.VTGateConn.MessageStream(stream.ctx, ks, shard, keyRange, name, func(s *sqltypes.Result) error {
		stream.respChan <- s
		return nil
	})

	return stream.Next()
}

// Next reads the new msg available in stream.
func (stream *VTGateStream) Next() *sqltypes.Result {
	ticker := time.Tick(10 * time.Second)
	select {
	case s := <-stream.respChan:
		return s
	case <-ticker:
		panic(fmt.Errorf("time limit exceeded"))
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

// keyRange created keyRange array for correponding ids
func keyRange(s ...int) []*query.Value {
	out := make([]*query.Value, 0, len(s))

	for _, v := range s {
		q := new(query.Value)
		q.Type = query.Type_INT64
		q.Value = []byte(fmt.Sprint(v))

		out = append(out, q)
	}

	return out
}
