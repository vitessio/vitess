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
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	cmp "vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/query"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

var testMessage = "{\"message\":\"hello world\"}"
var testShardedMessagef = "{\"message\": \"hello world\", \"id\": %d}"

var createMessage = `
create table vitess_message(
	# required columns
	id bigint NOT NULL COMMENT 'often an event id, can also be auto-increment or a sequence',
	priority tinyint NOT NULL DEFAULT '50' COMMENT 'lower number priorities process first',
	epoch bigint NOT NULL DEFAULT '0' COMMENT 'Vitess increments this each time it sends a message, and is used for incremental backoff doubling',
	time_next bigint DEFAULT 0 COMMENT 'the earliest time the message will be sent in epoch nanoseconds. Must be null if time_acked is set',
	time_acked bigint DEFAULT NULL COMMENT 'the time the message was acked in epoch nanoseconds. Must be null if time_next is set',

	# add as many custom fields here as required
	# optional - these are suggestions
	tenant_id bigint,
	message json,

	# required indexes
	primary key(id),
	index next_idx(time_next),
	index poller_idx(time_acked, priority, time_next desc)

	# add any secondary indexes or foreign keys - no restrictions
) comment 'vitess_message,vt_ack_wait=1,vt_purge_after=3,vt_batch_size=2,vt_cache_size=10,vt_poller_interval=1'
`

func TestMessage(t *testing.T) {
	ctx := context.Background()

	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	streamConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer streamConn.Close()

	utils.Exec(t, conn, fmt.Sprintf("use %s", lookupKeyspace))
	utils.Exec(t, conn, createMessage)
	clusterInstance.VtctlProcess.ExecuteCommand(fmt.Sprintf("ReloadSchemaKeyspace %s", lookupKeyspace))

	defer utils.Exec(t, conn, "drop table vitess_message")

	utils.Exec(t, streamConn, "set workload = 'olap'")
	err = streamConn.ExecuteStreamFetch("stream * from vitess_message")
	require.NoError(t, err)

	wantFields := []*querypb.Field{{
		Name: "id",
		Type: sqltypes.Int64,
	}, {
		Name: "tenant_id",
		Type: sqltypes.Int64,
	}, {
		Name: "message",
		Type: sqltypes.TypeJSON,
	}}
	gotFields, err := streamConn.Fields()
	for i, field := range gotFields {
		// Remove other artifacts.
		gotFields[i] = &querypb.Field{
			Name: field.Name,
			Type: field.Type,
		}
	}
	require.NoError(t, err)
	cmp.MustMatch(t, wantFields, gotFields)

	utils.Exec(t, conn, fmt.Sprintf("insert into vitess_message(id, tenant_id, message) values(1, 1, '%s')", testMessage))

	// account for jitter in timings, maxJitter uses the current hardcoded value for jitter in message_manager.go
	jitter := int64(0)
	maxJitter := int64(1.4 * 1e9)

	// Consume first message.
	start := time.Now().UnixNano()
	got, err := streamConn.FetchNext(nil)
	require.NoError(t, err)

	want := []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(1),
		sqltypes.TestValue(sqltypes.TypeJSON, testMessage),
	}
	cmp.MustMatch(t, want, got)

	qr := utils.Exec(t, conn, "select time_next, epoch from vitess_message where id = 1")
	next, epoch := getTimeEpoch(qr)
	jitter += epoch * maxJitter
	// epoch could be 0 or 1, depending on how fast the row is updated
	switch epoch {
	case 0:
		if !(start-1e9 < next && next < (start+jitter)) {
			t.Errorf("next: %d. must be within 1s of start: %d", next/1e9, (start+jitter)/1e9)
		}
	case 1:
		if !(start < next && next < (start+jitter)+3e9) {
			t.Errorf("next: %d. must be about 1s after start: %d", next/1e9, (start+jitter)/1e9)
		}
	default:
		t.Errorf("epoch: %d, must be 0 or 1", epoch)
	}

	// Consume the resend.
	_, err = streamConn.FetchNext(nil)
	require.NoError(t, err)
	qr = utils.Exec(t, conn, "select time_next, epoch from vitess_message where id = 1")
	next, epoch = getTimeEpoch(qr)
	jitter += epoch * maxJitter
	// epoch could be 1 or 2, depending on how fast the row is updated
	switch epoch {
	case 1:
		if !(start < next && next < (start+jitter)+3e9) {
			t.Errorf("next: %d. must be about 1s after start: %d", next/1e9, (start+jitter)/1e9)
		}
	case 2:
		if !(start+2e9 < next && next < (start+jitter)+6e9) {
			t.Errorf("next: %d. must be about 3s after start: %d", next/1e9, (start+jitter)/1e9)
		}
	default:
		t.Errorf("epoch: %d, must be 1 or 2", epoch)
	}

	// Ack the message.
	qr = utils.Exec(t, conn, "update vitess_message set time_acked = 123, time_next = null where id = 1 and time_acked is null")
	assert.Equal(t, uint64(1), qr.RowsAffected)

	// Within 3+1 seconds, the row should be deleted.
	time.Sleep(4 * time.Second)
	qr = utils.Exec(t, conn, "select time_acked, epoch from vitess_message where id = 1")
	assert.Equal(t, 0, len(qr.Rows))
}

var createThreeColMessage = `
create table vitess_message3(
	# required columns
	id bigint NOT NULL COMMENT 'often an event id, can also be auto-increment or a sequence',
	priority tinyint NOT NULL DEFAULT '50' COMMENT 'lower number priorities process first',
	epoch bigint NOT NULL DEFAULT '0' COMMENT 'Vitess increments this each time it sends a message, and is used for incremental backoff doubling',
	time_next bigint DEFAULT 0 COMMENT 'the earliest time the message will be sent in epoch nanoseconds. Must be null if time_acked is set',
	time_acked bigint DEFAULT NULL COMMENT 'the time the message was acked in epoch nanoseconds. Must be null if time_next is set',

	# add as many custom fields here as required
	# optional - these are suggestions
	tenant_id bigint,
	message json,

	# custom to this test
	msg1 varchar(128),
	msg2 bigint,

	# required indexes
	primary key(id),
	index next_idx(time_next),
	index poller_idx(time_acked, priority, time_next desc)

	# add any secondary indexes or foreign keys - no restrictions
) comment 'vitess_message,vt_ack_wait=1,vt_purge_after=3,vt_batch_size=2,vt_cache_size=10,vt_poller_interval=1'
`

func TestThreeColMessage(t *testing.T) {
	ctx := context.Background()

	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	streamConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer streamConn.Close()

	utils.Exec(t, conn, fmt.Sprintf("use %s", lookupKeyspace))
	utils.Exec(t, conn, createThreeColMessage)
	defer utils.Exec(t, conn, "drop table vitess_message3")

	utils.Exec(t, streamConn, "set workload = 'olap'")
	err = streamConn.ExecuteStreamFetch("stream * from vitess_message3")
	require.NoError(t, err)

	wantFields := []*querypb.Field{{
		Name: "id",
		Type: sqltypes.Int64,
	}, {
		Name: "tenant_id",
		Type: sqltypes.Int64,
	}, {
		Name: "message",
		Type: sqltypes.TypeJSON,
	}, {
		Name: "msg1",
		Type: sqltypes.VarChar,
	}, {
		Name: "msg2",
		Type: sqltypes.Int64,
	}}
	gotFields, err := streamConn.Fields()
	for i, field := range gotFields {
		// Remove other artifacts.
		gotFields[i] = &querypb.Field{
			Name: field.Name,
			Type: field.Type,
		}
	}
	require.NoError(t, err)
	cmp.MustMatch(t, wantFields, gotFields)

	utils.Exec(t, conn, fmt.Sprintf("insert into vitess_message3(id, tenant_id, message, msg1, msg2) values(1, 3, '%s', 'hello world', 3)", testMessage))

	got, err := streamConn.FetchNext(nil)
	require.NoError(t, err)
	want := []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(3),
		sqltypes.TestValue(sqltypes.TypeJSON, testMessage),
		sqltypes.NewVarChar("hello world"),
		sqltypes.NewInt64(3),
	}
	cmp.MustMatch(t, want, got)

	// Verify Ack.
	qr := utils.Exec(t, conn, "update vitess_message3 set time_acked = 123, time_next = null where id = 1 and time_acked is null")
	assert.Equal(t, uint64(1), qr.RowsAffected)
}

var createSpecificStreamingColsMessage = `create table vitess_message4(
	# required columns
	id bigint NOT NULL COMMENT 'often an event id, can also be auto-increment or a sequence',
	priority tinyint NOT NULL DEFAULT '50' COMMENT 'lower number priorities process first',
	epoch bigint NOT NULL DEFAULT '0' COMMENT 'Vitess increments this each time it sends a message, and is used for incremental backoff doubling',
	time_next bigint DEFAULT 0 COMMENT 'the earliest time the message will be sent in epoch nanoseconds. Must be null if time_acked is set',
	time_acked bigint DEFAULT NULL COMMENT 'the time the message was acked in epoch nanoseconds. Must be null if time_next is set',

	# add as many custom fields here as required
	# optional - these are suggestions
	tenant_id bigint,
	message json,

	# custom to this test
	msg1 varchar(128),
	msg2 bigint,

	# required indexes
	primary key(id),
	index next_idx(time_next),
	index poller_idx(time_acked, priority, time_next desc)

	# add any secondary indexes or foreign keys - no restrictions
) comment 'vitess_message,vt_message_cols=id|msg1,vt_ack_wait=1,vt_purge_after=3,vt_batch_size=2,vt_cache_size=10,vt_poller_interval=1'`

func TestSpecificStreamingColsMessage(t *testing.T) {
	ctx := context.Background()

	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	streamConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer streamConn.Close()

	utils.Exec(t, conn, fmt.Sprintf("use %s", lookupKeyspace))
	utils.Exec(t, conn, createSpecificStreamingColsMessage)
	defer utils.Exec(t, conn, "drop table vitess_message4")

	utils.Exec(t, streamConn, "set workload = 'olap'")
	err = streamConn.ExecuteStreamFetch("stream * from vitess_message4")
	require.NoError(t, err)

	wantFields := []*querypb.Field{{
		Name: "id",
		Type: sqltypes.Int64,
	}, {
		Name: "msg1",
		Type: sqltypes.VarChar,
	}}
	gotFields, err := streamConn.Fields()
	for i, field := range gotFields {
		// Remove other artifacts.
		gotFields[i] = &querypb.Field{
			Name: field.Name,
			Type: field.Type,
		}
	}
	require.NoError(t, err)
	cmp.MustMatch(t, wantFields, gotFields)

	utils.Exec(t, conn, "insert into vitess_message4(id, msg1, msg2) values(1, 'hello world', 3)")

	got, err := streamConn.FetchNext(nil)
	require.NoError(t, err)
	want := []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewVarChar("hello world"),
	}
	cmp.MustMatch(t, want, got)

	// Verify Ack.
	qr := utils.Exec(t, conn, "update vitess_message4 set time_acked = 123, time_next = null where id = 1 and time_acked is null")
	assert.Equal(t, uint64(1), qr.RowsAffected)
}

func getTimeEpoch(qr *sqltypes.Result) (int64, int64) {
	if len(qr.Rows) != 1 {
		return 0, 0
	}
	t, _ := evalengine.ToInt64(qr.Rows[0][0])
	e, _ := evalengine.ToInt64(qr.Rows[0][1])
	return t, e
}

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

	assertClientCount(t, 1, shard0Primary)
	assertClientCount(t, 0, shard0Replica)
	assertClientCount(t, 1, shard1Primary)

	// do planned reparenting, make one replica as primary
	// and validate client connection count in correspond tablets
	clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"PlannedReparentShard", "--",
		"--keyspace_shard", userKeyspace+"/-80",
		"--new_primary", shard0Replica.Alias)
	// validate topology
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Validate")
	require.Nil(t, err)

	// Verify connection has migrated.
	// The wait must be at least 6s which is how long vtgate will
	// wait before retrying: that is 30s/5 where 30s is the default
	// message_stream_grace_period.
	time.Sleep(10 * time.Second)
	assertClientCount(t, 0, shard0Primary)
	assertClientCount(t, 1, shard0Replica)
	assertClientCount(t, 1, shard1Primary)
	session := stream.Session("@primary", nil)
	msg3 := fmt.Sprintf(testShardedMessagef, 3)
	cluster.ExecuteQueriesUsingVtgate(t, session, fmt.Sprintf("insert into sharded_message (id, tenant_id, message) values (3,3,'%s')", msg3))

	// validate that we have received inserted message
	stream.Next()

	// make old primary again as new primary
	clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"PlannedReparentShard", "--",
		"--keyspace_shard", userKeyspace+"/-80",
		"--new_primary", shard0Primary.Alias)
	// validate topology
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Validate")
	require.Nil(t, err)
	time.Sleep(10 * time.Second)
	assertClientCount(t, 1, shard0Primary)
	assertClientCount(t, 0, shard0Replica)
	assertClientCount(t, 1, shard1Primary)

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
	// client connection count in vttablet of the primary
	assertClientCount(t, 0, shard0Primary)
	assertClientCount(t, 0, shard1Primary)

	ctx := context.Background()
	// first connection with vtgate
	stream, err := VtgateGrpcConn(ctx, clusterInstance)
	require.Nil(t, err)
	_, err = stream.MessageStream(userKeyspace, "", nil, name)
	require.Nil(t, err)
	// validate client count of vttablet
	time.Sleep(time.Second)
	assertClientCount(t, 1, shard0Primary)
	assertClientCount(t, 1, shard1Primary)
	// second connection with vtgate, secont connection
	// will only be used for client connection counts
	stream1, err := VtgateGrpcConn(ctx, clusterInstance)
	require.Nil(t, err)
	_, err = stream1.MessageStream(userKeyspace, "", nil, name)
	require.Nil(t, err)
	// validate client count of vttablet
	time.Sleep(time.Second)
	assertClientCount(t, 2, shard0Primary)
	assertClientCount(t, 2, shard1Primary)

	// insert data in primary and validate that we receive this
	// in message stream
	session := stream.Session("@primary", nil)
	// insert data in primary
	msg2 := fmt.Sprintf(testShardedMessagef, 2)
	msg5 := fmt.Sprintf(testShardedMessagef, 5)
	cluster.ExecuteQueriesUsingVtgate(t, session, fmt.Sprintf("insert into sharded_message (id, tenant_id, message) values (2,2,'%s')", msg2))
	cluster.ExecuteQueriesUsingVtgate(t, session, fmt.Sprintf("insert into sharded_message (id, tenant_id, message) values (5,5,'%s')", msg5))
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
	assertClientCount(t, 1, shard0Primary)
	assertClientCount(t, 1, shard1Primary)

	stream1.Close()
}

func testMessaging(t *testing.T, name, ks string) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	stream, err := VtgateGrpcConn(ctx, clusterInstance)
	require.Nil(t, err)
	defer stream.Close()

	session := stream.Session("@primary", nil)
	msg4 := fmt.Sprintf(testShardedMessagef, 4)
	msg1 := fmt.Sprintf(testShardedMessagef, 1)
	cluster.ExecuteQueriesUsingVtgate(t, session, fmt.Sprintf("insert into "+name+" (id, tenant_id, message) values (4,4,'%s')", msg4))
	cluster.ExecuteQueriesUsingVtgate(t, session, fmt.Sprintf("insert into "+name+" (id, tenant_id, message) values (1,1,'%s')", msg1))

	// validate fields
	res, err := stream.MessageStream(ks, "", nil, name)
	require.Nil(t, err)
	require.Equal(t, 3, len(res.Fields))
	validateField(t, res.Fields[0], "id", query.Type_INT64)
	validateField(t, res.Fields[1], "tenant_id", query.Type_INT64)
	validateField(t, res.Fields[2], "message", query.Type_JSON)

	// validate recieved msgs
	resMap := make(map[string]string)
	res, err = stream.Next()
	require.Nil(t, err)
	for _, row := range res.Rows {
		resMap[row[0].ToString()] = row[1].ToString()
	}

	if name == "sharded_message" {
		res, err = stream.Next()
		require.Nil(t, err)
		for _, row := range res.Rows {
			resMap[row[0].ToString()] = row[1].ToString()
		}
	}

	assert.Equal(t, "1", resMap["1"])
	assert.Equal(t, "4", resMap["4"])

	resMap = make(map[string]string)
	stream.ClearMem()
	// validate message ack with id 4
	qr, err := session.Execute(context.Background(), "update "+name+" set time_acked = 1, time_next = null where id in (4) and time_acked is null", nil)
	require.Nil(t, err)
	assert.Equal(t, uint64(1), qr.RowsAffected)

	for res, err = stream.Next(); err == nil; res, err = stream.Next() {
		for _, row := range res.Rows {
			resMap[row[0].ToString()] = row[1].ToString()
		}
	}

	assert.Equal(t, "1", resMap["1"])

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
	mem      *sqltypes.Result
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
	session := stream.Session("@primary", nil)
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

			if stream.mem != nil && stream.mem.Equal(qr) {
				continue
			}

			stream.mem = qr
			stream.respChan <- qr
		}
	}()
	return qr, nil
}

// ClearMem cleares the last result stored.
func (stream *VTGateStream) ClearMem() {
	stream.mem = nil
}

// Next reads the new msg available in stream.
func (stream *VTGateStream) Next() (*sqltypes.Result, error) {
	timer := time.NewTimer(10 * time.Second)
	select {
	case s := <-stream.respChan:
		return s, nil
	case <-timer.C:
		return nil, fmt.Errorf("time limit exceeded")
	}
}

// assertClientCount read connected client count from the vttablet debug vars.
func assertClientCount(t *testing.T, expected int, vttablet *cluster.Vttablet) {
	var vars struct {
		Messages map[string]int
	}

	parseDebugVars(t, &vars, vttablet)

	got := vars.Messages["sharded_message.ClientCount"]
	if got != expected {
		t.Fatalf("wrong number of clients: got %d, expected %d. messages:\n%#v", got, expected, vars.Messages)
	}
}

func parseDebugVars(t *testing.T, output interface{}, vttablet *cluster.Vttablet) {
	debugVarURL := fmt.Sprintf("http://%s:%d/debug/vars", vttablet.VttabletProcess.TabletHostname, vttablet.HTTPPort)
	resp, err := http.Get(debugVarURL)
	if err != nil {
		t.Fatalf("failed to fetch %q: %v", debugVarURL, err)
	}

	respByte, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status code %d while fetching %q:\n%s", resp.StatusCode, debugVarURL, respByte)
	}

	if err := json.Unmarshal(respByte, output); err != nil {
		t.Fatalf("failed to unmarshal JSON from %q: %v", debugVarURL, err)
	}
}
