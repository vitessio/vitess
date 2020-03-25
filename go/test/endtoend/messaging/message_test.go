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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var createMessage = `create table vitess_message(
	id bigint,
	priority bigint default 0,
	time_next bigint default 0,
	epoch bigint,
	time_acked bigint,
	message varchar(128),
	primary key(id),
	index next_idx(priority, time_next),
	index ack_idx(time_acked))
comment 'vitess_message,vt_ack_wait=1,vt_purge_after=3,vt_batch_size=2,vt_cache_size=10,vt_poller_interval=1'`

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

	exec(t, conn, fmt.Sprintf("use %s", lookupKeyspace))
	exec(t, conn, createMessage)
	defer exec(t, conn, "drop table vitess_message")

	exec(t, streamConn, "set workload = 'olap'")
	err = streamConn.ExecuteStreamFetch("stream * from vitess_message")
	require.NoError(t, err)

	wantFields := []*querypb.Field{{
		Name: "id",
		Type: sqltypes.Int64,
	}, {
		Name: "message",
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
	assert.Equal(t, wantFields, gotFields)

	exec(t, conn, "insert into vitess_message(id, message) values(1, 'hello world')")

	// Consume first message.
	start := time.Now().UnixNano()
	got, err := streamConn.FetchNext()
	require.NoError(t, err)

	want := []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewVarChar("hello world"),
	}
	assert.Equal(t, want, got)

	qr := exec(t, conn, "select time_next, epoch from vitess_message where id = 1")
	next, epoch := getTimeEpoch(qr)
	// epoch could be 0 or 1, depending on how fast the row is updated
	switch epoch {
	case 0:
		if !(start-1e9 < next && next < start) {
			t.Errorf("next: %d. must be within 1s of start: %d", next/1e9, start/1e9)
		}
	case 1:
		if !(start < next && next < start+3e9) {
			t.Errorf("next: %d. must be about 1s after start: %d", next/1e9, start/1e9)
		}
	default:
		t.Errorf("epoch: %d, must be 0 or 1", epoch)
	}

	// Consume the resend.
	_, err = streamConn.FetchNext()
	require.NoError(t, err)
	qr = exec(t, conn, "select time_next, epoch from vitess_message where id = 1")
	next, epoch = getTimeEpoch(qr)
	// epoch could be 1 or 2, depending on how fast the row is updated
	switch epoch {
	case 1:
		if !(start < next && next < start+3e9) {
			t.Errorf("next: %d. must be about 1s after start: %d", next/1e9, start/1e9)
		}
	case 2:
		if !(start+2e9 < next && next < start+6e9) {
			t.Errorf("next: %d. must be about 3s after start: %d", next/1e9, start/1e9)
		}
	default:
		t.Errorf("epoch: %d, must be 1 or 2", epoch)
	}

	// Ack the message.
	qr = exec(t, conn, "update vitess_message set time_acked = 123, time_next = null where id = 1 and time_acked is null")
	assert.Equal(t, uint64(1), qr.RowsAffected)

	// Within 3+1 seconds, the row should be deleted.
	time.Sleep(4 * time.Second)
	qr = exec(t, conn, "select time_acked, epoch from vitess_message where id = 1")
	assert.Equal(t, 0, len(qr.Rows))
}

var createThreeColMessage = `create table vitess_message3(
	id bigint,
	priority bigint default 0,
	time_next bigint default 0,
	epoch bigint,
	time_acked bigint,
	msg1 varchar(128),
	msg2 bigint,
	primary key(id),
	index next_idx(priority, time_next),
	index ack_idx(time_acked))
comment 'vitess_message,vt_ack_wait=1,vt_purge_after=3,vt_batch_size=2,vt_cache_size=10,vt_poller_interval=1'`

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

	exec(t, conn, fmt.Sprintf("use %s", lookupKeyspace))
	exec(t, conn, createThreeColMessage)
	defer exec(t, conn, "drop table vitess_message3")

	exec(t, streamConn, "set workload = 'olap'")
	err = streamConn.ExecuteStreamFetch("stream * from vitess_message3")
	require.NoError(t, err)

	wantFields := []*querypb.Field{{
		Name: "id",
		Type: sqltypes.Int64,
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
	assert.Equal(t, wantFields, gotFields)

	exec(t, conn, "insert into vitess_message3(id, msg1, msg2) values(1, 'hello world', 3)")

	got, err := streamConn.FetchNext()
	require.NoError(t, err)
	want := []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewVarChar("hello world"),
		sqltypes.NewInt64(3),
	}
	assert.Equal(t, want, got)

	// Verify Ack.
	qr := exec(t, conn, "update vitess_message3 set time_acked = 123, time_next = null where id = 1 and time_acked is null")
	assert.Equal(t, uint64(1), qr.RowsAffected)
}

func getTimeEpoch(qr *sqltypes.Result) (int64, int64) {
	if len(qr.Rows) != 1 {
		return 0, 0
	}
	t, _ := sqltypes.ToInt64(qr.Rows[0][0])
	e, _ := sqltypes.ToInt64(qr.Rows[0][1])
	return t, e
}
