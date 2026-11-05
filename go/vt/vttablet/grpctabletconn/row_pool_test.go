/*
Copyright 2026 The Vitess Authors.

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

package grpctabletconn

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// rowsPacket builds a response of n two-column rows; the row at bigSlot, if
// any, carries a value of bigBytes.
func rowsPacket(n, bigSlot, bigBytes int) *binlogdatapb.VStreamRowsResponse {
	resp := &binlogdatapb.VStreamRowsResponse{Rows: make([]*querypb.Row, n)}
	for i := range resp.Rows {
		val := []byte("s")
		if i == bigSlot {
			val = bytes.Repeat([]byte("b"), bigBytes)
		}
		resp.Rows[i] = &querypb.Row{
			Lengths: []int64{1, int64(len(val))},
			Values:  append([]byte("1"), val...),
		}
	}
	return resp
}

// TestVStreamRowsTrimRowPool drives one pooled response through the steps of
// the VStreamRows receive loop (UnmarshalVT, trim, ResetVT) with the real
// generated code, checking that the tracker's incremental accounting matches
// the pool's real capacities, that uniform packets and heartbeats reuse the
// pool without growing or dropping it, and that a large row moving between
// slots is not retained in every slot it visited. The receive loop itself is
// covered by the tabletconntest suite (testVStreamRows).
func TestVStreamRowsTrimRowPool(t *testing.T) {
	r := binlogdatapb.VStreamRowsResponseFromVTPool()
	defer func() {
		r.Rows = nil
		r.ReturnToVTPool()
	}()
	recv := func(resp *binlogdatapb.VStreamRowsResponse) {
		data, err := resp.MarshalVT()
		require.NoError(t, err)
		require.NoError(t, r.UnmarshalVT(data))
	}
	var pool rowPoolTracker
	heartbeat := &binlogdatapb.VStreamRowsResponse{Heartbeat: true}

	// Steady state: the slots are filled once and then reused as they are.
	uniform := rowsPacket(1000, -1, 0)
	warm := 0
	for i := range 20 {
		recv(uniform)
		require.Len(t, r.Rows, 1000)
		r.Rows = pool.trim(r.Rows)
		require.NotNil(t, r.Rows, "packet %d: pool dropped under uniform rows", i)
		r.ResetVT()
		retained := sqltypes.RowPoolRetained(r.Rows)
		require.Positive(t, retained)
		require.Equal(t, retained, pool.total, "packet %d: incremental accounting drifted", i)
		if i == 0 {
			warm = retained
			continue
		}
		require.Equal(t, warm, retained, "packet %d: pool grew under uniform rows", i)

		// A heartbeat has no rows to judge the pool by and leaves it alone.
		recv(heartbeat)
		require.Empty(t, r.Rows)
		r.Rows = pool.trim(r.Rows)
		require.NotNil(t, r.Rows, "packet %d: a heartbeat dropped the pool", i)
		r.ResetVT()
	}

	// Bounded: a 4 MiB row visits a different slot on every other packet.
	// After each packet the pool is within that packet's budget, and the
	// small-row packets in between drop it rather than keep the large slot.
	const bigBytes = 4 << 20
	drops := 0
	for i := range 20 {
		for _, p := range []*binlogdatapb.VStreamRowsResponse{
			rowsPacket(200, (i*37)%200, bigBytes),
			rowsPacket(200, -1, 0),
		} {
			recv(p)
			budget := sqltypes.RowPoolPacketBudget(r.Rows)
			r.Rows = pool.trim(r.Rows)
			if r.Rows == nil {
				drops++
			}
			require.LessOrEqual(t, sqltypes.RowPoolRetained(r.Rows), budget, "packet %d", i)
			require.Equal(t, sqltypes.RowPoolRetained(r.Rows), pool.total, "packet %d: incremental accounting drifted", i)
			r.ResetVT()
		}
	}
	require.Positive(t, drops)
	require.LessOrEqual(t, sqltypes.RowPoolRetained(r.Rows), sqltypes.RowPoolMinBytes)
}
