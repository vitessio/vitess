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

package vstreamer

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/sqlparser"
)

// resultStreamer streams the results of the requested query
// along with the GTID of the snapshot. This is used by vdiff
// to synchronize the target to that GTID before comparing
// the results.
type resultStreamer struct {
	ctx    context.Context
	cancel func()

	cp        dbconfigs.Connector
	query     string
	tableName sqlparser.TableIdent
	send      func(*binlogdatapb.VStreamResultsResponse) error
	vse       *Engine
}

func newResultStreamer(ctx context.Context, cp dbconfigs.Connector, query string, send func(*binlogdatapb.VStreamResultsResponse) error, vse *Engine) *resultStreamer {
	ctx, cancel := context.WithCancel(ctx)
	return &resultStreamer{
		ctx:    ctx,
		cancel: cancel,
		cp:     cp,
		query:  query,
		send:   send,
		vse:    vse,
	}
}

func (rs *resultStreamer) Cancel() {
	rs.cancel()
}

func (rs *resultStreamer) Stream() error {
	_, fromTable, err := analyzeSelect(rs.query)
	if err != nil {
		return err
	}
	rs.tableName = fromTable

	conn, err := snapshotConnect(rs.ctx, rs.cp)
	if err != nil {
		return err
	}
	defer conn.Close()
	gtid, err := conn.streamWithSnapshot(rs.ctx, rs.tableName.String(), rs.query)
	if err != nil {
		return err
	}

	// first call the callback with the fields
	flds, err := conn.Fields()
	if err != nil {
		return err
	}

	err = rs.send(&binlogdatapb.VStreamResultsResponse{
		Fields: flds,
		Gtid:   gtid,
	})
	if err != nil {
		return fmt.Errorf("stream send error: %v", err)
	}

	response := &binlogdatapb.VStreamResultsResponse{}
	byteCount := 0
	for {
		select {
		case <-rs.ctx.Done():
			return fmt.Errorf("stream ended: %v", rs.ctx.Err())
		default:
		}

		// check throttler.
		if !rs.vse.throttlerClient.ThrottleCheckOKOrWait(rs.ctx) {
			continue
		}

		row, err := conn.FetchNext()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}
		response.Rows = append(response.Rows, sqltypes.RowToProto3(row))
		for _, s := range row {
			byteCount += s.Len()
		}

		if byteCount >= *PacketSize {
			rs.vse.resultStreamerNumRows.Add(int64(len(response.Rows)))
			rs.vse.resultStreamerNumPackets.Add(int64(1))
			err = rs.send(response)
			if err != nil {
				return err
			}
			// empty the rows so we start over, but we keep the
			// same capacity
			response.Rows = nil
			byteCount = 0
		}
	}

	if len(response.Rows) > 0 {
		rs.vse.resultStreamerNumRows.Add(int64(len(response.Rows)))
		err = rs.send(response)
		if err != nil {
			return err
		}
	}

	return nil
}
