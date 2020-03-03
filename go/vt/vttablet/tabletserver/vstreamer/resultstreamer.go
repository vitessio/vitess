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

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
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
}

func newResultStreamer(ctx context.Context, cp dbconfigs.Connector, query string, send func(*binlogdatapb.VStreamResultsResponse) error) *resultStreamer {
	ctx, cancel := context.WithCancel(ctx)
	return &resultStreamer{
		ctx:    ctx,
		cancel: cancel,
		cp:     cp,
		query:  query,
		send:   send,
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
		err = rs.send(response)
		if err != nil {
			return err
		}
	}

	return nil
}

func (rs *resultStreamer) startStreaming(conn *mysql.Conn) (string, error) {
	lockConn, err := rs.mysqlConnect()
	if err != nil {
		return "", err
	}
	// To be safe, always unlock tables, even if lock tables might fail.
	defer func() {
		_, err := lockConn.ExecuteFetch("unlock tables", 0, false)
		if err != nil {
			log.Warning("Unlock tables failed: %v", err)
		} else {
			log.Infof("Tables unlocked", rs.tableName)
		}
		lockConn.Close()
	}()

	log.Infof("Locking table %s for copying", rs.tableName)
	if _, err := lockConn.ExecuteFetch(fmt.Sprintf("lock tables %s read", sqlparser.String(rs.tableName)), 0, false); err != nil {
		return "", err
	}
	pos, err := lockConn.MasterPosition()
	if err != nil {
		return "", err
	}

	if err := conn.ExecuteStreamFetch(rs.query); err != nil {
		return "", err
	}

	return mysql.EncodePosition(pos), nil
}

func (rs *resultStreamer) mysqlConnect() (*mysql.Conn, error) {
	cp, err := rs.cp.MysqlParams()
	if err != nil {
		return nil, err
	}
	return mysql.Connect(rs.ctx, cp)
}
