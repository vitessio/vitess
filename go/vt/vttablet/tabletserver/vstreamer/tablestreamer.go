/*
Copyright 2023 The Vitess Authors.

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
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/vttablet"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	schema2 "vitess.io/vitess/go/vt/schema"
)

/*
	TableStreamer is a VStreamer that streams all tables in a keyspace. It iterates through all tables in a keyspace
	and streams them one by one. It is not resilient: if there is any error that breaks the stream, for example,
	reparenting or a network error, it will not recover and a new workflow will have to be created.
*/

// TableStreamer exposes an externally usable interface to tableStreamer.
type TableStreamer interface {
	Stream() error
	Cancel()
}

type tableStreamer struct {
	ctx    context.Context
	cancel func()

	cp      dbconfigs.Connector
	se      *schema.Engine
	send    func(*binlogdatapb.VStreamTablesResponse) error
	vschema *localVSchema
	vse     *Engine

	snapshotConn *snapshotConn
	tables       []string
	gtid         string
}

func newTableStreamer(ctx context.Context, cp dbconfigs.Connector, se *schema.Engine, vschema *localVSchema,
	send func(response *binlogdatapb.VStreamTablesResponse) error, vse *Engine) *tableStreamer {
	ctx, cancel := context.WithCancel(ctx)
	return &tableStreamer{
		ctx:     ctx,
		cancel:  cancel,
		cp:      cp,
		se:      se,
		send:    send,
		vschema: vschema,
		vse:     vse,
	}
}

func (ts *tableStreamer) Cancel() {
	log.Info("TableStreamer Cancel() called")
	ts.cancel()
}

func (ts *tableStreamer) Stream() error {
	// Ensure that the schema engine is Open. If vttablet came up non_serving, it may not have been initialized.
	var err error
	if err = ts.se.Open(); err != nil {
		return err
	}

	conn, err := snapshotConnect(ts.ctx, ts.cp)
	if err != nil {
		return err
	}
	defer conn.Close()
	ts.snapshotConn = conn

	_, err = conn.ExecuteFetch("set session session_track_gtids = START_GTID", 1, false)
	if err != nil {
		// session_track_gtids = START_GTID unsupported or cannot execute. Resort to LOCK-based snapshot
		ts.gtid, err = conn.startSnapshotAllTables(ts.ctx)
	} else {
		// session_track_gtids = START_GTID supported. Get a transaction with consistent GTID without LOCKing tables.
		ts.gtid, err = conn.startSnapshotWithConsistentGTID(ts.ctx)
	}
	if err != nil {
		return err
	}

	if _, err := conn.ExecuteFetch("set names 'binary'", 1, false); err != nil {
		return err
	}
	if _, err := conn.ExecuteFetch(fmt.Sprintf("set @@session.net_read_timeout = %v", vttablet.VReplicationNetReadTimeout), 1, false); err != nil {
		return err
	}
	if _, err := conn.ExecuteFetch(fmt.Sprintf("set @@session.net_write_timeout = %v", vttablet.VReplicationNetWriteTimeout), 1, false); err != nil {
		return err
	}

	rs, err := conn.ExecuteFetch("show full tables", -1, true)
	if err != nil {
		return err
	}
	for _, row := range rs.Rows {
		tableName := row[0].ToString()
		tableType := row[1].ToString()
		if tableType != tmutils.TableBaseTable {
			continue
		}
		if schema2.IsInternalOperationTableName(tableName) {
			log.Infof("Skipping internal table %s", tableName)
			continue
		}
		ts.tables = append(ts.tables, tableName)
	}
	log.Infof("Found %d tables to stream: %s", len(ts.tables), strings.Join(ts.tables, ", "))
	for _, tableName := range ts.tables {
		log.Infof("Streaming table %s", tableName)
		if err := ts.streamTable(ts.ctx, tableName); err != nil {
			return err
		}
		log.Infof("Finished streaming table %s", tableName)
	}
	log.Infof("Finished streaming %d tables", len(ts.tables))
	return nil
}

func (ts *tableStreamer) newRowStreamer(ctx context.Context, query string, lastpk []sqltypes.Value,
	send func(*binlogdatapb.VStreamRowsResponse) error) (*rowStreamer, func(), error) {

	vse := ts.vse
	if atomic.LoadInt32(&vse.isOpen) == 0 {
		return nil, nil, errors.New("VStreamer is not open")
	}
	vse.mu.Lock()
	defer vse.mu.Unlock()

	rowStreamer := newRowStreamer(ctx, vse.env.Config().DB.FilteredWithDB(), vse.se, query, lastpk, vse.lvschema,
		send, vse, RowStreamerModeAllTables, ts.snapshotConn)

	idx := vse.streamIdx
	vse.rowStreamers[idx] = rowStreamer
	vse.streamIdx++
	// Now that we've added the stream, increment wg.
	// This must be done before releasing the lock.
	vse.wg.Add(1)

	// Remove stream from map and decrement wg when it ends.
	cancel := func() {
		vse.mu.Lock()
		defer vse.mu.Unlock()
		delete(vse.rowStreamers, idx)
		vse.wg.Done()
	}
	return rowStreamer, cancel, nil
}

func (ts *tableStreamer) streamTable(ctx context.Context, tableName string) error {
	query := fmt.Sprintf("select * from %s", sqlescape.EscapeID(tableName))

	send := func(response *binlogdatapb.VStreamRowsResponse) error {
		return ts.send(&binlogdatapb.VStreamTablesResponse{
			TableName: tableName,
			Fields:    response.GetFields(),
			Pkfields:  response.GetPkfields(),
			Gtid:      ts.gtid,
			Rows:      response.GetRows(),
			Lastpk:    response.Lastpk,
		})
	}
	rs, cancel, err := ts.newRowStreamer(ctx, query, nil, send)
	if err != nil {
		return err
	}
	defer cancel()

	if rs.Stream() != nil {
		return err
	}
	rs.vse.tableStreamerNumTables.Add(int64(1))

	return nil
}
