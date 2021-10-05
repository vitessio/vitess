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
	"sync"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

// parallelRowStreamer is used for copying the existing rows of a table
// before vreplication begins streaming binlogs. The rowStreamer
// responds to a request with the GTID position as of which it
// streams the rows of a table. This allows vreplication to synchronize
// its events as of the returned GTID before adding the new rows.
// For every set of rows sent, the last pk value is also sent.
// This allows for the streaming to be resumed based on the last
// pk value processed.
type parallelRowStreamer struct {
	ctx    context.Context
	cancel func()

	cp      dbconfigs.Connector
	se      *schema.Engine
	send    func(*binlogdatapb.VStreamRowsResponse) error
	vschema *localVSchema

	vse *Engine

	queries []string
	lastpks [][]sqltypes.Value

	// tableRowStreamerMap maps a table name and a single streamer for that table
	tableRowStreamerMap map[string]*singleRowStreamer

	// mutex will be used to serialize single streamer send()ings
	mu sync.Mutex
}

func newParallelRowStreamer(ctx context.Context, cp dbconfigs.Connector, se *schema.Engine, queries []string, lastpks [][]sqltypes.Value, vschema *localVSchema, send func(*binlogdatapb.VStreamRowsResponse) error, vse *Engine) *parallelRowStreamer {
	ctx, cancel := context.WithCancel(ctx)
	return &parallelRowStreamer{
		ctx:     ctx,
		cancel:  cancel,
		cp:      cp,
		se:      se,
		send:    send,
		vschema: vschema,
		vse:     vse,

		queries:             queries,
		lastpks:             lastpks,
		tableRowStreamerMap: map[string]*singleRowStreamer{},
	}
}

func (rs *parallelRowStreamer) Cancel() {
	log.Info("Rowstreamer Cancel() called")
	rs.cancel()
}

func (rs *parallelRowStreamer) Stream() error {
	// Ensure sh is Open. If vttablet came up in a non_serving role,
	// the schema engine may not have been initialized.
	if err := rs.se.Open(); err != nil {
		return err
	}
	if err := rs.buildPlans(); err != nil {
		return err
	}
	lockConn, err := snapshotConnect(rs.ctx, rs.cp)
	if err != nil {
		return err
	}
	defer lockConn.Close()

	for _, streamer := range rs.tableRowStreamerMap {
		conn, err := snapshotConnect(rs.ctx, rs.cp)
		if err != nil {
			return err
		}
		defer conn.Close()

		streamer.conn = conn
	}
	return rs.streamQueries(lockConn)
}

// buildPlans builds a plan for each query
func (rs *parallelRowStreamer) buildPlans() error {
	if len(rs.queries) != len(rs.lastpks) {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "mismatching length of queries (%v) and lastpks (%v)", len(rs.queries), len(rs.lastpks))
	}
	for i := range rs.queries {
		if err := rs.buildPlan(rs.queries[i], rs.lastpks[i]); err != nil {
			return err
		}
	}
	return nil
}

func (rs *parallelRowStreamer) buildPlan(query string, lastpk []sqltypes.Value) error {
	// This pre-parsing is required to extract the table name
	// and create its metadata.
	sel, fromTable, err := analyzeSelect(query)
	if err != nil {
		return err
	}
	st, err := rs.se.GetTableForPos(fromTable, "")
	if err != nil {
		return err
	}
	ti := &Table{
		Name:   st.Name,
		Fields: st.Fields,
	}
	// The plan we build is identical to the one for vstreamer.
	// This is because the row format of a read is identical
	// to the row format of a binlog event. So, the same
	// filtering will work.
	plan, err := buildTablePlan(ti, rs.vschema, query)
	if err != nil {
		log.Errorf("%s", err.Error())
		return err
	}
	streamer := newSingleRowStreamer(rs, plan, lastpk)

	if err := streamer.analyzeCommentDirectives(sel.Comments); err != nil {
		return err
	}
	if err := streamer.buildPKColumns(st); err != nil {
		return err
	}
	if err := streamer.buildSelect(); err != nil {
		return err
	}
	rs.tableRowStreamerMap[ti.Name] = streamer
	return err
}

// tableNames returns a slice of all table names handled by this streamer
func (rs *parallelRowStreamer) tableNames() []string {
	names := []string{}
	for name := range rs.tableRowStreamerMap {
		names = append(names, name)
	}
	return names
}

func (rs *parallelRowStreamer) streamQueries(lockConn *snapshotConn) error {

	handler := func() error {
		for _, streamer := range rs.tableRowStreamerMap {
			if err := streamer.conn.startTransactionWithConsistentSnapshot(rs.ctx); err != nil {
				return err
			}
		}
		return nil
	}
	gtid, err := lockConn.lockTablesWithHandler(rs.ctx, rs.tableNames(), handler)
	if err != nil {
		return err
	}
	// status at this point: we have a GTID, and multiple (one per table) transactions with consistent snapshots.
	// Tables are unlocked, and the GTID is consitent for all snapshots.
	// We can now proceed to read from the tables.

	// Run all sigle row streamers in parallel.
	// Wait for all to complete.
	// Collect errors, propagate a representative non-nil error if any.
	var wg sync.WaitGroup
	var errMu sync.Mutex
	for _, streamer := range rs.tableRowStreamerMap {
		streamer := streamer
		wg.Add(1)
		go func() {
			defer wg.Done()
			streamError := streamer.streamQuery(rs.ctx, gtid)
			if streamError != nil {
				errMu.Lock()
				defer errMu.Unlock()
				err = streamError
			}
		}()
	}
	wg.Wait()

	return err
}

// atomicSend is called by the single streamers, and makes sure to serialize calls to the send function
func (rs *parallelRowStreamer) atomicSend(resp *binlogdatapb.VStreamRowsResponse) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.send(resp)
}
