// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"strconv"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vttablet/faketmclient"
	"github.com/youtube/vitess/go/vt/vttablet/tmclient"
	"github.com/youtube/vitess/go/vt/wrangler"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains common test helper.

func runCommand(t *testing.T, wi *Instance, wr *wrangler.Wrangler, args []string) error {
	// Limit the scope of the context e.g. to implicitly terminate stray Go
	// routines.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker, done, err := wi.RunCommand(ctx, args, wr, false /* runFromCli */)
	if err != nil {
		return fmt.Errorf("Worker creation failed: %v", err)
	}
	if err := wi.WaitForCommand(worker, done); err != nil {
		return fmt.Errorf("Worker failed: %v", err)
	}

	t.Logf("Got status: %v", worker.StatusAsText())
	if worker.State() != WorkerStateDone {
		return fmt.Errorf("Worker finished but not successfully: %v", err)
	}
	return nil
}

// expectBlpCheckpointCreationQueries fakes out the queries which vtworker
// sends out to create the Binlog Player (BLP) checkpoint.
func expectBlpCheckpointCreationQueries(f *FakePoolConnection) {
	f.addExpectedQuery("CREATE DATABASE IF NOT EXISTS _vt", nil)
	f.addExpectedQuery("CREATE TABLE IF NOT EXISTS _vt.blp_checkpoint (\n"+
		"  source_shard_uid INT(10) UNSIGNED NOT NULL,\n"+
		"  pos VARBINARY(64000) DEFAULT NULL,\n"+
		"  max_tps BIGINT(20) NOT NULL,\n"+
		"  max_replication_lag BIGINT(20) NOT NULL,\n"+
		"  time_updated BIGINT(20) UNSIGNED NOT NULL,\n"+
		"  transaction_timestamp BIGINT(20) UNSIGNED NOT NULL,\n"+
		"  flags VARBINARY(250) DEFAULT NULL,\n"+
		"  PRIMARY KEY (source_shard_uid)\n) ENGINE=InnoDB", nil)
	f.addExpectedQuery("INSERT INTO _vt.blp_checkpoint (source_shard_uid, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, flags) VALUES (0, 'MariaDB/12-34-5678', *", nil)
}

// sourceRdonlyFactory fakes out the MIN, MAX query on the primary key.
// (This query is used to calculate the split points for reading a table
// using multiple threads.)
func sourceRdonlyFactory(t *testing.T, dbName, tableName string, min, max int) func() (dbconnpool.PoolConnection, error) {
	f := NewFakePoolConnectionQuery(t, "sourceRdonly")
	f.addExpectedExecuteFetch(ExpectedExecuteFetch{
		Query: fmt.Sprintf("SELECT MIN(`id`), MAX(`id`) FROM `%s`.`%s`", dbName, tableName),
		QueryResult: &sqltypes.Result{
			Fields: []*querypb.Field{
				{
					Name: "min",
					Type: sqltypes.Int64,
				},
				{
					Name: "max",
					Type: sqltypes.Int64,
				},
			},
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte(strconv.Itoa(min))),
					sqltypes.MakeString([]byte(strconv.Itoa(max))),
				},
			},
		},
	})
	f.enableInfinite()
	return f.getFactory()
}

// fakeTMCTopo is a FakeTabletManagerClient extension that implements ChangeType
// using the provided topo server.
type fakeTMCTopo struct {
	tmclient.TabletManagerClient
	server topo.Server
}

func newFakeTMCTopo(ts topo.Server) tmclient.TabletManagerClient {
	return &fakeTMCTopo{
		TabletManagerClient: faketmclient.NewFakeTabletManagerClient(),
		server:              ts,
	}
}

// ChangeType is part of the tmclient.TabletManagerClient interface.
func (f *fakeTMCTopo) ChangeType(ctx context.Context, tablet *topodatapb.Tablet, dbType topodatapb.TabletType) error {
	_, err := f.server.UpdateTabletFields(ctx, tablet.Alias, func(t *topodatapb.Tablet) error {
		t.Type = dbType
		return nil
	})
	return err
}
