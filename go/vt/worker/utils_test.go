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

package worker

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/faketmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This file contains common test helper.

func runCommand(t *testing.T, wi *Instance, wr *wrangler.Wrangler, args []string) error {
	// Limit the scope of the context e.g. to implicitly terminate stray Go
	// routines.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker, done, err := wi.RunCommand(ctx, args, wr, false /* runFromCli */)
	if err != nil {
		return vterrors.Wrap(err, "Worker creation failed")
	}
	if err := wi.WaitForCommand(worker, done); err != nil {
		return vterrors.Wrap(err, "Worker failed")
	}

	t.Logf("Got status: %v", worker.StatusAsText())
	if worker.State() != WorkerStateDone {
		return vterrors.Wrap(err, "Worker finished but not successfully")
	}
	return nil
}

// sourceRdonlyFakeDB fakes out the MIN, MAX query on the primary key.
// (This query is used to calculate the split points for reading a table
// using multiple threads.)
func sourceRdonlyFakeDB(t *testing.T, dbName, tableName string, min, max int) *fakesqldb.DB {
	f := fakesqldb.New(t).OrderMatters()
	f.AddExpectedExecuteFetch(fakesqldb.ExpectedExecuteFetch{
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
					sqltypes.NewInt64(int64(min)),
					sqltypes.NewInt64(int64(max)),
				},
			},
		},
	})
	f.EnableInfinite()
	return f
}

// fakeTMCTopo is a FakeTabletManagerClient extension that implements ChangeType
// using the provided topo server.
type fakeTMCTopo struct {
	tmclient.TabletManagerClient
	server *topo.Server
}

func newFakeTMCTopo(ts *topo.Server) tmclient.TabletManagerClient {
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
