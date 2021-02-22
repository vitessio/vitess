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

package vreplication

import (
	"sync"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/grpcclient"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer"
)

var (
	_ VStreamerClient = (*mysqlConnector)(nil)
	_ VStreamerClient = (*tabletConnector)(nil)
)

// VStreamerClient exposes the core interface of a vstreamer
type VStreamerClient interface {
	Open(context.Context) error
	Close(context.Context) error

	// VStream streams VReplication events based on the specified filter.
	VStream(ctx context.Context, startPos string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error

	// VStreamRows streams rows of a table from the specified starting point.
	VStreamRows(ctx context.Context, query string, lastpk *querypb.QueryResult, send func(*binlogdatapb.VStreamRowsResponse) error) error
}

type externalConnector struct {
	mu         sync.Mutex
	dbconfigs  map[string]*dbconfigs.DBConfigs
	connectors map[string]*mysqlConnector
}

func newExternalConnector(dbcfgs map[string]*dbconfigs.DBConfigs) *externalConnector {
	return &externalConnector{
		dbconfigs:  dbcfgs,
		connectors: make(map[string]*mysqlConnector),
	}
}

func (ec *externalConnector) Close() {
	for _, c := range ec.connectors {
		c.shutdown()
	}
	ec.connectors = make(map[string]*mysqlConnector)
}

func (ec *externalConnector) Get(name string) (*mysqlConnector, error) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	if c, ok := ec.connectors[name]; ok {
		return c, nil
	}

	// Construct
	config := tabletenv.NewDefaultConfig()
	config.DB = ec.dbconfigs[name]
	if config.DB == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "external mysqlConnector %v not found", name)
	}
	c := &mysqlConnector{}
	c.env = tabletenv.NewEnv(config, name)
	c.se = schema.NewEngine(c.env)
	c.vstreamer = vstreamer.NewEngine(c.env, nil, c.se, nil, "")
	c.vstreamer.InitDBConfig("")
	c.se.InitDBConfig(c.env.Config().DB.AllPrivsWithDB())

	// Open
	if err := c.se.Open(); err != nil {
		return nil, vterrors.Wrapf(err, "external mysqlConnector: %v", name)
	}
	c.vstreamer.Open()

	// Register
	ec.connectors[name] = c
	return c, nil
}

//-----------------------------------------------------------

type mysqlConnector struct {
	env       tabletenv.Env
	se        *schema.Engine
	vstreamer *vstreamer.Engine
}

func (c *mysqlConnector) shutdown() {
	c.vstreamer.Close()
	c.se.Close()
}

func (c *mysqlConnector) Open(ctx context.Context) error {
	return nil
}

func (c *mysqlConnector) Close(ctx context.Context) error {
	return nil
}

func (c *mysqlConnector) VStream(ctx context.Context, startPos string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	return c.vstreamer.Stream(ctx, startPos, tablePKs, filter, send)
}

func (c *mysqlConnector) VStreamRows(ctx context.Context, query string, lastpk *querypb.QueryResult, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	var row []sqltypes.Value
	if lastpk != nil {
		r := sqltypes.Proto3ToResult(lastpk)
		if len(r.Rows) != 1 {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected lastpk input: %v", lastpk)
		}
		row = r.Rows[0]
	}
	return c.vstreamer.StreamRows(ctx, query, row, send)
}

//-----------------------------------------------------------

type tabletConnector struct {
	tablet *topodatapb.Tablet
	target *querypb.Target
	qs     queryservice.QueryService
}

func newTabletConnector(tablet *topodatapb.Tablet) *tabletConnector {
	return &tabletConnector{
		tablet: tablet,
		target: &querypb.Target{
			Keyspace:   tablet.Keyspace,
			Shard:      tablet.Shard,
			TabletType: tablet.Type,
		},
	}
}

func (tc *tabletConnector) Open(ctx context.Context) error {
	var err error
	tc.qs, err = tabletconn.GetDialer()(tc.tablet, grpcclient.FailFast(true))
	return err
}

func (tc *tabletConnector) Close(ctx context.Context) error {
	return tc.qs.Close(ctx)
}

func (tc *tabletConnector) VStream(ctx context.Context, startPos string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	return tc.qs.VStream(ctx, tc.target, startPos, tablePKs, filter, send)
}

func (tc *tabletConnector) VStreamRows(ctx context.Context, query string, lastpk *querypb.QueryResult, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	return tc.qs.VStreamRows(ctx, tc.target, query, lastpk, send)
}
