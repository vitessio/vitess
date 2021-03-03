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
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	"context"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer"
)

// NewReplicaConnector returns replica connector
//
// This is used by binlog server to make vstream connection
// using the vstream connection, it will parse the events from binglog
// to fetch the corresponding GTID for required recovery time
func NewReplicaConnector(connParams *mysql.ConnParams) *replicaConnector {

	// Construct
	config := tabletenv.NewDefaultConfig()
	dbCfg := &dbconfigs.DBConfigs{
		Host: connParams.Host,
		Port: connParams.Port,
	}
	dbCfg.SetDbParams(*connParams, *connParams)
	config.DB = dbCfg
	c := &replicaConnector{conn: connParams}
	env := tabletenv.NewEnv(config, "source")
	c.se = schema.NewEngine(env)
	c.se.SkipMetaCheck = true
	c.vstreamer = vstreamer.NewEngine(env, nil, c.se, nil, "")
	c.se.InitDBConfig(dbconfigs.New(connParams))

	// Open

	c.vstreamer.Open()

	return c
}

//-----------------------------------------------------------

type replicaConnector struct {
	conn      *mysql.ConnParams
	se        *schema.Engine
	vstreamer *vstreamer.Engine
}

func (c *replicaConnector) shutdown() {
	c.vstreamer.Close()
	c.se.Close()
}

func (c *replicaConnector) Open(ctx context.Context) error {
	return nil
}

func (c *replicaConnector) Close(ctx context.Context) error {
	c.shutdown()
	return nil
}

func (c *replicaConnector) VStream(ctx context.Context, startPos string, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	return c.vstreamer.Stream(ctx, startPos, nil, filter, send)
}

// VStreamRows streams rows from query result
func (c *replicaConnector) VStreamRows(ctx context.Context, query string, lastpk *querypb.QueryResult, send func(*binlogdatapb.VStreamRowsResponse) error) error {
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
