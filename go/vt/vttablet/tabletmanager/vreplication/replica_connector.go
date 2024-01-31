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
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	"context"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer"
)

// NewReplicaConnector returns replica connector
//
// This is used by binlog server to make vstream connection
// using the vstream connection, it will parse the events from binglog
// to fetch the corresponding GTID for required recovery time
func NewReplicaConnector(venv *vtenv.Environment, connParams *mysql.ConnParams) *ReplicaConnector {
	// Construct
	config := tabletenv.NewDefaultConfig()
	dbCfg := &dbconfigs.DBConfigs{
		Host: connParams.Host,
		Port: connParams.Port,
	}
	dbCfg.SetDbParams(*connParams, *connParams, *connParams)
	config.DB = dbCfg
	c := &ReplicaConnector{conn: connParams}
	env := tabletenv.NewEnv(venv, config, "source")
	c.se = schema.NewEngine(env)
	c.se.SkipMetaCheck = true
	c.vstreamer = vstreamer.NewEngine(env, nil, c.se, nil, "")
	c.se.InitDBConfig(dbconfigs.New(connParams))

	// Open

	c.vstreamer.Open()

	return c
}

//-----------------------------------------------------------

type ReplicaConnector struct {
	conn      *mysql.ConnParams
	se        *schema.Engine
	vstreamer *vstreamer.Engine
}

func (c *ReplicaConnector) Close() error {
	c.vstreamer.Close()
	c.se.Close()
	return nil
}

func (c *ReplicaConnector) VStream(ctx context.Context, startPos string, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	return c.vstreamer.Stream(ctx, startPos, nil, filter, throttlerapp.ReplicaConnectorName, send)
}
