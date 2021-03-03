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

package vtbench

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type clientConn interface {
	connect(ctx context.Context, cp ConnParams) error
	execute(ctx context.Context, query string, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error)
}

type mysqlClientConn struct {
	conn *mysql.Conn
}

func (c *mysqlClientConn) connect(ctx context.Context, cp ConnParams) error {
	conn, err := mysql.Connect(ctx, &mysql.ConnParams{
		Host:       cp.Hosts[0],
		Port:       cp.Port,
		DbName:     cp.DB,
		Uname:      cp.Username,
		Pass:       cp.Password,
		UnixSocket: cp.UnixSocket,
	})

	if err != nil {
		return err
	}

	c.conn = conn

	return nil
}

func (c *mysqlClientConn) execute(ctx context.Context, query string, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	if len(bindVars) != 0 {
		panic("cannot have bind vars for mysql protocol")
	}

	return c.conn.ExecuteFetch(query, 10000 /* maxrows */, true /* wantfields*/)
}

// used to ensure grpc.WithBlock() is added once to the options
var withBlockOnce sync.Once

type grpcVtgateConn struct {
	session *vtgateconn.VTGateSession
}

var vtgateConns = map[string]*vtgateconn.VTGateConn{}

func (c *grpcVtgateConn) connect(ctx context.Context, cp ConnParams) error {
	withBlockOnce.Do(func() {
		grpcclient.RegisterGRPCDialOptions(func(opts []grpc.DialOption) ([]grpc.DialOption, error) {
			return append(opts, grpc.WithBlock()), nil
		})
	})

	address := fmt.Sprintf("%v:%v", cp.Hosts[0], cp.Port)

	conn, ok := vtgateConns[address]
	if !ok {
		var err error
		conn, err = vtgateconn.DialProtocol(ctx, "grpc", address)
		if err != nil {
			return err
		}
		vtgateConns[address] = conn
	}

	c.session = conn.Session(cp.DB, nil)

	return nil
}

func (c *grpcVtgateConn) execute(ctx context.Context, query string, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return c.session.Execute(ctx, query, bindVars)
}

type grpcVttabletConn struct {
	qs     queryservice.QueryService
	target querypb.Target
}

var vttabletConns = map[string]queryservice.QueryService{}

func (c *grpcVttabletConn) connect(ctx context.Context, cp ConnParams) error {
	withBlockOnce.Do(func() {
		grpcclient.RegisterGRPCDialOptions(func(opts []grpc.DialOption) ([]grpc.DialOption, error) {
			return append(opts, grpc.WithBlock()), nil
		})
	})

	// parse the "db" into the keyspace/shard target
	keyspace, tabletType, dest, err := topoproto.ParseDestination(cp.DB, topodatapb.TabletType_MASTER)
	if err != nil {
		return err
	}

	qs, ok := vttabletConns[cp.Hosts[0]]
	if !ok {
		tablet := topodatapb.Tablet{
			Hostname: cp.Hosts[0],
			PortMap:  map[string]int32{"grpc": int32(cp.Port)},
			Keyspace: keyspace,
		}
		var err error
		qs, err = tabletconn.GetDialer()(&tablet, true)
		if err != nil {
			return err
		}
		vttabletConns[cp.Hosts[0]] = qs
	}

	c.qs = qs

	shard, ok := dest.(key.DestinationShard)
	if !ok {
		return fmt.Errorf("invalid destination shard")
	}

	c.target = querypb.Target{
		Keyspace:   keyspace,
		Shard:      string(shard),
		TabletType: tabletType,
	}

	return nil
}

func (c *grpcVttabletConn) execute(ctx context.Context, query string, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return c.qs.Execute(ctx, &c.target, query, bindVars, 0, 0, nil)
}
