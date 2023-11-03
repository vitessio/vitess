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

// Package vtgate provides a proxy service that accepts incoming mysql protocol
// connections and proxies to a vtgate using GRPC
package vtgateproxy

import (
	"context"
	"flag"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/grpcclient"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/vterrors"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

var (
	dialTimeout = flag.Duration("dial_timeout", 5*time.Second, "dialer timeout for the GRPC connection")

	defaultDDLStrategy = flag.String("ddl_strategy", string(schema.DDLStrategyDirect), "Set default strategy for DDL statements. Override with @@ddl_strategy session variable")
	sysVarSetEnabled   = flag.Bool("enable_system_settings", true, "This will enable the system settings to be changed per session at the database connection level")

	vtGateProxy *VTGateProxy = &VTGateProxy{
		targetConns: map[string]*vtgateconn.VTGateConn{},
		mu:          sync.Mutex{},
	}
)

type VTGateProxy struct {
	targetConns map[string]*vtgateconn.VTGateConn
	mu          sync.Mutex
}

func (proxy *VTGateProxy) getConnection(ctx context.Context, target string) (*vtgateconn.VTGateConn, error) {
	// If the connection exists, return it
	proxy.mu.Lock()
	conn, _ := proxy.targetConns[target]
	if conn != nil {
		proxy.mu.Unlock()
		return conn, nil
	}
	proxy.mu.Unlock()

	// Otherwise create a new connection after dropping the lock, allowing multiple requests to
	// race to create the conn for now.
	//	grpcclient.RegisterGRPCDialOptions(func(opts []grpc.DialOption) ([]grpc.DialOption, error) {
	//		return append(opts, grpc.WithBlock()), nil
	//	})

	grpcclient.RegisterGRPCDialOptions(func(opts []grpc.DialOption) ([]grpc.DialOption, error) {
		return append(opts, grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`)), nil
	})

	conn, err := vtgateconn.DialProtocol(ctx, "grpc", target)
	if err != nil {
		return nil, err
	}

	proxy.mu.Lock()
	proxy.targetConns[target] = conn
	proxy.mu.Unlock()

	return conn, nil
}

func (proxy *VTGateProxy) NewSession(ctx context.Context, options *querypb.ExecuteOptions, connectionAttributes map[string]string) (*vtgateconn.VTGateSession, error) {
	target, ok := connectionAttributes["target"]
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "no target string supplied by client")
	}

	conn, err := proxy.getConnection(ctx, target)
	if err != nil {
		return nil, err
	}

	return conn.Session("", options), nil
}

// CloseSession closes the session, rolling back any implicit transactions. This has the
// same effect as if a "rollback" statement was executed, but does not affect the query
// statistics.
func (proxy *VTGateProxy) CloseSession(ctx context.Context, session *vtgateconn.VTGateSession) error {
	return session.CloseSession(ctx)
}

// ResolveTransaction resolves the specified 2PC transaction.
func (proxy *VTGateProxy) ResolveTransaction(ctx context.Context, dtid string) error {
	return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "not implemented")
}

// Prepare supports non-streaming prepare statement query with multi shards
func (proxy *VTGateProxy) Prepare(ctx context.Context, session *vtgateconn.VTGateSession, sql string, bindVariables map[string]*querypb.BindVariable) (newsession *vtgateconn.VTGateSession, fld []*querypb.Field, err error) {
	return nil, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "not implemented")
}

func (proxy *VTGateProxy) Execute(ctx context.Context, session *vtgateconn.VTGateSession, sql string, bindVariables map[string]*querypb.BindVariable) (qr *sqltypes.Result, err error) {
	return session.Execute(ctx, sql, bindVariables)
}

func (proxy *VTGateProxy) StreamExecute(ctx context.Context, session *vtgateconn.VTGateSession, sql string, bindVariables map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error {
	stream, err := session.StreamExecute(ctx, sql, bindVariables)
	if err != nil {
		return err
	}

	for {
		qr, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		callback(qr)
	}

	return nil
}

func Init() {
}
