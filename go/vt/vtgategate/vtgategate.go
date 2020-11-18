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

// Package vtgategate implements a proxy layer to route mysql protocol
// connections to a downstream vtgate over GRPC.
package vtgategate

import (
	"context"
	"flag"
	"fmt"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"

	// Import and register the gRPC vtgateconn client
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
)

var (
	vtgateAddress    = flag.String("vtgate_address", "", "Target vtgate host")
	normalizeQueries = flag.Bool("normalize_queries", true, "Rewrite queries with bind vars. Turn this off if the app itself sends normalized queries with bind vars.")

	vtGate *VTGategate
)

// VTGategate implements the proxy layer
type VTGategate struct {
	conn   vtgateconn.Impl
	dialer vtgateconn.DialerFunc
}

// Init initializes the VTGategate daemon
func Init() error {
	vtGate = &VTGategate{}
	return vtGate.init()
}

func (vtg *VTGategate) init() error {
	vtg.dialer = grpcvtgateconn.DialWithOpts(context.TODO(), grpc.WithBackoffConfig(grpc.DefaultBackoffConfig))

	// XXX/demmer for now connect right away here but instead we need some way to defer until
	// the execute
	if *vtgateAddress == "" {
		return fmt.Errorf("-vtgate_address not specified")
	}

	conn, err := vtg.dialer(context.TODO(), *vtgateAddress)

	if err != nil {
		return fmt.Errorf("error connecting: %v", err)
	}

	vtg.conn = conn
	return nil
}

// Execute executes the query
func (vtg *VTGategate) Execute(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable) (newSession *vtgatepb.Session, qr *sqltypes.Result, err error) {
	return vtg.conn.Execute(ctx, session, sql, bindVariables)
}

// ExecuteBatch executes a batch query
func (vtg *VTGategate) ExecuteBatch(ctx context.Context, session *vtgatepb.Session, sqlList []string, bindVariablesList []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	return vtg.conn.ExecuteBatch(ctx, session, sqlList, bindVariablesList)
}

// StreamExecute executes a stream query
func (vtg *VTGategate) StreamExecute(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error {
	return fmt.Errorf("XXX/demmer implement")
	//	return vtg.conn.StreamExecute(ctx, session, sql, bindVariables, callback)
}

// CloseSession ends a session
func (vtg *VTGategate) CloseSession(ctx context.Context, session *vtgatepb.Session) error {
	return nil
}

// Prepare prepares a statement
func (vtg *VTGategate) Prepare(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable) (newSession *vtgatepb.Session, fld []*querypb.Field, err error) {
	return nil, nil, fmt.Errorf("Prepared statements not supported")
}
