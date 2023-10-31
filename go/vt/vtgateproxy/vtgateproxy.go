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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	defaultDDLStrategy = flag.String("ddl_strategy", string(schema.DDLStrategyDirect), "Set default strategy for DDL statements. Override with @@ddl_strategy session variable")
	sysVarSetEnabled   = flag.Bool("enable_system_settings", true, "This will enable the system settings to be changed per session at the database connection level")

	vtGateProxy *VTGateProxy
)

type VTGateProxy struct {
}

// CloseSession closes the session, rolling back any implicit transactions. This has the
// same effect as if a "rollback" statement was executed, but does not affect the query
// statistics.
func (proxy *VTGateProxy) CloseSession(ctx context.Context, session *vtgatepb.Session) error {
	return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "not implemented")
}

// ResolveTransaction resolves the specified 2PC transaction.
func (proxy *VTGateProxy) ResolveTransaction(ctx context.Context, dtid string) error {
	return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "not implemented")
}

// Prepare supports non-streaming prepare statement query with multi shards
func (proxy *VTGateProxy) Prepare(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable) (newSession *vtgatepb.Session, fld []*querypb.Field, err error) {
	return nil, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "not implemented")
}

func (proxy *VTGateProxy) Execute(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable) (newSession *vtgatepb.Session, qr *sqltypes.Result, err error) {
	return nil, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "not implemented")
}

func (proxy *VTGateProxy) StreamExecute(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error {
	return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "not implemented")
}

func Init() {}
