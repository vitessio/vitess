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

// Package vtgateservice provides to interface definition for the
// vtgate service
package vtgateservice

import (
	"context"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

// VTGateService is the interface implemented by the VTGate service,
// that RPC server implementations will call.
type VTGateService interface {
	Execute(ctx context.Context, mysqlCtx MySQLConnection, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable) (*vtgatepb.Session, *sqltypes.Result, error)
	ExecuteBatch(ctx context.Context, session *vtgatepb.Session, sqlList []string, bindVariablesList []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error)
	StreamExecute(ctx context.Context, mysqlCtx MySQLConnection, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) (*vtgatepb.Session, error)
	// Prepare statement support
	Prepare(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable) (*vtgatepb.Session, []*querypb.Field, error)

	// CloseSession closes the session, rolling back any implicit transactions.
	// This has the same effect as if a "rollback" statement was executed,
	// but does not affect the query statistics.
	CloseSession(ctx context.Context, session *vtgatepb.Session) error

	// 2PC support
	ResolveTransaction(ctx context.Context, dtid string) error

	// Update Stream methods
	VStream(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid, filter *binlogdatapb.Filter, flags *vtgatepb.VStreamFlags, send func([]*binlogdatapb.VEvent) error) error

	// HandlePanic should be called with defer at the beginning of each
	// RPC implementation method, before calling any of the previous methods
	HandlePanic(err *error)
}

// MySQLConnection is an interface that allows to execute operations on the provided connection id.
// This is used by vtgate executor to execute kill queries.
type MySQLConnection interface {
	// KillQuery stops the an executing query on the connection.
	KillQuery(uint32) error
	// KillConnection closes the connection and also stops any executing query on it.
	KillConnection(context.Context, uint32) error
}
