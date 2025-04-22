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

package vitessdriver

import (
	"context"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"
)

// fakeVTGateService has the server side of this fake
type (
	fakeVTGateService struct {
		execMap map[string]execMapResponse
	}

	execMapResponse struct {
		execQuery   *queryExecute
		paramsCount uint16
		result      *sqltypes.Result
		session     *vtgatepb.Session
		err         error
	}

	// queryExecute contains all the fields we use to test Execute
	queryExecute struct {
		SQL           string
		BindVariables map[string]*querypb.BindVariable
		Session       *vtgatepb.Session
	}
)

func (q *queryExecute) Equal(q2 *queryExecute) bool {
	return q.SQL == q2.SQL &&
		reflect.DeepEqual(q.BindVariables, q2.BindVariables) &&
		proto.Equal(q.Session, q2.Session)
}

// Execute is part of the VTGateService interface
func (f *fakeVTGateService) Execute(
	ctx context.Context,
	mysqlCtx vtgateservice.MySQLConnection,
	session *vtgatepb.Session,
	sql string,
	bindVariables map[string]*querypb.BindVariable,
	prepared bool,
) (*vtgatepb.Session, *sqltypes.Result, error) {
	execCase, ok := f.execMap[sql]
	if !ok {
		return session, nil, fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecute{
		SQL:           sql,
		BindVariables: bindVariables,
		Session:       session,
	}
	if !query.Equal(execCase.execQuery) {
		return session, nil, fmt.Errorf("Execute request mismatch: got %+v, want %+v", query, execCase.execQuery)
	}
	if execCase.session != nil {
		proto.Reset(session)
		proto.Merge(session, execCase.session)
	}
	return session, execCase.result, nil
}

// ExecuteBatch is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteBatch(ctx context.Context, session *vtgatepb.Session, sql []string, bindVariables []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	if len(sql) == 1 {
		execCase, ok := f.execMap[sql[0]]
		if !ok {
			return session, nil, fmt.Errorf("no match for: %s", sql)
		}
		if bindVariables == nil {
			bindVariables = make([]map[string]*querypb.BindVariable, 1)
		}
		query := &queryExecute{
			SQL:           sql[0],
			BindVariables: bindVariables[0],
			Session:       session,
		}
		if !query.Equal(execCase.execQuery) {
			return session, nil, fmt.Errorf("Execute request mismatch: got %+v, want %+v", query, execCase.execQuery)
		}
		if execCase.session != nil {
			proto.Reset(session)
			proto.Merge(session, execCase.session)
		}
		return session, []sqltypes.QueryResponse{
			{QueryResult: execCase.result},
		}, nil
	}
	return session, nil, nil
}

// StreamExecute is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecute(ctx context.Context, mysqlCtx vtgateservice.MySQLConnection, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) (*vtgatepb.Session, error) {
	execCase, ok := f.execMap[sql]
	if !ok {
		return session, fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecute{
		SQL:           sql,
		BindVariables: bindVariables,
		Session:       session,
	}
	if !query.Equal(execCase.execQuery) {
		return session, fmt.Errorf("request mismatch: got %+v, want %+v", query, execCase.execQuery)
	}
	if execCase.result != nil {
		result := &sqltypes.Result{
			Fields: execCase.result.Fields,
		}
		if err := callback(result); err != nil {
			return execCase.session, err
		}
		for _, row := range execCase.result.Rows {
			result := &sqltypes.Result{
				Rows: [][]sqltypes.Value{row},
			}
			if err := callback(result); err != nil {
				return execCase.session, err
			}
		}
	}
	return execCase.session, nil
}

// Prepare is part of the VTGateService interface
func (f *fakeVTGateService) Prepare(ctx context.Context, session *vtgatepb.Session, sql string) (*vtgatepb.Session, []*querypb.Field, uint16, error) {
	execCase, ok := f.execMap[sql]
	if !ok {
		return session, nil, 0, fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecute{
		SQL:     sql,
		Session: session,
	}
	if !query.Equal(execCase.execQuery) {
		return session, nil, 0, fmt.Errorf("Prepare request mismatch: got %+v, want %+v", query, execCase.execQuery)
	}
	if execCase.session != nil {
		proto.Reset(session)
		proto.Merge(session, execCase.session)
	}
	return session, execCase.result.Fields, execCase.paramsCount, nil
}

func (f *fakeVTGateService) CloseSession(ctx context.Context, session *vtgatepb.Session) error {
	return nil
}

func (f *fakeVTGateService) VStream(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid, filter *binlogdatapb.Filter, flags *vtgatepb.VStreamFlags, send func([]*binlogdatapb.VEvent) error) error {
	return nil
}

// ExecuteMulti is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteMulti(ctx context.Context, mysqlCtx vtgateservice.MySQLConnection, session *vtgatepb.Session, sqlString string) (newSession *vtgatepb.Session, qrs []*sqltypes.Result, err error) {
	queries, err := sqlparser.NewTestParser().SplitStatementToPieces(sqlString)
	if err != nil {
		return session, nil, err
	}
	var result *sqltypes.Result
	for _, query := range queries {
		session, result, err = f.Execute(ctx, mysqlCtx, session, query, nil, false)
		if err != nil {
			return session, qrs, err
		}
		qrs = append(qrs, result)
	}
	return session, qrs, nil
}

// StreamExecuteMulti is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteMulti(ctx context.Context, mysqlCtx vtgateservice.MySQLConnection, session *vtgatepb.Session, sqlString string, callback func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error) (*vtgatepb.Session, error) {
	queries, err := sqlparser.NewTestParser().SplitStatementToPieces(sqlString)
	if err != nil {
		return session, err
	}
	for idx, query := range queries {
		firstPacket := true
		session, err = f.StreamExecute(ctx, mysqlCtx, session, query, nil, func(result *sqltypes.Result) error {
			err = callback(sqltypes.QueryResponse{QueryResult: result}, idx < len(queries)-1, firstPacket)
			firstPacket = false
			return err
		})
		if err != nil {
			return session, err
		}
	}
	return session, nil
}

// HandlePanic is part of the VTGateService interface
func (f *fakeVTGateService) HandlePanic(err *error) {
	if x := recover(); x != nil {
		*err = fmt.Errorf("uncaught panic: %v", x)
	}
}

// CreateFakeServer returns the fake server for the tests
func CreateFakeServer() vtgateservice.VTGateService {
	return &fakeVTGateService{
		execMap: createExecMap(),
	}
}

func createExecMap() map[string]execMapResponse {
	return map[string]execMapResponse{
		"request": {
			execQuery: &queryExecute{
				SQL: "request",
				BindVariables: map[string]*querypb.BindVariable{
					"v1": sqltypes.Int64BindVariable(0),
				},
				Session: &vtgatepb.Session{
					TargetString: "@rdonly",
					Autocommit:   true,
				},
			},
			result:      &result1,
			paramsCount: 1,
			session:     nil,
		},
		"requestDates": {
			execQuery: &queryExecute{
				SQL: "requestDates",
				BindVariables: map[string]*querypb.BindVariable{
					"v1": sqltypes.Int64BindVariable(0),
				},
				Session: &vtgatepb.Session{
					TargetString: "@rdonly",
					Autocommit:   true,
				},
			},
			result:      &result2,
			paramsCount: 1,
			session:     nil,
		},
		"txRequest": {
			execQuery: &queryExecute{
				SQL: "txRequest",
				BindVariables: map[string]*querypb.BindVariable{
					"v1": sqltypes.Int64BindVariable(0),
				},
				Session: session1,
			},
			result:      &sqltypes.Result{},
			paramsCount: 1,
			session:     session2,
		},
		"distributedTxRequest": {
			execQuery: &queryExecute{
				SQL: "distributedTxRequest",
				BindVariables: map[string]*querypb.BindVariable{
					"v1": sqltypes.Int64BindVariable(1),
				},
				Session: &vtgatepb.Session{
					InTransaction: true,
					ShardSessions: []*vtgatepb.Session_ShardSession{
						{
							Target: &querypb.Target{
								Keyspace:   "ks",
								Shard:      "1",
								TabletType: topodatapb.TabletType_PRIMARY,
							},
							TransactionId: 1,
						},
					},
					TargetString: "@rdonly",
				},
			},
			result:      &sqltypes.Result{},
			paramsCount: 1,
			session:     session2,
		},
		"begin": {
			execQuery: &queryExecute{
				SQL: "begin",
				Session: &vtgatepb.Session{
					TargetString: "@primary",
					Autocommit:   true,
				},
			},
			result:  &sqltypes.Result{},
			session: session1,
		},
		"commit": {
			execQuery: &queryExecute{
				SQL:     "commit",
				Session: session2,
			},
			result: &sqltypes.Result{},
			session: &vtgatepb.Session{
				TargetString: "@primary",
				Autocommit:   true,
			},
		},
		"rollback": {
			execQuery: &queryExecute{
				SQL:     "rollback",
				Session: session2,
			},
			result: &sqltypes.Result{},
			session: &vtgatepb.Session{
				TargetString: "@primary",
			},
		},
		"use @rdonly": {
			execQuery: &queryExecute{
				SQL: "use @rdonly",
				Session: &vtgatepb.Session{
					TargetString: "@primary",
					Autocommit:   true,
				},
			},
			result: &sqltypes.Result{},
			session: &vtgatepb.Session{
				TargetString: "@rdonly",
				SessionUUID:  "1111",
			},
		},
	}
}

var result1 = sqltypes.Result{
	Fields: []*querypb.Field{
		{
			Name: "field1",
			Type: sqltypes.Int16,
		},
		{
			Name: "field2",
			Type: sqltypes.VarChar,
		},
	},
	RowsAffected: 123,
	InsertID:     72,
	Rows: [][]sqltypes.Value{
		{
			sqltypes.NewVarBinary("1"),
			sqltypes.NewVarBinary("value1"),
		},
		{
			sqltypes.NewVarBinary("2"),
			sqltypes.NewVarBinary("value2"),
		},
	},
}

var result2 = sqltypes.Result{
	Fields: []*querypb.Field{
		{
			Name: "fieldDatetime",
			Type: sqltypes.Datetime,
		},
		{
			Name: "fieldDate",
			Type: sqltypes.Date,
		},
	},
	RowsAffected: 42,
	InsertID:     73,
	Rows: [][]sqltypes.Value{
		{
			sqltypes.NewVarBinary("2009-03-29 17:22:11"),
			sqltypes.NewVarBinary("2006-07-02"),
		},
		{
			sqltypes.NewVarBinary("0000-00-00 00:00:00"),
			sqltypes.NewVarBinary("0000-00-00"),
		},
	},
}

var session1 = &vtgatepb.Session{
	InTransaction: true,
	TargetString:  "@rdonly",
}

var session2 = &vtgatepb.Session{
	InTransaction: true,
	ShardSessions: []*vtgatepb.Session_ShardSession{
		{
			Target: &querypb.Target{
				Keyspace:   "ks",
				Shard:      "1",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 1,
		},
	},
	TargetString: "@rdonly",
}
