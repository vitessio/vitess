/*
Copyright 2026 The Vitess Authors.

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

package vtgate

import (
	"context"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/resultcharset"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// With --convert-result-charset, vtgate tells every tablet that it converts
// the text of results itself, and converts every result that a tablet returns
// to its connection character set.
//
// A tablet started with --queryserver-config-raw-result-charset then returns
// the text of each column as MySQL sends it with character_set_results = NULL:
// a latin1_german2_ci VARCHAR(10) with 'Müller' arrives as 4D FC 6C 6C 65 72
// with the collation 31 and the length 10. The conversion makes it 4D C3 BC 6C
// 6C 65 72 with the length 40, and the field keeps the collation of the
// column, which vtgate needs to send the column to a client that asked for
// character_set_results = NULL or binary.
//
// A tablet without the flag, or one that predates it, returns every text
// column in its connection character set with its connection collation, and
// for a utf8mb4 tablet the conversion changes nothing.
//
// Without --convert-result-charset, vtgate does neither: no request is
// marked, so a tablet with the flag converts its results itself, and vtgate
// returns what the tablet sent, as before.
//
// The conversion is applied in the TabletGateway, which every query that
// vtgate sends to a tablet goes through: the QueryService of the gateway and
// the ones of QueryServiceByAlias.

// markRawResultCharset returns a wrapper that, with --convert-result-charset,
// marks the context of every request with
// queryservice.ContextWithRawResultCharset before it calls wrapper.
func markRawResultCharset(wrapper queryservice.WrapperFunc) queryservice.WrapperFunc {
	return func(ctx context.Context, target *querypb.Target, conn queryservice.QueryService, name string, opts queryservice.WrapOpts, inner func(context.Context, *querypb.Target, queryservice.QueryService) (bool, error)) error {
		if convertResultCharset {
			ctx = queryservice.ContextWithRawResultCharset(ctx)
		}
		return wrapper(ctx, target, conn, name, opts, inner)
	}
}

// resultCharsetService converts the results of the methods of a QueryService
// that return rows to the connection collation that conn returns, with
// --convert-result-charset.
type resultCharsetService struct {
	queryservice.QueryService
	conn func() collations.ID
}

// withResultCharset returns qs with the methods that return rows converting
// their results to the connection collation that conn returns, with
// --convert-result-charset.
func withResultCharset(qs queryservice.QueryService, conn func() collations.ID) queryservice.QueryService {
	return &resultCharsetService{QueryService: qs, conn: conn}
}

func (s *resultCharsetService) conversion(fields []*querypb.Field) *resultcharset.Conversion {
	return resultcharset.ToConnection(fields, s.conn(), false)
}

func (s *resultCharsetService) result(qr *sqltypes.Result) *sqltypes.Result {
	if qr == nil || !convertResultCharset {
		return qr
	}
	return s.conversion(qr.Fields).Result(qr)
}

func (s *resultCharsetService) stream(callback func(*sqltypes.Result) error) func(*sqltypes.Result) error {
	if !convertResultCharset {
		return callback
	}
	stream := resultcharset.NewStream(s.conversion)
	return func(qr *sqltypes.Result) error {
		return callback(stream.Result(qr))
	}
}

// Execute implements queryservice.QueryService.
func (s *resultCharsetService) Execute(ctx context.Context, session queryservice.Session, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID, reservedID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	qr, err := s.QueryService.Execute(ctx, session, target, sql, bindVariables, transactionID, reservedID, options)
	return s.result(qr), err
}

// StreamExecute implements queryservice.QueryService.
func (s *resultCharsetService) StreamExecute(ctx context.Context, session queryservice.Session, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, reservedID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	return s.QueryService.StreamExecute(ctx, session, target, sql, bindVariables, transactionID, reservedID, options, s.stream(callback))
}

// BeginExecute implements queryservice.QueryService.
func (s *resultCharsetService) BeginExecute(ctx context.Context, session queryservice.Session, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions) (queryservice.TransactionState, *sqltypes.Result, error) {
	state, qr, err := s.QueryService.BeginExecute(ctx, session, target, preQueries, sql, bindVariables, reservedID, options)
	return state, s.result(qr), err
}

// BeginStreamExecute implements queryservice.QueryService.
func (s *resultCharsetService) BeginStreamExecute(ctx context.Context, session queryservice.Session, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (queryservice.TransactionState, error) {
	return s.QueryService.BeginStreamExecute(ctx, session, target, preQueries, sql, bindVariables, reservedID, options, s.stream(callback))
}

// MessageStream implements queryservice.QueryService.
func (s *resultCharsetService) MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) error {
	return s.QueryService.MessageStream(ctx, target, name, s.stream(callback))
}

// ReserveBeginExecute implements queryservice.QueryService.
func (s *resultCharsetService) ReserveBeginExecute(ctx context.Context, session queryservice.Session, target *querypb.Target, preQueries []string, postBeginQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (queryservice.ReservedTransactionState, *sqltypes.Result, error) {
	state, qr, err := s.QueryService.ReserveBeginExecute(ctx, session, target, preQueries, postBeginQueries, sql, bindVariables, options)
	return state, s.result(qr), err
}

// ReserveBeginStreamExecute implements queryservice.QueryService.
func (s *resultCharsetService) ReserveBeginStreamExecute(ctx context.Context, session queryservice.Session, target *querypb.Target, preQueries []string, postBeginQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (queryservice.ReservedTransactionState, error) {
	return s.QueryService.ReserveBeginStreamExecute(ctx, session, target, preQueries, postBeginQueries, sql, bindVariables, options, s.stream(callback))
}

// ReserveExecute implements queryservice.QueryService.
func (s *resultCharsetService) ReserveExecute(ctx context.Context, session queryservice.Session, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (queryservice.ReservedState, *sqltypes.Result, error) {
	state, qr, err := s.QueryService.ReserveExecute(ctx, session, target, preQueries, sql, bindVariables, transactionID, options)
	return state, s.result(qr), err
}

// ReserveStreamExecute implements queryservice.QueryService.
func (s *resultCharsetService) ReserveStreamExecute(ctx context.Context, session queryservice.Session, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (queryservice.ReservedState, error) {
	return s.QueryService.ReserveStreamExecute(ctx, session, target, preQueries, sql, bindVariables, transactionID, options, s.stream(callback))
}
