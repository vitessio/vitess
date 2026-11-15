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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/vtgate/executorcontext"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// rawTablet is a tablet that returns one result for every method that returns
// rows, and records for each call whether its request carried
// queryservice.ContextWithRawResultCharset.
type rawTablet struct {
	queryservice.QueryService

	result func() *sqltypes.Result

	mu     sync.Mutex
	marked map[string]bool
}

func (r *rawTablet) record(ctx context.Context, name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.marked[name] = queryservice.IsRawResultCharset(ctx)
}

func (r *rawTablet) stream(ctx context.Context, name string, callback func(*sqltypes.Result) error) error {
	r.record(ctx, name)
	qr := r.result()
	if err := callback(&sqltypes.Result{Fields: qr.Fields}); err != nil {
		return err
	}
	return callback(&sqltypes.Result{Rows: qr.Rows})
}

func (r *rawTablet) Execute(ctx context.Context, _ queryservice.Session, _ *querypb.Target, _ string, _ map[string]*querypb.BindVariable, _, _ int64, _ *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	r.record(ctx, "Execute")
	return r.result(), nil
}

func (r *rawTablet) StreamExecute(ctx context.Context, _ queryservice.Session, _ *querypb.Target, _ string, _ map[string]*querypb.BindVariable, _ int64, _ int64, _ *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	return r.stream(ctx, "StreamExecute", callback)
}

func (r *rawTablet) BeginExecute(ctx context.Context, _ queryservice.Session, _ *querypb.Target, _ []string, _ string, _ map[string]*querypb.BindVariable, _ int64, _ *querypb.ExecuteOptions) (queryservice.TransactionState, *sqltypes.Result, error) {
	r.record(ctx, "BeginExecute")
	return queryservice.TransactionState{TransactionID: 1}, r.result(), nil
}

func (r *rawTablet) BeginStreamExecute(ctx context.Context, _ queryservice.Session, _ *querypb.Target, _ []string, _ string, _ map[string]*querypb.BindVariable, _ int64, _ *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (queryservice.TransactionState, error) {
	return queryservice.TransactionState{TransactionID: 1}, r.stream(ctx, "BeginStreamExecute", callback)
}

func (r *rawTablet) MessageStream(ctx context.Context, _ *querypb.Target, _ string, callback func(*sqltypes.Result) error) error {
	return r.stream(ctx, "MessageStream", callback)
}

func (r *rawTablet) ReserveBeginExecute(ctx context.Context, _ queryservice.Session, _ *querypb.Target, _ []string, _ []string, _ string, _ map[string]*querypb.BindVariable, _ *querypb.ExecuteOptions) (queryservice.ReservedTransactionState, *sqltypes.Result, error) {
	r.record(ctx, "ReserveBeginExecute")
	return queryservice.ReservedTransactionState{ReservedID: 1, TransactionID: 1}, r.result(), nil
}

func (r *rawTablet) ReserveBeginStreamExecute(ctx context.Context, _ queryservice.Session, _ *querypb.Target, _ []string, _ []string, _ string, _ map[string]*querypb.BindVariable, _ *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (queryservice.ReservedTransactionState, error) {
	return queryservice.ReservedTransactionState{ReservedID: 1, TransactionID: 1}, r.stream(ctx, "ReserveBeginStreamExecute", callback)
}

func (r *rawTablet) ReserveExecute(ctx context.Context, _ queryservice.Session, _ *querypb.Target, _ []string, _ string, _ map[string]*querypb.BindVariable, _ int64, _ *querypb.ExecuteOptions) (queryservice.ReservedState, *sqltypes.Result, error) {
	r.record(ctx, "ReserveExecute")
	return queryservice.ReservedState{ReservedID: 1}, r.result(), nil
}

func (r *rawTablet) ReserveStreamExecute(ctx context.Context, _ queryservice.Session, _ *querypb.Target, _ []string, _ string, _ map[string]*querypb.BindVariable, _ int64, _ *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (queryservice.ReservedState, error) {
	return queryservice.ReservedState{ReservedID: 1}, r.stream(ctx, "ReserveStreamExecute", callback)
}

// Begin is called by none of the methods of the test, and returns no rows; it
// records the mark to show that requests without rows carry it as well.
func (r *rawTablet) Begin(ctx context.Context, _ queryservice.Session, _ *querypb.Target, _ *querypb.ExecuteOptions) (queryservice.TransactionState, error) {
	r.record(ctx, "Begin")
	return queryservice.TransactionState{TransactionID: 1}, nil
}

// callAll calls every method of qs that returns rows, and returns the result
// of each, with the packets of a stream put together.
func callAll(t *testing.T, ctx context.Context, qs queryservice.QueryService, target *querypb.Target) map[string]*sqltypes.Result {
	t.Helper()
	results := map[string]*sqltypes.Result{}
	collect := func(name string) func(*sqltypes.Result) error {
		results[name] = &sqltypes.Result{}
		return func(qr *sqltypes.Result) error {
			if qr.Fields != nil {
				results[name].Fields = qr.Fields
			}
			results[name].Rows = append(results[name].Rows, qr.Rows...)
			return nil
		}
	}
	var err error
	results["Execute"], err = qs.Execute(ctx, nil, target, "select l, n from t", nil, 0, 0, nil)
	require.NoError(t, err)
	require.NoError(t, qs.StreamExecute(ctx, nil, target, "select l, n from t", nil, 0, 0, nil, collect("StreamExecute")))
	_, results["BeginExecute"], err = qs.BeginExecute(ctx, nil, target, nil, "select l, n from t", nil, 0, nil)
	require.NoError(t, err)
	_, err = qs.BeginStreamExecute(ctx, nil, target, nil, "select l, n from t", nil, 0, nil, collect("BeginStreamExecute"))
	require.NoError(t, err)
	require.NoError(t, qs.MessageStream(ctx, target, "msg", collect("MessageStream")))
	_, results["ReserveBeginExecute"], err = qs.ReserveBeginExecute(ctx, nil, target, nil, nil, "select l, n from t", nil, nil)
	require.NoError(t, err)
	_, err = qs.ReserveBeginStreamExecute(ctx, nil, target, nil, nil, "select l, n from t", nil, nil, collect("ReserveBeginStreamExecute"))
	require.NoError(t, err)
	_, results["ReserveExecute"], err = qs.ReserveExecute(ctx, nil, target, nil, "select l, n from t", nil, 0, nil)
	require.NoError(t, err)
	_, err = qs.ReserveStreamExecute(ctx, nil, target, nil, "select l, n from t", nil, 0, nil, collect("ReserveStreamExecute"))
	require.NoError(t, err)
	_, err = qs.Begin(ctx, nil, target, nil)
	require.NoError(t, err)
	return results
}

// rawLatin1Result is a latin1_german2_ci VARCHAR(10) with 'Müller' and an INT
// as a tablet with --queryserver-config-raw-result-charset returns them to
// vtgate: 4D FC 6C 6C 65 72 with the collation 31 and the length 10, as MySQL
// 8.0.46 and 8.4.13 send it with character_set_results = NULL.
func rawLatin1Result() *sqltypes.Result {
	return &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "l", Type: sqltypes.VarChar, Charset: 31, ColumnLength: 10},
			{Name: "n", Type: sqltypes.Int32, Charset: 63, ColumnLength: 11},
		},
		Rows: []sqltypes.Row{{sqltypes.MakeTrusted(sqltypes.VarChar, []byte("M\xFCller")), sqltypes.NewInt32(1)}},
	}
}

// convertedLatin1Result is the same as MySQL sends it with
// character_set_results = utf8mb4, which is what a tablet without the flag
// returns: 4D C3 BC 6C 6C 65 72 with the collation 255 and the length 40.
func convertedLatin1Result(collation uint32) *sqltypes.Result {
	return &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "l", Type: sqltypes.VarChar, Charset: collation, ColumnLength: 40},
			{Name: "n", Type: sqltypes.Int32, Charset: 63, ColumnLength: 11},
		},
		Rows: []sqltypes.Row{{sqltypes.MakeTrusted(sqltypes.VarChar, []byte("M\xC3\xBCller")), sqltypes.NewInt32(1)}},
	}
}

// TestTabletGatewayResultCharset checks the requests that vtgate sends to a
// tablet through the TabletGateway and through QueryServiceByAlias.
//
// With --convert-result-charset, every request is marked with
// queryservice.ContextWithRawResultCharset, and the result of every method
// that returns rows arrives converted to utf8mb4 with the collation of the
// column. A result that a tablet converted already does not change.
//
// Without it, no request is marked and every result arrives as the tablet
// sent it.
func TestTabletGatewayResultCharset(t *testing.T) {
	ctx := t.Context()
	target := &querypb.Target{Keyspace: "ks", Shard: "0", TabletType: topodatapb.TabletType_PRIMARY}
	hc := discovery.NewFakeHealthCheck(nil)
	tg := NewTabletGateway(ctx, hc, &executorcontext.FakeTopoServer{}, "cell")
	t.Cleanup(func() { _ = tg.Close(ctx) })

	tablet := &rawTablet{result: rawLatin1Result, marked: map[string]bool{}}
	hc.AddFakeTablet("cell", "1.1.1.1", 1001, "ks", "0", topodatapb.TabletType_PRIMARY, true, 10, nil, func(*topodatapb.Tablet) queryservice.QueryService {
		return tablet
	})
	var alias *topodatapb.TabletAlias
	for _, info := range hc.GetAllTablets() {
		alias = info.Alias
	}
	byAlias, err := tg.QueryServiceByAlias(ctx, alias, target)
	require.NoError(t, err)

	methods := []string{
		"Execute", "StreamExecute", "BeginExecute", "BeginStreamExecute", "MessageStream",
		"ReserveBeginExecute", "ReserveBeginStreamExecute", "ReserveExecute", "ReserveStreamExecute",
	}
	t.Cleanup(func() { convertResultCharset = false })
	for name, qs := range map[string]queryservice.QueryService{"gateway": tg, "by alias": byAlias} {
		t.Run(name, func(t *testing.T) {
			for _, convert := range []bool{false, true} {
				convertResultCharset = convert
				tablet.result = rawLatin1Result
				tablet.marked = map[string]bool{}
				want := rawLatin1Result()
				if convert {
					want = convertedLatin1Result(31)
				}
				results := callAll(t, ctx, qs, target)
				for _, method := range methods {
					assert.Equal(t, want, results[method], "%s with --convert-result-charset=%v", method, convert)
					assert.Equal(t, convert, tablet.marked[method], method)
				}
				assert.Equal(t, convert, tablet.marked["Begin"], "Begin")
			}

			// A tablet without the raw flag has converted the result itself.
			tablet.result = func() *sqltypes.Result { return convertedLatin1Result(255) }
			results := callAll(t, ctx, qs, target)
			for _, method := range methods {
				assert.Equal(t, convertedLatin1Result(255), results[method], method)
			}
		})
	}
}
