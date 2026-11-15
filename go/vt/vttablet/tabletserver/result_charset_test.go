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

package tabletserver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/sqlmode"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const rawCharsetQuery = "select * from test_table limit 1000"

// rawCharsetResult is a row as MySQL 8.0.46 and 8.4.13 send it with
// character_set_results = NULL: a latin1_swedish_ci VARCHAR(10) with 'Müller'
// as 4D FC 6C 6C 65 72 with the collation 8 and the length 10, a
// utf8mb4_0900_ai_ci TEXT with the length 65535 and an INT.
func rawCharsetResult() *sqltypes.Result {
	return &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "l", Type: sqltypes.VarChar, Charset: 8, ColumnLength: 10},
			{Name: "t", Type: sqltypes.Text, Charset: uint32(collations.CollationUtf8mb4ID), ColumnLength: 65535, Flags: 16},
			{Name: "n", Type: sqltypes.Int32, Charset: collations.CollationBinaryID, ColumnLength: 11, Flags: 32768},
		},
		Rows: []sqltypes.Row{{
			sqltypes.MakeTrusted(sqltypes.VarChar, []byte("M\xFCller")),
			sqltypes.MakeTrusted(sqltypes.Text, []byte("\xC3\xA9")),
			sqltypes.NewInt32(1),
		}},
	}
}

// convertedCharsetResult is the same row as MySQL sends it with
// character_set_results = utf8mb4 on a connection with utf8mb4_0900_ai_ci.
func convertedCharsetResult() *sqltypes.Result {
	return &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "l", Type: sqltypes.VarChar, Charset: uint32(collations.CollationUtf8mb4ID), ColumnLength: 40},
			{Name: "t", Type: sqltypes.Text, Charset: uint32(collations.CollationUtf8mb4ID), ColumnLength: 262140, Flags: 16},
			{Name: "n", Type: sqltypes.Int32, Charset: collations.CollationBinaryID, ColumnLength: 11, Flags: 32768},
		},
		Rows: []sqltypes.Row{{
			sqltypes.MakeTrusted(sqltypes.VarChar, []byte("M\xC3\xBCller")),
			sqltypes.MakeTrusted(sqltypes.Text, []byte("\xC3\xA9")),
			sqltypes.NewInt32(1),
		}},
	}
}

// markedCharsetResult is what a tablet returns to a caller that converts
// results itself: the text as MySQL sent it, with the length of the utf8mb4
// TEXT that MySQL sends for character_set_results = utf8mb4 (see
// resultcharset.ConnectionLengths).
func markedCharsetResult() *sqltypes.Result {
	qr := rawCharsetResult()
	qr.Fields[1].ColumnLength = 262140
	return qr
}

func setupRawCharsetTest(t *testing.T, raw bool) (*fakesqldb.DB, *TabletServer) {
	cfg := tabletenv.NewDefaultConfig()
	cfg.RawResultCharset = raw
	db, tsv := setupTabletServerTestCustom(t, t.Context(), cfg, "", vtenv.NewTestEnv())
	t.Cleanup(db.Close)
	t.Cleanup(tsv.StopService)
	db.AddQuery(rawCharsetQuery, rawCharsetResult())
	return db, tsv
}

var rawCharsetTarget = &querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

// allFields asks for the fields with their collations, as vtgate does.
func allFields() *querypb.ExecuteOptions {
	return &querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL}
}

func streamAll(t *testing.T, tsv *TabletServer, ctx context.Context, options *querypb.ExecuteOptions) *sqltypes.Result {
	t.Helper()
	got := &sqltypes.Result{}
	err := tsv.StreamExecute(ctx, nil, rawCharsetTarget, rawCharsetQuery, nil, 0, 0, options, func(qr *sqltypes.Result) error {
		if qr.Fields != nil {
			got.Fields = qr.Fields
		}
		got.Rows = append(got.Rows, qr.Rows...)
		return nil
	})
	require.NoError(t, err)
	return got
}

// TestRawResultCharsetExecute checks what a tablet that reads with
// character_set_results = NULL returns: a caller that does not mark its request
// gets what MySQL sends with character_set_results = utf8mb4, and a caller that
// marks it gets the text as MySQL sent it. A tablet without the flag returns
// the result of MySQL as it is.
func TestRawResultCharsetExecute(t *testing.T) {
	t.Run("flag off", func(t *testing.T) {
		_, tsv := setupRawCharsetTest(t, false)
		for _, ctx := range []context.Context{t.Context(), queryservice.ContextWithRawResultCharset(t.Context())} {
			got, err := tsv.Execute(ctx, nil, rawCharsetTarget, rawCharsetQuery, nil, 0, 0, allFields())
			require.NoError(t, err)
			assert.Equal(t, rawCharsetResult(), got)
			assert.Equal(t, rawCharsetResult(), streamAll(t, tsv, ctx, allFields()))
		}
	})

	_, tsv := setupRawCharsetTest(t, true)

	t.Run("caller without the mark", func(t *testing.T) {
		got, err := tsv.Execute(t.Context(), nil, rawCharsetTarget, rawCharsetQuery, nil, 0, 0, allFields())
		require.NoError(t, err)
		assert.Equal(t, convertedCharsetResult(), got)
		assert.Equal(t, convertedCharsetResult(), streamAll(t, tsv, t.Context(), allFields()))
	})

	t.Run("caller with the mark", func(t *testing.T) {
		ctx := queryservice.ContextWithRawResultCharset(t.Context())
		got, err := tsv.Execute(ctx, nil, rawCharsetTarget, rawCharsetQuery, nil, 0, 0, allFields())
		require.NoError(t, err)
		assert.Equal(t, markedCharsetResult(), got)
		assert.Equal(t, markedCharsetResult(), streamAll(t, tsv, ctx, allFields()))
	})

	t.Run("caller with the mark and without the collations", func(t *testing.T) {
		// Fields without collations cannot be converted by the caller, so
		// the tablet converts the result, and then strips the fields.
		ctx := queryservice.ContextWithRawResultCharset(t.Context())
		options := &querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_TYPE_ONLY}
		want := convertedCharsetResult().StripMetadata(querypb.ExecuteOptions_TYPE_ONLY)
		got, err := tsv.Execute(ctx, nil, rawCharsetTarget, rawCharsetQuery, nil, 0, 0, options)
		require.NoError(t, err)
		assert.Equal(t, want, got)
		assert.Equal(t, want, streamAll(t, tsv, ctx, options))
	})

	t.Run("reserved and in a transaction", func(t *testing.T) {
		state, got, err := tsv.ReserveBeginExecute(t.Context(), nil, rawCharsetTarget, nil, nil, rawCharsetQuery, nil, allFields())
		require.NoError(t, err)
		assert.Equal(t, convertedCharsetResult(), got)
		got, err = tsv.Execute(queryservice.ContextWithRawResultCharset(t.Context()), nil, rawCharsetTarget, rawCharsetQuery, nil, state.TransactionID, state.ReservedID, allFields())
		require.NoError(t, err)
		assert.Equal(t, markedCharsetResult(), got)
		require.NoError(t, tsv.Release(t.Context(), rawCharsetTarget, state.TransactionID, state.ReservedID))
	})
}

// TestRawResultCharsetConsolidator checks that two callers that share one result
// through the consolidator, one with the mark and one without, each get their
// own view of it, and that the shared result does not change.
func TestRawResultCharsetConsolidator(t *testing.T) {
	_, tsv := setupRawCharsetTest(t, true)

	shared := rawCharsetResult()
	fake := sync2.NewFakeConsolidator()
	pending := &sync2.FakePendingResult{Consolidator: fake}
	pending.SetResult(shared)
	fake.CreateReturn = &sync2.FakeConsolidatorCreateReturn{Created: false, PendingResult: pending}
	tsv.qe.consolidator = fake

	options := &querypb.ExecuteOptions{Consolidator: querypb.ExecuteOptions_CONSOLIDATOR_ENABLED, IncludedFields: querypb.ExecuteOptions_ALL}
	plain, err := tsv.Execute(t.Context(), nil, rawCharsetTarget, rawCharsetQuery, nil, 0, 0, options)
	require.NoError(t, err)
	marked, err := tsv.Execute(queryservice.ContextWithRawResultCharset(t.Context()), nil, rawCharsetTarget, rawCharsetQuery, nil, 0, 0, options)
	require.NoError(t, err)
	plainAgain, err := tsv.Execute(t.Context(), nil, rawCharsetTarget, rawCharsetQuery, nil, 0, 0, options)
	require.NoError(t, err)

	require.Len(t, fake.CreateCalls, 3)
	assert.Equal(t, convertedCharsetResult(), plain)
	assert.Equal(t, markedCharsetResult(), marked)
	assert.Equal(t, convertedCharsetResult(), plainAgain)
	assert.Equal(t, rawCharsetResult(), shared)
}

// TestRawResultCharsetFieldsOnly checks a result without rows, which is what a
// query for the fields alone (where 1 != 1) returns.
func TestRawResultCharsetFieldsOnly(t *testing.T) {
	db, tsv := setupRawCharsetTest(t, true)
	query := "select * from test_table where 1 != 1"
	db.AddQuery(query+" limit 10001", &sqltypes.Result{Fields: rawCharsetResult().Fields})

	got, err := tsv.Execute(t.Context(), nil, rawCharsetTarget, query, nil, 0, 0, allFields())
	require.NoError(t, err)
	assert.Equal(t, &sqltypes.Result{Fields: convertedCharsetResult().Fields}, got)
	got, err = tsv.Execute(queryservice.ContextWithRawResultCharset(t.Context()), nil, rawCharsetTarget, query, nil, 0, 0, allFields())
	require.NoError(t, err)
	assert.Equal(t, &sqltypes.Result{Fields: markedCharsetResult().Fields}, got)
}

// TestRawResultCharsetClientSetsResultCharset checks that a reserved connection
// on which the client set character_set_results returns results as MySQL sends
// them, to every caller: MySQL converts them for the session already.
func TestRawResultCharsetClientSetsResultCharset(t *testing.T) {
	db, tsv := setupRawCharsetTest(t, true)
	db.AddQuery("set names 'latin1'", &sqltypes.Result{})

	state, _, err := tsv.ReserveExecute(t.Context(), nil, rawCharsetTarget, nil, "set names latin1", nil, 0, nil)
	require.NoError(t, err)
	require.NotZero(t, state.ReservedID)
	t.Cleanup(func() {
		assert.NoError(t, tsv.Release(t.Context(), rawCharsetTarget, 0, state.ReservedID))
	})

	got, err := tsv.Execute(t.Context(), nil, rawCharsetTarget, rawCharsetQuery, nil, 0, state.ReservedID, allFields())
	require.NoError(t, err)
	assert.Equal(t, rawCharsetResult(), got)
	got = &sqltypes.Result{}
	err = tsv.StreamExecute(t.Context(), nil, rawCharsetTarget, rawCharsetQuery, nil, 0, state.ReservedID, allFields(), func(qr *sqltypes.Result) error {
		if qr.Fields != nil {
			got.Fields = qr.Fields
		}
		got.Rows = append(got.Rows, qr.Rows...)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, rawCharsetResult(), got)

	// another connection still converts
	got, err = tsv.Execute(t.Context(), nil, rawCharsetTarget, rawCharsetQuery, nil, 0, 0, allFields())
	require.NoError(t, err)
	assert.Equal(t, convertedCharsetResult(), got)
}

// TestRawResultCharsetSessionSetup checks that only the pools that serve the
// queries of clients set character_set_results to NULL: the schema engine,
// which opens its connections first, sets the connections up as before.
func TestRawResultCharsetSessionSetup(t *testing.T) {
	plainSetup := sqlmode.SessionSetupQuery("utf8mb4", "utf8mb4_0900_ai_ci")
	rawSetup := sqlmode.SessionSetupQueryWithRawResults("utf8mb4", "utf8mb4_0900_ai_ci")

	db, tsv := setupRawCharsetTest(t, false)
	_, err := tsv.Execute(t.Context(), nil, rawCharsetTarget, rawCharsetQuery, nil, 0, 0, allFields())
	require.NoError(t, err)
	assert.Zero(t, db.GetQueryCalledNum(rawSetup))
	assert.NotZero(t, db.GetQueryCalledNum(plainSetup))

	db, tsv = setupRawCharsetTest(t, true)
	plainAtStart := db.GetQueryCalledNum(plainSetup)
	assert.NotZero(t, plainAtStart, "the schema engine and the other pools keep the plain setup")
	rawAtStart := db.GetQueryCalledNum(rawSetup)
	assert.NotZero(t, rawAtStart, "the query engine opens a connection of its pool at start")

	for _, stream := range []bool{false, true} {
		if stream {
			streamAll(t, tsv, t.Context(), allFields())
		} else {
			_, err = tsv.Execute(t.Context(), nil, rawCharsetTarget, rawCharsetQuery, nil, 0, 0, allFields())
			require.NoError(t, err)
		}
	}
	state, _, err := tsv.BeginExecute(t.Context(), nil, rawCharsetTarget, nil, rawCharsetQuery, nil, 0, allFields())
	require.NoError(t, err)
	_, err = tsv.Commit(t.Context(), rawCharsetTarget, state.TransactionID)
	require.NoError(t, err)

	// a stream pool connection and a transaction pool connection more, and
	// no other connection
	assert.Equal(t, rawAtStart+2, db.GetQueryCalledNum(rawSetup))
	assert.Equal(t, plainAtStart, db.GetQueryCalledNum(plainSetup))
}
