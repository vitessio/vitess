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

package vtsql

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtadmin/vtsql/fakevtsql"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func assertImmediateCaller(t *testing.T, im *querypb.VTGateCallerID, expected string) {
	t.Helper()

	require.NotNil(t, im, "immediate caller cannot be nil")
	assert.Equal(t, expected, im.Username, "immediate caller username mismatch")
}

func assertEffectiveCaller(t *testing.T, ef *vtrpcpb.CallerID, principal string, component string, subcomponent string) {
	t.Helper()

	require.NotNil(t, ef, "effective caller cannot be nil")
	assert.Equal(t, ef.Principal, principal, "effective caller principal mismatch")
	assert.Equal(t, ef.Component, component, "effective caller component mismatch")
	assert.Equal(t, ef.Subcomponent, subcomponent, "effective caller subcomponent mismatch")
}

func Test_getQueryContext(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	creds := &StaticAuthCredentials{
		EffectiveUser: "efuser",
		StaticAuthClientCreds: &grpcclient.StaticAuthClientCreds{
			Username: "imuser",
		},
	}
	db := &VTGateProxy{creds: creds}

	outctx := db.getQueryContext(ctx)
	assert.NotEqual(t, ctx, outctx, "getQueryContext should return a modified context when credentials are set")
	assertEffectiveCaller(t, callerid.EffectiveCallerIDFromContext(outctx), "efuser", "vtadmin", "")
	assertImmediateCaller(t, callerid.ImmediateCallerIDFromContext(outctx), "imuser")

	db.creds = nil
	outctx = db.getQueryContext(ctx)
	assert.Equal(t, ctx, outctx, "getQueryContext should not modify the context when credentials are not set")

	callerctx := callerid.NewContext(
		ctx,
		callerid.NewEffectiveCallerID("other principal", "vtctld", ""),
		callerid.NewImmediateCallerID("other_user"),
	)
	db.creds = creds

	outctx = db.getQueryContext(callerctx)
	assert.NotEqual(t, callerctx, outctx, "getQueryContext should override an existing callerid in the context")
	assertEffectiveCaller(t, callerid.EffectiveCallerIDFromContext(outctx), "efuser", "vtadmin", "")
	assertImmediateCaller(t, callerid.ImmediateCallerIDFromContext(outctx), "imuser")
}

// TestVExplainRunsExecutingTypesReadOnly checks that the VEXPLAIN types that run
// the statement they explain (QUERIES, ALL and TRACE) run it on one connection
// inside a read-only transaction that is then rolled back, so that it cannot
// change any table, even through a stored function running with its definer's
// privileges, while the types that do not run it use no transaction.
func TestVExplainRunsExecutingTypesReadOnly(t *testing.T) {
	parser := sqlparser.NewTestParser()
	for _, tc := range []struct {
		query string
		want  []string
	}{{
		query: "vexplain all select * from customers",
		want: []string{
			"1: start transaction read only",
			"1: vexplain all select * from customers",
			"1: rollback",
		},
	}, {
		query: "vexplain mysqlplan select * from customers",
		want:  []string{"1: vexplain mysqlplan select * from customers"},
	}} {
		t.Run(tc.query, func(t *testing.T) {
			log := &fakevtsql.StatementLog{}
			db := sql.OpenDB(&fakevtsql.Connector{Log: log})
			t.Cleanup(func() { db.Close() })
			proxy := &VTGateProxy{conn: db, cluster: &vtadminpb.Cluster{Id: "c0", Name: "cluster0"}}

			stmt, err := parser.Parse(tc.query)
			require.NoError(t, err)
			_, err = proxy.VExplain(t.Context(), tc.query, stmt.(*sqlparser.VExplainStmt))
			require.NoError(t, err)
			require.Equal(t, tc.want, log.Statements())
		})
	}
}
