package vtsql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/grpcclient"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func assertImmediateCaller(t *testing.T, im *querypb.VTGateCallerID, expected string) {
	require.NotNil(t, im, "immediate caller cannot be nil")
	assert.Equal(t, im.Username, expected, "immediate caller username mismatch")
}

func assertEffectiveCaller(t *testing.T, ef *vtrpcpb.CallerID, principal string, component string, subcomponent string) {
	require.NotNil(t, ef, "effective caller cannot be nil")
	assert.Equal(t, ef.Principal, principal, "effective caller principal mismatch")
	assert.Equal(t, ef.Component, component, "effective caller component mismatch")
	assert.Equal(t, ef.Subcomponent, subcomponent, "effective caller subcomponent mismatch")
}

func Test_getQueryContext(t *testing.T) {
	ctx := context.Background()

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
