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
	t.Helper()

	require.NotNil(t, im, "immediate caller cannot be nil")
	assert.Equal(t, im.Username, expected, "immediate caller username mismatch")
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
