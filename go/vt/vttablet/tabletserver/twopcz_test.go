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
	"html"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
)

// TestTwopczHandlerEscapesActionMessage checks that the twopcz page HTML-escapes
// the reflected Action/dtid form values so a crafted request cannot inject markup
// into the rendered response.
func TestTwopczHandlerEscapesActionMessage(t *testing.T) {
	ctx := t.Context()
	txe, _, db, closer := newTestTxExecutor(t, ctx)
	defer closer()

	db.AddQueryPattern(`select t\.dtid, t\.state, t\.time_created, s\.statement.*`, &sqltypes.Result{})
	db.AddQueryPattern(`select t\.dtid, t\.state, t\.time_created, p\.keyspace, p\.shard.*`, &sqltypes.Result{})

	payload := "<script>alert(1)</script>"
	form := url.Values{}
	form.Set("Action", payload)
	form.Set("dtid", "zz")

	resp := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/twopcz?"+form.Encode(), nil)
	require.NoError(t, err)

	twopczHandler(txe, resp, req)

	body := resp.Body.String()
	assert.NotContains(t, body, payload)
	assert.Contains(t, body, html.EscapeString(payload))
}
