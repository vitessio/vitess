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

package txserializer

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// Placeholder standing in for a customer literal. It is not a credential.
const redactHotRowKey = "customer where ssn = 'custvalue991288'"

// TestRedactDebugUIQueriesHidesStatementOnHotrows is /debug/hotrows.
// The serializer is constructed after the flag is set, which is the order
// vttablet uses: flags are parsed, then the query engine builds the serializer.
func TestRedactDebugUIQueriesHidesStatementOnHotrows(t *testing.T) {
	fs := servenv.GetFlagSetFor("vttablet")
	require.NotNil(t, fs.Lookup("redact-debug-ui-queries"))
	require.NoError(t, fs.Set("redact-debug-ui-queries", "true"))
	t.Cleanup(func() {
		require.NoError(t, fs.Set("redact-debug-ui-queries", "false"))
	})
	require.True(t, streamlog.GetQueryLogConfig().RedactDebugUIQueries)

	cfg := tabletenv.NewDefaultConfig()
	txs := New(tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TxSerializerRedactDebugUI"))

	done1, _, err := txs.Wait(t.Context(), redactHotRowKey, "customer")
	require.NoError(t, err)
	t.Cleanup(done1)
	done2, _, err := txs.Wait(t.Context(), redactHotRowKey, "customer")
	require.NoError(t, err)
	t.Cleanup(done2)
	require.NotEmpty(t, txs.Items())

	req := httptest.NewRequest(http.MethodGet, "/debug/hotrows", nil)
	rec := httptest.NewRecorder()
	txs.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.NotContains(t, body, "custvalue991288")
	require.NotContains(t, body, "where ssn")
}
