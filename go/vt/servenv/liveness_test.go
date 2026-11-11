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

package servenv

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/acl/acltest"
	"vitess.io/vitess/go/vt/servenv/testutils"
)

// TestLivenessHandler checks that /debug/liveness answers 200 under every
// security policy, because liveness probes call it without credentials.
func TestLivenessHandler(t *testing.T) {
	server := testutils.HTTPTestServer()
	defer server.Close()

	for _, mode := range []acltest.Mode{acltest.AllowAll, acltest.ReadOnly, acltest.DenyAll} {
		t.Run(mode.String(), func(t *testing.T) {
			acltest.SetMode(t, mode)

			resp, err := http.Get(server.URL + "/debug/liveness")
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			// Make sure we can read the body, even though it's empty.
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			t.Logf("body: %q", body)
		})
	}
}
