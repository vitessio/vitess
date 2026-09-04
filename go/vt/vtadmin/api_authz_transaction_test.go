/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 the "License";
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtadmin_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtadmin"
	"vitess.io/vitess/go/vt/vtadmin/rbac"
	"vitess.io/vitess/go/vt/vtenv"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// TestConcludeTransactionAuthorization verifies that concluding an unresolved
// transaction requires a write-level action (put) on the cluster, and is not
// permitted to read-only actors. ConcludeTransaction is destructive: it
// finalizes and removes an unresolved transaction record.
func TestConcludeTransactionAuthorization(t *testing.T) {
	t.Parallel()

	opts := vtadmin.Options{
		RBAC: &rbac.Config{
			Rules: []*struct {
				Resource string
				Actions  []string
				Subjects []string
				Clusters []string
			}{
				{
					Resource: "Cluster",
					Actions:  []string{"get"},
					Subjects: []string{"user:readonly"},
					Clusters: []string{"*"},
				},
				{
					Resource: "Cluster",
					Actions:  []string{"put"},
					Subjects: []string{"user:allowed"},
					Clusters: []string{"*"},
				},
			},
		},
	}
	err := opts.RBAC.Reify()
	require.NoError(t, err, "failed to reify authorization rules: %+v", opts.RBAC.Rules)

	api := vtadmin.NewAPI(vtenv.NewTestEnv(), testClusters(t), opts)
	t.Cleanup(func() {
		if err := api.Close(); err != nil {
			t.Logf("api did not close cleanly: %s", err.Error())
		}
	})

	t.Run("read-only actor is not permitted", func(t *testing.T) {
		t.Parallel()

		actor := &rbac.Actor{Name: "readonly"}
		ctx := rbac.NewContext(t.Context(), actor)

		resp, err := api.ConcludeTransaction(ctx, &vtadminpb.ConcludeTransactionRequest{
			ClusterId: "test",
			Dtid:      "dtid-1",
		})
		assert.ErrorContains(t, err, "unauthorized", "actor %+v should not be permitted to ConcludeTransaction", actor)
		assert.Nil(t, resp, "actor %+v should not be permitted to ConcludeTransaction", actor)
	})

	t.Run("actor with put action is permitted", func(t *testing.T) {
		t.Parallel()

		actor := &rbac.Actor{Name: "allowed"}
		ctx := rbac.NewContext(t.Context(), actor)

		_, err := api.ConcludeTransaction(ctx, &vtadminpb.ConcludeTransactionRequest{
			ClusterId: "test",
			Dtid:      "dtid-1",
		})
		require.NoError(t, err, "actor %+v should be authorized to ConcludeTransaction", actor)
	})
}
