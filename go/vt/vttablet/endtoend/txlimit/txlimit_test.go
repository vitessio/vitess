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

package txlimit

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestQueryTimeoutInTransactionReleasesTheLimiterSlot verifies that a
// transaction whose query times out gives its transaction-limiter slot back.
// Inside a transaction, the query timeout kills the connection, and the
// rollback that follows finds the transaction gone; the user must still be
// able to begin a new transaction afterwards.
func TestQueryTimeoutInTransactionReleasesTheLimiterSlot(t *testing.T) {
	client := framework.NewClient()
	other := framework.NewClient()

	// The user's one slot is taken while a transaction is open.
	require.NoError(t, client.Begin(false))
	err := other.Begin(false)
	require.Error(t, err)
	require.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err), "%v", err)
	require.ErrorContains(t, err, "per-user transaction pool connection limit exceeded")

	// The query outlasts the query timeout and is killed with its connection.
	_, err = client.Execute("select sleep(30)", nil)
	require.Error(t, err)
	require.Error(t, client.Rollback(), "the rollback must find the transaction gone")

	// The slot is free again.
	require.NoError(t, other.Begin(false))
	require.NoError(t, other.Rollback())
}
