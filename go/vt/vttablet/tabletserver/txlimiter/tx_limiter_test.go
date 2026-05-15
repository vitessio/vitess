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

package txlimiter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func resetVariables(txl *Impl) {
	txl.rejections.ResetAll()
	txl.rejectionsDryRun.ResetAll()
}

func createCallers(username, principal, component, subcomponent string) (*querypb.VTGateCallerID, *vtrpcpb.CallerID) {
	im := callerid.NewImmediateCallerID(username)
	ef := callerid.NewEffectiveCallerID(principal, component, subcomponent)
	return im, ef
}

func TestTxLimiter_DisabledAllowsAll(t *testing.T) {
	cfg := tabletenv.NewDefaultConfig()
	cfg.TxPool.Size = 10
	cfg.TransactionLimitPerUser = 0.1
	cfg.EnableTransactionLimit = false
	cfg.EnableTransactionLimitDryRun = false
	cfg.TransactionLimitByUsername = false
	cfg.TransactionLimitByPrincipal = false
	cfg.TransactionLimitByComponent = false
	cfg.TransactionLimitBySubcomponent = false
	limiter := New(tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TabletServerTest"))
	im, ef := createCallers("", "", "", "")
	for i := range 5 {
		assert.Truef(t, limiter.Get(im, ef), "Transaction number %d, Get()", i)
	}
}

func TestTxLimiter_LimitsOnlyOffendingUser(t *testing.T) {
	cfg := tabletenv.NewDefaultConfig()
	cfg.TxPool.Size = 10
	cfg.TransactionLimitPerUser = 0.3
	cfg.EnableTransactionLimit = true
	cfg.EnableTransactionLimitDryRun = false
	cfg.TransactionLimitByUsername = true
	cfg.TransactionLimitByPrincipal = false
	cfg.TransactionLimitByComponent = false
	cfg.TransactionLimitBySubcomponent = false

	// This should allow 3 slots to all users
	newlimiter := New(tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TabletServerTest"))
	limiter, ok := newlimiter.(*Impl)
	require.Truef(t, ok, "New returned limiter of unexpected type: got %T, want %T", newlimiter, limiter)
	resetVariables(limiter)
	im1, ef1 := createCallers("user1", "", "", "")
	im2, ef2 := createCallers("user2", "", "", "")

	// user1 uses 3 slots
	for i := range 3 {
		assert.Truef(t, limiter.Get(im1, ef1), "Transaction number %d, Get(im1, ef1)", i)
	}

	// user1 not allowed to use 4th slot, which increases counter
	assert.False(t, limiter.Get(im1, ef1), "Get(im1, ef1) after using up all allowed attempts")

	key1 := limiter.extractKey(im1, ef1)
	assert.Equalf(t, int64(1), limiter.rejections.Counts()[key1], "Rejections count for %s", key1)

	// user2 uses 3 slots
	for i := range 3 {
		assert.Truef(t, limiter.Get(im2, ef2), "Transaction number %d, Get(im2, ef2)", i)
	}

	// user2 not allowed to use 4th slot, which increases counter
	assert.False(t, limiter.Get(im2, ef2), "Get(im2, ef2) after using up all allowed attempts")
	key2 := limiter.extractKey(im2, ef2)
	assert.Equalf(t, int64(1), limiter.rejections.Counts()[key2], "Rejections count for %s", key2)

	// user1 releases a slot, which allows to get another
	limiter.Release(im1, ef1)
	assert.True(t, limiter.Get(im1, ef1), "Get(im1, ef1) after releasing")

	// Rejection count for user 1 should still be 1.
	assert.Equalf(t, int64(1), limiter.rejections.Counts()[key1], "Rejections count for %s", key1)
}

func TestTxLimiterDryRun(t *testing.T) {
	cfg := tabletenv.NewDefaultConfig()
	cfg.TxPool.Size = 10
	cfg.TransactionLimitPerUser = 0.3
	cfg.EnableTransactionLimit = true
	cfg.EnableTransactionLimitDryRun = true
	cfg.TransactionLimitByUsername = true
	cfg.TransactionLimitByPrincipal = false
	cfg.TransactionLimitByComponent = false
	cfg.TransactionLimitBySubcomponent = false

	// This should allow 3 slots to all users
	newlimiter := New(tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TabletServerTest"))
	limiter, ok := newlimiter.(*Impl)
	require.Truef(t, ok, "New returned limiter of unexpected type: got %T, want %T", newlimiter, limiter)
	resetVariables(limiter)
	im, ef := createCallers("user", "", "", "")
	key := limiter.extractKey(im, ef)

	// uses 3 slots
	for i := range 3 {
		assert.Truef(t, limiter.Get(im, ef), "Transaction number %d, Get(im, ef)", i)
	}

	// allowed to use 4th slot, but dry run rejection counter increased
	assert.True(t, limiter.Get(im, ef), "Get(im, ef) after using up all allowed attempts")

	assert.Equalf(t, int64(0), limiter.rejections.Counts()[key], "Rejections count for %s", key)
	assert.Equalf(t, int64(1), limiter.rejectionsDryRun.Counts()[key], "RejectionsDryRun count for %s", key)
}
