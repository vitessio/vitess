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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestDenyRuleDoesNotLetAdmittedWritesThrough verifies that once a rule that
// denies a table is loaded, no write to that table that was admitted under the
// previous rules reaches MySQL anymore: each such write has either finished
// already or never runs.
//
// MoveTables SwitchTraffic relies on this. It loads the deny rule on the
// source primary (tmState.applyDenyList), runs LOCK TABLES, which only waits
// for writes that hold a metadata lock, and records the source position the
// target must catch up to. A write that passed checkPermissions before the
// rule was loaded, but was still waiting for a transaction pool connection,
// holds no metadata lock: if it reaches MySQL later, it commits after that
// position, is acknowledged to the client, and never reaches the target.
func TestDenyRuleDoesNotLetAdmittedWritesThrough(t *testing.T) {
	ctx := t.Context()
	cfg := tabletenv.NewDefaultConfig()
	// A single transaction pool connection, so that an autocommit write can
	// be made to wait for one, as it does when the pool is saturated.
	cfg.TxPool.Size = 1
	cfg.TxPool.Timeout = time.Minute
	db, tsv := setupTabletServerTestCustom(t, ctx, cfg, "", vtenv.NewTestEnv())
	defer db.Close()
	defer tsv.StopService()
	target := querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}

	var writeReachedMySQL atomic.Bool
	db.AddQueryPatternWithCallback(`insert into test_table.*`, &sqltypes.Result{RowsAffected: 1}, func(string) {
		writeReachedMySQL.Store(true)
	})

	// Another transaction holds the only connection.
	state, err := tsv.Begin(ctx, nil, &target, nil)
	require.NoError(t, err)

	// A write is admitted under the current rules and waits for a connection.
	writeDone := make(chan error, 1)
	go func() {
		_, err := tsv.Execute(ctx, nil, &target, "insert into test_table(pk, `name`) values (1, 'a')", nil, 0, 0, nil)
		writeDone <- err
	}()
	require.Eventually(t, func() bool {
		return tsv.te.txPool.scp.conns.Metrics.WaitCount() > 0
	}, 30*time.Second, time.Millisecond)

	// The deny rule for the table is loaded, as tmState.applyDenyList does.
	const ruleSource = "DenyListQueryRules"
	denyRule := rules.NewQueryRule("enforce denied tables", "denied_table", rules.QRFailRetry)
	denyRule.AddTableCond("test_table")
	denyRules := rules.New()
	denyRules.Add(denyRule)
	tsv.RegisterQueryRuleSource(ruleSource)
	defer tsv.UnRegisterQueryRuleSource(ruleSource)

	type loaded struct {
		err               error
		writeReachedMySQL bool
	}
	rulesLoaded := make(chan loaded, 1)
	go func() {
		err := tsv.SetQueryRules(ruleSource, denyRules)
		rulesLoaded <- loaded{err: err, writeReachedMySQL: writeReachedMySQL.Load()}
	}()

	releaseConnection := func() {
		_, err := tsv.Rollback(ctx, &target, state.TransactionID)
		require.NoError(t, err)
	}
	var atLoad loaded
	select {
	case atLoad = <-rulesLoaded:
		// The rule was loaded while the write still waited for the held
		// connection.
		releaseConnection()
	case <-time.After(time.Second):
		// Loading the rule waits for the write, which needs the connection.
		releaseConnection()
		select {
		case atLoad = <-rulesLoaded:
		case <-time.After(30 * time.Second):
			require.FailNow(t, "the deny rule was not loaded after the waiting write could run")
		}
	}
	require.NoError(t, atLoad.err)

	// The rule is in effect: a new write is denied.
	_, err = tsv.Execute(ctx, nil, &target, "insert into test_table(pk, `name`) values (2, 'b')", nil, 0, 0, nil)
	require.ErrorContains(t, err, "disallowed due to rule: enforce denied tables")

	select {
	case <-writeDone:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the write did not finish")
	}
	assert.False(t, !atLoad.writeReachedMySQL && writeReachedMySQL.Load(),
		"a write admitted before the deny rule was loaded reached MySQL after it was loaded")
}
