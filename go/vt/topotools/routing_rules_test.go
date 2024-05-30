/*
Copyright 2021 The Vitess Authors.

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

package topotools

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestRoutingRulesRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()

	rules := map[string][]string{
		"t1": {"t2", "t3"},
		"t4": {"t5"},
	}

	err := SaveRoutingRules(ctx, ts, rules)
	require.NoError(t, err, "could not save routing rules to topo %v", rules)

	roundtripRules, err := GetRoutingRules(ctx, ts)
	require.NoError(t, err, "could not fetch routing rules from topo")

	assert.Equal(t, rules, roundtripRules)
}

func TestRoutingRulesErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()
	factory.SetError(errors.New("topo failure for testing"))

	t.Run("GetRoutingRules error", func(t *testing.T) {

		rules, err := GetRoutingRules(ctx, ts)
		assert.Error(t, err, "expected error from GetRoutingRules, got rules=%v", rules)
	})

	t.Run("SaveRoutingRules error", func(t *testing.T) {
		rules := map[string][]string{
			"t1": {"t2", "t3"},
			"t4": {"t5"},
		}

		err := SaveRoutingRules(ctx, ts, rules)
		assert.Error(t, err, "expected error from GetRoutingRules, got rules=%v", rules)
	})
}

func TestShardRoutingRulesRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()

	srr := map[string]string{
		"ks1.shard1": "ks2",
		"ks3.shard2": "ks4",
	}

	err := SaveShardRoutingRules(ctx, ts, srr)
	require.NoError(t, err, "could not save shard routing rules to topo %v", err)

	roundtripRules, err := GetShardRoutingRules(ctx, ts)
	require.NoError(t, err, "could not fetch shard routing rules from topo: %v", err)

	assert.Equal(t, srr, roundtripRules)
}

func TestKeyspaceRoutingRulesRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()

	rulesMap := map[string]string{
		"ks1": "ks2",
		"ks4": "ks5",
	}

	err := UpdateKeyspaceRoutingRules(ctx, ts, "test", func(ctx context.Context, rules *map[string]string) error {
		for k, v := range rulesMap {
			(*rules)[k] = v
		}
		return nil
	})
	require.NoError(t, err, "could not save keyspace routing rules to topo %v", rulesMap)

	roundtripRulesMap, err := GetKeyspaceRoutingRules(ctx, ts)
	require.NoError(t, err, "could not fetch keyspace routing rules from topo")
	assert.EqualValues(t, rulesMap, roundtripRulesMap)
}

// TestSaveKeyspaceRoutingRulesLocked confirms that saveKeyspaceRoutingRulesLocked() can only be called
// with a locked routing_rules lock.
func TestSaveKeyspaceRoutingRulesLocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()

	rulesMap := map[string]string{
		"ks1": "ks2",
		"ks4": "ks5",
	}

	t.Run("unlocked, doesn't exist", func(t *testing.T) {
		err := saveKeyspaceRoutingRulesLocked(ctx, ts, rulesMap)
		require.Errorf(t, err, "node doesn't exist: routing_rules")
	})

	t.Run("create", func(t *testing.T) {
		err := ts.CreateKeyspaceRoutingRules(ctx, buildKeyspaceRoutingRules(&rulesMap))
		require.NoError(t, err)
	})

	t.Run("create again", func(t *testing.T) {
		err := ts.CreateKeyspaceRoutingRules(ctx, buildKeyspaceRoutingRules(&rulesMap))
		require.True(t, topo.IsErrType(err, topo.NodeExists))
	})

	t.Run("unlocked", func(t *testing.T) {
		err := saveKeyspaceRoutingRulesLocked(ctx, ts, rulesMap)
		require.Errorf(t, err, "routing_rules is not locked (no locksInfo)")
	})

	// declare and acquire lock
	lock, err := topo.NewRoutingRulesLock(ctx, ts, "test")
	require.NoError(t, err)
	lockCtx, unlock, err := lock.Lock(ctx)
	require.NoError(t, err)
	defer unlock(&err)

	t.Run("locked, locked ctx", func(t *testing.T) {
		err = saveKeyspaceRoutingRulesLocked(lockCtx, ts, rulesMap)
		require.NoError(t, err)
	})
	t.Run("locked, unlocked ctx", func(t *testing.T) {
		err = saveKeyspaceRoutingRulesLocked(ctx, ts, rulesMap)
		require.Errorf(t, err, "routing_rules is not locked (no locksInfo)")
	})
}
