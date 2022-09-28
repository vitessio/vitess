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

	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestRoutingRulesRoundTrip(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("zone1")

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
	ctx := context.Background()
	ts, factory := memorytopo.NewServerAndFactory("zone1")
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
	ctx := context.Background()
	ts := memorytopo.NewServer("zone1")

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
