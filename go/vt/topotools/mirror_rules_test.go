/*
Copyright 2024 The Vitess Authors.

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

func TestMirrorRulesRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()

	rules := map[string]map[string]float32{
		"k1.t1@replica": {
			"k2": 50.0,
		},
		"k1.t4": {
			"k3": 75.0,
		},
	}

	err := SaveMirrorRules(ctx, ts, rules)
	require.NoError(t, err, "could not save mirror rules to topo %v", rules)

	roundtripRules, err := GetMirrorRules(ctx, ts)
	require.NoError(t, err, "could not fetch mirror rules from topo")

	assert.Equal(t, rules, roundtripRules)
}

func TestMirrorRulesErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()
	factory.SetError(errors.New("topo failure for testing"))

	t.Run("GetMirrorRules error", func(t *testing.T) {
		rules, err := GetMirrorRules(ctx, ts)
		assert.Error(t, err, "expected error from GetMirrorRules, got rules=%v", rules)
	})

	t.Run("SaveMirrorRules error", func(t *testing.T) {
		rules := map[string]map[string]float32{
			"k1.t1@replica": {
				"k2": 50.0,
			},
			"k1.t4": {
				"k3": 75.0,
			},
		}

		err := SaveMirrorRules(ctx, ts, rules)
		assert.Error(t, err, "expected error from SaveMirrorRules, got rules=%v", rules)
	})
}
