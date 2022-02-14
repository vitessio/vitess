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
