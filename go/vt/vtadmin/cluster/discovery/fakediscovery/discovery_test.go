package fakediscovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

func TestDiscoverVTGates(t *testing.T) {
	fake := New()
	gates := []*vtadminpb.VTGate{
		{
			Hostname: "gate1",
		},
		{
			Hostname: "gate2",
		},
		{
			Hostname: "gate3",
		},
	}

	fake.AddTaggedGates(nil, gates...)
	fake.AddTaggedGates([]string{"tag1:val1"}, gates[0], gates[1])
	fake.AddTaggedGates([]string{"tag2:val2"}, gates[0], gates[2])

	actual, err := fake.DiscoverVTGates(context.Background(), nil)
	assert.NoError(t, err)
	assert.ElementsMatch(t, gates, actual)

	actual, err = fake.DiscoverVTGates(context.Background(), []string{"tag1:val1"})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []*vtadminpb.VTGate{gates[0], gates[1]}, actual)

	actual, err = fake.DiscoverVTGates(context.Background(), []string{"tag2:val2"})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []*vtadminpb.VTGate{gates[0], gates[2]}, actual)

	actual, err = fake.DiscoverVTGates(context.Background(), []string{"tag1:val1", "tag2:val2"})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []*vtadminpb.VTGate{gates[0]}, actual)

	actual, err = fake.DiscoverVTGates(context.Background(), []string{"differentTag:val"})
	assert.NoError(t, err)
	assert.Equal(t, []*vtadminpb.VTGate{}, actual)

	fake.SetGatesError(true)

	actual, err = fake.DiscoverVTGates(context.Background(), nil)
	assert.Error(t, err)
	assert.Nil(t, actual)
}
