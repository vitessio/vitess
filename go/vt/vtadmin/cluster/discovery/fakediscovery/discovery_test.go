/*
Copyright 2020 The Vitess Authors.

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

package fakediscovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

func TestDiscoverVTGates(t *testing.T) {
	t.Parallel()

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

	ctx := context.Background()

	fake.AddTaggedGates(nil, gates...)
	fake.AddTaggedGates([]string{"tag1:val1"}, gates[0], gates[1])
	fake.AddTaggedGates([]string{"tag2:val2"}, gates[0], gates[2])

	actual, err := fake.DiscoverVTGates(ctx, nil)
	assert.NoError(t, err)
	assert.ElementsMatch(t, gates, actual)

	actual, err = fake.DiscoverVTGates(ctx, []string{"tag1:val1"})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []*vtadminpb.VTGate{gates[0], gates[1]}, actual)

	actual, err = fake.DiscoverVTGates(ctx, []string{"tag2:val2"})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []*vtadminpb.VTGate{gates[0], gates[2]}, actual)

	actual, err = fake.DiscoverVTGates(ctx, []string{"tag1:val1", "tag2:val2"})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []*vtadminpb.VTGate{gates[0]}, actual)

	actual, err = fake.DiscoverVTGates(ctx, []string{"differentTag:val"})
	assert.NoError(t, err)
	assert.Equal(t, []*vtadminpb.VTGate{}, actual)

	fake.SetGatesError(true)

	actual, err = fake.DiscoverVTGates(ctx, nil)
	assert.Error(t, err)
	assert.Nil(t, actual)
}
