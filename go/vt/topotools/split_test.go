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

package topotools

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestValidateForReshard(t *testing.T) {
	testcases := []struct {
		sources []string
		targets []string
		out     string
	}{{
		sources: []string{"-80", "80-"},
		targets: []string{"-40", "40-"},
		out:     "",
	}, {
		sources: []string{"52-53"},
		targets: []string{"5200-5240", "5240-5280", "5280-52c0", "52c0-5300"},
		out:     "",
	}, {
		sources: []string{"5200-5300"},
		targets: []string{"520000-524000", "524000-528000", "528000-52c000", "52c000-530000"},
		out:     "",
	}, {
		sources: []string{"-80", "80-"},
		targets: []string{"-4000000000000000", "4000000000000000-8000000000000000", "8000000000000000-80c0000000000000", "80c0000000000000-"},
		out:     "",
	}, {
		sources: []string{"80-", "-80"},
		targets: []string{"-40", "40-"},
		out:     "",
	}, {
		sources: []string{"-40", "40-80", "80-"},
		targets: []string{"-30", "30-"},
		out:     "",
	}, {
		sources: []string{"0"},
		targets: []string{"-40", "40-"},
		out:     "",
	}, {
		sources: []string{"0003-"},
		targets: []string{"000300-000380", "000380-000400", "0004-"},
		out:     "",
	}, {
		sources: []string{"-40", "40-80", "80-"},
		targets: []string{"-40", "40-"},
		out:     "same keyrange is present in source and target: -40",
	}, {
		sources: []string{"-30", "30-80"},
		targets: []string{"-40", "40-"},
		out:     "source and target keyranges don't match: -80 vs -",
	}, {
		sources: []string{"-30", "20-80"},
		targets: []string{"-40", "40-"},
		out:     "shards don't form a contiguous keyrange",
	}}
	buildShards := func(shards []string) []*topo.ShardInfo {
		sis := make([]*topo.ShardInfo, 0, len(shards))
		for _, shard := range shards {
			_, kr, err := topo.ValidateShardName(shard)
			if err != nil {
				panic(err)
			}
			sis = append(sis, topo.NewShardInfo("", shard, &topodatapb.Shard{KeyRange: kr}, nil))
		}
		return sis
	}

	for _, tcase := range testcases {
		sources := buildShards(tcase.sources)
		targets := buildShards(tcase.targets)
		err := ValidateForReshard(sources, targets)
		if tcase.out == "" {
			assert.NoError(t, err)
		} else {
			assert.EqualError(t, err, tcase.out)
		}
	}
}
