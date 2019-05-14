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

package vtgate

import (
	"strings"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
)

// shardPositions is a data structure to keep track of vstream shard positions.
type shardPositions struct {
	ordered   []topo.KeyspaceShard
	positions map[topo.KeyspaceShard]string
}

func newShardPositions(pos string) (*shardPositions, error) {
	positions := strings.Split(pos, "|")
	sp := &shardPositions{
		ordered:   make([]topo.KeyspaceShard, 0, len(positions)),
		positions: make(map[topo.KeyspaceShard]string, len(positions)),
	}
	for _, shardPos := range positions {
		parts := strings.Split(shardPos, "@")
		if len(parts) != 2 {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid position %s", pos)
		}
		keyspace, shard, err := topoproto.ParseKeyspaceShard(parts[0])
		if err != nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid position %s: %v", pos, err)
		}

		ks := topo.KeyspaceShard{Keyspace: keyspace, Shard: shard}
		sp.ordered = append(sp.ordered, ks)
		sp.positions[ks] = parts[1]
	}
	return sp, nil
}

func (sp shardPositions) String() string {
	var buf strings.Builder
	separator := ""
	for _, ks := range sp.ordered {
		buf.WriteString(separator + ks.Keyspace + ":" + ks.Shard + "@" + sp.positions[ks])
		separator = "|"
	}
	return buf.String()
}
