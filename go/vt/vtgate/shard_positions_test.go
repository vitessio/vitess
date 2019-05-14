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
	"testing"

	"vitess.io/vitess/go/vt/topo"
)

func TestShardPositions(t *testing.T) {
	testcases := []struct {
		input  string
		output string
	}{{
		// Single position.
		input:  "a:-80@b",
		output: "a:-80@b",
	}, {
		// '/' position.
		input:  "a/-80@b",
		output: "a:-80@b",
	}, {
		// Multiple positions.
		input:  "a:-80@b|b:80-@c",
		output: "a:-80@b|b:80-@c",
	}, {
		// Preserve order.
		input:  "b:-80@b|a:80-@c",
		output: "b:-80@b|a:80-@c",
	}, {
		input:  "a/-80",
		output: "invalid position a/-80",
	}, {
		input:  "a@b",
		output: "invalid position a@b: invalid shard path: a",
	}}

	for _, tcase := range testcases {
		sp, err := newShardPositions(tcase.input)
		if err != nil {
			if err.Error() != tcase.output {
				t.Errorf("shard_positions(%s) err: %v, want %v", tcase.input, err, tcase.output)
			}
			continue
		}
		if got := sp.String(); got != tcase.output {
			t.Errorf("shard_positions(%s): %v, want %v", tcase.input, got, tcase.output)
		}
	}
}

func TestShardPositionsUpdate(t *testing.T) {
	sp, err := newShardPositions("a/-80@b|b/80-@c")
	if err != nil {
		t.Fatal(err)
	}
	sp.positions[topo.KeyspaceShard{Keyspace: "a", Shard: "-80"}] = "c"
	sp.positions[topo.KeyspaceShard{Keyspace: "b", Shard: "80-"}] = "d"

	if got, want := sp.String(), "a:-80@c|b:80-@d"; got != want {
		t.Errorf("updated positions: %v, want %v", got, want)
	}
}
