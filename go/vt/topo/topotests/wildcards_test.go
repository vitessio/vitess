/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package topotests

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

type topoLayout struct {
	keyspaces []string
	shards    map[string][]string
}

func (l *topoLayout) initTopo(t *testing.T, ts *topo.Server) {
	ctx := context.Background()
	for _, keyspace := range l.keyspaces {
		if err := ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}); err != nil {
			t.Fatalf("CreateKeyspace(%v) failed: %v", keyspace, err)
		}
	}

	for keyspace, shards := range l.shards {
		for _, shard := range shards {
			if err := ts.CreateShard(ctx, keyspace, shard); err != nil {
				t.Fatalf("CreateShard(%v, %v) failed: %v", keyspace, shard, err)
			}
		}
	}
}

func validateKeyspaceWildcard(t *testing.T, l *topoLayout, param string, expected []string) {
	ts := memorytopo.NewServer()
	l.initTopo(t, ts)

	ctx := context.Background()
	r, err := ts.ResolveKeyspaceWildcard(ctx, param)
	if err != nil {
		if expected != nil {
			t.Errorf("was not expecting an error but got: %v", err)
		}
		return
	}

	if len(r) != len(expected) {
		t.Errorf("got wrong result: %v", r)
		return
	}
	for i, e := range expected {
		if r[i] != e {
			t.Errorf("got wrong result[%v]: %v", i, r)
		}
	}
}

func TestKeyspaceWildcards(t *testing.T) {
	l := &topoLayout{
		keyspaces: []string{"aaaaa", "aabbb", "bbbbb"},
	}
	validateKeyspaceWildcard(t, l, "*", []string{"aaaaa", "aabbb", "bbbbb"})
	validateKeyspaceWildcard(t, l, "aa*", []string{"aaaaa", "aabbb"})
	validateKeyspaceWildcard(t, l, "??b??", []string{"aabbb", "bbbbb"})
	validateKeyspaceWildcard(t, l, "ccc", []string{"ccc"})

	validateKeyspaceWildcard(t, l, "ccc\\", nil)
}

func validateShardWildcard(t *testing.T, l *topoLayout, param string, expected []topo.KeyspaceShard) {
	ts := memorytopo.NewServer()
	l.initTopo(t, ts)

	ctx := context.Background()
	r, err := ts.ResolveShardWildcard(ctx, param)
	if err != nil {
		if expected != nil {
			t.Errorf("was not expecting an error but got: %v", err)
		}
		return
	}

	if len(r) != len(expected) {
		t.Errorf("got wrong result: %v", r)
		return
	}
	for i, e := range expected {
		if r[i] != e {
			t.Errorf("got wrong result[%v]: %v", i, r)
		}
	}
}

func TestShardWildcards(t *testing.T) {
	l := &topoLayout{
		keyspaces: []string{"aaaaa", "bbbbb"},
		shards: map[string][]string{
			"aaaaa": {"s0", "s1"},
			"bbbbb": {"-40", "40-80", "80-c0", "c0-"},
		},
	}
	validateShardWildcard(t, l, "*/*", []topo.KeyspaceShard{
		{Keyspace: "aaaaa", Shard: "s0"},
		{Keyspace: "aaaaa", Shard: "s1"},
		{Keyspace: "bbbbb", Shard: "-40"},
		{Keyspace: "bbbbb", Shard: "40-80"},
		{Keyspace: "bbbbb", Shard: "80-c0"},
		{Keyspace: "bbbbb", Shard: "c0-"},
	})
	validateShardWildcard(t, l, "aaaaa/*", []topo.KeyspaceShard{
		{Keyspace: "aaaaa", Shard: "s0"},
		{Keyspace: "aaaaa", Shard: "s1"},
	})
	validateShardWildcard(t, l, "*/s1", []topo.KeyspaceShard{
		{Keyspace: "aaaaa", Shard: "s1"},
	})
	validateShardWildcard(t, l, "*/*0*", []topo.KeyspaceShard{
		{Keyspace: "aaaaa", Shard: "s0"},
		{Keyspace: "bbbbb", Shard: "-40"},
		{Keyspace: "bbbbb", Shard: "40-80"},
		{Keyspace: "bbbbb", Shard: "80-c0"},
		{Keyspace: "bbbbb", Shard: "c0-"},
	})
	validateShardWildcard(t, l, "aaaaa/ccccc", []topo.KeyspaceShard{
		{Keyspace: "aaaaa", Shard: "ccccc"},
	})
	validateShardWildcard(t, l, "ccccc/s0", []topo.KeyspaceShard{
		{Keyspace: "ccccc", Shard: "s0"},
	})
	validateShardWildcard(t, l, "bbbbb/C0-", []topo.KeyspaceShard{
		{Keyspace: "bbbbb", Shard: "c0-"},
	})

	// error cases
	l = &topoLayout{
		keyspaces: []string{"aaaaa", "bbbbb"},
		shards: map[string][]string{
			"aaaaa": nil,
		},
	}

	// these two will return an error as GetShardNames("aaaaa")
	// will return an error.
	validateShardWildcard(t, l, "aaaaa/bbbb*", nil)
	validateShardWildcard(t, l, "aaaa*/bbbb*", nil)

	// GetShardNames("bbbbb") will return ErrNoNode, so we get empty lists
	// in this case, as the keyspace is a wildcard.
	validateShardWildcard(t, l, "bbbb*/cccc*", []topo.KeyspaceShard{})

	// GetShardNames("bbbbb") returns ErrNoNode, so we get an error
	// in this case, as keyspace is not a wildcard.
	validateShardWildcard(t, l, "bbbbb/cccc*", nil)

	// GetKeyspaces() will fail hard in this one, so we get an error
	l = &topoLayout{}
	validateShardWildcard(t, l, "*/s1", nil)

	// GetKeyspaces() will return an empty list, so no error, no result
	l = &topoLayout{
		keyspaces: []string{},
	}
	validateShardWildcard(t, l, "*/s1", []topo.KeyspaceShard{})
}
