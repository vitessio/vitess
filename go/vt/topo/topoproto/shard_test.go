// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topoproto

import (
	"testing"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestParseKeyspaceShard(t *testing.T) {
	testcases := []struct {
		keyspaceShard string
		keyspace      string
		shard         string
	}{{
		keyspaceShard: "key01/shard0",
		keyspace:      "key01",
		shard:         "shard0",
	}, {
		keyspaceShard: "key01:shard0",
		keyspace:      "key01",
		shard:         "shard0",
	}}
	for _, tcase := range testcases {
		k, s, err := ParseKeyspaceShard(tcase.keyspaceShard)
		if err != nil {
			t.Fatalf("Failed to parse valid keyspace/shard pair: %s", tcase.keyspaceShard)
		}
		if k != tcase.keyspace {
			t.Errorf("keyspace parsed from keyspace/shard pair %s is %s, but expect %s", tcase.keyspaceShard, k, tcase.keyspace)
		}
		if s != tcase.shard {
			t.Errorf("shard parsed from keyspace/shard pair %s is %s, but expect %s", tcase.keyspaceShard, s, tcase.shard)
		}
	}
}

func TestParseKeyspaceOptionalShard(t *testing.T) {
	testcases := []struct {
		keyspaceShard string
		keyspace      string
		shard         string
	}{{
		keyspaceShard: "ks",
		keyspace:      "ks",
		shard:         "",
	}, {
		keyspaceShard: "/-80",
		keyspace:      "",
		shard:         "-80",
	}, {
		keyspaceShard: "ks/-80",
		keyspace:      "ks",
		shard:         "-80",
	}, {
		keyspaceShard: "ks/",
		keyspace:      "ks",
		shard:         "",
	}, {
		keyspaceShard: "ks:",
		keyspace:      "ks",
		shard:         "",
	}, {
		keyspaceShard: "ks:-80",
		keyspace:      "ks",
		shard:         "-80",
	}, {
		keyspaceShard: ":-80",
		keyspace:      "",
		shard:         "-80",
	}}

	for _, tcase := range testcases {
		if keyspace, shard := ParseKeyspaceOptionalShard(tcase.keyspaceShard); keyspace != tcase.keyspace || shard != tcase.shard {
			t.Errorf("parseKeyspaceShard(%s): %s:%s, want %s:%s", tcase.keyspaceShard, keyspace, shard, tcase.keyspace, tcase.shard)
		}
	}
}

func TestSourceShardAsHTML(t *testing.T) {
	s := &topodatapb.Shard_SourceShard{
		Uid:      123,
		Keyspace: "source_keyspace",
		Shard:    "source_shard",
		KeyRange: &topodatapb.KeyRange{
			Start: []byte{0x80},
		},
		Tables: []string{"table1", "table2"},
	}
	got := string(SourceShardAsHTML(s))
	expected := "<b>Uid</b>: 123</br>\n" +
		"<b>Source</b>: source_keyspace/source_shard</br>\n" +
		"<b>KeyRange</b>: 80-</br>\n" +
		"<b>Tables</b>: table1 table2</br>\n"
	if got != expected {
		t.Errorf("got wrong SourceShardAsHTML output, got:\n%vexpected:\n%v", got, expected)
	}
}
