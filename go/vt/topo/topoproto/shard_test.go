// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topoproto

import (
	"testing"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestParseKeyspaceShard(t *testing.T) {
	zkPath := "/zk/global/ks/sh"
	keyspace := "key01"
	shard := "shard0"
	keyspaceShard := keyspace + "/" + shard

	if _, _, err := ParseKeyspaceShard(zkPath); err == nil {
		t.Errorf("zk path: %s should cause error.", zkPath)
	}
	k, s, err := ParseKeyspaceShard(keyspaceShard)
	if err != nil {
		t.Fatalf("Failed to parse valid keyspace/shard pair: %s", keyspaceShard)
	}
	if k != keyspace {
		t.Errorf("keyspace parsed from keyspace/shard pair %s is %s, but expect %s", keyspaceShard, k, keyspace)
	}
	if s != shard {
		t.Errorf("shard parsed from keyspace/shard pair %s is %s, but expect %s", keyspaceShard, s, shard)
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
