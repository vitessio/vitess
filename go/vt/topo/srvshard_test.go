// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/vt/key"
)

type reflectSrvKeyspace struct {
	Partitions         map[string]*KeyspacePartition
	Shards             []SrvShard
	TabletTypes        []TabletType
	ShardingColumnName string
	ShardingColumnType key.KeyspaceIdType
	ServedFrom         map[string]string
	version            int64
}

type extraSrvKeyspace struct {
	Extra              int
	Partitions         map[TabletType]*KeyspacePartition
	Shards             []SrvShard
	TabletTypes        []TabletType
	ShardingColumnName string
	ShardingColumnType key.KeyspaceIdType
	ServedFrom         map[TabletType]string
	version            int64
}

func TestSrvKeySpace(t *testing.T) {
	reflected, err := bson.Marshal(&reflectSrvKeyspace{
		Partitions: map[string]*KeyspacePartition{
			string(TYPE_MASTER): &KeyspacePartition{
				Shards: []SrvShard{
					SrvShard{
						Name:        "test_shard",
						ServedTypes: []TabletType{TYPE_MASTER},
						MasterCell:  "test_cell",
					},
				},
			},
		},
		TabletTypes:        []TabletType{TYPE_MASTER},
		ShardingColumnName: "video_id",
		ShardingColumnType: key.KIT_UINT64,
		ServedFrom: map[string]string{
			string(TYPE_REPLICA): "other_keyspace",
		},
	})
	if err != nil {
		t.Error(err)
	}
	want := string(reflected)

	custom := SrvKeyspace{
		Partitions: map[TabletType]*KeyspacePartition{
			TYPE_MASTER: &KeyspacePartition{
				Shards: []SrvShard{
					SrvShard{
						Name:        "test_shard",
						ServedTypes: []TabletType{TYPE_MASTER},
						MasterCell:  "test_cell",
						TabletTypes: []TabletType{},
					},
				},
			},
		},
		Shards:             []SrvShard{},
		TabletTypes:        []TabletType{TYPE_MASTER},
		ShardingColumnName: "video_id",
		ShardingColumnType: key.KIT_UINT64,
		ServedFrom: map[TabletType]string{
			TYPE_REPLICA: "other_keyspace",
		},
	}

	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%#v, got\n%#v", want, got)
	}

	var unmarshalled SrvKeyspace
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(custom, unmarshalled) {
		t.Errorf("want \n%#v, got \n%#v", custom, unmarshalled)
	}

	extra, err := bson.Marshal(&extraSrvKeyspace{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(extra, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
}
