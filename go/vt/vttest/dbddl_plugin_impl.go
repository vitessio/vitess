/*
Copyright 2021 The Vitess Authors.

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

package vttest

import (
	"context"
	"fmt"

	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

var globalDb *LocalCluster

type dbDDL struct{}

func (plugin *dbDDL) CreateDatabase(_ context.Context, name string) error {
	ks := &vttestpb.Keyspace{
		Name: name,
		Shards: []*vttestpb.Shard{{
			Name: "0",
		}},
	}
	for _, shard := range globalDb.shardNames(ks) {
		err := globalDb.Execute([]string{fmt.Sprintf("create database `%s`", shard)}, "")
		if err != nil {
			return err
		}
	}
	globalDb.Topology.Keyspaces = append(globalDb.Topology.Keyspaces, ks)

	return nil
}
func (plugin *dbDDL) DropDatabase(ctx context.Context, name string) error {
	panic(1)
}
