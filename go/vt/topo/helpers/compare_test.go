/*
Copyright 2017 Google Inc.

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

package helpers

import (
	"testing"

	"golang.org/x/net/context"
)

func TestBasicCompare(t *testing.T) {
	ctx := context.Background()
	fromTS, toTS := createSetup(ctx, t)

	// check compare keyspace compare
	err := CompareKeyspaces(ctx, fromTS, toTS)
	if err == nil {
		t.Fatalf("Compare keyspaces is not failing when topos are not in sync")
	}

	CopyKeyspaces(ctx, fromTS, toTS)

	err = CompareKeyspaces(ctx, fromTS, toTS)
	if err != nil {
		t.Fatalf("Compare keyspaces failed: %v", err)
	}

	// check shard copy
	err = CompareShards(ctx, fromTS, toTS)
	if err == nil {
		t.Fatalf("Compare shards is not failing when topos are not in sync")
	}

	CopyShards(ctx, fromTS, toTS)

	err = CompareShards(ctx, fromTS, toTS)
	if err != nil {
		t.Fatalf("Compare shards failed: %v", err)
	}

	// check ShardReplication compare
	err = CompareShardReplications(ctx, fromTS, toTS)
	if err == nil {
		t.Fatalf("Compare shard replications is not failing when topos are not in sync")
	}

	CopyShardReplications(ctx, fromTS, toTS)

	err = CompareShardReplications(ctx, fromTS, toTS)
	if err != nil {
		t.Fatalf("Compare shard replications failed: %v", err)
	}

	// check tablet compare
	err = CompareTablets(ctx, fromTS, toTS)
	if err == nil {
		t.Fatalf("Compare tablets is not failing when topos are not in sync")
	}

	CopyTablets(ctx, fromTS, toTS)

	err = CompareTablets(ctx, fromTS, toTS)
	if err != nil {
		t.Fatalf("Compare tablets failed: %v", err)
	}
}
