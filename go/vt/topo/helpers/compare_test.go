package helpers

import (
	"testing"

	"context"
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

	err = CompareRoutingRules(ctx, fromTS, toTS)
	if err == nil {
		t.Fatalf("Compare routing rules is not failing when topos are not in sync")
	}

	CopyRoutingRules(ctx, fromTS, toTS)

	err = CompareRoutingRules(ctx, fromTS, toTS)
	if err != nil {
		t.Fatalf("Compare routing rules failed: %v", err)
	}
}
