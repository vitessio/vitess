// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

// This file contains keyspace utility functions

// FindAllShardsInKeyspace reads and returns all the existing shards in
// a keyspace. It doesn't take any lock.
func FindAllShardsInKeyspace(ts Server, keyspace string) (map[string]*ShardInfo, error) {
	shards, err := ts.GetShardNames(keyspace)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*ShardInfo, len(shards))
	for _, shard := range shards {
		si, err := ts.GetShard(keyspace, shard)
		if err != nil {
			return nil, err
		}
		result[shard] = si
	}
	return result, nil
}
