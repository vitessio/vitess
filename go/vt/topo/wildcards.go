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

package topo

import (
	"fmt"
	"path"
	"strings"

	"github.com/youtube/vitess/go/fileutil"
	"golang.org/x/net/context"
)

// WildcardBackend is a subset of Server for the methods used by the
// wildcard code. This lets us test with a very simple fake topo server.
type WildcardBackend interface {
	// GetKeyspaces returns the known keyspaces. They shall be sorted.
	GetKeyspaces(ctx context.Context) ([]string, error)

	// GetShard reads a shard and returns it.
	// Can return ErrNoNode
	GetShard(ctx context.Context, keyspace, shard string) (*ShardInfo, error)

	// GetShardNames returns the known shards in a keyspace.
	// Can return ErrNoNode
	GetShardNames(ctx context.Context, keyspace string) ([]string, error)
}

// ResolveKeyspaceWildcard will resolve keyspace wildcards.
// - If the param is not a wildcard, it will just be returned (if the keyspace
//   doesn't exist, it is still returned).
// - If the param is a wildcard, it will get all keyspaces and returns
//   the ones which match the wildcard (which may be an empty list).
func ResolveKeyspaceWildcard(ctx context.Context, server WildcardBackend, param string) ([]string, error) {
	if !fileutil.HasWildcard(param) {
		return []string{param}, nil
	}

	var result []string

	keyspaces, err := server.GetKeyspaces(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read keyspaces from topo: %v", err)
	}
	for _, k := range keyspaces {
		matched, err := path.Match(param, k)
		if err != nil {
			return nil, fmt.Errorf("invalid pattern %v: %v", param, err)
		}
		if matched {
			result = append(result, k)
		}
	}
	return result, nil
}

// KeyspaceShard is a type used by ResolveShardWildcard
type KeyspaceShard struct {
	Keyspace string
	Shard    string
}

// ResolveShardWildcard will resolve shard wildcards. Both keyspace and shard
// names can use wildcard. Errors talking to the topology server are returned.
// ErrNoNode is ignored if it's the result of resolving a wildcard. Examples:
// - */* returns all keyspace/shard pairs, or empty list if none.
// - user/* returns all shards in user keyspace (or error if user keyspace
//   doesn't exist)
// - us*/* returns all shards in all keyspaces that start with 'us'. If no such
//   keyspace exists, list is empty (it is not an error).
func ResolveShardWildcard(ctx context.Context, server WildcardBackend, param string) ([]KeyspaceShard, error) {
	parts := strings.Split(param, "/")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid shard path: %v", param)
	}
	result := make([]KeyspaceShard, 0, 1)

	// get all the matched keyspaces first, remember if it was a wildcard
	keyspaceHasWildcards := fileutil.HasWildcard(parts[0])
	matchedKeyspaces, err := ResolveKeyspaceWildcard(ctx, server, parts[0])
	if err != nil {
		return nil, err
	}

	// for each matched keyspace, get the shards
	for _, matchedKeyspace := range matchedKeyspaces {
		shard := parts[1]
		if fileutil.HasWildcard(shard) {
			// get all the shards for the keyspace
			shardNames, err := server.GetShardNames(ctx, matchedKeyspace)
			switch err {
			case nil:
				// got all the shards, we can keep going
			case ErrNoNode:
				// keyspace doesn't exist
				if keyspaceHasWildcards {
					// that's the */* case when a keyspace has no shards
					continue
				}
				return nil, fmt.Errorf("keyspace %v doesn't exist", matchedKeyspace)
			default:
				return nil, fmt.Errorf("cannot read keyspace shards for %v: %v", matchedKeyspace, err)
			}
			for _, s := range shardNames {
				matched, err := path.Match(shard, s)
				if err != nil {
					return nil, fmt.Errorf("Invalid pattern %v: %v", shard, err)
				}
				if matched {
					result = append(result, KeyspaceShard{matchedKeyspace, s})
				}
			}
		} else {
			// if the shard name contains a '-', we assume it's the
			// name for a ranged based shard, so we lower case it.
			if strings.Contains(shard, "-") {
				shard = strings.ToLower(shard)
			}
			if keyspaceHasWildcards {
				// keyspace was a wildcard, shard is not, just try it
				_, err := server.GetShard(ctx, matchedKeyspace, shard)
				switch err {
				case nil:
					// shard exists, add it
					result = append(result, KeyspaceShard{matchedKeyspace, shard})
				case ErrNoNode:
					// no shard, ignore
				default:
					// other error
					return nil, fmt.Errorf("Cannot read shard %v/%v: %v", matchedKeyspace, shard, err)
				}
			} else {
				// keyspace and shards are not wildcards, just add the value
				result = append(result, KeyspaceShard{matchedKeyspace, shard})
			}
		}
	}
	return result, nil
}
