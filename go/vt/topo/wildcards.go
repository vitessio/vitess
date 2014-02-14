// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"fmt"
	"path"
	"strings"

	"github.com/youtube/vitess/go/fileutil"
)

// WildcardBackend is a subset of Server for the methods used by the
// wildcard code. This lets us test with a very simple fake topo server.
type WildcardBackend interface {
	// GetKeyspaces returns the known keyspaces. They shall be sorted.
	GetKeyspaces() ([]string, error)

	// GetShard reads a shard and returns it.
	// Can return ErrNoNode
	GetShard(keyspace, shard string) (*ShardInfo, error)

	// GetShardNames returns the known shards in a keyspace.
	// Can return ErrNoNode
	GetShardNames(keyspace string) ([]string, error)
}

// ResolveKeyspaceWildcard will resolve keyspace wildcards.
// - If the param is not a wildcard, it will just be returned (if the keyspace
//   doesn't exist, it is still returned).
// - If the param is a wildcard, it will get all keyspaces and returns
//   the ones which match the wildcard (which may be an empty list).
func ResolveKeyspaceWildcard(server WildcardBackend, param string) ([]string, error) {
	result := make([]string, 0)

	if !fileutil.HasWildcard(param) {
		return []string{param}, nil
	}

	keyspaces, err := server.GetKeyspaces()
	if err != nil {
		return nil, err
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
func ResolveShardWildcard(server WildcardBackend, param string) ([]KeyspaceShard, error) {
	parts := strings.Split(param, "/")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid shard path: %v", param)
	}
	result := make([]KeyspaceShard, 0, 1)

	// get all the matched keyspaces first, remember if it was a wildcard
	keyspaceHasWildcards := fileutil.HasWildcard(parts[0])
	var matchedKeyspaces []string
	if keyspaceHasWildcards {
		keyspaces, err := server.GetKeyspaces()
		if err != nil {
			return nil, fmt.Errorf("failed to read keyspaces from topo: %v", err)
		}
		for _, k := range keyspaces {
			matched, err := path.Match(parts[0], k)
			if err != nil {
				return nil, fmt.Errorf("invalid pattern: %v", err)
			}
			if matched {
				matchedKeyspaces = append(matchedKeyspaces, k)
			}
		}
	} else {
		matchedKeyspaces = []string{parts[0]}
	}

	// for each matched keyspace, get the shards
	for _, matchedKeyspace := range matchedKeyspaces {
		if fileutil.HasWildcard(parts[1]) {
			// get all the shards for the keyspace
			shardNames, err := server.GetShardNames(matchedKeyspace)
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
				matched, err := path.Match(parts[1], s)
				if err != nil {
					return nil, fmt.Errorf("Invalid pattern %v: %v", parts[1], err)
				}
				if matched {
					result = append(result, KeyspaceShard{matchedKeyspace, s})
				}
			}
		} else {
			if keyspaceHasWildcards {
				// keyspace was a wildcard, shard is not, just try it
				_, err := server.GetShard(matchedKeyspace, parts[1])
				switch err {
				case nil:
					// shard exists, add it
					result = append(result, KeyspaceShard{matchedKeyspace, parts[1]})
				case ErrNoNode:
					// no shard, ignore
				default:
					// other error
					return nil, fmt.Errorf("Cannot read shard %v/%v: %v", matchedKeyspace, parts[1], err)
				}
			} else {
				// keyspace and shards are not wildcards, just add the value
				result = append(result, KeyspaceShard{matchedKeyspace, parts[1]})
			}
		}
	}
	return result, nil
}
