/*
Copyright 2019 The Vitess Authors.

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
	"path"
	"strings"
	"sync"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/fileutil"
	"vitess.io/vitess/go/vt/log"
)

// ResolveKeyspaceWildcard will resolve keyspace wildcards.
// - If the param is not a wildcard, it will just be returned (if the keyspace
//   doesn't exist, it is still returned).
// - If the param is a wildcard, it will get all keyspaces and returns
//   the ones which match the wildcard (which may be an empty list).
func (ts *Server) ResolveKeyspaceWildcard(ctx context.Context, param string) ([]string, error) {
	if !fileutil.HasWildcard(param) {
		return []string{param}, nil
	}

	var result []string

	keyspaces, err := ts.GetKeyspaces(ctx)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to read keyspaces from topo")
	}
	for _, k := range keyspaces {
		matched, err := path.Match(param, k)
		if err != nil {
			return nil, vterrors.Wrapf(err, "invalid pattern %v", param)
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
func (ts *Server) ResolveShardWildcard(ctx context.Context, param string) ([]KeyspaceShard, error) {
	parts := strings.Split(param, "/")
	if len(parts) != 2 {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid shard path: %v", param)
	}
	result := make([]KeyspaceShard, 0, 1)

	// get all the matched keyspaces first, remember if it was a wildcard
	keyspaceHasWildcards := fileutil.HasWildcard(parts[0])
	matchedKeyspaces, err := ts.ResolveKeyspaceWildcard(ctx, parts[0])
	if err != nil {
		return nil, err
	}

	// for each matched keyspace, get the shards
	for _, matchedKeyspace := range matchedKeyspaces {
		shard := parts[1]
		if fileutil.HasWildcard(shard) {
			// get all the shards for the keyspace
			shardNames, err := ts.GetShardNames(ctx, matchedKeyspace)
			switch {
			case err == nil:
				// got all the shards, we can keep going
			case IsErrType(err, NoNode):
				// keyspace doesn't exist
				if keyspaceHasWildcards {
					// that's the */* case when a keyspace has no shards
					continue
				}
				return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "keyspace %v doesn't exist", matchedKeyspace)
			default:
				return nil, vterrors.Wrapf(err, "cannot read keyspace shards for %v", matchedKeyspace)
			}
			for _, s := range shardNames {
				matched, err := path.Match(shard, s)
				if err != nil {
					return nil, vterrors.Wrapf(err, "invalid pattern %v", shard)
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
				_, err := ts.GetShard(ctx, matchedKeyspace, shard)
				switch {
				case err == nil:
					// shard exists, add it
					result = append(result, KeyspaceShard{matchedKeyspace, shard})
				case IsErrType(err, NoNode):
					// no shard, ignore
				default:
					// other error
					return nil, vterrors.Wrapf(err, "cannot read shard %v/%v", matchedKeyspace, shard)
				}
			} else {
				// keyspace and shards are not wildcards, just add the value
				result = append(result, KeyspaceShard{matchedKeyspace, shard})
			}
		}
	}
	return result, nil
}

// ResolveWildcards resolves paths like:
// /keyspaces/*/Keyspace
// into real existing paths
//
// If you send paths that don't contain any wildcard and
// don't exist, this function will return an empty array.
func (ts *Server) ResolveWildcards(ctx context.Context, cell string, paths []string) ([]string, error) {
	results := make([][]string, len(paths))
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	var firstError error

	for i, p := range paths {
		wg.Add(1)
		parts := strings.Split(p, "/")
		go func(i int) {
			defer wg.Done()
			subResult, err := ts.resolveRecursive(ctx, cell, parts, true)
			if err != nil {
				mu.Lock()
				if firstError != nil {
					log.Infof("Multiple error: %v", err)
				} else {
					firstError = err
				}
				mu.Unlock()
			} else {
				results[i] = subResult
			}
		}(i)
	}

	wg.Wait()
	if firstError != nil {
		return nil, firstError
	}

	result := make([]string, 0, 32)
	for i := 0; i < len(paths); i++ {
		subResult := results[i]
		if subResult != nil {
			result = append(result, subResult...)
		}
	}

	return result, nil
}

func (ts *Server) resolveRecursive(ctx context.Context, cell string, parts []string, toplevel bool) ([]string, error) {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return nil, err
	}

	for i, part := range parts {
		if fileutil.HasWildcard(part) {
			var children []DirEntry
			var err error
			parentPath := strings.Join(parts[:i], "/")
			children, err = conn.ListDir(ctx, parentPath, false /*full*/)
			if err != nil {
				// we asked for something like
				// /keyspaces/aaa/* and
				// /keyspaces/aaa doesn't exist
				// -> return empty list, no error
				if IsErrType(err, NoNode) {
					return nil, nil
				}
				// otherwise we return the error
				return nil, err
			}

			results := make([][]string, len(children))
			wg := &sync.WaitGroup{}
			mu := &sync.Mutex{}
			var firstError error

			for j, child := range children {
				matched, err := path.Match(part, child.Name)
				if err != nil {
					return nil, err
				}
				if matched {
					// we have a match!
					wg.Add(1)
					newParts := make([]string, len(parts))
					copy(newParts, parts)
					newParts[i] = child.Name
					go func(j int) {
						defer wg.Done()
						subResult, err := ts.resolveRecursive(ctx, cell, newParts, false)
						if err != nil {
							mu.Lock()
							if firstError != nil {
								log.Infof("Multiple error: %v", err)
							} else {
								firstError = err
							}
							mu.Unlock()
						} else {
							results[j] = subResult
						}
					}(j)
				}
			}

			wg.Wait()
			if firstError != nil {
				return nil, firstError
			}

			result := make([]string, 0, 32)
			for j := 0; j < len(children); j++ {
				subResult := results[j]
				if subResult != nil {
					result = append(result, subResult...)
				}
			}

			// we found a part that is a wildcard, we
			// added the children already, we're done
			return result, nil
		}
	}

	// no part contains a wildcard, add the path if it exists, and done
	p := strings.Join(parts, "/")
	if toplevel {
		// for whatever the user typed at the toplevel, we don't
		// check it exists or not, we just return it
		return []string{p}, nil
	}

	// This is an expanded path, we need to check if it exists.
	if _, err = conn.ListDir(ctx, p, false /*full*/); err == nil {
		// The path exists as a directory, return it.
		return []string{p}, nil
	}
	_, _, err = conn.Get(ctx, p)
	if err == nil {
		// The path exists as a file, return it.
		return []string{p}, nil
	} else if IsErrType(err, NoNode) {
		// The path doesn't exist, don't return anything.
		return nil, nil
	}
	return nil, err
}
