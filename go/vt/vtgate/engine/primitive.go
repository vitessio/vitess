// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package engine

import (
	"github.com/youtube/vitess/go/sqltypes"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/vtgate/queryinfo"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"
)

// SeqVarName is a reserved bind var name for sequence values.
const SeqVarName = "__seq"

// ListVarName is a reserved bind var name for list vars.
// This is used for sending different IN clause values
// to different shards.
const ListVarName = "__vals"

// VCursor defines the interface the engine will use
// to execute routes.
type VCursor interface {
	ExecuteMultiShard(keyspace string, shardQueries map[string]querytypes.BoundQuery, notInTransaction bool) (*sqltypes.Result, error)
	StreamExecuteMulti(query string, keyspace string, shardVars map[string]map[string]interface{}, callback func(reply *sqltypes.Result) error) error
	GetAnyShard(keyspace string) (ks, shard string, err error)
	ScatterConnExecute(query string, bindVars map[string]interface{}, keyspace string, shards []string, notInTransaction bool) (*sqltypes.Result, error)
	GetKeyspaceShards(keyspace string) (string, *topodatapb.SrvKeyspace, []*topodatapb.ShardReference, error)
	GetShardForKeyspaceID(allShards []*topodatapb.ShardReference, keyspaceID []byte) (string, error)
	ExecuteShard(keyspace string, shardQueries map[string]querytypes.BoundQuery) (*sqltypes.Result, error)
	Execute(query string, bindvars map[string]interface{}) (*sqltypes.Result, error)
	ExecuteShow(query string, bindvars map[string]interface{}, keyspace string) (*sqltypes.Result, error)
}

// Plan represents the execution strategy for a given query.
// For now it's a simple wrapper around the real instructions.
// An instruction (aka Primitive) is typically a tree where
// each node does its part by combining the results of the
// sub-nodes.
type Plan struct {
	// Original is the original query.
	Original string `json:",omitempty"`
	// Instructions contains the instructions needed to
	// fulfil the query.
	Instructions Primitive `json:",omitempty"`
}

// Size is defined so that Plan can be given to a cache.LRUCache.
// VTGate needs to maintain a cache of plans. It uses LRUCache, which
// in turn requires its objects to define a Size function.
func (pln *Plan) Size() int {
	return 1
}

// Primitive is the interface that needs to be satisfied by
// all primitives of a plan.
type Primitive interface {
	Execute(vcursor VCursor, queryConstruct *queryinfo.QueryConstruct, joinvars map[string]interface{}, wantfields bool) (*sqltypes.Result, error)
	StreamExecute(vcursor VCursor, queryConstruct *queryinfo.QueryConstruct, joinvars map[string]interface{}, wantields bool, callback func(*sqltypes.Result) error) error
	GetFields(vcursor VCursor, queryConstruct *queryinfo.QueryConstruct, joinvars map[string]interface{}) (*sqltypes.Result, error)
}
