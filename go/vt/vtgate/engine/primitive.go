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

package engine

import (
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
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
	Context() context.Context
	Execute(query string, bindvars map[string]interface{}, isDML bool) (*sqltypes.Result, error)
	ExecuteMultiShard(keyspace string, shardQueries map[string]querytypes.BoundQuery, isDML bool) (*sqltypes.Result, error)
	ExecuteStandalone(query string, bindvars map[string]interface{}, keyspace, shard string) (*sqltypes.Result, error)
	StreamExecuteMulti(query string, keyspace string, shardVars map[string]map[string]interface{}, callback func(reply *sqltypes.Result) error) error
	GetKeyspaceShards(vkeyspace *vindexes.Keyspace) (string, []*topodatapb.ShardReference, error)
	GetShardForKeyspaceID(allShards []*topodatapb.ShardReference, keyspaceID []byte) (string, error)
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
	Execute(vcursor VCursor, bindVars, joinvars map[string]interface{}, wantfields bool) (*sqltypes.Result, error)
	StreamExecute(vcursor VCursor, bindVars, joinvars map[string]interface{}, wantields bool, callback func(*sqltypes.Result) error) error
	GetFields(vcursor VCursor, bindVars, joinvars map[string]interface{}) (*sqltypes.Result, error)
}
