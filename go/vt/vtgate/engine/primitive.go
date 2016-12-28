// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package engine

import "github.com/youtube/vitess/go/sqltypes"

// SeqVarName is a reserved bind var name for sequence values.
const SeqVarName = "__seq"

// ListVarName is a reserved bind var name for list vars.
// This is used for sending different IN clause values
// to different shards.
const ListVarName = "__vals"

// VCursor defines the interface the engine will use
// to execute routes.
type VCursor interface {
	ExecuteRoute(route *Route, joinvars map[string]interface{}) (*sqltypes.Result, error)
	StreamExecuteRoute(route *Route, joinvars map[string]interface{}, sendReply func(*sqltypes.Result) error) error
	GetRouteFields(route *Route, joinvars map[string]interface{}) (*sqltypes.Result, error)
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
	Execute(vcursor VCursor, joinvars map[string]interface{}, wantfields bool) (*sqltypes.Result, error)
	StreamExecute(vcursor VCursor, joinvars map[string]interface{}, wantields bool, sendReply func(*sqltypes.Result) error) error
	GetFields(vcursor VCursor, joinvars map[string]interface{}) (*sqltypes.Result, error)
}
