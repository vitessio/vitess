// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import "github.com/youtube/vitess/go/vt/vtgate/planbuilder"

type routingMap map[string][]interface{}

func (rtm routingMap) Add(shard string, id interface{}) {
	ids := rtm[shard]
	// Check for duplicates
	for _, current := range ids {
		if id == current {
			return
		}
	}
	rtm[shard] = append(ids, id)
}

func (rtm routingMap) Shards() []string {
	shards := make([]string, 0, len(rtm))
	for k := range rtm {
		shards = append(shards, k)
	}
	return shards
}

func (rtm routingMap) ShardVars(bv map[string]interface{}) map[string]map[string]interface{} {
	shardVars := make(map[string]map[string]interface{}, len(rtm))
	for shard, vals := range rtm {
		newbv := make(map[string]interface{}, len(bv)+1)
		for k, v := range bv {
			newbv[k] = v
		}
		newbv[planbuilder.ListVarName] = vals
		shardVars[shard] = newbv
	}
	return shardVars
}
