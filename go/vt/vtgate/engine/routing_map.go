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

import querypb "github.com/youtube/vitess/go/vt/proto/query"

type routingMap map[string][]*querypb.Value

func (rtm routingMap) Add(shard string, id *querypb.Value) {
	rtm[shard] = append(rtm[shard], id)
}

func (rtm routingMap) Shards() []string {
	shards := make([]string, 0, len(rtm))
	for k := range rtm {
		shards = append(shards, k)
	}
	return shards
}

func (rtm routingMap) ShardVars(bv map[string]*querypb.BindVariable) map[string]map[string]*querypb.BindVariable {
	shardVars := make(map[string]map[string]*querypb.BindVariable, len(rtm))
	for shard, vals := range rtm {
		newbv := make(map[string]*querypb.BindVariable, len(bv)+1)
		for k, v := range bv {
			newbv[k] = v
		}
		newbv[ListVarName] = &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: vals,
		}
		shardVars[shard] = newbv
	}
	return shardVars
}
