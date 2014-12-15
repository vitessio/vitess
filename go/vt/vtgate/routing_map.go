// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

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
