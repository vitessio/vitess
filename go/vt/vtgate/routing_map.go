// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

type routingMap map[string][]interface{}

func (rtm routingMap) Add(shard string, id interface{}) {
	ids, ok := rtm[shard]
	if !ok {
		ids = make([]interface{}, 0, 8)
		rtm[shard] = ids
	}
	// Check for duplicates
	for _, current := range ids {
		if id == current {
			return
		}
	}
	ids = append(ids, id)
}

func (rtm routingMap) Shards() []string {
	shards := make([]string, 0, len(rtm))
	for k := range rtm {
		shards = append(shards, k)
	}
	return shards
}
