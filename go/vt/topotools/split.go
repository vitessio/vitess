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

package topotools

import (
	"errors"
	"fmt"

	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
)

// ValidateForReshard returns an error if sourceShards cannot reshard into
// targetShards.
func ValidateForReshard(sourceShards, targetShards []*topo.ShardInfo) error {
	for _, source := range sourceShards {
		for _, target := range targetShards {
			if key.KeyRangeEqual(source.KeyRange, target.KeyRange) {
				return fmt.Errorf("same keyrange is present in source and target: %v", key.KeyRangeString(source.KeyRange))
			}
		}
	}
	sourcekr, err := combineKeyRanges(sourceShards)
	if err != nil {
		return err
	}
	targetkr, err := combineKeyRanges(targetShards)
	if err != nil {
		return err
	}
	if !key.KeyRangeEqual(sourcekr, targetkr) {
		return fmt.Errorf("source and target keyranges don't match: %v vs %v", key.KeyRangeString(sourcekr), key.KeyRangeString(targetkr))
	}
	return nil
}

func combineKeyRanges(shards []*topo.ShardInfo) (*topodatapb.KeyRange, error) {
	if len(shards) == 0 {
		return nil, fmt.Errorf("there are no shards to combine")
	}
	result := shards[0].KeyRange
	krmap := make(map[string]*topodatapb.KeyRange)
	for _, si := range shards[1:] {
		krmap[si.ShardName()] = si.KeyRange
	}
	for len(krmap) != 0 {
		foundOne := false
		for k, kr := range krmap {
			newkr, ok := key.KeyRangeAdd(result, kr)
			if ok {
				foundOne = true
				result = newkr
				delete(krmap, k)
			}
		}
		if !foundOne {
			return nil, errors.New("shards don't form a contiguous keyrange")
		}
	}
	return result, nil
}
