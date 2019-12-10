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

Re-runs InitialSharding using multiple-split-diff.

*/

package multisplitdiff

import (
	"testing"

	sharding "vitess.io/vitess/go/test/endtoend/sharding/initialsharding"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestInitialShardingMultiSplitDiff(t *testing.T) {
	code, err := sharding.ClusterWrapper(false)
	if err != nil {
		t.Errorf("setup failed with status code %d", code)
	}
	sharding.TestInitialSharding(t, &sharding.ClusterInstance.Keyspaces[0], querypb.Type_UINT64, false, false, true)
	defer sharding.ClusterInstance.Teardown()
}
