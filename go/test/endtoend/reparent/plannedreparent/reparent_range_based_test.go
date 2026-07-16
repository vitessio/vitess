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

package plannedreparent

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vitesst"
)

func TestReparentGracefulRangeBased(t *testing.T) {
	ctx := t.Context()

	ShardName = "0000000000000000-ffffffffffffffff"
	defer func() { ShardName = "0" }()

	clusterInstance := SetupRangeBasedCluster(ctx, t)
	defer TeardownCluster(t, clusterInstance)
	tablets := shardTablets(clusterInstance)

	// Perform a graceful reparent operation
	_, err := Prs(t, clusterInstance, tablets[1])
	require.NoError(t, err)
	ValidateTopology(t, clusterInstance, false)
	CheckPrimaryTablet(t, clusterInstance, tablets[1])
	ConfirmReplication(t, tablets[1], []*vitesst.Tablet{tablets[0]})
}
