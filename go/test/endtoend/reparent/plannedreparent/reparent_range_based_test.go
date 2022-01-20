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
	"context"
	"testing"

	"vitess.io/vitess/go/test/endtoend/reparent/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestReparentGracefulRangeBased(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	utils.ShardName = "0000000000000000-ffffffffffffffff"
	defer func() { utils.ShardName = "0" }()

	clusterInstance := utils.SetupRangeBasedCluster(ctx, t)
	defer utils.TeardownCluster(clusterInstance)
	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets

	// Perform a graceful reparent operation
	_, err := utils.Prs(t, clusterInstance, tablets[1])
	require.NoError(t, err)
	utils.ValidateTopology(t, clusterInstance, false)
	utils.CheckPrimaryTablet(t, clusterInstance, tablets[1])
	utils.ConfirmReplication(t, tablets[1], []*cluster.Vttablet{tablets[0]})
}
