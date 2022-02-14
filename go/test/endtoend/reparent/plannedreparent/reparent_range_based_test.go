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
