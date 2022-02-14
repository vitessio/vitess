package onlineddl

import (
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/stretchr/testify/require"
)

// WaitForVReplicationStatus waits for a vreplication stream to be in one of given states, or timeout
func WaitForVReplicationStatus(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, timeout time.Duration, expectStatuses ...string) (status string) {

	query, err := sqlparser.ParseAndBind("select workflow, state from _vt.vreplication where workflow=%a",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)

	statusesMap := map[string]bool{}
	for _, status := range expectStatuses {
		statusesMap[status] = true
	}
	startTime := time.Now()
	lastKnownStatus := ""
	for time.Since(startTime) < timeout {
		countMatchedShards := 0

		for _, shard := range shards {
			r, err := shard.Vttablets[0].VttabletProcess.QueryTablet(query, "", false)
			require.NoError(t, err)

			for _, row := range r.Named().Rows {
				lastKnownStatus = row["state"].ToString()
				if row["workflow"].ToString() == uuid && statusesMap[lastKnownStatus] {
					countMatchedShards++
				}
			}
		}
		if countMatchedShards == len(shards) {
			return lastKnownStatus
		}
		time.Sleep(1 * time.Second)
	}
	return lastKnownStatus
}
