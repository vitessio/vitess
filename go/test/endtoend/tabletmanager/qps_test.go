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
package tabletmanager

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestQPS(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	vtGateConn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer vtGateConn.Close()

	replicaConn, err := mysql.Connect(ctx, &replicaTabletParams)
	require.Nil(t, err)
	defer replicaConn.Close()

	// Sanity Check
	exec(t, vtGateConn, "delete from t1")
	exec(t, vtGateConn, "insert into t1(id, value) values(1,'a'), (2,'b')")
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("a")] [VARCHAR("b")]]`)

	// Test that VtTabletStreamHealth reports a QPS >0.0.
	// Therefore, issue several reads first.
	// NOTE: This may be potentially flaky because we'll observe a QPS >0.0
	//       exactly "once" for the duration of one sampling interval (5s) and
	//       after that we'll see 0.0 QPS rates again. If this becomes actually
	//       flaky, we need to read continuously in a separate thread.

	n := 0
	for n < 15 {
		n++
		// Run queries via vtGate so that they are counted.
		exec(t, vtGateConn, "select * from t1")
	}

	// This may take up to 5 seconds to become true because we sample the query
	// counts for the rates only every 5 seconds.

	var qpsIncreased bool
	timeout := time.Now().Add(12 * time.Second)
	for time.Now().Before(timeout) {
		result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("VtTabletStreamHealth", "-count", "1", masterTablet.Alias)
		require.Nil(t, err)
		var streamHealthResponse querypb.StreamHealthResponse

		err = json.Unmarshal([]byte(result), &streamHealthResponse)
		require.Nil(t, err)

		realTimeStats := streamHealthResponse.GetRealtimeStats()
		qps := realTimeStats.GetQps()
		if qps > 0.0 {
			qpsIncreased = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.True(t, qpsIncreased, "qps should be more that 0")
}
