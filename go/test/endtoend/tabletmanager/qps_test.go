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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
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
	utils.Exec(t, vtGateConn, "delete from t1")
	utils.Exec(t, vtGateConn, "insert into t1(id, value) values(1,'a'), (2,'b')")
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("a")] [VARCHAR("b")]]`)

	// Test that tablet health stream reports a QPS >0.0.
	// Therefore, issue several reads first.
	// NOTE: This may be potentially flaky because we'll observe a QPS >0.0
	//       exactly "once" for the duration of one sampling interval (5s) and
	//       after that we'll see 0.0 QPS rates again. If this becomes actually
	//       flaky, we need to read continuously in a separate thread.

	for n := 0; n < 15; n++ {
		// Run queries via vtGate so that they are counted.
		utils.Exec(t, vtGateConn, "select * from t1")
	}

	// This may take up to 5 seconds to become true because we sample the query
	// counts for the rates only every 5 seconds.

	var qpsIncreased bool
	timeout := time.Now().Add(12 * time.Second)
	for time.Now().Before(timeout) {
		shrs, err := clusterInstance.StreamTabletHealth(ctx, &primaryTablet, 1)
		require.Nil(t, err)

		streamHealthResponse := shrs[0]
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
