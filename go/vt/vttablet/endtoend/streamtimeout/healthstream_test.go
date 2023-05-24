/*
Copyright 2023 The Vitess Authors.

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

package streamtimeout

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

// TestSchemaChangeTimedout ensures that the timeout functionality is working properly
// to prevent queries from hanging up and causing a mutex to be locked forever.
func TestSchemaChangeTimedout(t *testing.T) {
	client := framework.NewClient()
	reloadEstimatedTime := 2 * time.Second

	ch := make(chan []string, 100)
	go func(ch chan []string) {
		client.StreamHealth(func(response *querypb.StreamHealthResponse) error {
			if response.RealtimeStats.TableSchemaChanged != nil {
				ch <- response.RealtimeStats.TableSchemaChanged
			}
			return nil
		})
	}(ch)

	// We will set up the MySQLHang simulation.
	// To avoid flakiness, we will retry the setup if the health_streamer sends a notification before the MySQLHang is simulated.
	attempt := 1
	var tableName string
loop:
	for {
		tableName = fmt.Sprintf("vitess_sc%d", attempt)

		// change the schema to trigger the health_streamer to send a notification at a later time.
		_, err := client.Execute("create table "+tableName+"(id bigint primary key)", nil)
		require.NoError(t, err)

		// start simulating a mysql stall until a query issued by the health_streamer would hang.
		err = cluster.SimulateMySQLHang()
		require.NoError(t, err)

		select {
		case <-ch: // get the schema notification
			// The health_streamer can send a notification between the time the schema is changed and the mysql stall is simulated.
			// In this rare case, we must retry the same setup again.
			cluster.StopSimulateMySQLHang()
			attempt++

			if attempt > 5 {
				t.Errorf("failed to setup MySQLHang even after several attempts")
				return
			}
			t.Logf("retrying setup for attempt %d", attempt)
		case <-time.After(reloadEstimatedTime):
			break loop
		}
	}
	defer cluster.StopSimulateMySQLHang()

	// We will wait for the health_streamer to attempt sending a notification.
	// It's important to keep in mind that the total wait time after the simulation should be shorter than the reload timeout.
	// This is because the query timeout triggers the *DBConn.Kill() method, which in turn holds the mutex lock on the health_streamer.
	// Although not indefinitely, this can result in longer wait times.
	// It's worth noting that the behavior of *DBConn.Kill() is outside the scope of this test.
	reloadInterval := config.SignalSchemaChangeReloadIntervalSeconds.Get()
	time.Sleep(reloadInterval)

	// pause simulating the mysql stall to allow the health_streamer to resume.
	err := cluster.PauseSimulateMySQLHang()
	require.NoError(t, err)

	// wait for the health_streamer to complete retrying the notification.
	reloadTimeout := config.SchemaChangeReloadTimeout
	retryEstimatedTime := reloadTimeout + reloadInterval + reloadEstimatedTime
	select {
	case res := <-ch: // get the schema notification
		utils.MustMatch(t, []string{tableName}, res, "unexpected result from schema reload response")
	case <-time.After(retryEstimatedTime):
		t.Errorf("timed out even after the mysql hang was no longer simulated")
	}
}
