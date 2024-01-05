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
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

// TestSchemaChangeTimedout ensures that the timeout functionality is working properly
// to prevent queries from hanging up and causing a mutex to be locked forever.
func TestSchemaChangeTimedout(t *testing.T) {
	const TableName = "vitess_healthstream"

	client := framework.NewClient()
	reloadEstimatedTime := 2 * time.Second

	err := cluster.SimulateMySQLHang()
	require.NoError(t, err)

	defer cluster.StopSimulateMySQLHang()

	ch := make(chan []string, 100)
	go func(ch chan []string) {
		client.StreamHealth(func(response *querypb.StreamHealthResponse) error {
			if response.RealtimeStats.TableSchemaChanged != nil {
				ch <- response.RealtimeStats.TableSchemaChanged
			}
			return nil
		})
	}(ch)

	// get a clean connection that skips toxyproxy to be able to change the schema in the underlying DB
	cleanParams := cluster.MySQLCleanConnParams()
	cleanConn, err := mysql.Connect(context.Background(), &cleanParams)
	require.NoError(t, err)
	defer cleanConn.Close()

	// change the schema to trigger the health_streamer to send a notification at a later time.
	_, err = cleanConn.ExecuteFetch("create table "+TableName+"(id bigint primary key)", -1, false)
	require.NoError(t, err)

	select {
	case <-ch: // get the schema notification
		t.Fatalf("received an schema change event from the HealthStreamer (is toxyproxy working?)")
	case <-time.After(reloadEstimatedTime):
		// Good, continue
	}

	// We will wait for the health_streamer to attempt sending a notification.
	// It's important to keep in mind that the total wait time after the simulation should be shorter than the reload timeout.
	// This is because the query timeout triggers the *DBConn.Kill() method, which in turn holds the mutex lock on the health_streamer.
	// Although not indefinitely, this can result in longer wait times.
	// It's worth noting that the behavior of *DBConn.Kill() is outside the scope of this test.
	reloadInterval := config.SignalSchemaChangeReloadInterval
	time.Sleep(reloadInterval)

	// pause simulating the mysql stall to allow the health_streamer to resume.
	err = cluster.PauseSimulateMySQLHang()
	require.NoError(t, err)

	// wait for the health_streamer to complete retrying the notification.
	reloadTimeout := config.SchemaChangeReloadTimeout
	retryEstimatedTime := reloadTimeout + reloadInterval + reloadEstimatedTime
	timeout := time.After(retryEstimatedTime)
	for {
		select {
		case res := <-ch: // get the schema notification
			if slices.Contains(res, TableName) {
				return
			}
		case <-timeout:
			t.Errorf("timed out even after the mysql hang was no longer simulated")
			return
		}
	}
}
