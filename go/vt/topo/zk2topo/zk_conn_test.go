/*
Copyright 2024 The Vitess Authors.

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

package zk2topo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/z-division/go-zookeeper/zk"

	"vitess.io/vitess/go/testfiles"
	"vitess.io/vitess/go/vt/zkctl"
)

func TestZkConnClosedOnDisconnect(t *testing.T) {
	zkd, serverAddr := zkctl.StartLocalZk(testfiles.GoVtTopoZk2topoZkID, testfiles.GoVtTopoZk2topoPort)
	defer zkd.Teardown()

	conn := Connect(serverAddr)
	defer conn.Close()

	_, _, err := conn.Get(context.Background(), "/")
	require.NoError(t, err, "Get() failed")

	require.True(t, conn.conn.State().IsConnected(), "Connection not connected")

	oldConn := conn.conn

	// force a disconnect
	err = zkd.Shutdown()
	require.NoError(t, err)
	err = zkd.Start()
	require.NoError(t, err)

	// do another get to trigger a new connection
	require.Eventually(t, func() bool {
		_, _, err = conn.Get(context.Background(), "/")
		return err == nil
	}, 10*time.Second, 100*time.Millisecond)

	// Check that old connection is closed
	_, _, err = oldConn.Get("/")
	require.ErrorContains(t, err, "zookeeper is closing")

	require.Equal(t, zk.StateDisconnected, oldConn.State(), "Connection is not in disconnected state")
}
