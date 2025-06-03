/*
Copyright 2021 The Vitess Authors.

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

package loadkeyspace

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	hostname        = "localhost"
	cell            = "zone1"
)

func TestNoInitialKeyspace(t *testing.T) {
	defer cluster.PanicHandler(t)
	var err error

	clusterInstance = cluster.NewCluster(cell, hostname)
	defer clusterInstance.Teardown()

	// Start topo server
	err = clusterInstance.StartTopo()
	require.NoError(t, err)

	// Start vtgate with the schema_change_signal flag
	clusterInstance.VtGateExtraArgs = []string{"--schema_change_signal"}
	err = clusterInstance.StartVtgate()
	require.NoError(t, err)

	logDir := clusterInstance.VtgateProcess.LogDir

	// teardown vtgate to flush logs
	err = clusterInstance.VtgateProcess.TearDown()
	require.NoError(t, err)

	// check info logs
	all, err := os.ReadFile(path.Join(logDir, "vtgate.INFO"))
	require.NoError(t, err)
	require.Contains(t, string(all), "No keyspace to load")
}
