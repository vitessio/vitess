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

package clustertest

import (
	"fmt"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestEtcdServer(t *testing.T) {
	defer cluster.PanicHandler(t)

	// Confirm basic cluster health.
	etcdHealthURL := fmt.Sprintf("http://%s:%d/health", clusterInstance.Hostname, clusterInstance.TopoPort)
	testURL(t, etcdHealthURL, "generic etcd url")

	// Confirm that we have the expected global keys for the
	// cluster's cell.
	keyPrefixes := []string{fmt.Sprintf("/vitess/global/cells/%s", cell)}
	baseEtcdctlCmdArgs := []string{"--endpoints", fmt.Sprintf("http://%s:%d", clusterInstance.TopoProcess.Host, clusterInstance.TopoProcess.Port),
		"get", "--keys-only", "-w", "simple", "--limit", "1", "--prefix"}
	for _, keyPrefix := range keyPrefixes {
		etcdctlCmd := exec.Command("etcdctl", append(baseEtcdctlCmdArgs, keyPrefix)...)
		out, err := etcdctlCmd.CombinedOutput()
		sout := string(out)
		require.NoError(t, err, sout)
		require.Contains(t, sout, keyPrefix) // Confirm that at least one key was returned
	}
}
