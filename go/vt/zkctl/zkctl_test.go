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
package zkctl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// This test depend on starting and stopping a ZK instance,
// but may leave files/processes behind if they don't succeed,
// so some manual cleanup may be required.

func TestLifeCycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode.")
	}

	config := "255@voltron:2888:3888:2181"
	myID := 255

	zkConf := MakeZkConfigFromString(config, uint32(myID))
	tpcKeepAliveCfg := "tcpKeepAlive=true"
	adminServerCfg := "admin.serverPort=8081"
	zkConf.Extra = []string{tpcKeepAliveCfg, adminServerCfg}

	zkObservedConf, err := MakeZooCfg([]string{zkConf.ConfigFile()}, zkConf, "header")
	require.NoError(t, err, "MakeZooCfg err: %v", err)
	require.Contains(t, zkObservedConf, fmt.Sprintf("\n%s\n", tpcKeepAliveCfg), "Expected tpcKeepAliveCfg in zkObservedConf")
	require.Contains(t, zkObservedConf, fmt.Sprintf("\n%s\n", adminServerCfg), "Expected adminServerCfg in zkObservedConf")

	zkd := NewZkd(zkConf)
	err = zkd.Init()
	require.NoError(t, err, "Init() err: %v", err)

	err = zkd.Shutdown()
	require.NoError(t, err, "Shutdown() err: %v", err)

	err = zkd.Start()
	require.NoError(t, err, "Start() err: %v", err)

	err = zkd.Teardown()
	require.NoError(t, err, "Teardown() err: %v", err)
}
