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
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This test depend on starting and stopping a ZK instance,
// but may leave files/processes behind if they don't succeed,
// so some manual cleanup may be required.

func TestLifeCycle(t *testing.T) {
	if testing.Short() || os.Getenv("CI") == "true" {
		t.Skip("skipping integration test in short mode and in CI (it's too flaky).")
	}

	leaderPort := 2888 + rand.IntN(100)
	electionPort := 3888 + rand.IntN(100)
	clientPort := 2181 + rand.IntN(100)
	adminPort := 8081 + rand.IntN(100)
	myID := 200 + rand.IntN(100)
	config := fmt.Sprintf("%d@voltron:%d:%d:%d", myID, leaderPort, electionPort, clientPort)

	zkConf := MakeZkConfigFromString(config, uint32(myID))
	tpcKeepAliveCfg := "tcpKeepAlive=true"
	adminServerCfg := fmt.Sprintf("admin.serverPort=%d", adminPort)
	zkConf.Extra = []string{tpcKeepAliveCfg, adminServerCfg}

	zkObservedConf, err := MakeZooCfg([]string{zkConf.ConfigFile()}, zkConf, "header")
	require.NoError(t, err)
	require.Contains(t, zkObservedConf, fmt.Sprintf("\n%s\n", tpcKeepAliveCfg), "Expected tpcKeepAliveCfg in zkObservedConf")
	require.Contains(t, zkObservedConf, fmt.Sprintf("\n%s\n", adminServerCfg), "Expected adminServerCfg in zkObservedConf")

	retryTimer := time.NewTimer(10 * time.Minute)
	defer retryTimer.Stop()

	zkd := NewZkd(zkConf)

	for {
		if err = zkd.Init(); err == nil {
			break
		}
		select {
		case <-retryTimer.C:
			require.FailNow(t, "timeout waiting for Zkd.Init() to succeed: ", err)
		default:
			time.Sleep(1 * time.Second)
		}
	}

	for {
		if err = zkd.Shutdown(); err == nil {
			break
		}
		select {
		case <-retryTimer.C:
			require.FailNow(t, "timeout waiting for Zkd.Shutdown() to succeed: ", err)
		default:
			time.Sleep(1 * time.Second)
		}
	}

	for {
		if err = zkd.Start(); err == nil {
			break
		}
		select {
		case <-retryTimer.C:
			require.FailNow(t, "timeout waiting for Zkd.Start() to succeed: ", err)
		default:
			time.Sleep(1 * time.Second)
		}
	}

	for {
		if err = zkd.Teardown(); err == nil {
			break
		}
		select {
		case <-retryTimer.C:
			require.FailNow(t, "timeout waiting for Zkd.Teardown() to succeed: ", err)
		default:
			time.Sleep(1 * time.Second)
		}
	}
}
