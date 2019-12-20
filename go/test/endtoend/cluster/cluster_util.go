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

package cluster

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	tabletpb "vitess.io/vitess/go/vt/proto/topodata"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

var (
	tmClient = tmc.NewClient()
)

// GetMasterPosition gets the master position of required vttablet
func GetMasterPosition(t *testing.T, vttablet Vttablet, hostname string) (string, string) {
	ctx := context.Background()
	vtablet := getTablet(vttablet.GrpcPort, hostname)
	pos, err := tmClient.MasterPosition(ctx, vtablet)
	require.NoError(t, err)
	gtID := strings.SplitAfter(pos, "/")[1]
	return pos, gtID
}

func getTablet(tabletGrpcPort int, hostname string) *tabletpb.Tablet {
	portMap := make(map[string]int32)
	portMap["grpc"] = int32(tabletGrpcPort)
	return &tabletpb.Tablet{Hostname: hostname, PortMap: portMap}
}
