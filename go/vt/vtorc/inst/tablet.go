/*
Copyright 2025 The Vitess Authors.

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

package inst

import (
	"context"
	"errors"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

// ErrTabletAliasNil is a fixed error message.
var ErrTabletAliasNil = errors.New("tablet alias is nil")

var tmc tmclient.TabletManagerClient

// InitializeTMC initializes the tablet manager client to use for all VTOrc RPC calls.
func InitializeTMC() tmclient.TabletManagerClient {
	tmc = tmclient.NewTabletManagerClient()
	return tmc
}

// fullStatus gets the full status of the MySQL running in vttablet.
func fullStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
	tmcCtx, tmcCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer tmcCancel()
	return tmc.FullStatus(tmcCtx, tablet)
}
