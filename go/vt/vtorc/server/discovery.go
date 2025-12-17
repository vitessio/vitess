/*
Copyright 2022 The Vitess Authors.

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

package server

import (
	"context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/logic"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// StartVTOrcDiscovery starts VTOrc discovery serving
func StartVTOrcDiscovery() error {
	cell := config.GetCell()
	if cell == "" {
		// TODO: remove warning in v25+.
		log.Fatal("--cell is a required flag")
		//log.Warning("WARNING: --cell will become a required vtorc flag in Vitess v25 and up")
	}

	ts := topo.Open()
	ctx, cancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer cancel()
	_, err := ts.GetCellInfo(ctx, cell, true /* strongRead */)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "failed to validate cell %s: %+v", cell, err)
	}

	log.Info("Starting Discovery")
	go logic.ContinuousDiscovery(ts)
	return nil
}
