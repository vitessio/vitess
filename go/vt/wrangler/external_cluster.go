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

package wrangler

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/vt/proto/topodata"
)

// MountExternalVitessCluster adds a topo record for cluster with specified parameters so that it is available to a Migrate command
func (wr *Wrangler) MountExternalVitessCluster(ctx context.Context, clusterName, topoType, topoServer, topoRoot string) error {
	vci, err := wr.TopoServer().GetExternalVitessCluster(ctx, clusterName)
	if err != nil {
		return err
	}
	if vci != nil {
		return fmt.Errorf("there is already a vitess cluster named %s", clusterName)
	}
	vc := &topodata.ExternalVitessCluster{
		TopoConfig: &topodata.TopoConfig{
			TopoType: topoType,
			Server:   topoServer,
			Root:     topoRoot,
		},
	}
	return wr.TopoServer().CreateExternalVitessCluster(ctx, clusterName, vc)
}

// UnmountExternalVitessCluster deletes a mounted cluster from the topo
func (wr *Wrangler) UnmountExternalVitessCluster(ctx context.Context, clusterName string) error {
	vci, err := wr.TopoServer().GetExternalVitessCluster(ctx, clusterName)
	if err != nil {
		return err
	}
	if vci == nil {
		return fmt.Errorf("there is no vitess cluster named %s", clusterName)
	}
	return wr.TopoServer().DeleteExternalVitessCluster(ctx, clusterName)
}
