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

package topo

import (
	"context"
	"path"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/event"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/events"
	"vitess.io/vitess/go/vt/vterrors"
)

// VitessClusterInfo is a meta struct that contains metadata to give the
// data more context and convenience. This is the main way we interact
// with a vitess cluster stored in the topo.
type VitessClusterInfo struct {
	ClusterName string
	version     Version
	*topodatapb.VitessCluster
}

// GetVitessClusterDir returns node path containing external vitess clusters
func GetVitessClusterDir() string {
	return path.Join(ExternalClustersFile, ExternalClusterVitess)
}

// GetVitessClusterPath returns node path containing external clusters
func GetVitessClusterPath(clusterName string) string {
	return path.Join(GetVitessClusterDir(), clusterName)
}

// CreateVitessCluster creates a topo record for the passed vitess cluster
func (ts *Server) CreateVitessCluster(ctx context.Context, clusterName string, value *topodatapb.VitessCluster) error {
	data, err := proto.Marshal(value)
	if err != nil {
		return err
	}

	if _, err := ts.globalCell.Create(ctx, GetVitessClusterPath(clusterName), data); err != nil {
		return err
	}

	event.Dispatch(&events.VitessClusterChange{
		ClusterName:   clusterName,
		VitessCluster: value,
		Status:        "created",
	})
	return nil
}

// GetVitessCluster returns a topo record for the named vitess cluster
func (ts *Server) GetVitessCluster(ctx context.Context, clusterName string) (*VitessClusterInfo, error) {
	data, version, err := ts.globalCell.Get(ctx, GetVitessClusterPath(clusterName))
	switch {
	case IsErrType(err, NoNode):
		return nil, nil
	case err == nil:
	default:
		return nil, err
	}
	vc := &topodatapb.VitessCluster{}
	if err = proto.Unmarshal(data, vc); err != nil {
		return nil, vterrors.Wrap(err, "bad vitess cluster data")
	}

	return &VitessClusterInfo{
		ClusterName:   clusterName,
		version:       version,
		VitessCluster: vc,
	}, nil
}

// UpdateVitessCluster updates the topo record for the named vitess cluster
func (ts *Server) UpdateVitessCluster(ctx context.Context, vc *VitessClusterInfo) error {
	//FIXME: check for cluster lock
	data, err := proto.Marshal(vc.VitessCluster)
	if err != nil {
		return err
	}
	version, err := ts.globalCell.Update(ctx, GetVitessClusterPath(vc.ClusterName), data, vc.version)
	if err != nil {
		return err
	}
	vc.version = version

	event.Dispatch(&events.VitessClusterChange{
		ClusterName:   vc.ClusterName,
		VitessCluster: vc.VitessCluster,
		Status:        "updated",
	})
	return nil
}

// DeleteVitessCluster deletes the topo record for the named vitess cluster
func (ts *Server) DeleteVitessCluster(ctx context.Context, clusterName string) error {
	if err := ts.globalCell.Delete(ctx, GetVitessClusterPath(clusterName), nil); err != nil {
		return err
	}

	event.Dispatch(&events.VitessClusterChange{
		ClusterName:   clusterName,
		VitessCluster: nil,
		Status:        "deleted",
	})
	return nil
}

// GetVitessClusters returns the list of external vitess clusters in the topology.
func (ts *Server) GetVitessClusters(ctx context.Context) ([]string, error) {
	children, err := ts.globalCell.ListDir(ctx, GetVitessClusterDir(), false /*full*/)
	switch {
	case err == nil:
		return DirEntriesToStringArray(children), nil
	case IsErrType(err, NoNode):
		return nil, nil
	default:
		return nil, err
	}
}
