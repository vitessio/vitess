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

// ExternalVitessClusterInfo is a meta struct that contains metadata to give the
// data more context and convenience. This is the main way we interact
// with a vitess cluster stored in the topo.
type ExternalVitessClusterInfo struct {
	ClusterName string
	version     Version
	*topodatapb.ExternalVitessCluster
}

// GetExternalVitessClusterDir returns node path containing external vitess clusters
func GetExternalVitessClusterDir() string {
	return path.Join(ExternalClustersFile, ExternalClusterVitess)
}

// GetExternalVitessClusterPath returns node path containing external clusters
func GetExternalVitessClusterPath(clusterName string) string {
	return path.Join(GetExternalVitessClusterDir(), clusterName)
}

// CreateExternalVitessCluster creates a topo record for the passed vitess cluster
func (ts *Server) CreateExternalVitessCluster(ctx context.Context, clusterName string, value *topodatapb.ExternalVitessCluster) error {
	data, err := proto.Marshal(value)
	if err != nil {
		return err
	}

	if _, err := ts.globalCell.Create(ctx, GetExternalVitessClusterPath(clusterName), data); err != nil {
		return err
	}

	event.Dispatch(&events.ExternalVitessClusterChange{
		ClusterName:           clusterName,
		ExternalVitessCluster: value,
		Status:                "created",
	})
	return nil
}

// GetExternalVitessCluster returns a topo record for the named vitess cluster
func (ts *Server) GetExternalVitessCluster(ctx context.Context, clusterName string) (*ExternalVitessClusterInfo, error) {
	data, version, err := ts.globalCell.Get(ctx, GetExternalVitessClusterPath(clusterName))
	switch {
	case IsErrType(err, NoNode):
		return nil, nil
	case err == nil:
	default:
		return nil, err
	}
	vc := &topodatapb.ExternalVitessCluster{}
	if err = proto.Unmarshal(data, vc); err != nil {
		return nil, vterrors.Wrap(err, "bad vitess cluster data")
	}

	return &ExternalVitessClusterInfo{
		ClusterName:           clusterName,
		version:               version,
		ExternalVitessCluster: vc,
	}, nil
}

// UpdateExternalVitessCluster updates the topo record for the named vitess cluster
func (ts *Server) UpdateExternalVitessCluster(ctx context.Context, vc *ExternalVitessClusterInfo) error {
	//FIXME: check for cluster lock
	data, err := proto.Marshal(vc.ExternalVitessCluster)
	if err != nil {
		return err
	}
	version, err := ts.globalCell.Update(ctx, GetExternalVitessClusterPath(vc.ClusterName), data, vc.version)
	if err != nil {
		return err
	}
	vc.version = version

	event.Dispatch(&events.ExternalVitessClusterChange{
		ClusterName:           vc.ClusterName,
		ExternalVitessCluster: vc.ExternalVitessCluster,
		Status:                "updated",
	})
	return nil
}

// DeleteExternalVitessCluster deletes the topo record for the named vitess cluster
func (ts *Server) DeleteExternalVitessCluster(ctx context.Context, clusterName string) error {
	if err := ts.globalCell.Delete(ctx, GetExternalVitessClusterPath(clusterName), nil); err != nil {
		return err
	}

	event.Dispatch(&events.ExternalVitessClusterChange{
		ClusterName:           clusterName,
		ExternalVitessCluster: nil,
		Status:                "deleted",
	})
	return nil
}

// GetExternalVitessClusters returns the list of external vitess clusters in the topology.
func (ts *Server) GetExternalVitessClusters(ctx context.Context) ([]string, error) {
	children, err := ts.globalCell.ListDir(ctx, GetExternalVitessClusterDir(), false /*full*/)
	switch {
	case err == nil:
		return DirEntriesToStringArray(children), nil
	case IsErrType(err, NoNode):
		return nil, nil
	default:
		return nil, err
	}
}
