/*
Copyright 2018 The Vitess Authors
 Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
     http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package grpcclustermanagerservice

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/topo"

	clustermanagerdatapb "vitess.io/vitess/go/vt/proto/clustermanagerdata"
	clustermanagerservicepb "vitess.io/vitess/go/vt/proto/clustermanagerservice"
)

// Server ...
type Server struct {
	cm *topo.ClusterManager
}

// SetShardConfig ...
func (s *Server) SetShardConfig(ctx context.Context, shardClusterConfig *clustermanagerdatapb.SetShardConfigRequest) (resp *clustermanagerdatapb.ClusterConfig, err error) {
	err = s.cm.SetShardConfig(
		ctx,
		shardClusterConfig.Cell,
		shardClusterConfig.Keyspace,
		shardClusterConfig.Shard,
		shardClusterConfig.TabletType,
		shardClusterConfig.Nodes,
	)
	return resp, nil
}

// GetClusterConfig ...
func (s *Server) GetClusterConfig(ctx context.Context, request *clustermanagerdatapb.GetClusterConfigRequest) (shardClusterConfig *clustermanagerdatapb.ClusterConfig, err error) {
	return shardClusterConfig, nil
}

// DeleteShardConfig ...
func (s *Server) DeleteShardConfig(ctx context.Context, request *clustermanagerdatapb.DeleteShardConfigRequest) (deleteShardClusterConfigResponse *clustermanagerdatapb.DeleteShardConfigResponse, err error) {
	return deleteShardClusterConfigResponse, nil
}

// StartServer
func StartServer(server *grpc.Server, cm *topo.ClusterManager) {
	clustermanagerservicepb.RegisterVtClusterManagerServer(server, &Server{cm: cm})
}
