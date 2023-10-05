/*
Copyright 2023 The Vitess Authors.

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

package workflow

import (
	"context"

	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vterrors"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func notExistsError(name string) error {
	return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "there is no vitess cluster named %s", name)
}

func (s *Server) MountRegister(ctx context.Context, req *vtctldatapb.MountRegisterRequest) (*vtctldatapb.MountRegisterResponse, error) {
	vci, err := s.ts.GetExternalVitessCluster(ctx, req.Name)
	if err != nil {
		return &vtctldatapb.MountRegisterResponse{},
			vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get external vitess cluster in MountRegister: %v", err)
	}
	if vci != nil {
		return &vtctldatapb.MountRegisterResponse{}, notExistsError(req.Name)
	}
	vc := &topodata.ExternalVitessCluster{
		TopoConfig: &topodata.TopoConfig{
			TopoType: req.TopoType,
			Server:   req.TopoServer,
			Root:     req.TopoRoot,
		},
	}
	return &vtctldatapb.MountRegisterResponse{}, s.ts.CreateExternalVitessCluster(ctx, req.Name, vc)
}

func (s *Server) MountUnregister(ctx context.Context, req *vtctldatapb.MountUnregisterRequest) (*vtctldatapb.MountUnregisterResponse, error) {
	vci, err := s.ts.GetExternalVitessCluster(ctx, req.Name)
	if err != nil {
		return &vtctldatapb.MountUnregisterResponse{},
			vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get external vitess cluster in MountUnregister: %v", err)
	}
	if vci == nil {
		return &vtctldatapb.MountUnregisterResponse{}, notExistsError(req.Name)
	}
	return &vtctldatapb.MountUnregisterResponse{}, s.ts.DeleteExternalVitessCluster(ctx, req.Name)
}

func (s *Server) MountList(ctx context.Context, req *vtctldatapb.MountListRequest) (*vtctldatapb.MountListResponse, error) {
	vciList, err := s.ts.GetExternalVitessClusters(ctx)
	if err != nil {
		return &vtctldatapb.MountListResponse{},
			vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get external vitess cluster in MountList: %v", err)
	}
	return &vtctldatapb.MountListResponse{Names: vciList}, nil
}

func (s *Server) MountShow(ctx context.Context, req *vtctldatapb.MountShowRequest) (*vtctldatapb.MountShowResponse, error) {
	vci, err := s.ts.GetExternalVitessCluster(ctx, req.Name)
	if err != nil {
		return &vtctldatapb.MountShowResponse{},
			vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get external vitess cluster in MountShow: %v", err)
	}
	if vci == nil {
		return &vtctldatapb.MountShowResponse{}, notExistsError(req.Name)
	}
	return &vtctldatapb.MountShowResponse{
		TopoType:   vci.TopoConfig.TopoType,
		TopoServer: vci.TopoConfig.Server,
		TopoRoot:   vci.TopoConfig.Root,
		Name:       req.Name,
	}, nil
}
