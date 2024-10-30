/*
Copyright 2024 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/require"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtenv"
)

// TestMount tests various Mount-related methods.
func TestMount(t *testing.T) {
	const (
		extCluster = "extcluster"
		topoType   = "etcd2"
		topoServer = "localhost:2379"
		topoRoot   = "/vitess/global"
	)
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell")
	tmc := &fakeTMC{}
	s := NewServer(vtenv.NewTestEnv(), ts, tmc)

	resp, err := s.MountRegister(ctx, &vtctldatapb.MountRegisterRequest{
		Name:       extCluster,
		TopoType:   topoType,
		TopoServer: topoServer,
		TopoRoot:   topoRoot,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	respList, err := s.MountList(ctx, &vtctldatapb.MountListRequest{})
	require.NoError(t, err)
	require.NotNil(t, respList)
	require.Equal(t, []string{extCluster}, respList.Names)

	respShow, err := s.MountShow(ctx, &vtctldatapb.MountShowRequest{
		Name: extCluster,
	})
	require.NoError(t, err)
	require.NotNil(t, respShow)
	require.Equal(t, extCluster, respShow.Name)
	require.Equal(t, topoType, respShow.TopoType)
	require.Equal(t, topoServer, respShow.TopoServer)
	require.Equal(t, topoRoot, respShow.TopoRoot)

	respUnregister, err := s.MountUnregister(ctx, &vtctldatapb.MountUnregisterRequest{
		Name: extCluster,
	})
	require.NoError(t, err)
	require.NotNil(t, respUnregister)

	respList, err = s.MountList(ctx, &vtctldatapb.MountListRequest{})
	require.NoError(t, err)
	require.NotNil(t, respList)
	require.Nil(t, respList.Names)
}
