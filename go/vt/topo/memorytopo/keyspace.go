/*
Copyright 2017 Google Inc.

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

package memorytopo

import (
	"fmt"
	"path"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// CreateKeyspace implements topo.Impl.CreateKeyspace
func (mt *MemoryTopo) CreateKeyspace(ctx context.Context, keyspace string, value *topodatapb.Keyspace) error {
	data, err := proto.Marshal(value)
	if err != nil {
		return err
	}

	keyspacePath := path.Join(keyspacesPath, keyspace, topo.KeyspaceFile)
	_, err = mt.Create(ctx, topo.GlobalCell, keyspacePath, data)
	return err
}

// UpdateKeyspace implements topo.Impl.UpdateKeyspace
func (mt *MemoryTopo) UpdateKeyspace(ctx context.Context, keyspace string, value *topodatapb.Keyspace, existingVersion int64) (int64, error) {
	data, err := proto.Marshal(value)
	if err != nil {
		return -1, err
	}

	keyspacePath := path.Join(keyspacesPath, keyspace, topo.KeyspaceFile)
	version, err := mt.Update(ctx, topo.GlobalCell, keyspacePath, data, VersionFromInt(existingVersion))
	if err != nil {
		return -1, err
	}
	return int64(version.(NodeVersion)), nil
}

// GetKeyspace implements topo.Impl.GetKeyspace
func (mt *MemoryTopo) GetKeyspace(ctx context.Context, keyspace string) (*topodatapb.Keyspace, int64, error) {
	keyspacePath := path.Join(keyspacesPath, keyspace, topo.KeyspaceFile)
	data, version, err := mt.Get(ctx, topo.GlobalCell, keyspacePath)
	if err != nil {
		return nil, 0, err
	}

	k := &topodatapb.Keyspace{}
	if err = proto.Unmarshal(data, k); err != nil {
		return nil, 0, fmt.Errorf("bad keyspace data %v", err)
	}

	return k, int64(version.(NodeVersion)), nil
}

// GetKeyspaces implements topo.Impl.GetKeyspaces
func (mt *MemoryTopo) GetKeyspaces(ctx context.Context) ([]string, error) {
	children, err := mt.ListDir(ctx, topo.GlobalCell, keyspacesPath)
	switch err {
	case nil:
		return children, nil
	case topo.ErrNoNode:
		return nil, nil
	default:
		return nil, err
	}
}

// DeleteKeyspace implements topo.Impl.DeleteKeyspace
func (mt *MemoryTopo) DeleteKeyspace(ctx context.Context, keyspace string) error {
	keyspacePath := path.Join(keyspacesPath, keyspace, topo.KeyspaceFile)
	return mt.Delete(ctx, topo.GlobalCell, keyspacePath, nil)
}
