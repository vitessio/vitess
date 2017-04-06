package memorytopo

import (
	"fmt"
	"path"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// GetSrvKeyspaceNames implements topo.Impl.GetSrvKeyspaceNames
func (mt *MemoryTopo) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	children, err := mt.ListDir(ctx, cell, keyspacesPath)
	switch err {
	case nil:
		return children, nil
	case topo.ErrNoNode:
		return nil, nil
	default:
		return nil, err
	}
}

// UpdateSrvKeyspace implements topo.Impl.UpdateSrvKeyspace
func (mt *MemoryTopo) UpdateSrvKeyspace(ctx context.Context, cell, keyspace string, srvKeyspace *topodatapb.SrvKeyspace) error {
	nodePath := path.Join(keyspacesPath, keyspace, topo.SrvKeyspaceFile)
	data, err := proto.Marshal(srvKeyspace)
	if err != nil {
		return err
	}
	_, err = mt.Update(ctx, cell, nodePath, data, nil)
	return err
}

// DeleteSrvKeyspace implements topo.Impl.DeleteSrvKeyspace
func (mt *MemoryTopo) DeleteSrvKeyspace(ctx context.Context, cell, keyspace string) error {
	nodePath := path.Join(keyspacesPath, keyspace, topo.SrvKeyspaceFile)
	return mt.Delete(ctx, cell, nodePath, nil)
}

// GetSrvKeyspace implements topo.Impl.GetSrvKeyspace
func (mt *MemoryTopo) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	nodePath := path.Join(keyspacesPath, keyspace, topo.SrvKeyspaceFile)
	data, _, err := mt.Get(ctx, cell, nodePath)
	if err != nil {
		return nil, err
	}
	srvKeyspace := &topodatapb.SrvKeyspace{}
	if err := proto.Unmarshal(data, srvKeyspace); err != nil {
		return nil, fmt.Errorf("SrvKeyspace unmarshal failed: %v %v", data, err)
	}
	return srvKeyspace, nil
}

// UpdateSrvVSchema implements topo.Impl.UpdateSrvVSchema
func (mt *MemoryTopo) UpdateSrvVSchema(ctx context.Context, cell string, srvVSchema *vschemapb.SrvVSchema) error {
	nodePath := topo.SrvVSchemaFile
	data, err := proto.Marshal(srvVSchema)
	if err != nil {
		return err
	}
	_, err = mt.Update(ctx, cell, nodePath, data, nil)
	return err
}

// GetSrvVSchema implements topo.Impl.GetSrvVSchema
func (mt *MemoryTopo) GetSrvVSchema(ctx context.Context, cell string) (*vschemapb.SrvVSchema, error) {
	nodePath := topo.SrvVSchemaFile
	data, _, err := mt.Get(ctx, cell, nodePath)
	if err != nil {
		return nil, err
	}
	srvVSchema := &vschemapb.SrvVSchema{}
	if err := proto.Unmarshal(data, srvVSchema); err != nil {
		return nil, fmt.Errorf("SrvVSchema unmarshal failed: %v %v", data, err)
	}
	return srvVSchema, nil
}
