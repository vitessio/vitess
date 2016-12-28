package memorytopo

import (
	"path"
	"sort"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// tabletPathForAlias converts a tablet alias to the node path.
func tabletPathForAlias(alias *topodatapb.TabletAlias) string {
	return path.Join(tabletsPath, topoproto.TabletAliasString(alias), topo.TabletFile)
}

// CreateTablet implements topo.Impl.CreateTablet
func (mt *MemoryTopo) CreateTablet(ctx context.Context, tablet *topodatapb.Tablet) error {
	data, err := proto.Marshal(tablet)
	if err != nil {
		return err
	}

	nodePath := tabletPathForAlias(tablet.Alias)
	_, err = mt.Create(ctx, tablet.Alias.Cell, nodePath, data)
	return err
}

// UpdateTablet implements topo.Impl.UpdateTablet
func (mt *MemoryTopo) UpdateTablet(ctx context.Context, tablet *topodatapb.Tablet, existingVersion int64) (int64, error) {
	data, err := proto.Marshal(tablet)
	if err != nil {
		return 0, err
	}

	nodePath := tabletPathForAlias(tablet.Alias)
	version, err := mt.Update(ctx, tablet.Alias.Cell, nodePath, data, VersionFromInt(existingVersion))
	if err != nil {
		return 0, err
	}
	return int64(version.(NodeVersion)), nil
}

// DeleteTablet implements topo.Impl.DeleteTablet
func (mt *MemoryTopo) DeleteTablet(ctx context.Context, alias *topodatapb.TabletAlias) error {
	nodePath := tabletPathForAlias(alias)
	return mt.Delete(ctx, alias.Cell, nodePath, nil)
}

// GetTablet implements topo.Impl.GetTablet
func (mt *MemoryTopo) GetTablet(ctx context.Context, alias *topodatapb.TabletAlias) (*topodatapb.Tablet, int64, error) {
	nodePath := tabletPathForAlias(alias)
	data, version, err := mt.Get(ctx, alias.Cell, nodePath)
	if err != nil {
		return nil, 0, err
	}

	tablet := &topodatapb.Tablet{}
	if err := proto.Unmarshal(data, tablet); err != nil {
		return nil, 0, err
	}
	return tablet, int64(version.(NodeVersion)), nil
}

// GetTabletsByCell implements topo.Impl.GetTabletsByCell
func (mt *MemoryTopo) GetTabletsByCell(ctx context.Context, cell string) ([]*topodatapb.TabletAlias, error) {
	// Check if the cell exists first. We need to return ErrNoNode if not.
	mt.mu.Lock()
	if _, ok := mt.cells[cell]; !ok {
		mt.mu.Unlock()
		return nil, topo.ErrNoNode
	}
	mt.mu.Unlock()

	children, err := mt.ListDir(ctx, cell, tabletsPath)
	if err != nil {
		if err == topo.ErrNoNode {
			return nil, nil
		}
		return nil, err
	}

	sort.Strings(children)
	result := make([]*topodatapb.TabletAlias, len(children))
	for i, child := range children {
		result[i], err = topoproto.ParseTabletAlias(child)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
