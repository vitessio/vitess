package memorytopo

import (
	"fmt"
	"sort"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// ListDir is part of the topo.Backend interface.
func (mt *MemoryTopo) ListDir(ctx context.Context, cell, dirPath string) ([]string, error) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	// Get the node to list.
	n := mt.nodeByPath(cell, dirPath)
	if n == nil {
		return nil, topo.ErrNoNode
	}

	// Check it's a directory.
	if !n.isDirectory() {
		return nil, fmt.Errorf("node %v in cell %v is not a directory", dirPath, cell)
	}

	var result []string
	for n := range n.children {
		result = append(result, n)
	}
	sort.Strings(result)
	return result, nil
}
