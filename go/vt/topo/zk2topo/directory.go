package zk2topo

import (
	"path"
	"sort"
	"sync"

	"context"

	"vitess.io/vitess/go/vt/topo"
)

// ListDir is part of the topo.Conn interface.
func (zs *Server) ListDir(ctx context.Context, dirPath string, full bool) ([]topo.DirEntry, error) {
	zkPath := path.Join(zs.root, dirPath)

	isRoot := false
	if dirPath == "" || dirPath == "/" {
		isRoot = true
	}

	children, _, err := zs.conn.Children(ctx, zkPath)
	if err != nil {
		return nil, convertError(err, zkPath)
	}
	sort.Strings(children)

	result := make([]topo.DirEntry, len(children))
	for i, child := range children {
		result[i].Name = child
	}

	if full {
		var wg sync.WaitGroup
		for i := range result {
			if isRoot && result[i].Name == electionsPath {
				// Shortcut here: we know it's an ephemeral directory.
				result[i].Type = topo.TypeDirectory
				result[i].Ephemeral = true
				continue
			}

			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				p := path.Join(zkPath, result[i].Name)
				_, stat, err := zs.conn.Get(ctx, p)
				if err != nil {
					return
				}
				if stat.NumChildren == 0 {
					result[i].Type = topo.TypeFile
				} else {
					result[i].Type = topo.TypeDirectory
				}
				if stat.EphemeralOwner != 0 {
					// This is an ephemeral node, we use this for locks.
					result[i].Ephemeral = true
				}
			}(i)
		}
		wg.Wait()
	}

	return result, nil
}
