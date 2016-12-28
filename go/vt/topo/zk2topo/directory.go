package zk2topo

import (
	"path"
	"sort"

	"golang.org/x/net/context"
)

// ListDir is part of the topo.Backend interface.
func (zs *Server) ListDir(ctx context.Context, cell, dirPath string) ([]string, error) {
	conn, root, err := zs.connForCell(ctx, cell)
	if err != nil {
		return nil, err
	}
	zkPath := path.Join(root, dirPath)

	children, _, err := conn.Children(ctx, zkPath)
	if err != nil {
		return nil, convertError(err)
	}
	sort.Strings(children)
	return children, nil
}
