package zktopo

import (
	"path"
	"sort"

	"golang.org/x/net/context"
)

// FIXME(alainjobart) Need to intercept these calls for existing objects.
// For now, this is only used for new objects, so it doesn't matter.

// ListDir is part of the topo.Backend interface.
func (zkts *Server) ListDir(ctx context.Context, cell, dirPath string) ([]string, error) {
	zkPath := path.Join(zkPathForCell(cell), dirPath)
	children, _, err := zkts.zconn.Children(zkPath)
	if err != nil {
		return nil, convertError(err)
	}
	sort.Strings(children)
	return children, nil
}
