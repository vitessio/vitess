package etcdtopo

import (
	"path"

	"github.com/youtube/vitess/go/vt/topo"

	"golang.org/x/net/context"
)

func (s *Server) clientForCell(cell string) (Client, error) {
	if cell == topo.GlobalCell {
		return s.getGlobal(), nil
	}
	cc, err := s.getCell(cell)
	if err != nil {
		return nil, err
	}
	return cc.Client, nil
}

// ListDir is part of the topo.Backend interface.
func (s *Server) ListDir(ctx context.Context, cell, dirPath string) ([]string, error) {
	c, err := s.clientForCell(cell)
	if err != nil {
		return nil, err
	}

	// clean all extra '/' before getting the contents.
	dirPath = path.Clean(dirPath)
	resp, err := c.Get(dirPath, true /* sort */, false /* recursive */)
	if err != nil {
		return nil, convertError(err)
	}

	var names []string
	for _, n := range resp.Node.Nodes {
		base := path.Base(n.Key)
		// we have to remove /vt from global listings, it is used for /vt/cells.
		if dirPath == "/" && base == "vt" {
			continue
		}
		names = append(names, base)
	}
	return names, nil
}
