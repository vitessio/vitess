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
