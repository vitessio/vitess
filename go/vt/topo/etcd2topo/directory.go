package etcd2topo

import (
	"path"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// ListDir is part of the topo.Backend interface.
func (s *Server) ListDir(ctx context.Context, cell, dirPath string) ([]string, error) {
	c, err := s.clientForCell(ctx, cell)
	if err != nil {
		return nil, err
	}
	nodePath := path.Join(c.root, dirPath) + "/"
	resp, err := s.global.cli.Get(ctx, nodePath,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithKeysOnly())
	if err != nil {
		return nil, convertError(err)
	}
	if len(resp.Kvs) == 0 {
		// No key starts with this prefix, means the directory
		// doesn't exist.
		return nil, topo.ErrNoNode
	}

	prefixLen := len(nodePath)
	var result []string
	for _, ev := range resp.Kvs {
		p := string(ev.Key)

		// Remove the prefix, base path.
		if !strings.HasPrefix(p, nodePath) {
			return nil, ErrBadResponse
		}
		p = p[prefixLen:]

		// Keep only the part until the first '/'.
		if i := strings.Index(p, "/"); i >= 0 {
			p = p[:i]
		}

		// Remove duplicates, add to list.
		if len(result) == 0 || result[len(result)-1] != p {
			result = append(result, p)
		}
	}

	return result, nil
}
