package etcd2topo

import (
	"path"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/clientv3"
	"github.com/youtube/vitess/go/vt/topo"
)

// Create is part of the topo.Backend interface.
func (s *Server) Create(ctx context.Context, cell, filePath string, contents []byte) (topo.Version, error) {
	c, err := s.clientForCell(ctx, cell)
	if err != nil {
		return nil, err
	}
	nodePath := path.Join(c.root, filePath)

	// We have to do a transaction, comparing existing version with 0.
	txnresp, err := c.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(nodePath), "=", 0)).
		Then(clientv3.OpPut(nodePath, string(contents))).
		Commit()
	if err != nil {
		return nil, convertError(err)
	}
	if !txnresp.Succeeded {
		return nil, topo.ErrNodeExists
	}
	return EtcdVersion(txnresp.Header.Revision), nil
}

// Update is part of the topo.Backend interface.
func (s *Server) Update(ctx context.Context, cell, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	c, err := s.clientForCell(ctx, cell)
	if err != nil {
		return nil, err
	}
	nodePath := path.Join(c.root, filePath)

	if version != nil {
		// We have to do a transaction.
		txnresp, err := c.cli.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(nodePath), "=", int64(version.(EtcdVersion)))).
			Then(clientv3.OpPut(nodePath, string(contents))).
			Commit()
		if err != nil {
			return nil, convertError(err)
		}
		if !txnresp.Succeeded {
			return nil, topo.ErrBadVersion
		}
		return EtcdVersion(txnresp.Header.Revision), nil
	}

	// This is just a regular unconditional Put here.
	resp, err := c.cli.Put(ctx, nodePath, string(contents))
	if err != nil {
		return nil, convertError(err)
	}
	return EtcdVersion(resp.Header.Revision), nil
}

// Get is part of the topo.Backend interface.
func (s *Server) Get(ctx context.Context, cell, filePath string) ([]byte, topo.Version, error) {
	c, err := s.clientForCell(ctx, cell)
	if err != nil {
		return nil, nil, err
	}
	nodePath := path.Join(c.root, filePath)

	resp, err := c.cli.Get(ctx, nodePath)
	if err != nil {
		return nil, nil, convertError(err)
	}
	if len(resp.Kvs) != 1 {
		return nil, nil, topo.ErrNoNode
	}

	return resp.Kvs[0].Value, EtcdVersion(resp.Kvs[0].ModRevision), nil
}

// Delete is part of the topo.Backend interface.
func (s *Server) Delete(ctx context.Context, cell, filePath string, version topo.Version) error {
	c, err := s.clientForCell(ctx, cell)
	if err != nil {
		return err
	}
	nodePath := path.Join(c.root, filePath)

	if version != nil {
		// We have to do a transaction.  If the transaction
		// doesnt' succeed, we also ask for the value of the
		// node. That way we'll know if it failed because it
		// didn't exist, or because the version was wrong.
		txnresp, err := c.cli.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(nodePath), "=", int64(version.(EtcdVersion)))).
			Then(clientv3.OpDelete(nodePath)).
			Else(clientv3.OpGet(nodePath)).
			Commit()
		if err != nil {
			return convertError(err)
		}
		if !txnresp.Succeeded {
			if len(txnresp.Responses) > 0 {
				if len(txnresp.Responses[0].GetResponseRange().Kvs) > 0 {
					return topo.ErrBadVersion
				}
			}
			return topo.ErrNoNode
		}
		return nil
	}

	// This is just a regular unconditional Delete here.
	resp, err := c.cli.Delete(ctx, nodePath)
	if err != nil {
		return convertError(err)
	}
	if resp.Deleted != 1 {
		return topo.ErrNoNode
	}
	return nil
}
