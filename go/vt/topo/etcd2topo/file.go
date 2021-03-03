/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package etcd2topo

import (
	"path"

	"context"

	"github.com/coreos/etcd/clientv3"

	"vitess.io/vitess/go/vt/topo"
)

// Create is part of the topo.Conn interface.
func (s *Server) Create(ctx context.Context, filePath string, contents []byte) (topo.Version, error) {
	nodePath := path.Join(s.root, filePath)

	// We have to do a transaction, comparing existing version with 0.
	// This means: if the file doesn't exist, create it.
	txnresp, err := s.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(nodePath), "=", 0)).
		Then(clientv3.OpPut(nodePath, string(contents))).
		Commit()
	if err != nil {
		return nil, convertError(err, nodePath)
	}
	if !txnresp.Succeeded {
		return nil, topo.NewError(topo.NodeExists, nodePath)
	}
	return EtcdVersion(txnresp.Header.Revision), nil
}

// Update is part of the topo.Conn interface.
func (s *Server) Update(ctx context.Context, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	nodePath := path.Join(s.root, filePath)

	if version != nil {
		// We have to do a transaction. This means: if the
		// current file revision is what we expect, save it.
		txnresp, err := s.cli.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(nodePath), "=", int64(version.(EtcdVersion)))).
			Then(clientv3.OpPut(nodePath, string(contents))).
			Commit()
		if err != nil {
			return nil, convertError(err, nodePath)
		}
		if !txnresp.Succeeded {
			return nil, topo.NewError(topo.BadVersion, nodePath)
		}
		return EtcdVersion(txnresp.Header.Revision), nil
	}

	// No version specified. We can use a simple unconditional Put.
	resp, err := s.cli.Put(ctx, nodePath, string(contents))
	if err != nil {
		return nil, convertError(err, nodePath)
	}
	return EtcdVersion(resp.Header.Revision), nil
}

// Get is part of the topo.Conn interface.
func (s *Server) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	nodePath := path.Join(s.root, filePath)

	resp, err := s.cli.Get(ctx, nodePath)
	if err != nil {
		return nil, nil, convertError(err, nodePath)
	}
	if len(resp.Kvs) != 1 {
		return nil, nil, topo.NewError(topo.NoNode, nodePath)
	}

	return resp.Kvs[0].Value, EtcdVersion(resp.Kvs[0].ModRevision), nil
}

// Delete is part of the topo.Conn interface.
func (s *Server) Delete(ctx context.Context, filePath string, version topo.Version) error {
	nodePath := path.Join(s.root, filePath)

	if version != nil {
		// We have to do a transaction. This means: if the
		// node revision is what we expect, delete it,
		// otherwise get the file. If the transaction doesn't
		// succeed, we also ask for the value of the
		// node. That way we'll know if it failed because it
		// didn't exist, or because the version was wrong.
		txnresp, err := s.cli.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(nodePath), "=", int64(version.(EtcdVersion)))).
			Then(clientv3.OpDelete(nodePath)).
			Else(clientv3.OpGet(nodePath)).
			Commit()
		if err != nil {
			return convertError(err, nodePath)
		}
		if !txnresp.Succeeded {
			if len(txnresp.Responses) > 0 {
				if len(txnresp.Responses[0].GetResponseRange().Kvs) > 0 {
					return topo.NewError(topo.BadVersion, nodePath)
				}
			}
			return topo.NewError(topo.NoNode, nodePath)
		}
		return nil
	}

	// This is just a regular unconditional Delete here.
	resp, err := s.cli.Delete(ctx, nodePath)
	if err != nil {
		return convertError(err, nodePath)
	}
	if resp.Deleted != 1 {
		return topo.NewError(topo.NoNode, nodePath)
	}
	return nil
}
