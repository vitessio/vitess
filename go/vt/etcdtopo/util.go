// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"path"

	"github.com/coreos/go-etcd/etcd"
)

// getNodeNames returns a list of sub-node names listed in the given Response.
// Key names are given as fully qualified paths in the Response, so we return
// the base name.
func getNodeNames(resp *etcd.Response) ([]string, error) {
	if resp.Node == nil {
		return nil, ErrBadResponse
	}

	names := make([]string, 0, len(resp.Node.Nodes))
	for _, n := range resp.Node.Nodes {
		names = append(names, path.Base(n.Key))
	}
	return names, nil
}
