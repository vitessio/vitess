// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"path"

	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"github.com/youtube/vitess/go/zk"
	"golang.org/x/net/context"
	"launchpad.net/gozk/zookeeper"
)

/*
This file contains the vschema management code for zktopo.Server
*/

const (
	vschemaPath = "vschema"
)

// SaveVSchema saves the JSON vschema into the topo.
func (zkts *Server) SaveVSchema(ctx context.Context, keyspace, vschema string) error {
	err := vindexes.ValidateVSchema([]byte(vschema))
	if err != nil {
		return err
	}
	vschemaPath := path.Join(GlobalKeyspacesPath, keyspace, vschemaPath)
	_, err = zk.CreateOrUpdate(zkts.zconn, vschemaPath, vschema, 0, zookeeper.WorldACL(zookeeper.PERM_ALL), true)
	return err
}

// GetVSchema fetches the JSON vschema from the topo.
func (zkts *Server) GetVSchema(ctx context.Context, keyspace string) (string, error) {
	vschemaPath := path.Join(GlobalKeyspacesPath, keyspace, vschemaPath)
	data, _, err := zkts.zconn.Get(vschemaPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			return "{}", nil
		}
		return "", err
	}
	return data, nil
}
