// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"encoding/json"
	"fmt"
	"path"

	"golang.org/x/net/context"
	"launchpad.net/gozk/zookeeper"

	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
)

/*
This file contains the vschema management code for zktopo.Server
*/

const (
	vschemaPath = "vschema"
)

// SaveVSchema saves the vschema into the topo.
func (zkts *Server) SaveVSchema(ctx context.Context, keyspace string, vschema *vschemapb.Keyspace) error {
	data, err := json.MarshalIndent(vschema, "", "  ")
	if err != nil {
		return err
	}
	vschemaPath := path.Join(GlobalKeyspacesPath, keyspace, vschemaPath)
	_, err = zk.CreateOrUpdate(zkts.zconn, vschemaPath, string(data), 0, zookeeper.WorldACL(zookeeper.PERM_ALL), true)
	return err
}

// GetVSchema fetches the JSON vschema from the topo.
func (zkts *Server) GetVSchema(ctx context.Context, keyspace string) (*vschemapb.Keyspace, error) {
	vschemaPath := path.Join(GlobalKeyspacesPath, keyspace, vschemaPath)
	data, _, err := zkts.zconn.Get(vschemaPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			return nil, topo.ErrNoNode
		}
		return nil, err
	}
	var vs vschemapb.Keyspace
	err = json.Unmarshal([]byte(data), &vs)
	if err != nil {
		return nil, fmt.Errorf("bad vschema data (%v): %q", err, data)
	}
	return &vs, nil
}
