// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
	// vindexes needs to be imported so that they register
	// themselves against vtgate/planbuilder. This will allow
	// us to sanity check the schema being uploaded.
	_ "github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"github.com/youtube/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

/*
This file contains the vschema management code for zktopo.Server
*/

const (
	globalVSchemaPath = "/zk/global/vt/vschema"
)

// SaveVSchema saves the JSON vschema into the topo.
func (zkts *Server) SaveVSchema(vschema string) error {
	_, err := planbuilder.NewSchema([]byte(vschema))
	if err != nil {
		return err
	}
	_, err = zk.CreateOrUpdate(zkts.zconn, globalVSchemaPath, vschema, 0, zookeeper.WorldACL(zookeeper.PERM_ALL), true)
	return err
}

// GetVSchema fetches the JSON vschema from the topo.
func (zkts *Server) GetVSchema() (string, error) {
	data, _, err := zkts.zconn.Get(globalVSchemaPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			return "{}", nil
		}
		return "", err
	}
	return data, nil
}
