// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk2topo

import (
	"fmt"
	"path"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// This file contains the vschema management code for zktopo.Server.
// Eventually this will be moved to the go/vt/topo package.

// SaveVSchema saves the vschema into the topo.
func (zs *Server) SaveVSchema(ctx context.Context, keyspace string, vschema *vschemapb.Keyspace) error {
	zkPath := path.Join(keyspacesPath, keyspace, topo.VSchemaFile)
	data, err := proto.Marshal(vschema)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		// No vschema, remove it. So we can remove the keyspace.
		err = zs.Delete(ctx, topo.GlobalCell, zkPath, nil)
	} else {
		_, err = zs.Update(ctx, topo.GlobalCell, zkPath, data, nil)
	}
	return err
}

// GetVSchema fetches the vschema from the topo.
func (zs *Server) GetVSchema(ctx context.Context, keyspace string) (*vschemapb.Keyspace, error) {
	zkPath := path.Join(keyspacesPath, keyspace, topo.VSchemaFile)
	data, _, err := zs.Get(ctx, topo.GlobalCell, zkPath)
	if err != nil {
		return nil, err
	}
	vs := &vschemapb.Keyspace{}
	if err := proto.Unmarshal(data, vs); err != nil {
		return nil, fmt.Errorf("bad vschema data (%v): %q", err, data)
	}
	return vs, nil
}
