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

package etcd2topo

import (
	"fmt"
	"path"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

/*
This file contains the vschema management code for etcdtopo.Server
*/

// SaveVSchema saves the JSON vschema into the topo.
func (s *Server) SaveVSchema(ctx context.Context, keyspace string, vschema *vschemapb.Keyspace) error {
	nodePath := path.Join(keyspacesPath, keyspace, topo.VSchemaFile)
	data, err := proto.Marshal(vschema)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		// No vschema, remove it. So we can remove the keyspace.
		err = s.Delete(ctx, topo.GlobalCell, nodePath, nil)
	} else {
		_, err = s.Update(ctx, topo.GlobalCell, nodePath, data, nil)
	}
	return err
}

// GetVSchema fetches the vschema from the topo.
func (s *Server) GetVSchema(ctx context.Context, keyspace string) (*vschemapb.Keyspace, error) {
	nodePath := path.Join(keyspacesPath, keyspace, topo.VSchemaFile)
	data, _, err := s.Get(ctx, topo.GlobalCell, nodePath)
	if err != nil {
		return nil, err
	}
	vs := &vschemapb.Keyspace{}
	if err := proto.Unmarshal(data, vs); err != nil {
		return nil, fmt.Errorf("bad vschema data (%v): %q", err, data)
	}
	return vs, nil
}
