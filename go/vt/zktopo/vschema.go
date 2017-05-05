/*
Copyright 2017 Google Inc.

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

package zktopo

import (
	"encoding/json"
	"fmt"
	"path"

	zookeeper "github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
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
	_, err = zk.CreateOrUpdate(zkts.zconn, vschemaPath, data, 0, zookeeper.WorldACL(zookeeper.PermAll), true)
	return convertError(err)
}

// GetVSchema fetches the JSON vschema from the topo.
func (zkts *Server) GetVSchema(ctx context.Context, keyspace string) (*vschemapb.Keyspace, error) {
	vschemaPath := path.Join(GlobalKeyspacesPath, keyspace, vschemaPath)
	data, _, err := zkts.zconn.Get(vschemaPath)
	if err != nil {
		return nil, convertError(err)
	}
	var vs vschemapb.Keyspace
	err = json.Unmarshal(data, &vs)
	if err != nil {
		return nil, fmt.Errorf("bad vschema data (%v): %q", err, data)
	}
	return &vs, nil
}
