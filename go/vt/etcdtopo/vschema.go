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

package etcdtopo

import (
	"encoding/json"
	"fmt"

	"golang.org/x/net/context"

	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
	"github.com/youtube/vitess/go/vt/topo"
)

/*
This file contains the vschema management code for etcdtopo.Server
*/

// SaveVSchema saves the JSON vschema into the topo.
func (s *Server) SaveVSchema(ctx context.Context, keyspace string, vschema *vschemapb.Keyspace) error {
	data, err := json.MarshalIndent(vschema, "", "  ")
	if err != nil {
		return err
	}
	_, err = s.getGlobal().Set(vschemaFilePath(keyspace), string(data), 0 /* ttl */)
	if err != nil {
		return convertError(err)
	}
	return nil
}

// GetVSchema fetches the vschema from the topo.
func (s *Server) GetVSchema(ctx context.Context, keyspace string) (*vschemapb.Keyspace, error) {
	resp, err := s.getGlobal().Get(vschemaFilePath(keyspace), false /* sort */, false /* recursive */)
	if err != nil {
		err = convertError(err)
		if err == topo.ErrNoNode {
			return nil, topo.ErrNoNode
		}
		return nil, err
	}
	if resp.Node == nil {
		return nil, ErrBadResponse
	}
	var vs vschemapb.Keyspace
	if err := json.Unmarshal([]byte(resp.Node.Value), &vs); err != nil {
		return nil, fmt.Errorf("bad vschema data (%v): %q", err, resp.Node.Value)
	}
	return &vs, nil
}
