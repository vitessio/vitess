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

package topo

import (
	"path"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/vterrors"

	"github.com/golang/protobuf/proto"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// SaveVSchema first validates the VSchema, then saves it.
// If the VSchema is empty, just remove it.
func (ts *Server) SaveVSchema(ctx context.Context, keyspace string, vschema *vschemapb.Keyspace) error {
	err := vindexes.ValidateKeyspace(vschema)
	if err != nil {
		return err
	}

	nodePath := path.Join(KeyspacesPath, keyspace, VSchemaFile)
	data, err := proto.Marshal(vschema)
	if err != nil {
		return err
	}

	_, err = ts.globalCell.Update(ctx, nodePath, data, nil)
	return err
}

// GetVSchema fetches the vschema from the topo.
func (ts *Server) GetVSchema(ctx context.Context, keyspace string) (*vschemapb.Keyspace, error) {
	nodePath := path.Join(KeyspacesPath, keyspace, VSchemaFile)
	data, _, err := ts.globalCell.Get(ctx, nodePath)
	if err != nil {
		return nil, err
	}
	var vs vschemapb.Keyspace
	err = proto.Unmarshal(data, &vs)
	if err != nil {
		return nil, vterrors.Wrapf(err, "bad vschema data: %q", data)
	}
	return &vs, nil
}

// SaveRoutingRules saves the routing rules into the topo.
func (ts *Server) SaveRoutingRules(ctx context.Context, routingRules *vschemapb.RoutingRules) error {
	data, err := proto.Marshal(routingRules)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		// No vschema, remove it. So we can remove the keyspace.
		return ts.globalCell.Delete(ctx, RoutingRulesFile, nil)
	}

	_, err = ts.globalCell.Update(ctx, RoutingRulesFile, data, nil)
	return err
}

// GetRoutingRules fetches the routing rules from the topo.
func (ts *Server) GetRoutingRules(ctx context.Context) (*vschemapb.RoutingRules, error) {
	rr := &vschemapb.RoutingRules{}
	data, _, err := ts.globalCell.Get(ctx, RoutingRulesFile)
	if err != nil {
		if IsErrType(err, NoNode) {
			return rr, nil
		}
		return nil, err
	}
	err = proto.Unmarshal(data, rr)
	if err != nil {
		return nil, vterrors.Wrapf(err, "bad routing rules data: %q", data)
	}
	return rr, nil
}
