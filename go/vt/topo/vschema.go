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

package topo

import (
	"path"

	"google.golang.org/protobuf/proto"

	"context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// SaveVSchema first validates the VSchema, then saves it.
// If the VSchema is empty, just remove it.
func (ts *Server) SaveVSchema(ctx context.Context, keyspace string, vschema *vschemapb.Keyspace) error {
	_, err := vindexes.BuildKeyspace(vschema)
	if err != nil {
		return err
	}

	nodePath := path.Join(KeyspacesPath, keyspace, VSchemaFile)
	data, err := vschema.MarshalVT()
	if err != nil {
		return err
	}

	_, err = ts.globalCell.Update(ctx, nodePath, data, nil)
	if err != nil {
		log.Errorf("failed to update vschema for keyspace %s: %v", keyspace, err)
	} else {
		log.Infof("successfully updated vschema for keyspace %s: %+v", keyspace, vschema)
	}
	return err
}

// DeleteVSchema delete the keyspace if it exists
func (ts *Server) DeleteVSchema(ctx context.Context, keyspace string) error {
	log.Infof("deleting vschema for keyspace %s", keyspace)
	nodePath := path.Join(KeyspacesPath, keyspace, VSchemaFile)
	return ts.globalCell.Delete(ctx, nodePath, nil)
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

// EnsureVSchema makes sure that a vschema is present for this keyspace or creates a blank one if it is missing
func (ts *Server) EnsureVSchema(ctx context.Context, keyspace string) error {
	vschema, err := ts.GetVSchema(ctx, keyspace)
	if err != nil && !IsErrType(err, NoNode) {
		log.Infof("error in getting vschema for keyspace %s: %v", keyspace, err)
	}
	if vschema == nil || IsErrType(err, NoNode) {
		err = ts.SaveVSchema(ctx, keyspace, &vschemapb.Keyspace{
			Sharded:  false,
			Vindexes: make(map[string]*vschemapb.Vindex),
			Tables:   make(map[string]*vschemapb.Table),
		})
		if err != nil {
			log.Errorf("could not create blank vschema: %v", err)
			return err
		}
	}
	return nil
}

// SaveRoutingRules saves the routing rules into the topo.
func (ts *Server) SaveRoutingRules(ctx context.Context, routingRules *vschemapb.RoutingRules) error {
	data, err := routingRules.MarshalVT()
	if err != nil {
		return err
	}

	if len(data) == 0 {
		// No vschema, remove it. So we can remove the keyspace.
		if err := ts.globalCell.Delete(ctx, RoutingRulesFile, nil); err != nil && !IsErrType(err, NoNode) {
			return err
		}
		return nil
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
	err = rr.UnmarshalVT(data)
	if err != nil {
		return nil, vterrors.Wrapf(err, "bad routing rules data: %q", data)
	}
	return rr, nil
}

// SaveShardRoutingRules saves the shard routing rules into the topo.
func (ts *Server) SaveShardRoutingRules(ctx context.Context, shardRoutingRules *vschemapb.ShardRoutingRules) error {
	data, err := shardRoutingRules.MarshalVT()
	if err != nil {
		return err
	}

	if len(data) == 0 {
		if err := ts.globalCell.Delete(ctx, ShardRoutingRulesFile, nil); err != nil && !IsErrType(err, NoNode) {
			return err
		}
		return nil
	}

	_, err = ts.globalCell.Update(ctx, ShardRoutingRulesFile, data, nil)
	return err
}

// GetShardRoutingRules fetches the shard routing rules from the topo.
func (ts *Server) GetShardRoutingRules(ctx context.Context) (*vschemapb.ShardRoutingRules, error) {
	srr := &vschemapb.ShardRoutingRules{}
	data, _, err := ts.globalCell.Get(ctx, ShardRoutingRulesFile)
	if err != nil {
		if IsErrType(err, NoNode) {
			return srr, nil
		}
		return nil, err
	}
	err = srr.UnmarshalVT(data)
	if err != nil {
		return nil, vterrors.Wrapf(err, "invalid shard routing rules: %q", data)
	}
	return srr, nil
}
