/*
Copyright 2024 The Vitess Authors.

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
	"context"
	"fmt"

	"vitess.io/vitess/go/vt/vterrors"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// KeyspaceRoutingRulesLock is a wrapper over TopoLock, to serialize updates to
// the global keyspace routing rules.
type KeyspaceRoutingRulesLock struct {
	*TopoLock
}

// KeyspaceRoutingRulesInfo contains a map of the keyspace routing rules as a
// map of from_keyspace->to_keyspace values along with metadata about the rules.
type KeyspaceRoutingRulesInfo struct {
	RoutingRules *vschemapb.KeyspaceRoutingRules
	Version      Version
}

func NewKeyspaceRoutingRulesLock(ctx context.Context, ts *Server, name string) (*KeyspaceRoutingRulesLock, error) {
	return &KeyspaceRoutingRulesLock{
		TopoLock: &TopoLock{
			Path: KeyspaceRoutingRulesFile,
			Name: fmt.Sprintf("KeyspaceRoutingRules:: %s", name),
			ts:   ts,
		},
	}, nil
}

func (ts *Server) GetKeyspaceRoutingRules(ctx context.Context) (*KeyspaceRoutingRulesInfo, error) {
	krr := &vschemapb.KeyspaceRoutingRules{}
	data, version, err := ts.globalCell.Get(ctx, KeyspaceRoutingRulesFile)
	if err != nil {
		if IsErrType(err, NoNode) {
			return &KeyspaceRoutingRulesInfo{
				RoutingRules: &vschemapb.KeyspaceRoutingRules{
					Rules: make(map[string]string),
				},
				Version: version,
			}, nil
		}
		return nil, err
	}

	if err = krr.UnmarshalVT(data); err != nil {
		return nil, vterrors.Wrap(err, "bad keyspace routing rules data")
	}

	return &KeyspaceRoutingRulesInfo{
		RoutingRules: krr,
		Version:      version,
	}, nil
}

func (ts *Server) SaveKeyspaceRoutingRules(ctx context.Context, krri *KeyspaceRoutingRulesInfo) error {
	if krri == nil || krri.RoutingRules == nil || len(krri.RoutingRules.GetRules()) == 0 {
		// No rules, remove it.
		if err := ts.globalCell.Delete(ctx, KeyspaceRoutingRulesFile, nil); err != nil && !IsErrType(err, NoNode) {
			return err
		}
		return nil
	}

	data, err := krri.RoutingRules.MarshalVT()
	if err != nil {
		return err
	}
	newVersion, err := ts.globalCell.Update(ctx, KeyspaceRoutingRulesFile, data, krri.Version)
	if err != nil {
		return err
	}
	krri.Version = newVersion

	return nil
}
