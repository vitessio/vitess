/*
Copyright 2021 The Vitess Authors.

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

package main

import (
	"context"

	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtgate/engine"

	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

var globalCreateDb func(ks *vttestpb.Keyspace) error
var globalDropDb func(ksName string) error

// DBDDL doesn't need to store any state - we use the global variables above instead
type DBDDL struct{}

// CreateDatabase implements the engine.DBDDLPlugin interface
func (plugin *DBDDL) CreateDatabase(_ context.Context, name string) error {
	ks := &vttestpb.Keyspace{
		Name: name,
		Shards: []*vttestpb.Shard{{
			Name: "0",
		}},
	}
	return globalCreateDb(ks)
}

// DropDatabase implements the engine.DBDDLPlugin interface
func (plugin *DBDDL) DropDatabase(_ context.Context, name string) error {
	return globalDropDb(name)
}

func init() {
	servenv.OnRun(func() {
		engine.DBDDLRegister("vttest", &DBDDL{})
	})
}
