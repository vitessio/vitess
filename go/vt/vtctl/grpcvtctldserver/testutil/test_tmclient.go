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

package testutil

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// tabletManagerClient implements the tmclient.TabletManagerClient for
// testing. It allows users to mock various tmclient methods.
type tabletManagerClient struct {
	tmclient.TabletManagerClient
	Schemas map[string]*tabletmanagerdatapb.SchemaDefinition
}

// GetSchema is part of the tmclient.TabletManagerClient interface.
func (c *tabletManagerClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, tablets []string, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	key := topoproto.TabletAliasString(tablet.Alias)

	schema, ok := c.Schemas[key]
	if !ok {
		return nil, fmt.Errorf("no schemas for %s", key)
	}

	return schema, nil
}

// TabletManagerClientProtocol is the protocol this package registers its client
// test implementation under. Users should set *tmclient.TabletManagerProtocol
// to this value before use.
const TabletManagerClientProtocol = "grpcvtctldserver.testutil"

// TabletManagerClient is the singleton test client instance. It is public and
// singleton to allow tests to mutate and verify its state.
var TabletManagerClient = &tabletManagerClient{
	Schemas: map[string]*tabletmanagerdatapb.SchemaDefinition{},
}

func init() {
	tmclient.RegisterTabletManagerClientFactory(TabletManagerClientProtocol, func() tmclient.TabletManagerClient {
		return TabletManagerClient
	})
}
