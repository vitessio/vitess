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

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// VtctldClient provides a partial mock implementation of the
// vtctldclient.VtctldClient interface for use in testing.
type VtctldClient struct {
	vtctldclient.VtctldClient

	GetSchemaResults map[string]struct {
		Response *vtctldatapb.GetSchemaResponse
		Error    error
	}
}

// Compile-time type assertion to make sure we haven't overriden a method
// incorrectly.
var _ vtctldclient.VtctldClient = (*VtctldClient)(nil)

// GetSchema is part of the vtctldclient.VtctldClient interface.
func (fake *VtctldClient) GetSchema(ctx context.Context, req *vtctldatapb.GetSchemaRequest, opts ...grpc.CallOption) (*vtctldatapb.GetSchemaResponse, error) {
	if fake.GetSchemaResults == nil {
		return nil, fmt.Errorf("%w: GetSchemaResults not set on fake vtctldclient", assert.AnError)
	}

	if req.TabletAlias == nil {
		return nil, fmt.Errorf("%w: req.TabletAlias == nil", assert.AnError)
	}

	key := topoproto.TabletAliasString(req.TabletAlias)

	if result, ok := fake.GetSchemaResults[key]; ok {
		return result.Response, result.Error
	}

	return nil, fmt.Errorf("%w: no result set for tablet alias %s", assert.AnError, key)
}
