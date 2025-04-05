/*
Copyright 2025 The Vitess Authors.

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

package vschema

import (
	"context"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// IVSchemaAPI is an interface for VSchemaAPI responsible for performing CRUD
// operations on VSchema.
type IVSchemaAPI interface {
	Create(ctx context.Context, req *vtctldatapb.VSchemaCreateRequest) error
	Get(ctx context.Context, req *vtctldatapb.VSchemaGetRequest) (*vschemapb.Keyspace, error)
	Update(ctx context.Context, req *vtctldatapb.VSchemaUpdateRequest) error
	Publish(ctx context.Context, req *vtctldatapb.VSchemaPublishRequest) error

	// Vindex related functions

	AddVindex(ctx context.Context, req *vtctldatapb.VSchemaAddVindexRequest) error
	RemoveVindex(ctx context.Context, req *vtctldatapb.VSchemaRemoveVindexRequest) error
	AddLookupVindex(ctx context.Context, req *vtctldatapb.VSchemaAddLookupVindexRequest) error

	// Table related functions

	AddTables(ctx context.Context, req *vtctldatapb.VSchemaAddTablesRequest) error
	RemoveTables(ctx context.Context, req *vtctldatapb.VSchemaRemoveTablesRequest) error
	SetPrimaryVindex(ctx context.Context, req *vtctldatapb.VSchemaSetPrimaryVindexRequest) error
	SetSequence(ctx context.Context, req *vtctldatapb.VSchemaSetSequenceRequest) error
	SetReference(ctx context.Context, req *vtctldatapb.VSchemaSetReferenceRequest) error
}
