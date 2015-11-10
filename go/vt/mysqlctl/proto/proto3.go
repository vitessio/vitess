// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"fmt"

	pb "github.com/youtube/vitess/go/vt/proto/replicationdata"
	pbt "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
)

// ReplicationStatusToProto translates a ReplicationStatus to
// proto, or panics
func ReplicationStatusToProto(r ReplicationStatus) *pb.Status {
	return &pb.Status{
		Position:            EncodeReplicationPosition(r.Position),
		SlaveIoRunning:      r.SlaveIORunning,
		SlaveSqlRunning:     r.SlaveSQLRunning,
		SecondsBehindMaster: uint32(r.SecondsBehindMaster),
		MasterHost:          r.MasterHost,
		MasterPort:          int32(r.MasterPort),
		MasterConnectRetry:  int32(r.MasterConnectRetry),
	}
}

// ProtoToReplicationStatus translates a proto ReplicationStatus, or panics
func ProtoToReplicationStatus(r *pb.Status) ReplicationStatus {
	pos, err := DecodeReplicationPosition(r.Position)
	if err != nil {
		panic(fmt.Errorf("cannot decode Position: %v", err))
	}
	return ReplicationStatus{
		Position:            pos,
		SlaveIORunning:      r.SlaveIoRunning,
		SlaveSQLRunning:     r.SlaveSqlRunning,
		SecondsBehindMaster: uint(r.SecondsBehindMaster),
		MasterHost:          r.MasterHost,
		MasterPort:          int(r.MasterPort),
		MasterConnectRetry:  int(r.MasterConnectRetry),
	}
}

// TableDefinitionToProto translates a TableDefinition to proto
func TableDefinitionToProto(t *TableDefinition) *pbt.TableDefinition {
	return &pbt.TableDefinition{
		Name:              t.Name,
		Schema:            t.Schema,
		Columns:           t.Columns,
		PrimaryKeyColumns: t.PrimaryKeyColumns,
		Type:              t.Type,
		DataLength:        t.DataLength,
		RowCount:          t.RowCount,
	}
}

// ProtoToTableDefinition translates a proto into a TableDefinition
func ProtoToTableDefinition(t *pbt.TableDefinition) *TableDefinition {
	return &TableDefinition{
		Name:              t.Name,
		Schema:            t.Schema,
		Columns:           t.Columns,
		PrimaryKeyColumns: t.PrimaryKeyColumns,
		Type:              t.Type,
		DataLength:        t.DataLength,
		RowCount:          t.RowCount,
	}
}

// SchemaDefinitionToProto translates a SchemaDefinition to proto
func SchemaDefinitionToProto(s *SchemaDefinition) *pbt.SchemaDefinition {
	result := &pbt.SchemaDefinition{
		DatabaseSchema: s.DatabaseSchema,
		Version:        s.Version,
	}
	if len(s.TableDefinitions) > 0 {
		result.TableDefinitions = make([]*pbt.TableDefinition, len(s.TableDefinitions))
		for i, t := range s.TableDefinitions {
			result.TableDefinitions[i] = TableDefinitionToProto(t)
		}
	}
	return result
}

// ProtoToSchemaDefinition translates a proto to a SchemaDefinition
func ProtoToSchemaDefinition(s *pbt.SchemaDefinition) *SchemaDefinition {
	result := &SchemaDefinition{
		DatabaseSchema: s.DatabaseSchema,
		Version:        s.Version,
	}
	if len(s.TableDefinitions) > 0 {
		result.TableDefinitions = make([]*TableDefinition, len(s.TableDefinitions))
		for i, t := range s.TableDefinitions {
			result.TableDefinitions[i] = ProtoToTableDefinition(t)
		}
	}
	return result
}
