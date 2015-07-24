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

// UserPermissionToProto translates a UserPermission to proto
func UserPermissionToProto(u *UserPermission) *pbt.UserPermission {
	return &pbt.UserPermission{
		Host:             u.Host,
		User:             u.User,
		PasswordChecksum: u.PasswordChecksum,
		Privileges:       u.Privileges,
	}
}

// ProtoToUserPermission translates a proto to a UserPermission
func ProtoToUserPermission(u *pbt.UserPermission) *UserPermission {
	return &UserPermission{
		Host:             u.Host,
		User:             u.User,
		PasswordChecksum: u.PasswordChecksum,
		Privileges:       u.Privileges,
	}
}

// DbPermissionToProto translates a DbPermission to proto
func DbPermissionToProto(d *DbPermission) *pbt.DbPermission {
	return &pbt.DbPermission{
		Host:       d.Host,
		Db:         d.Db,
		User:       d.User,
		Privileges: d.Privileges,
	}
}

// ProtoToDbPermission translates a proto to a DbPermission
func ProtoToDbPermission(d *pbt.DbPermission) *DbPermission {
	return &DbPermission{
		Host:       d.Host,
		Db:         d.Db,
		User:       d.User,
		Privileges: d.Privileges,
	}
}

// HostPermissionToProto translates a HostPermission to proto
func HostPermissionToProto(h *HostPermission) *pbt.HostPermission {
	return &pbt.HostPermission{
		Host:       h.Host,
		Db:         h.Db,
		Privileges: h.Privileges,
	}
}

// ProtoToHostPermission translates a proto to a HostPermission
func ProtoToHostPermission(h *pbt.HostPermission) *HostPermission {
	return &HostPermission{
		Host:       h.Host,
		Db:         h.Db,
		Privileges: h.Privileges,
	}
}

// PermissionsToProto translates a Permissions to proto
func PermissionsToProto(h *Permissions) *pbt.Permissions {
	result := &pbt.Permissions{}
	if len(h.UserPermissions) > 0 {
		result.UserPermissions = make([]*pbt.UserPermission, len(h.UserPermissions))
		for i, u := range h.UserPermissions {
			result.UserPermissions[i] = UserPermissionToProto(u)
		}
	}
	if len(h.DbPermissions) > 0 {
		result.DbPermissions = make([]*pbt.DbPermission, len(h.DbPermissions))
		for i, d := range h.DbPermissions {
			result.DbPermissions[i] = DbPermissionToProto(d)
		}
	}
	if len(h.HostPermissions) > 0 {
		result.HostPermissions = make([]*pbt.HostPermission, len(h.HostPermissions))
		for i, h := range h.HostPermissions {
			result.HostPermissions[i] = HostPermissionToProto(h)
		}
	}
	return result
}

// ProtoToPermissions translates a proto to a Permissions
func ProtoToPermissions(h *pbt.Permissions) *Permissions {
	result := &Permissions{}
	if len(h.UserPermissions) > 0 {
		result.UserPermissions = make([]*UserPermission, len(h.UserPermissions))
		for i, u := range h.UserPermissions {
			result.UserPermissions[i] = ProtoToUserPermission(u)
		}
	}
	if len(h.DbPermissions) > 0 {
		result.DbPermissions = make([]*DbPermission, len(h.DbPermissions))
		for i, d := range h.DbPermissions {
			result.DbPermissions[i] = ProtoToDbPermission(d)
		}
	}
	if len(h.HostPermissions) > 0 {
		result.HostPermissions = make([]*HostPermission, len(h.HostPermissions))
		for i, h := range h.HostPermissions {
			result.HostPermissions[i] = ProtoToHostPermission(h)
		}
	}
	return result
}
