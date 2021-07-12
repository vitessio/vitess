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

package rbac

type Action string

const (
	GetAction  Action = "get"
	ListAction Action = "list"
)

type Resource string

const (
	ClusterResource Resource = "Cluster"

	KeyspaceResource Resource = "Keyspace"
	ShardResource    Resource = "Shard"
	TabletResource   Resource = "Tablet"
	VTGateResource   Resource = "VTGate"

	SrvVSchemaResource Resource = "SrvVSchema"
	VSchemaResource    Resource = "VSchema"

	BackupResource Resource = "Backup"
	SchemaResource Resource = "Schema"
)
