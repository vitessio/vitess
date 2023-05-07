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

package workflow

// VReplicationWorkflowType specifies whether workflow is
// MoveTables or Reshard and maps directly to what is stored
// in the backend database.
type VReplicationWorkflowType int

// VReplicationWorkflowType enums.
const (
	MoveTablesWorkflow = VReplicationWorkflowType(iota)
	ReshardWorkflow
	MigrateWorkflow
)

// Type is the type of a workflow as a string and maps directly
// to what is provided and presented to the user.
type Type string

// Workflow string types.
const (
	TypeMoveTables Type = "MoveTables"
	TypeReshard    Type = "Reshard"
	TypeMigrate    Type = "Migrate"
)

var TypeStrMap = map[VReplicationWorkflowType]Type{
	MoveTablesWorkflow: TypeMoveTables,
	ReshardWorkflow:    TypeReshard,
	MigrateWorkflow:    TypeMigrate,
}
var TypeIntMap = map[Type]VReplicationWorkflowType{
	TypeMoveTables: MoveTablesWorkflow,
	TypeReshard:    ReshardWorkflow,
	TypeMigrate:    MigrateWorkflow,
}

// State represents the state of a workflow.
type State struct {
	Workflow       string
	SourceKeyspace string
	TargetKeyspace string
	WorkflowType   Type

	ReplicaCellsSwitched    []string
	ReplicaCellsNotSwitched []string

	RdonlyCellsSwitched    []string
	RdonlyCellsNotSwitched []string

	WritesSwitched bool

	// Partial MoveTables info
	IsPartialMigration    bool
	ShardsAlreadySwitched []string
	ShardsNotYetSwitched  []string
}
