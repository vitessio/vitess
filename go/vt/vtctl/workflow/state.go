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

import (
	"fmt"
	"strings"
)

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

func (s *State) String() string {
	var stateInfo []string
	if !s.IsPartialMigration { // shard level traffic switching is all or nothing
		if len(s.RdonlyCellsNotSwitched) == 0 && len(s.ReplicaCellsNotSwitched) == 0 && len(s.ReplicaCellsSwitched) > 0 {
			stateInfo = append(stateInfo, "All Reads Switched")
		} else if len(s.RdonlyCellsSwitched) == 0 && len(s.ReplicaCellsSwitched) == 0 {
			stateInfo = append(stateInfo, "Reads Not Switched")
		} else {
			stateInfo = append(stateInfo, "Reads partially switched")
			if len(s.ReplicaCellsNotSwitched) == 0 {
				stateInfo = append(stateInfo, "All Replica Reads Switched")
			} else if len(s.ReplicaCellsSwitched) == 0 {
				stateInfo = append(stateInfo, "Replica not switched")
			} else {
				stateInfo = append(stateInfo, "Replica switched in cells: "+strings.Join(s.ReplicaCellsSwitched, ","))
			}
			if len(s.RdonlyCellsNotSwitched) == 0 {
				stateInfo = append(stateInfo, "All Rdonly Reads Switched")
			} else if len(s.RdonlyCellsSwitched) == 0 {
				stateInfo = append(stateInfo, "Rdonly not switched")
			} else {
				stateInfo = append(stateInfo, "Rdonly switched in cells: "+strings.Join(s.RdonlyCellsSwitched, ","))
			}
		}
	}
	if s.WritesSwitched {
		stateInfo = append(stateInfo, "Writes Switched")
	} else if s.IsPartialMigration {
		// For partial migrations, the traffic switching is all or nothing
		// at the shard level, so reads are effectively switched on the
		// shard when writes are switched.
		if len(s.ShardsAlreadySwitched) > 0 && len(s.ShardsNotYetSwitched) > 0 {
			stateInfo = append(stateInfo, fmt.Sprintf("Reads partially switched, for shards: %s", strings.Join(s.ShardsAlreadySwitched, ",")))
			stateInfo = append(stateInfo, fmt.Sprintf("Writes partially switched, for shards: %s", strings.Join(s.ShardsAlreadySwitched, ",")))
		} else {
			if len(s.ShardsAlreadySwitched) == 0 {
				stateInfo = append(stateInfo, "Reads Not Switched")
				stateInfo = append(stateInfo, "Writes Not Switched")
			} else {
				stateInfo = append(stateInfo, "All Reads Switched")
				stateInfo = append(stateInfo, "All Writes Switched")
			}
		}
	} else {
		stateInfo = append(stateInfo, "Writes Not Switched")
	}
	return strings.Join(stateInfo, ". ")
}
