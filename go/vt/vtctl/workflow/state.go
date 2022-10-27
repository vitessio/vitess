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

// Type is the type of a workflow.
type Type string

// Workflow types.
const (
	TypeReshard    Type = "Reshard"
	TypeMoveTables Type = "MoveTables"
)

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
