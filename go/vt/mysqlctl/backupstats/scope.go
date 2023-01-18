/*
Copyright 2022 The Vitess Authors.

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

package backupstats

type (
	// Scope is used to specify the scope of Stats. In this way the same Stats
	// can be passed down to different layers and components of backup and
	// restore operations, while allowing each component to emit stats without
	// overwriting the stats of other components.
	Scope struct {
		Type  ScopeType
		Value ScopeValue
	}

	// ScopeType is used to specify the type of scope being set.
	ScopeType int
	// ScopeValue is used to specify the value of the scope being set.
	ScopeValue = string
)

const (
	// ScopeComponent is used to specify the type of component, such as
	// "BackupEngine" or "BackupStorage".
	ScopeComponent ScopeType = iota
	// ScopeImplementation is used to specify the specific component, such as
	// "Builtin" and "XtraBackup" in the case of a "BackupEngine" componenet.
	ScopeImplementation
	// ScopeOperation is used to specify the type of operation. Examples of
	// high-level operations are "Backup" and "Restore", and examples of
	// low-level operations like "Read" and "Write".
	ScopeOperation
)

func Component(c ComponentType) Scope {
	return Scope{ScopeComponent, c.String()}
}

func Implementation(v ScopeValue) Scope {
	return Scope{ScopeImplementation, v}
}

func Operation(v ScopeValue) Scope {
	return Scope{ScopeOperation, v}
}
