/*
Copyright 2019 The Vitess Authors.

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

package automation

import (
	automationpb "vitess.io/vitess/go/vt/proto/automation"
)

// Task implementations can be executed by the scheduler.
type Task interface {
	// Run executes the task using the key/values from parameters.
	// "newTaskContainers" contains new tasks which the task can emit. They'll be inserted in the cluster operation directly after this task. It may be "nil".
	// "output" may be empty. It contains any text which maybe must e.g. to debug the task or show it in the UI.
	Run(parameters map[string]string) (newTaskContainers []*automationpb.TaskContainer, output string, err error)

	// RequiredParameters() returns a list of parameter keys which must be provided as input for Run().
	RequiredParameters() []string

	// OptionalParameters() returns a list of parameter keys which are optional input for Run().
	OptionalParameters() []string
}
