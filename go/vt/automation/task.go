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
