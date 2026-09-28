/*
Copyright 2024 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/logutil"
)

// serverOptions configure a Workflow Server. serverOptions are set by
// the ServerOption values passed to the server functions.
type serverOptions struct {
	logger logutil.Logger
}

// ServerOption configures how we perform the certain operations.
type ServerOption interface {
	apply(*serverOptions)
}

// funcServerOption wraps a function that modifies serverOptions into
// an implementation of the ServerOption interface.
type funcServerOption struct {
	f func(*serverOptions)
}

func (fso *funcServerOption) apply(so *serverOptions) {
	fso.f(so)
}

func newFuncServerOption(f func(*serverOptions)) *funcServerOption {
	return &funcServerOption{
		f: f,
	}
}

// WithLogger determines the customer logger to use. If this option
// is not provided then the default system logger will be used.
func WithLogger(l logutil.Logger) ServerOption {
	return newFuncServerOption(func(o *serverOptions) {
		o.logger = l
	})
}

// workflowActionOptions configure a workflow's optional behavior when
// performing actions in the worfklow server. Note: these should be used
// for options that are rarely used so that most callers do not need to
// specify any values for them. workflowActionOptions are set by the
// WorkflowActionOption values passed to the server functions.
type workflowActionOptions struct {
	ignoreSourceKeyspace bool
}

// WorkflowActionOption alters how we perform the certain workflow operations
// in the worfklow server.
type WorkflowActionOption interface {
	apply(*workflowActionOptions)
}

// funcWorkflowActionOption wraps a function that modifies workflowActionOptions
// into an implementation of the WorkflowActionOption interface.
type funcWorkflowActionOption struct {
	f func(*workflowActionOptions)
}

func (fwo *funcWorkflowActionOption) apply(wo *workflowActionOptions) {
	fwo.f(wo)
}

func newFuncWorkflowActionOption(f func(*workflowActionOptions)) *funcWorkflowActionOption {
	return &funcWorkflowActionOption{
		f: f,
	}
}

func IgnoreSourceKeyspace() WorkflowActionOption {
	return newFuncWorkflowActionOption(func(o *workflowActionOptions) {
		o.ignoreSourceKeyspace = true
	})
}
