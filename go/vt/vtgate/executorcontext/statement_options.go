/*
Copyright 2026 The Vitess Authors.

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

package executorcontext

import (
	"context"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// StatementOptions are the execute options of a statement: those that its
// comment directives set, and its timeout. VTGate applies them to the options
// of each query that it sends to vttablet for the statement, and for the
// statements that it runs on the statement's behalf, such as lookup vindex
// queries. They are never written into the session, so they don't carry over
// to the session's next statement. An option that a statement doesn't set
// keeps the session's value, except for the timeout.
type StatementOptions struct {
	WorkloadName string
	Priority     string
	Consolidator querypb.ExecuteOptions_Consolidator

	// timeout is the authoritative timeout in milliseconds: the
	// QUERY_TIMEOUT_MS directive, otherwise the session's or the default
	// query timeout. Unlike the other options, it replaces the session's
	// value even when it is nil.
	timeout *int64
}

// WithoutTimeout returns a copy of opts without the timeout.
func (opts *StatementOptions) WithoutTimeout() *StatementOptions {
	c := *opts
	c.timeout = nil
	return &c
}

type statementOptionsKey struct{}

// WithStatementOptions returns a copy of ctx that carries opts.
func WithStatementOptions(ctx context.Context, opts *StatementOptions) context.Context {
	return context.WithValue(ctx, statementOptionsKey{}, opts)
}

// StatementOptionsFromContext returns the options of the statement that ctx
// belongs to.
func StatementOptionsFromContext(ctx context.Context) (*StatementOptions, bool) {
	opts, ok := ctx.Value(statementOptionsKey{}).(*StatementOptions)
	return opts, ok && opts != nil
}

// ExecuteOptions returns the options for one call to vttablet: a copy of the
// session's options, with the options of the statement that ctx belongs to
// applied. It returns nil when there are no options to send.
func ExecuteOptions(ctx context.Context, session *SafeSession) *querypb.ExecuteOptions {
	var options *querypb.ExecuteOptions
	// The explicit session.Session selector spells out why the nil check
	// on the embedded session is needed.
	//nolint:staticcheck // QF1008
	if session != nil && session.Session != nil && session.Session.Options != nil {
		options = session.Session.Options.CloneVT()
	}
	opts, ok := StatementOptionsFromContext(ctx)
	if !ok {
		return options
	}
	if options == nil {
		if *opts == (StatementOptions{}) {
			return nil
		}
		options = &querypb.ExecuteOptions{}
	}
	if opts.WorkloadName != "" {
		options.WorkloadName = opts.WorkloadName
	}
	if opts.Priority != "" {
		options.Priority = opts.Priority
	}
	if opts.Consolidator != querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED {
		options.Consolidator = opts.Consolidator
	}
	options.Timeout = nil
	if opts.timeout != nil {
		options.Timeout = &querypb.ExecuteOptions_AuthoritativeTimeout{AuthoritativeTimeout: *opts.timeout}
	}
	return options
}
