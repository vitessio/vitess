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

package grpcvtctldserver

import (
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/vtctl/internal/vtctlflags"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/proto/vtrpc"
)

// checkQueriesEnabled extracts the common task of checking the enable_queries
// flag, annotating the span, and constructing the error for all query rpcs on
// grpcvtctldserver.
func checkQueriesEnabled(span trace.Span) error {
	if !vtctlflags.AreQueriesEnabled() {
		span.Annotate("queries_enabled", false)
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "query commands are disabled (set the -enable_queries flag on vtctld to enable)")
	}

	return nil
}
