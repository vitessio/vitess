//go:build !debug2PC

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

package vtgate

import (
	"context"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// This file defines debug constants that are always false.
// This file is used for building production code.
// We use go build directives to include a file that defines the constant to true
// when certain tags are provided while building binaries.
// This allows to have debugging code written in normal code flow without affecting
// production performance.

const DebugTwoPc = false

func checkTestFailure(_ context.Context, _ string, _ *querypb.Target) error {
	return nil
}
