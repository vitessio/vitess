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

package vtgate

import (
	"testing"

	"vitess.io/vitess/go/vt/vtgate/vtgateservice"
)

// NewMetadataGateForTest returns a VTGate backed by the executor test
// sandbox. External tests use it to drive the gRPC service without an
// import cycle.
func NewMetadataGateForTest(t *testing.T) vtgateservice.VTGateService {
	t.Helper()
	executor, _, _, _, _ := createExecutorEnv(t)
	return newVTGate(executor, nil, nil, nil, nil)
}
