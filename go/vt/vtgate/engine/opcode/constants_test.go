/*
Copyright 2023 The Vitess Authors.

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

package opcode

import (
	"testing"

	"vitess.io/vitess/go/sqltypes"
)

func TestCheckAllAggrOpCodes(t *testing.T) {
	// This test is just checking that we never reach the panic when using Type() on valid opcodes
	for i := AggregateOpcode(0); i < _NumOfOpCodes; i++ {
		i.Type(sqltypes.Null)
	}
}
