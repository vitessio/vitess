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

package planbuilder

import (
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ logicalPlan = (*fkVerify)(nil)

type verifyLP struct {
	verify logicalPlan
	typ    string
}

// fkVerify is the logicalPlan for engine.FkVerify.
type fkVerify struct {
	input  logicalPlan
	verify []*verifyLP
}

// newFkVerify builds a new fkVerify.
func newFkVerify(input logicalPlan, verify []*verifyLP) *fkVerify {
	return &fkVerify{
		input:  input,
		verify: verify,
	}
}

// Primitive implements the logicalPlan interface
func (fkc *fkVerify) Primitive() engine.Primitive {
	var verify []*engine.Verify
	for _, v := range fkc.verify {
		verify = append(verify, &engine.Verify{
			Exec: v.verify.Primitive(),
			Typ:  v.typ,
		})
	}
	return &engine.FkVerify{
		Exec:   fkc.input.Primitive(),
		Verify: verify,
	}
}
