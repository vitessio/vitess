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

package planbuilder

import (
	"vitess.io/vitess/go/vt/vtgate/engine"
)

type projection struct {
	source    logicalPlan
	primitive *engine.Projection
}

var _ logicalPlan = (*projection)(nil)

// Primitive implements the logicalPlan interface
func (p *projection) Primitive() engine.Primitive {
	if p.primitive == nil {
		panic("WireUp not yet run")
	}
	p.primitive.Input = p.source.Primitive()
	return p.primitive
}
