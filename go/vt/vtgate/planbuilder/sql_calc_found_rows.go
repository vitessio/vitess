/*
Copyright 2020 The Vitess Authors.

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

var _ logicalPlan = (*sqlCalcFoundRows)(nil)

type sqlCalcFoundRows struct {
	LimitQuery, CountQuery logicalPlan
}

// Primitive implements the logicalPlan interface
func (s *sqlCalcFoundRows) Primitive() engine.Primitive {
	countPrim := s.CountQuery.Primitive()
	rb, ok := countPrim.(*engine.Route)
	if ok {
		// if our count query is an aggregation, we want the no-match result to still return a zero
		rb.NoRoutesSpecialHandling = true
	}
	return engine.SQLCalcFoundRows{
		LimitPrimitive: s.LimitQuery.Primitive(),
		CountPrimitive: countPrim,
	}
}
