/*
Copyright 2019 The Vitess Authors.

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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// primitiveBuilder is the top level type for building plans.
// It contains the current logicalPlan tree, the symtab and
// the jointab. It can create transient planBuilders due
// to the recursive nature of SQL.
type primitiveBuilder struct {
	vschema plancontext.VSchema
	jt      *jointab
	plan    logicalPlan
	st      *symtab
}

func newPrimitiveBuilder(vschema plancontext.VSchema, jt *jointab) *primitiveBuilder {
	return &primitiveBuilder{
		vschema: vschema,
		jt:      jt,
	}
}

type primitiveWrapper struct {
	prim engine.Primitive
	gen4Plan
}

func (p *primitiveWrapper) WireupGen4(semTable *semantics.SemTable) error {
	return nil
}

func (p *primitiveWrapper) Primitive() engine.Primitive {
	return p.prim
}

func (p *primitiveWrapper) Inputs() []logicalPlan {
	return nil
}

func (p *primitiveWrapper) Rewrite(inputs ...logicalPlan) error {
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "can't rewrite")
}

func (p *primitiveWrapper) ContainsTables() semantics.TableSet {
	return semantics.EmptyTableSet()
}

func (p *primitiveWrapper) OutputColumns() []sqlparser.SelectExpr {
	return nil
}

var _ logicalPlan = (*primitiveWrapper)(nil)
